#include "pool_manager.h"

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <mntent.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/sysmacros.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <csignal>
#include <cstdarg>
#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <unordered_set>

#include "device_exclusion.h"
#include "device_header.h"
#include "device_wipe.h"
#include "metadata.h"
#include "pool_validation.h"
#include "util.h"
#include "wal.h"

#ifdef __linux__
#include <linux/fs.h>
#endif

namespace maru
{

// ROOF (marufs) superblock magic "FGSB" at offset 0 of a device it formatted
// (roof-dev kernel/src/layout/offsets.h). Such a device is never ours.
static constexpr uint32_t kRoofSuperblockMagic = 0x46475342;

// ---------------------------------------------------------------------------
// client_id helpers: parse "hostname:pid" and check locality
// ---------------------------------------------------------------------------

static std::string getLocalHostname() {
    char buf[256];
    if (::gethostname(buf, sizeof(buf)) == 0) {
        buf[sizeof(buf) - 1] = '\0';
        return std::string(buf);
    }
    return "";
}

static const std::string &localHostname() {
    static const std::string h = getLocalHostname();
    return h;
}

/// Extract PID from "hostname:pid". Returns 0 on parse failure.
static pid_t pidFromClientId(const std::string &clientId) {
    auto pos = clientId.rfind(':');
    if (pos == std::string::npos || pos + 1 >= clientId.size()) return 0;
    try {
        return static_cast<pid_t>(std::stoi(clientId.substr(pos + 1)));
    } catch (...) {
        return 0;
    }
}

/// Check if client_id is from this node.
static bool isLocalClient(const std::string &clientId) {
    auto pos = clientId.rfind(':');
    if (pos == std::string::npos) return false;
    return clientId.substr(0, pos) == localHostname();
}

/// Overload for char[] fields.
static bool isLocalClient(const char *clientId) {
    return isLocalClient(std::string(clientId));
}

static pid_t pidFromClientId(const char *clientId) {
    return pidFromClientId(std::string(clientId));
}

static void setClientId(char *dest, size_t maxLen, const std::string &src) {
    std::strncpy(dest, src.c_str(), maxLen - 1);
    dest[maxLen - 1] = '\0';
}

// Sysfs and device path constants

// DEV_DAX mode specific paths
static constexpr const char *kSysBusDaxDevices = "/sys/bus/dax/devices";
static constexpr const char *kSysClassDax = "/sys/class/dax";
static constexpr const char *kSysDevChar = "/sys/dev/char";
static constexpr const char *kAlignAttr = "/align";
static constexpr const char *kDevPrefix = "/dev/";

// FS_DAX mode specific paths
static constexpr const char *kSysClassBlock = "/sys/class/block";
static constexpr const char *kQueueLogicalBlockSize =
    "/queue/logical_block_size";
static constexpr const char *kDeviceAttr = "/device";
static constexpr const char *kProcMounts = "/proc/mounts";

// Common paths
static constexpr const char *kSizeAttr = "/size";

static uint64_t alignUp(uint64_t v, uint64_t align)
{
    if (align == 0)
    {
        return v;
    }
    uint64_t rem = v % align;
    if (rem == 0)
    {
        return v;
    }
    uint64_t result = v + (align - rem);
    if (result < v)
    {
        return UINT64_MAX; // overflow guard
    }
    return result;
}

static bool findMountPoint(const std::string &deviceName,
                           std::string &mountPointOut)
{
    FILE *fp = setmntent(kProcMounts, "r");
    if (!fp)
    {
        return false;
    }

    std::string devicePath = std::string(kDevPrefix) + deviceName;
    bool found = false;

    struct mntent *entry;
    while ((entry = getmntent(fp)) != nullptr)
    {
        if (devicePath == entry->mnt_fsname)
        {
            if (entry->mnt_opts && std::strstr(entry->mnt_opts, "dax"))
            {
                mountPointOut = entry->mnt_dir;
                found = true;
                break;
            }
        }
    }

    endmntent(fp);
    return found;
}

static bool readSysfsSizeBytes(const std::string &path, uint64_t &sizeOut)
{
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0)
    {
        return false;
    }
    char buf[64] = {0};
    ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
    ::close(fd);
    if (n <= 0)
    {
        return false;
    }
    sizeOut = std::strtoull(buf, nullptr, 10);
    return sizeOut > 0;
}

static bool getBlockLogicalBlockSize(const std::string &devPath,
                                     uint64_t &out)
{
#ifdef BLKSSZGET
    int fd = ::open(devPath.c_str(), O_RDONLY);
    if (fd >= 0)
    {
        int blksz = 0;
        if (::ioctl(fd, BLKSSZGET, &blksz) == 0 && blksz > 0)
        {
            ::close(fd);
            out = static_cast<uint64_t>(blksz);
            return true;
        }
        ::close(fd);
    }
#endif
    std::string base = devPath.substr(devPath.find_last_of('/') + 1);
    std::string bsPath =
        std::string(kSysClassBlock) + "/" + base + kQueueLogicalBlockSize;
    uint64_t v = 0;
    if (readSysfsSizeBytes(bsPath, v))
    {
        out = v;
        return true;
    }
    return false;
}

static bool readDaxAlignBytes(const std::string &devName, uint64_t &alignOut)
{
    std::string path =
        std::string(kSysBusDaxDevices) + "/" + devName + kAlignAttr;
    return readSysfsSizeBytes(path, alignOut);
}

static std::string baseName(const std::string &path)
{
    size_t pos = path.find_last_of('/');
    if (pos == std::string::npos)
    {
        return path;
    }
    return path.substr(pos + 1);
}

static bool parseRegionIndexFromTarget(const std::string &target,
                                       uint32_t &outIndex)
{
    size_t pos = target.find("region");
    if (pos == std::string::npos)
    {
        return false;
    }
    pos += std::strlen("region");
    if (pos >= target.size() ||
        !std::isdigit(static_cast<unsigned char>(target[pos])))
    {
        return false;
    }
    char *end = nullptr;
    unsigned long v = std::strtoul(target.c_str() + pos, &end, 10);
    if (end == target.c_str() + pos)
    {
        return false;
    }
    outIndex = static_cast<uint32_t>(v);
    return true;
}

static std::string makeFsDaxFilePath(const std::string &mountPoint,
                                     uint64_t regionId)
{
    char filename[512];
    std::snprintf(filename, sizeof(filename), "%s/maru_%llu.dat", mountPoint.c_str(), (unsigned long long)regionId);
    return std::string(filename);
}

static int createFsDaxFile(const std::string &mountPoint, uint64_t regionId,
                           uint64_t size)
{
    std::string filename = makeFsDaxFilePath(mountPoint, regionId);
    int fd = ::open(filename.c_str(), O_CREAT | O_RDWR | O_EXCL, 0600);
    if (fd < 0)
    {
        return -errno;
    }
    if (::ftruncate(fd, static_cast<off_t>(size)) != 0)
    {
        int err = errno;
        ::close(fd);
        ::unlink(filename.c_str());
        return -err;
    }
    ::close(fd);
    return 0;
}

static void deleteFsDaxFile(const std::string &mountPoint, uint64_t regionId)
{
    std::string filename = makeFsDaxFilePath(mountPoint, regionId);
    ::unlink(filename.c_str());
}

bool parseRegionIndexFromDaxName(const std::string &devName, uint32_t &outIndex)
{
    // The kernel names every dev_dax "dax<dax_region id>.<dev_dax id>", whatever
    // the provider is (pmem, CXL region, or hmem), so the device name carries the
    // region index directly. Unlike the sysfs link target it does not depend on
    // where the dax_region sits in the device hierarchy.
    if (devName.compare(0, 3, "dax") != 0)
    {
        return false;
    }
    const char *p = devName.c_str() + 3;
    if (!std::isdigit(static_cast<unsigned char>(*p)))
    {
        return false;
    }
    char *end = nullptr;
    unsigned long v = std::strtoul(p, &end, 10);
    if (end == p || *end != '.' ||
        !std::isdigit(static_cast<unsigned char>(end[1])))
    {
        return false;
    }
    outIndex = static_cast<uint32_t>(v);
    return true;
}

static bool getRegionIndexForDax(const std::string &devName,
                                 uint32_t &outIndex)
{
    // Prefer the device name: parseRegionIndexFromTarget() takes the first
    // "region<N>" anywhere in the link target, which picks up an unrelated
    // ancestor when one happens to be named that way, and finds nothing at all
    // for an hmem-backed device (".../devices/platform/hmem.0/dax0.0" has no
    // region component). The link target stays as a fallback for any layout
    // whose device name does not parse.
    if (parseRegionIndexFromDaxName(devName, outIndex))
    {
        return true;
    }

    std::string sysfsPath = std::string(kSysBusDaxDevices) + "/" + devName;
    char buf[PATH_MAX];
    ssize_t n = ::readlink(sysfsPath.c_str(), buf, sizeof(buf) - 1);
    if (n <= 0)
    {
        return false;
    }
    buf[n] = '\0';
    return parseRegionIndexFromTarget(buf, outIndex);
}

static bool getRegionIndexForPmem(const std::string &blockName,
                                  uint32_t &outIndex)
{
    char buf[PATH_MAX];
    std::string path = std::string(kSysClassBlock) + "/" + blockName;
    ssize_t n = ::readlink(path.c_str(), buf, sizeof(buf) - 1);
    if (n > 0)
    {
        buf[n] = '\0';
        if (parseRegionIndexFromTarget(buf, outIndex))
        {
            return true;
        }
    }
    path = std::string(kSysClassBlock) + "/" + blockName + kDeviceAttr;
    n = ::readlink(path.c_str(), buf, sizeof(buf) - 1);
    if (n > 0)
    {
        buf[n] = '\0';
        if (parseRegionIndexFromTarget(buf, outIndex))
        {
            return true;
        }
    }
    return false;
}

PoolManager::PoolManager(const std::string &stateDir, int gracePeriodSec)
    : stateDir_(stateDir), gracePeriodSec_(gracePeriodSec)
{
    metadata_ = std::make_unique<MetadataStore>(stateDir);
    wal_ = std::make_unique<WalStore>(stateDir);
}

PoolManager::~PoolManager() = default;

uint32_t PoolManager::allocationCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return static_cast<uint32_t>(allocations_.size());
}

bool isDeviceDaxBound(const std::string &sysfsDevicePath)
{
    char target[PATH_MAX];
    ssize_t n = ::readlink((sysfsDevicePath + "/driver").c_str(), target, sizeof(target));
    if (n <= 0 || n == static_cast<ssize_t>(sizeof(target)))
    {
        return false;
    }
    return baseName(std::string(target, n)) == "device_dax";
}

int PoolManager::scanDevices(std::vector<DeviceInfo> &outDevices)
{
    // Scan for DEV_DAX devices
    std::map<uint32_t, std::string> byRegion;
    DIR *dir = ::opendir(kSysBusDaxDevices);
    if (dir)
    {
        struct dirent *ent = nullptr;
        while ((ent = ::readdir(dir)) != nullptr)
        {
            const char *name = ent->d_name;
            if (name[0] == '.')
            {
                continue;
            }
            if (std::strncmp(name, "dax", 3) != 0)
            {
                continue;
            }
            if (!isDeviceDaxBound(std::string(kSysBusDaxDevices) + "/" + name))
            {
                // kmem devices are system RAM and have no /dev/dax node.
                // Unbound devices can be reconsidered on the next scan.
                continue;
            }
            uint32_t regionId = 0;
            if (!getRegionIndexForDax(name, regionId))
            {
                // Skipping silently here is what made an undiscoverable device
                // look like an absent one: the daemon only reports "no CXL/DAX
                // devices found" and never says which device it passed over.
                logf(LogLevel::Warn,
                     "scanDevices: cannot determine dax_region index for /dev/%s "
                     "(unexpected device name and no region in its sysfs link) — "
                     "skipping, its capacity will not be available",
                     name);
                continue;
            }
            std::string devPath = std::string(kDevPrefix) + name;
            byRegion[regionId] = devPath;
        }
        ::closedir(dir);
    }

    for (const auto &kv : byRegion)
    {
        outDevices.push_back(DeviceInfo{kv.first, kv.second, DaxType::DEV_DAX});
    }

    // Scan for FS_DAX devices (pmem block devices)
    std::map<uint32_t, std::string> byPool;
    dir = ::opendir(kSysClassBlock);
    if (dir)
    {
        struct dirent *ent = nullptr;
        while ((ent = ::readdir(dir)) != nullptr)
        {
            const char *name = ent->d_name;
            if (name[0] == '.')
            {
                continue;
            }
            if (std::strncmp(name, "pmem", 4) != 0)
            {
                continue;
            }
            uint32_t poolId = 0;
            if (!getRegionIndexForPmem(name, poolId))
            {
                const char *p = name + 4;
                if (!std::isdigit(static_cast<unsigned char>(*p)))
                {
                    continue;
                }
                char *end = nullptr;
                unsigned long v = std::strtoul(p, &end, 10);
                if (end == p)
                {
                    continue;
                }
                poolId = static_cast<uint32_t>(v);
            }
            std::string mountPoint;
            if (!findMountPoint(name, mountPoint))
            {
                logf(LogLevel::Debug,
                     "scanDevices: /dev/%s is not mounted with dax option, skipping",
                     name);
                continue;
            }
            logf(LogLevel::Debug, "scanDevices: found FS_DAX device %s mounted at %s", name, mountPoint.c_str());
            byPool[poolId] = mountPoint;
        }
        ::closedir(dir);
    }

    for (const auto &kv : byPool)
    {
        outDevices.push_back(DeviceInfo{kv.first, kv.second, DaxType::FS_DAX});
    }

    std::unordered_set<std::string> present;
    for (const auto &dev : outDevices) present.insert(dev.devPath);
    for (auto it = failedDevices_.begin(); it != failedDevices_.end();)
    {
        if (!present.count(it->first)) it = failedDevices_.erase(it);
        else ++it;
    }

    return 0;
}

int PoolManager::getDeviceSize(const std::string &path, uint64_t &sizeOut)
{
    struct stat st
    {
    };
    if (::stat(path.c_str(), &st) != 0)
    {
        return -errno;
    }

    if (S_ISDIR(st.st_mode))
    {
        struct statfs fs;
        if (::statfs(path.c_str(), &fs) != 0)
        {
            return -errno;
        }
        sizeOut =
            static_cast<uint64_t>(fs.f_bsize) * static_cast<uint64_t>(fs.f_blocks);
        return 0;
    }

    if (S_ISCHR(st.st_mode))
    {
        std::string base = path.substr(path.find_last_of('/') + 1);
        std::string classPath = std::string(kSysClassDax) + "/" + base + kSizeAttr;
        if (readSysfsSizeBytes(classPath, sizeOut))
        {
            return 0;
        }
        char sysfsPath[256];
        std::snprintf(sysfsPath, sizeof(sysfsPath), "%s/%u:%u%s", kSysDevChar, major(st.st_rdev), minor(st.st_rdev), kSizeAttr);
        if (readSysfsSizeBytes(sysfsPath, sizeOut))
        {
            return 0;
        }
    }
    return -ENOTSUP;
}

int PoolManager::poolBuildFailed(const std::string &path, const char *stage, int error)
{
    auto failure = std::make_pair(std::string(stage), error);
    auto previous = failedDevices_.find(path);
    if (previous == failedDevices_.end() || previous->second != failure)
    {
        logf(LogLevel::Warn, "failed to load pool from %s (%s): %d (%s)",
             path.c_str(), stage, error, std::strerror(-error));
        failedDevices_[path] = std::move(failure);
    }
    return error;
}

int PoolManager::buildPoolFromDevice(uint32_t poolId, const std::string &path,
                                      DaxType type, PoolState &out)
{
    if (isExcluded(path)) return -EPERM;
    uint64_t size = 0;
    int rc = getDeviceSize(path, size);
    if (rc != 0 || size == 0)
    {
        return poolBuildFailed(path, "device size", rc != 0 ? rc : -EINVAL);
    }

    // Read device alignment first (needed for mmap-based header access)
    uint64_t devAlign = alignBytes_;
    if (type == DaxType::DEV_DAX)
    {
        uint64_t da = 0;
        if (readDaxAlignBytes(baseName(path), da))
        {
            devAlign = da;
        }
    }

    const uint64_t dataOffset = type == DaxType::DEV_DAX ? devAlign : 0;
    if (devAlign == 0 || size <= dataOffset)
        return poolBuildFailed(path, "device layout", -EUCLEAN);

    PoolState pool{};
    pool.poolId = poolId;
    pool.devPath = path;
    pool.dataBegin = dataOffset;
    pool.dataEnd = size;
    pool.totalSize = size - dataOffset;
    pool.freeSize = size - dataOffset;
    pool.type = type;
    if (type == DaxType::FS_DAX)
    {
        uint64_t blksz = 0;
        if (getBlockLogicalBlockSize(path, blksz) && blksz > 0)
        {
            if (blksz > devAlign)
            {
                devAlign = blksz;
            }
        }
    }
    pool.alignBytes = devAlign;
    pool.freeList.push_back(Extent{dataOffset, size - dataOffset});

    PoolState loaded = pool;
    rc = metadata_->load(poolId, loaded);
    if (rc == 0)
    {
        // The data range comes from the device. RMs before recovery validation
        // (#99) replaced the saved total with the device size on load but kept
        // the saved free list, so a checkpoint can hold a total that does not
        // match its own extents. The extents and allocations are still checked
        // against the device range below and after WAL replay.
        if (loaded.totalSize != pool.totalSize)
        {
            logf(LogLevel::Warn,
                 "%s: ignoring checkpoint total size %llu; the device data size is %llu",
                 path.c_str(), static_cast<unsigned long long>(loaded.totalSize),
                 static_cast<unsigned long long>(pool.totalSize));
            loaded.totalSize = pool.totalSize;
        }
        loaded.hasCheckpoint = true;
        pool = loaded;
    }
    else if (rc != -ENOENT)
    {
        poolBuildFailed(path, "checkpoint read", rc);
        return -EUCLEAN;
    }
    if (validateFreeList(pool) != 0)
        return poolBuildFailed(path, "checkpoint data range (offline repair required)", -EUCLEAN);

    if (type == DaxType::DEV_DAX)
    {
        DeviceHeader hdr{};
        const int hrc = readDeviceHeader(path, hdr, devAlign);
        uint32_t magic = 0;
        std::memcpy(&magic, hdr.magic, sizeof(magic));
        if (reinit_.count(baseName(path)) && (hrc == 0 || hrc == -ENODATA))
        {
            pool.needsHeaderInit = true; // include --reinit: fresh header and pool
        }
        else if (hrc == -ENODATA && magic == kRoofSuperblockMagic)
        {
            return poolBuildFailed(path, "ROOF filesystem on device, not registering it", -EBUSY);
        }
        else if (hrc == -ENODATA)
        {
            pool.needsHeaderInit = true;
        }
        else if (hrc != 0)
        {
            return poolBuildFailed(path, "read device header", hrc);
        }
        else
        {
            if (hdr.version != kDeviceHeaderVersion)
                return poolBuildFailed(path, "device header version", -EUCLEAN);
            pool.deviceUuid = uuidToString(hdr.uuid);
        }
    }

    out = std::move(pool);
    if (failedDevices_.erase(path))
    {
        logf(LogLevel::Info, "device recovered: %s", path.c_str());
    }
    return 0;
}

int PoolManager::loadPools()
{
    std::lock_guard<std::mutex> lock(mu_);
    return loadPoolsLocked();
}

int PoolManager::rescanDevices()
{
    std::lock_guard<std::mutex> lock(mu_);
    return rescanDevicesLocked();
}

bool PoolManager::hasPools() const
{
    std::lock_guard<std::mutex> lock(mu_);
    return !pools_.empty();
}

static std::string describeAllocations(const std::map<uint64_t, Allocation> &live)
{
    std::string text;
    for (const auto &[id, a] : live)
        text += "region=" + std::to_string(id) + " pool=" + std::to_string(a.poolId) +
                " size=" + std::to_string(a.allocLength) + " client=" + a.clientId + "\n";
    return text;
}

int PoolManager::recoverPools(std::vector<PoolState> &pools,
                              std::map<uint64_t, Allocation> &allocations,
                              uint64_t &nextRegionId, std::string &unavailablePools)
{
    std::set<uint32_t> referenced;
    int rc = wal_->referencedPools(referenced);
    if (rc != 0 && rc != -ENOENT) return rc;

    // Validate saved allocations before replay, even if a later FREE could
    // hide them. Never initialize a header over pool history, except on a
    // cleared device whose history leaves no live allocation.
    std::set<uint32_t> cleared;
    for (const auto &pool : pools)
    {
        bool hasAllocations = false;
        for (const auto &[id, a] : allocations)
        {
            if (a.poolId != pool.poolId) continue;
            hasAllocations = true;
            if (id != a.handle.regionId || !validAllocation(pool, a))
                return poolBuildFailed(pool.devPath, "checkpoint allocation range", -EUCLEAN);
        }
        if (!pool.hasCheckpoint && hasAllocations)
            return poolBuildFailed(pool.devPath, "missing pool checkpoint", -EUCLEAN);
        if (!pool.needsHeaderInit || reinit_.count(baseName(pool.devPath)) ||
            !(pool.hasCheckpoint || hasAllocations || referenced.count(pool.poolId)))
            continue;
        // Volatile CXL memory comes back zeroed, header included, after a
        // power cycle. Such a device gets a fresh header when no allocation
        // on it is live after replay; its history is replayed and validated
        // like any other.
        bool zeroed = false;
        rc = readHeaderAreaZeroed(pool.devPath, pool.alignBytes, zeroed);
        if (rc != 0) return poolBuildFailed(pool.devPath, "read device header area", rc);
        if (!zeroed)
            return poolBuildFailed(pool.devPath, "missing header with saved pool history", -EUCLEAN);
        cleared.insert(pool.poolId);
    }

    rc = wal_->replay(pools, allocations, nextRegionId);
    if (rc != 0 && rc != -ENOENT) return rc;
    for (const auto &pool : pools)
    {
        if (!cleared.count(pool.poolId)) continue;
        std::map<uint64_t, Allocation> live;
        for (const auto &[id, a] : allocations)
            if (a.poolId == pool.poolId) live.emplace(id, a);
        if (live.empty()) continue;
        logf(LogLevel::Error, "%s was cleared but holds live allocations:\n%s",
             pool.devPath.c_str(), describeAllocations(live).c_str());
        return poolBuildFailed(pool.devPath, "cleared device with live allocations", -EUCLEAN);
    }
    for (auto &pool : pools)
    {
        coalesceFreeList(pool);
        recomputeFreeSize(pool);
        if (validatePoolState(pool, allocations) != 0)
            return poolBuildFailed(pool.devPath, "recovered allocation/free-list consistency", -EUCLEAN);
    }

    // No device writes until every staged pool and the entire WAL pass.
    for (auto &pool : pools)
    {
        if (!pool.needsHeaderInit) continue;
        DeviceHeader hdr{};
        initDeviceHeader(hdr);
        rc = writeDeviceHeader(pool.devPath, hdr, pool.alignBytes);
        if (rc != 0) return poolBuildFailed(pool.devPath, "write device header", rc);
        pool.deviceUuid = uuidToString(hdr.uuid);
        pool.needsHeaderInit = false;
        if (cleared.count(pool.poolId))
            logf(LogLevel::Warn,
                 "%s was cleared (no header, zeroed header area) with no live allocations; "
                 "wrote a fresh device header: UUID=%s",
                 pool.devPath.c_str(), pool.deviceUuid.c_str());
        else
            logf(LogLevel::Info, "Auto-initialized device header on %s: UUID=%s",
                 pool.devPath.c_str(), pool.deviceUuid.c_str());
    }

    unavailablePools = describeUnavailable(referenced, allocations, pools);
    return 0;
}

// Missing devices must retain their WAL, including records not imported into
// allocations yet. Excluded pools hold no live allocations (checked when they
// are excluded and at startup), so their history does not block checkpoints.
std::string PoolManager::describeUnavailable(
    const std::set<uint32_t> &referenced,
    const std::map<uint64_t, Allocation> &allocations,
    const std::vector<PoolState> &staged) const
{
    std::set<uint32_t> available = excludedPoolIds();
    for (const auto &p : pools_) available.insert(p.poolId);
    for (const auto &p : staged) available.insert(p.poolId);
    std::set<uint32_t> missing;
    for (uint32_t id : referenced)
        if (!available.count(id)) missing.insert(id);
    for (const auto &[id, a] : allocations)
        if (!available.count(a.poolId)) missing.insert(a.poolId);
    std::string ids;
    for (uint32_t id : missing) ids += (ids.empty() ? "" : ",") + std::to_string(id);
    return ids.empty() ? "" : "no device for pool(s) " + ids + " referenced by WAL/checkpoint";
}

void PoolManager::refreshUnavailableLocked()
{
    std::set<uint32_t> referenced;
    const int rc = wal_->referencedPools(referenced);
    if (rc != 0 && rc != -ENOENT)
    {
        setCheckpointBlock("cannot read WAL (rc=" + std::to_string(rc) + ")");
        return;
    }
    unavailablePools_ = describeUnavailable(referenced, allocations_, {});
    setCheckpointBlock(unavailablePools_);
}

bool PoolManager::isExcluded(const std::string &devPath) const
{
    return devPath.rfind("/dev/", 0) == 0 && excluded_.count(devPath.substr(5));
}

std::set<uint32_t> PoolManager::excludedPoolIds() const
{
    std::set<uint32_t> ids;
    for (const auto &name : excluded_)
    {
        uint32_t id = 0;
        if (parseRegionIndexFromDaxName(name, id)) ids.insert(id);
    }
    return ids;
}

// Drop checkpoint entries of excluded pools that the WAL freed, so they do
// not look like missing-pool allocations. Live ones mean the list was edited
// by hand or the device was never drained: refuse rather than guess.
int PoolManager::dropExcludedLedger(std::map<uint64_t, Allocation> &allocations,
                                    std::string &why)
{
    const auto pools = excludedPoolIds();
    if (pools.empty()) return 0;
    std::map<uint64_t, Allocation> live = allocations;
    const int rc = wal_->liveAllocations(pools, live);
    if (rc != 0)
    {
        why = "cannot read WAL for excluded devices (rc=" + std::to_string(rc) + ")";
        return rc;
    }
    if (!live.empty())
    {
        why = "excluded device(s) still hold live allocations; include them or release:\n" +
              describeAllocations(live);
        return -EBUSY;
    }
    for (auto it = allocations.begin(); it != allocations.end();)
        it = pools.count(it->second.poolId) ? allocations.erase(it) : std::next(it);
    return 0;
}

// Remove backing files of freed regions: a crash between logging a FREE and
// the unlink leaves one behind. Only this RM creates maru_<id>.dat in the
// mount. An FS_DAX pool ID comes from sysfs numbering, not from the mount, so
// a file is kept if any pool owns its region ID.
void PoolManager::removeOrphanFsDaxFiles(const PoolState &pool)
{
    if (pool.type != DaxType::FS_DAX) return;
    // A missing pool may own files here, including WAL-only allocations that
    // replay did not import.
    if (!unavailablePools_.empty())
    {
        logf(LogLevel::Info, "keeping unowned files in %s: %s",
             pool.devPath.c_str(), unavailablePools_.c_str());
        return;
    }
    DIR *dir = ::opendir(pool.devPath.c_str());
    if (!dir)
    {
        logf(LogLevel::Warn, "cannot scan %s for orphan files: %s",
             pool.devPath.c_str(), std::strerror(errno));
        return;
    }
    std::vector<std::string> orphans;
    while (const struct dirent *ent = ::readdir(dir))
    {
        const char *name = ent->d_name;
        if (std::strncmp(name, "maru_", 5) != 0 ||
            !std::isdigit(static_cast<unsigned char>(name[5])))
            continue;
        char *end = nullptr;
        errno = 0;
        const unsigned long long id = std::strtoull(name + 5, &end, 10);
        const std::string path = makeFsDaxFilePath(pool.devPath, id);
        // Only the exact name allocation would create, e.g. not maru_007.dat.
        if (errno != 0 || std::strcmp(end, ".dat") != 0 || path != pool.devPath + "/" + name)
            continue;
        // Leave IDs this state never issued: a fresh or wrong state directory
        // must not delete another instance's files. A crash before an ALLOC
        // was logged therefore leaves its file, and the first allocation
        // that draws that ID fails once and moves past it.
        if (id >= nextRegionId_ || allocations_.count(id)) continue;
        orphans.push_back(path);
    }
    ::closedir(dir);
    for (const auto &path : orphans)
    {
        if (::unlink(path.c_str()) == 0)
            logf(LogLevel::Info, "removed orphan FS_DAX file %s", path.c_str());
        else
            logf(LogLevel::Warn, "cannot remove orphan FS_DAX file %s: %s",
                 path.c_str(), std::strerror(errno));
    }
}

void PoolManager::setCheckpointBlock(std::string reason)
{
    if (reason == checkpointBlock_) return;
    if (reason.empty())
        logf(LogLevel::Info, "checkpoints enabled");
    else
        logf(LogLevel::Warn, "checkpoints paused, WAL retained: %s", reason.c_str());
    checkpointBlock_ = std::move(reason);
}

int PoolManager::loadPoolsLocked()
{
    setCheckpointBlock("recovery has not completed");
    // Before any device is opened: an unreadable list must not mean "none".
    const int excludeRc = loadExcludedDevices(stateDir_, excluded_);
    if (excludeRc != 0)
    {
        logf(LogLevel::Error, "cannot read excluded device list in %s: %d (%s)",
             stateDir_.c_str(), excludeRc, std::strerror(-excludeRc));
        return excludeRc;
    }
    for (const auto &name : excluded_)
        logf(LogLevel::Info, "device /dev/%s is excluded (maru-rm-ctl include to use it)",
             name.c_str());
    // A previous RM may have renamed the list into place without syncing it,
    // and that state lived only in its memory. Confirm it again so no
    // excluded device is handed over while a power loss could still undo it.
    excludedSyncRc_ = excluded_.empty() ? 0 : syncExcludedDevices(stateDir_);
    if (excludedSyncRc_ != 0)
        logf(LogLevel::Warn, "the excluded device list in %s is not durable yet: %s",
             stateDir_.c_str(), std::strerror(-excludedSyncRc_));
    std::map<uint64_t, Allocation> stagedAllocations;
    uint64_t stagedNextRegionId = 1;
    const int globalRc = metadata_->loadGlobal(stagedAllocations, stagedNextRegionId);
    hasGlobalCheckpoint_ = globalRc == 0;
    if (globalRc != 0 && globalRc != -ENOENT)
    {
        logf(LogLevel::Error, "cannot restore global metadata: %d", globalRc);
        return globalRc;
    }

    std::vector<DeviceInfo> devices;
    int rc = scanDevices(devices);
    if (rc != 0) return rc;
    std::vector<PoolState> stagedPools;
    for (const auto &dev : devices)
    {
        if (isExcluded(dev.devPath)) continue;
        PoolState pool;
        rc = buildPoolFromDevice(dev.poolId, dev.devPath, dev.type, pool);
        if (rc == -EUCLEAN) return rc;
        if (rc != 0) continue; // Transient device errors remain retryable.
        if (pool.hasCheckpoint && globalRc == -ENOENT)
            return poolBuildFailed(pool.devPath, "missing global checkpoint", -EUCLEAN);
        stagedPools.push_back(std::move(pool));
    }
    std::string why;
    rc = dropExcludedLedger(stagedAllocations, why);
    if (rc != 0)
    {
        logf(LogLevel::Error, "%s", why.c_str());
        return rc;
    }
    std::string unavailable;
    rc = recoverPools(stagedPools, stagedAllocations, stagedNextRegionId, unavailable);
    if (rc != 0) return rc;
    // Every complete record passed; never append after an incomplete one.
    rc = wal_->discardTornTail();
    if (rc != 0)
    {
        logf(LogLevel::Error, "cannot discard incomplete WAL tail: %d", rc);
        return rc;
    }

    // Initialize the persisted secret before recomputing restored tokens.
    rc = initSecret(stateDir_, !stagedAllocations.empty());
    if (rc != 0) return rc;
    clientAllocCounts_.clear();
    for (auto &[regionId, alloc] : stagedAllocations)
    {
        alloc.handle.authToken = computeAuthToken(
            alloc.handle, alloc.nonce, std::string(alloc.clientId));
        ++clientAllocCounts_[alloc.clientId];
    }
    pools_ = std::move(stagedPools);
    allocations_ = std::move(stagedAllocations);
    nextRegionId_ = stagedNextRegionId;
    unavailablePools_ = std::move(unavailable);
    for (const auto &pool : pools_) removeOrphanFsDaxFiles(pool);
    setCheckpointBlock(unavailablePools_);
    return 0;
}

int PoolManager::rescanDevicesLocked()
{
    std::vector<DeviceInfo> devices;
    int rc = scanDevices(devices);
    if (rc != 0)
    {
        setCheckpointBlock("device rescan failed (rc=" + std::to_string(rc) + ")");
        return rc;
    }

    std::vector<PoolState> stagedPools;
    for (const auto &dev : devices)
    {
        bool exists = false;
        for (const auto &pool : pools_)
            if (pool.poolId == dev.poolId || pool.devPath == dev.devPath) exists = true;
        if (exists || isExcluded(dev.devPath)) continue;
        PoolState pool;
        rc = buildPoolFromDevice(dev.poolId, dev.devPath, dev.type, pool);
        if (rc == -EUCLEAN)
        {
            setCheckpointBlock("device " + dev.devPath + " failed recovery validation");
            return rc;
        }
        if (rc != 0) continue;
        if (pool.hasCheckpoint && !hasGlobalCheckpoint_)
        {
            setCheckpointBlock("device " + dev.devPath + " has no global checkpoint");
            return poolBuildFailed(pool.devPath, "missing global checkpoint", -EUCLEAN);
        }
        stagedPools.push_back(std::move(pool));
    }

    // Without a new pool there is nothing to import from the WAL or ledger.
    if (stagedPools.empty())
    {
        setCheckpointBlock(unavailablePools_);
        return 0;
    }

    rc = addPoolsLocked(stagedPools);
    if (rc != 0)
        setCheckpointBlock("rescan recovery failed (rc=" + std::to_string(rc) + ")");
    return rc;
}

// Recover `staged` against the live ledger and publish them; on failure
// nothing in memory changes.
int PoolManager::addPoolsLocked(std::vector<PoolState> &staged)
{
    StagedRecovery recovery;
    recovery.pools = std::move(staged);
    const int rc = stagePoolsLocked(recovery);
    if (rc == 0) publishPoolsLocked(recovery);
    return rc;
}

// Validate pools against a copy of the ledger and write headers for new
// devices. Only device headers are written; nothing in memory changes.
int PoolManager::stagePoolsLocked(StagedRecovery &recovery)
{
    recovery.allocations = allocations_;
    recovery.nextRegionId = nextRegionId_;
    return recoverPools(recovery.pools, recovery.allocations, recovery.nextRegionId,
                        recovery.unavailable);
}

void PoolManager::publishPoolsLocked(StagedRecovery &recovery)
{
    const size_t firstNew = pools_.size();
    for (auto &pool : recovery.pools) pools_.push_back(std::move(pool));
    allocations_ = std::move(recovery.allocations);
    nextRegionId_ = recovery.nextRegionId;
    clientAllocCounts_.clear();
    for (auto &[id, a] : allocations_)
    {
        a.handle.authToken = computeAuthToken(a.handle, a.nonce, std::string(a.clientId));
        ++clientAllocCounts_[a.clientId];
    }
    unavailablePools_ = std::move(recovery.unavailable);
    for (size_t i = firstNew; i < pools_.size(); ++i) removeOrphanFsDaxFiles(pools_[i]);
    setCheckpointBlock(unavailablePools_);
}

void PoolManager::recomputeFreeSize(PoolState &pool)
{
    pool.freeSize = 0;
    for (const auto &ex : pool.freeList)
    {
        pool.freeSize += ex.length;
    }
}

void PoolManager::coalesceFreeList(PoolState &pool)
{
    std::sort(
        pool.freeList.begin(),
        pool.freeList.end(),
        [](const Extent &a, const Extent &b)
        {
            return a.offset < b.offset;
        });
    std::vector<Extent> merged;
    for (const auto &ex : pool.freeList)
    {
        if (merged.empty())
        {
            merged.push_back(ex);
            continue;
        }
        Extent &last = merged.back();
        if (last.offset + last.length >= ex.offset)
        {
            uint64_t end = std::max(last.offset + last.length, ex.offset + ex.length);
            last.length = end - last.offset;
        }
        else
        {
            merged.push_back(ex);
        }
    }
    pool.freeList.swap(merged);
}

void PoolManager::insertExtentSorted(PoolState &pool, uint64_t offset,
                                     uint64_t length)
{
    // Binary search for insertion point in sorted free list
    auto it = std::lower_bound(
        pool.freeList.begin(),
        pool.freeList.end(),
        offset,
        [](const Extent &e, uint64_t off)
        {
            return e.offset < off;
        });

    // Try merge with previous extent
    if (it != pool.freeList.begin())
    {
        auto prev = std::prev(it);
        if (prev->offset + prev->length >= offset)
        {
            uint64_t newEnd = std::max(prev->offset + prev->length, offset + length);
            prev->length = newEnd - prev->offset;
            // Try merge with next extent too
            if (it != pool.freeList.end() &&
                prev->offset + prev->length >= it->offset)
            {
                uint64_t end =
                    std::max(prev->offset + prev->length, it->offset + it->length);
                prev->length = end - prev->offset;
                pool.freeList.erase(it);
            }
            return;
        }
    }

    // Try merge with next extent
    if (it != pool.freeList.end() && offset + length >= it->offset)
    {
        uint64_t newEnd = std::max(offset + length, it->offset + it->length);
        it->offset = offset;
        it->length = newEnd - offset;
        return;
    }

    // No merge possible, insert new extent
    pool.freeList.insert(it, Extent{offset, length});
}

PoolState *PoolManager::findPoolById(uint32_t poolId)
{
    for (auto &pool : pools_)
    {
        if (pool.poolId == poolId)
        {
            return &pool;
        }
    }
    return nullptr;
}

bool PoolManager::allocateFromPool(PoolState &pool, uint64_t size,
                                   Allocation &outAlloc)
{
    if (nextRegionId_ == UINT64_MAX || !validPoolLayout(pool)) return false;
    uint64_t alignedSize = alignUp(size, pool.alignBytes);
    if (alignedSize == UINT64_MAX || alignedSize > pool.freeSize) return false;
    for (size_t i = 0; i < pool.freeList.size(); ++i)
    {
        Extent ex = pool.freeList[i];
        if (!validExtent(pool, ex.offset, ex.length)) return false;
        uint64_t aligned = alignUp(ex.offset, pool.alignBytes);
        if (!validExtent(pool, aligned, alignedSize)) continue;
        uint64_t end = aligned + alignedSize;
        if (end > ex.offset + ex.length)
        {
            continue;
        }

        // Consume the ID even on failure: a file left by a crash before the
        // WAL append must not make every retry collide with it.
        uint64_t regionId = nextRegionId_++;
        // Create the file before splitting the extent so a failure leaves the
        // free list consistent with freeSize.
        if (pool.type == DaxType::FS_DAX &&
            createFsDaxFile(pool.devPath, regionId, alignedSize) != 0)
        {
            return false;
        }

        bool hasFront = (aligned > ex.offset);
        bool hasBack = (end < ex.offset + ex.length);

        if (hasFront && hasBack)
        {
            // Split: keep front fragment in place, insert back after it
            pool.freeList[i] = Extent{ex.offset, aligned - ex.offset};
            pool.freeList.insert(pool.freeList.begin() +
                                     static_cast<std::ptrdiff_t>(i) + 1,
                                 Extent{end, (ex.offset + ex.length) - end});
        }
        else if (hasFront)
        {
            // Only front fragment remains
            pool.freeList[i] = Extent{ex.offset, aligned - ex.offset};
        }
        else if (hasBack)
        {
            // Only back fragment remains
            pool.freeList[i] = Extent{end, (ex.offset + ex.length) - end};
        }
        else
        {
            // Exact fit: remove
            pool.freeList.erase(pool.freeList.begin() +
                                static_cast<std::ptrdiff_t>(i));
        }

        Handle h{};
        h.regionId = regionId;
        h.offset = (pool.type == DaxType::FS_DAX) ? 0 : aligned;
        h.length = alignedSize;
        outAlloc.handle = h;
        outAlloc.nonce = generateNonce();
        outAlloc.requestedSize = size;
        outAlloc.allocLength = alignedSize;
        outAlloc.poolId = pool.poolId;
        outAlloc.realOffset = aligned;
        return true;
    }
    return false;
}

// TODO(kihwan): normalize devPath with realpath() to handle symlinks/trailing slashes
PoolState *PoolManager::findPoolByPath(const std::string &devPath)
{
    for (auto &pool : pools_)
    {
        if (pool.devPath == devPath)
        {
            return &pool;
        }
    }
    return nullptr;
}

PoolState *PoolManager::findPoolForRegion(uint64_t regionId)
{
    auto it = allocations_.find(regionId);
    if (it == allocations_.end())
    {
        return nullptr;
    }
    return findPoolById(it->second.poolId);
}

std::string PoolManager::getDeviceUuidForRegion(uint64_t regionId)
{
    std::lock_guard<std::mutex> lock(mu_);
    auto *pool = findPoolForRegion(regionId);
    if (pool)
    {
        return pool->deviceUuid;
    }
    return "";
}

int PoolManager::alloc(uint64_t size, const std::string &clientId, Handle &out,
                       std::string &devPath, std::string &deviceUuid,
                       const std::string &daxPath, uint64_t &requestedSizeOut)
{
    if (clientId.empty())
    {
        return -EINVAL;
    }
    // Reject zero or unreasonably large sizes to prevent alignUp() overflow
    // and nonsensical allocations. 256 TiB is well beyond any CXL device.
    if (size == 0 || size > (1ULL << 48))
    {
        return -EINVAL;
    }

    std::lock_guard<std::mutex> lock(mu_);
    if (pools_.empty())
    {
        return -ENODEV;
    }
    // Replace a log holding a record that could not be removed; this must
    // run before any tentative change to the pools.
    if (wal_->appendsDisabled())
    {
        checkpointLocked();
    }

    Allocation alloc{};
    PoolState *selectedPool = nullptr;

    if (!daxPath.empty())
    {
        selectedPool = findPoolByPath(daxPath);
        if (!selectedPool)
        {
            if (isExcluded(daxPath))
                logf(LogLevel::Warn, "alloc for excluded device %s rejected", daxPath.c_str());
            return -ENOENT;
        }
        if (validateFreeList(*selectedPool) != 0) return -EUCLEAN;
        if (!allocateFromPool(*selectedPool, size, alloc))
        {
            return -ENOMEM;
        }
        setClientId(alloc.clientId, kMaxClientIdLen, clientId);
        selectedPool->freeSize -= alloc.allocLength;
    }
    else
    {
        for (auto &pool : pools_)
        {
            if (validateFreeList(pool) != 0) return -EUCLEAN;
            if (allocateFromPool(pool, size, alloc))
            {
                setClientId(alloc.clientId, kMaxClientIdLen, clientId);
                pool.freeSize -= alloc.allocLength;
                selectedPool = &pool;
                break;
            }
        }
        if (!selectedPool)
        {
            return -ENOMEM;
        }
    }

    alloc.handle.authToken = computeAuthToken(alloc.handle, alloc.nonce, clientId);

    // Acknowledge only a durable record: otherwise recovery could hand this
    // range to another client after a restart.
    const int walRc = wal_->appendAlloc(alloc);
    if (walRc != 0)
    {
        insertExtentSorted(*selectedPool, alloc.realOffset, alloc.allocLength);
        selectedPool->freeSize += alloc.allocLength;
        if (selectedPool->type == DaxType::FS_DAX)
        {
            deleteFsDaxFile(selectedPool->devPath, alloc.handle.regionId);
        }
        logWalAppendFailure("allocation", walRc);
        return walRc;
    }

    // Cache owner PID start time for reaper PID-reuse detection (local only)
    if (!clientId.empty())
    {
        ++clientAllocCounts_[clientId];
        if (isLocalClient(clientId))
        {
            pid_t pid = pidFromClientId(clientId);
            if (pid > 0 && pidStartTimes_.find(pid) == pidStartTimes_.end())
            {
                uint64_t st = getPidStartTime(pid);
                if (st != 0)
                {
                    pidStartTimes_[pid] = st;
                }
            }
        }
    }

    allocations_[alloc.handle.regionId] = alloc;

    if (selectedPool->type == DaxType::FS_DAX)
    {
        devPath = makeFsDaxFilePath(selectedPool->devPath, alloc.handle.regionId);
    }
    else
    {
        devPath = selectedPool->devPath;
    }

    deviceUuid = selectedPool->deviceUuid;
    requestedSizeOut = alloc.requestedSize;
    out = alloc.handle;
    if (++opCount_ % checkpointInterval_ == 0)
    {
        checkpointLocked();
    }
    return 0;
}

int PoolManager::free(const Handle &handle, const std::string &clientId)
{
    if (clientId.empty())
    {
        return -EINVAL;
    }

    std::lock_guard<std::mutex> lock(mu_);

    auto globalIt = allocations_.find(handle.regionId);
    if (globalIt == allocations_.end())
    {
        return -ENOENT;
    }

    const Allocation &alloc = globalIt->second;
    if (std::strcmp(alloc.clientId, clientId.c_str()) != 0)
    {
        return -EPERM;
    }

    return doFreeAllocation(handle.regionId);
}

void PoolManager::getStats(std::vector<PoolState> &out)
{
    std::lock_guard<std::mutex> lock(mu_);
    out = pools_;
}

bool PoolManager::hasExistingAllocations()
{
    std::lock_guard<std::mutex> lock(mu_);
    return !allocations_.empty();
}

int PoolManager::getPathForHandle(const Handle &handle, std::string &outPath)
{
    std::lock_guard<std::mutex> lock(mu_);

    auto globalIt = allocations_.find(handle.regionId);
    if (globalIt == allocations_.end())
    {
        return -ENOENT;
    }

    const Allocation &alloc = globalIt->second;

    PoolState *pool = findPoolById(alloc.poolId);
    if (!pool)
    {
        return -ENOENT;
    }

    if (pool->type == DaxType::FS_DAX)
    {
        outPath = makeFsDaxFilePath(pool->devPath, handle.regionId);
    }
    else
    {
        outPath = pool->devPath;
    }
    return 0;
}

void PoolManager::reapExpired(uint64_t &reapedCount)
{
    std::lock_guard<std::mutex> lock(mu_);
    reapedCount = 0;

    // Collect client_ids whose grace period has expired
    auto now = SteadyClock::now();
    std::unordered_set<std::string> expiredClients(reapRetries_.begin(), reapRetries_.end());
    reapRetries_.clear();
    for (auto it = disconnectedClients_.begin(); it != disconnectedClients_.end(); )
    {
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            now - it->second).count();
        if (elapsed >= gracePeriodSec_)
        {
            logf(LogLevel::Info, "reaping disconnected client: %s (disconnected %lds ago)",
                 it->first.c_str(), static_cast<long>(elapsed));
            expiredClients.insert(it->first);
            it = disconnectedClients_.erase(it);
        }
        else
        {
            ++it;
        }
    }

    std::vector<uint64_t> toFree;
    for (const auto &kv : allocations_)
    {
        const char *cid = kv.second.clientId;
        if (cid[0] == '\0')
        {
            continue;
        }

        // Check if this client's grace period has expired (works for both local and remote)
        if (expiredClients.count(cid))
        {
            toFree.push_back(kv.first);
            continue;
        }

        // For local clients, also check PID liveness
        if (!isLocalClient(cid))
        {
            continue;
        }
        pid_t pid = pidFromClientId(cid);
        if (pid <= 0)
        {
            continue;
        }
        if (::kill(pid, 0) == 0)
        {
            // Process exists — verify it's the same process via start time
            auto stIt = pidStartTimes_.find(pid);
            if (stIt != pidStartTimes_.end())
            {
                uint64_t currentSt = getPidStartTime(pid);
                if (currentSt != 0 && currentSt != stIt->second)
                {
                    // PID was reused by a different process
                    toFree.push_back(kv.first);
                }
            }
            continue;
        }
        if (errno == ESRCH)
        {
            toFree.push_back(kv.first);
        }
    }

    for (uint64_t regionId : toFree)
    {
        const auto allocIt = allocations_.find(regionId);
        const std::string cid = allocIt != allocations_.end() ? allocIt->second.clientId : "";
        const int rc = doFreeAllocation(regionId);
        if (rc == 0)
        {
            ++reapedCount;
        }
        else if (rc != -ENOENT && rc != -EUCLEAN && expiredClients.count(cid))
        {
            // The free was not logged (e.g. WAL append failed); retry this
            // expired client next pass. Local dead PIDs are found again anyway.
            reapRetries_.insert(cid);
        }
    }
}

void PoolManager::logWalAppendFailure(const char *operation, int error)
{
    const auto now = SteadyClock::now();
    if (now < nextWalFailureLog_) return;
    logf(LogLevel::Error, "WAL append failed, %s rejected: %d (repeats muted for 1 min)",
         operation, error);
    nextWalFailureLog_ = now + std::chrono::minutes(1);
}

int PoolManager::checkpointLocked()
{
    if (!checkpointBlock_.empty())
    {
        const auto now = std::chrono::steady_clock::now();
        if (now >= nextSkippedCheckpointLog_)
        {
            logf(LogLevel::Warn, "checkpoint skipped, WAL retained: %s",
                 checkpointBlock_.c_str());
            nextSkippedCheckpointLog_ = now + std::chrono::minutes(1);
        }
        return -EAGAIN;
    }
    const int rc = wal_->checkpoint(pools_, *metadata_, allocations_, nextRegionId_);
    if (rc == 0) hasGlobalCheckpoint_ = true;
    else logf(LogLevel::Error, "checkpoint failed; keeping WAL: %d", rc);
    return rc;
}

int PoolManager::checkpoint()
{
    std::lock_guard<std::mutex> lock(mu_);
    return checkpointLocked();
}

void PoolManager::clientDisconnected(const std::string &clientId)
{
    if (clientId.empty()) return;
    std::lock_guard<std::mutex> lock(mu_);
    // Only track if this client actually has allocations
    if (clientAllocCounts_.find(clientId) != clientAllocCounts_.end())
    {
        disconnectedClients_[clientId] = SteadyClock::now();
        logf(LogLevel::Debug, "client disconnected: %s (grace period %ds)",
             clientId.c_str(), gracePeriodSec_);
    }
}

void PoolManager::clientReconnected(const std::string &clientId)
{
    if (clientId.empty()) return;
    std::lock_guard<std::mutex> lock(mu_);
    reapRetries_.erase(clientId);
    auto it = disconnectedClients_.find(clientId);
    if (it != disconnectedClients_.end())
    {
        disconnectedClients_.erase(it);
        logf(LogLevel::Debug, "client reconnected: %s (grace period cancelled)",
             clientId.c_str());
    }
}

int PoolManager::verifyAndFree(const Handle &handle, const std::string &clientId)
{
    if (clientId.empty())
    {
        return -EINVAL;
    }

    std::lock_guard<std::mutex> lock(mu_);

    auto globalIt = allocations_.find(handle.regionId);
    if (globalIt == allocations_.end())
    {
        return -ENOENT;
    }

    // Verify auth token (bound to the allocating client_id)
    const Allocation &alloc = globalIt->second;
    uint64_t expectedToken = computeAuthToken(handle, alloc.nonce,
                                               std::string(alloc.clientId));
    if (handle.authToken != expectedToken)
    {
        return -EACCES;
    }

    // Verify ownership
    if (std::strcmp(alloc.clientId, clientId.c_str()) != 0)
    {
        return -EPERM;
    }

    return doFreeAllocation(handle.regionId);
}

int PoolManager::doFreeAllocation(uint64_t regionId)
{
    auto globalIt = allocations_.find(regionId);
    if (globalIt == allocations_.end())
    {
        return -ENOENT;
    }

    const Allocation &alloc = globalIt->second;

    PoolState *targetPool = findPoolById(alloc.poolId);
    if (!targetPool)
    {
        return -ENOENT;
    }

    if (!validAllocation(*targetPool, alloc) || validateFreeList(*targetPool) != 0 ||
        alloc.allocLength > targetPool->totalSize - targetPool->freeSize)
        return -EUCLEAN;
    for (const auto &e : targetPool->freeList)
        if (alloc.realOffset < e.offset + e.length &&
            e.offset < alloc.realOffset + alloc.allocLength) return -EUCLEAN;

    // Log the free before releasing anything: a failed append leaves the
    // allocation intact so the caller (or the reaper) can retry.
    if (wal_->appendsDisabled())
    {
        checkpointLocked();
    }
    bool written = false;
    const int walRc = wal_->appendFree(regionId, &written);
    if (walRc != 0 && !written)
    {
        logWalAppendFailure("free", walRc);
        return walRc;
    }
    if (walRc != 0)
    {
        // The FREE is in the log but not known to be on disk. Rejecting it
        // would keep the client's mapping while recovery may still free the
        // range for someone else; completing it risks only a leak.
        logf(LogLevel::Error,
             "WAL FREE for region %llu written but not synced (%d); completing the free",
             static_cast<unsigned long long>(regionId), walRc);
    }

    if (targetPool->type == DaxType::FS_DAX)
    {
        deleteFsDaxFile(targetPool->devPath, regionId);
    }

    insertExtentSorted(*targetPool, alloc.realOffset, alloc.allocLength);
    targetPool->freeSize += alloc.allocLength;

    // Update client refcount
    if (alloc.clientId[0] != '\0')
    {
        auto countIt = clientAllocCounts_.find(alloc.clientId);
        if (countIt != clientAllocCounts_.end())
        {
            if (--countIt->second == 0)
            {
                clientAllocCounts_.erase(countIt);
                if (isLocalClient(alloc.clientId))
                {
                    pid_t pid = pidFromClientId(alloc.clientId);
                    if (pid > 0) pidStartTimes_.erase(pid);
                }
            }
        }
    }

    allocations_.erase(globalIt);

    if (++opCount_ % checkpointInterval_ == 0)
    {
        checkpointLocked();
    }
    return 0;
}

int PoolManager::verifyAndGetPath(const Handle &handle, std::string &outPath)
{
    std::lock_guard<std::mutex> lock(mu_);

    auto globalIt = allocations_.find(handle.regionId);
    if (globalIt == allocations_.end())
    {
        return -ENOENT;
    }

    // Verify auth token (bound to the allocating client_id)
    const Allocation &alloc = globalIt->second;
    uint64_t expectedToken = computeAuthToken(handle, alloc.nonce,
                                               std::string(alloc.clientId));
    if (handle.authToken != expectedToken)
    {
        return -EACCES;
    }

    PoolState *pool = findPoolById(alloc.poolId);
    if (!pool)
    {
        return -ENOENT;
    }

    if (pool->type == DaxType::FS_DAX)
    {
        outPath = makeFsDaxFilePath(pool->devPath, handle.regionId);
    }
    else
    {
        outPath = pool->devPath;
    }
    return 0;
}

// ---------------------------------------------------------------------------
// maru-rm-ctl commands
// ---------------------------------------------------------------------------

static void appendf(std::string &out, const char *fmt, ...)
{
    char buf[512];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    out += buf;
}

static bool localOwnerAlive(const char *clientId)
{
    const pid_t pid = pidFromClientId(clientId);
    return pid > 0 && (::kill(pid, 0) == 0 || errno == EPERM);
}

static const char *ownerState(const char *clientId)
{
    if (!isLocalClient(clientId)) return "remote";
    return localOwnerAlive(clientId) ? "running" : "exited";
}

int PoolManager::adminStatus(std::string &out)
{
    std::lock_guard<std::mutex> lock(mu_);
    std::map<uint32_t, size_t> counts;
    for (const auto &[id, a] : allocations_) ++counts[a.poolId];
    for (const auto &p : pools_)
        appendf(out, "pool=%u device=%s state=active total=%llu free=%llu allocations=%zu\n",
                p.poolId, p.devPath.c_str(), static_cast<unsigned long long>(p.totalSize),
                static_cast<unsigned long long>(p.freeSize), counts[p.poolId]);
    for (const auto &name : excluded_)
        appendf(out, "device=/dev/%s state=excluded\n", name.c_str());
    if (excludedSyncRc_ != 0)
        out += "excluded_list=not-durable reason=" + std::string(std::strerror(-excludedSyncRc_)) +
               "\n";
    if (checkpointBlock_.empty())
        out += "checkpoints=enabled\n";
    else
        out += "checkpoints=paused reason=" + checkpointBlock_ + "\n";
    return 0;
}

int PoolManager::adminAllocations(const std::string &device, std::string &out)
{
    std::string name;
    uint32_t poolFilter = 0;
    if (!device.empty() &&
        (!normalizeDaxName(device, name) || !parseRegionIndexFromDaxName(name, poolFilter)))
    {
        out = "invalid device name: " + device + "\n";
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    // A loaded pool's allocations are current. A pool without a device only
    // has its checkpoint entries here and the WAL holds the rest, so list it
    // from both, as adminExclude does.
    std::set<uint32_t> absent;
    if (device.empty())
    {
        const int rc = wal_->referencedPools(absent);
        if (rc != 0 && rc != -ENOENT)
        {
            out = "cannot read WAL: " + std::string(std::strerror(-rc)) + "\n";
            return rc;
        }
        for (const auto &[id, a] : allocations_) absent.insert(a.poolId);
    }
    else
    {
        absent.insert(poolFilter);
    }
    for (const auto &p : pools_) absent.erase(p.poolId);
    std::map<uint64_t, Allocation> live;
    for (const auto &[id, a] : allocations_)
        if (absent.count(a.poolId)) live.emplace(id, a);
    if (!absent.empty())
    {
        const int rc = wal_->liveAllocations(absent, live);
        if (rc != 0)
        {
            out = "cannot read WAL: " + std::string(std::strerror(-rc)) + "\n";
            return rc;
        }
    }
    for (const auto &[id, a] : allocations_)
        if (!absent.count(a.poolId) && (device.empty() || a.poolId == poolFilter))
            live.emplace(id, a);
    for (const auto &[id, a] : live)
    {
        const PoolState *pool = findPoolById(a.poolId);
        appendf(out, "region=%llu pool=%u device=%s size=%llu offset=%llu client=%s owner=%s\n",
                static_cast<unsigned long long>(id), a.poolId,
                pool ? pool->devPath.c_str() : "(not present)",
                static_cast<unsigned long long>(a.allocLength),
                static_cast<unsigned long long>(a.realOffset), a.clientId,
                ownerState(a.clientId));
    }
    return 0;
}

// The list on disk is only written by these commands. A hand edit while the
// RM runs would be silently overwritten, so refuse instead.
int PoolManager::checkExcludedFileLocked(std::string &out)
{
    std::set<std::string> onDisk;
    const int rc = loadExcludedDevices(stateDir_, onDisk);
    if (rc != 0)
    {
        out = "cannot read the excluded device list: " + std::string(std::strerror(-rc)) + "\n";
        return rc;
    }
    if (onDisk != excluded_)
    {
        out = "excluded_devices was changed outside maru-rm-ctl; restart the RM to apply "
              "the file, or restore it\n";
        return -ESTALE;
    }
    return 0;
}

// The exclusion is in effect, but a power loss could undo it: say so, and how
// to confirm it, before anyone hands the device over.
static std::string unsyncedExclusionNote(const std::string &stateDir, int syncRc,
                                         const std::string &name)
{
    return "the excluded device list is not durable yet: syncing " + stateDir + " failed (" +
           std::strerror(-syncRc) + "). A power loss could return /dev/" + name +
           " to the RM, so do not hand it over; rerun 'maru-rm-ctl exclude " + name +
           "' until it succeeds\n";
}

int PoolManager::adminExclude(const std::string &device, std::string &out)
{
    std::string name;
    uint32_t poolId = 0;
    if (!normalizeDaxName(device, name) || !parseRegionIndexFromDaxName(name, poolId))
    {
        out = "invalid device name: " + device + "\n";
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    const std::string path = "/dev/" + name;
    const int fileRc = checkExcludedFileLocked(out);
    if (fileRc != 0) return fileRc;
    if (excluded_.count(name))
    {
        // An earlier save may have renamed the list into place without
        // syncing it: a retry confirms durability before reporting success.
        if (excludedSyncRc_ != 0) excludedSyncRc_ = syncExcludedDevices(stateDir_);
        if (excludedSyncRc_ != 0)
        {
            out = path + " is excluded, but " +
                  unsyncedExclusionNote(stateDir_, excludedSyncRc_, name);
            return kAdminIncomplete;
        }
        out = path + " is already excluded\n";
        return 0;
    }
    // After a failed sync the WAL may hold an ALLOC that is not in memory,
    // e.g. on this device. Replace the log first, or refuse.
    if (wal_->appendsDisabled() && checkpointBlock_.empty()) checkpointLocked();
    if (wal_->appendsDisabled())
    {
        out = "the WAL holds a record whose outcome is unknown (a sync failed) and no "
              "checkpoint could replace it" +
              (checkpointBlock_.empty() ? std::string() : ": " + checkpointBlock_) +
              "; retry once a checkpoint succeeds\n";
        return -EAGAIN;
    }

    auto loaded = pools_.end();
    for (auto it = pools_.begin(); it != pools_.end(); ++it)
        if (it->devPath == path) loaded = it;
    if (loaded == pools_.end())
        for (auto it = pools_.begin(); it != pools_.end(); ++it)
            if (it->poolId == poolId)
            {
                out = "pool " + std::to_string(poolId) + " is served by " + it->devPath +
                      ", not " + path + "\n";
                return -EINVAL;
            }

    // A loaded pool's allocations are current. A pool without a device only
    // has its checkpoint entries here; the WAL holds the rest.
    std::map<uint64_t, Allocation> live;
    for (const auto &[id, a] : allocations_)
        if (a.poolId == poolId) live.emplace(id, a);
    if (loaded == pools_.end())
    {
        const int rc = wal_->liveAllocations({poolId}, live);
        if (rc != 0)
        {
            out = "cannot read WAL: " + std::string(std::strerror(-rc)) + "\n";
            return rc;
        }
    }
    if (!live.empty())
    {
        out = path + " still holds " + std::to_string(live.size()) +
              " allocation(s); stop their owners (or release them) and retry:\n";
        for (const auto &[id, a] : live)
            appendf(out, "region=%llu size=%llu client=%s owner=%s\n",
                    static_cast<unsigned long long>(id),
                    static_cast<unsigned long long>(a.allocLength), a.clientId,
                    ownerState(a.clientId));
        return -EBUSY;
    }

    auto next = excluded_;
    next.insert(name);
    int syncRc = 0;
    const int rc = saveExcludedDevices(stateDir_, next, syncRc);
    if (rc != 0)
    {
        out = "cannot save excluded device list: " + std::string(std::strerror(-rc)) + "\n";
        return rc;
    }
    // Renamed into place: memory must follow the file even if it is not
    // durable yet; that is reported below instead of a plain success.
    excludedSyncRc_ = syncRc;
    excluded_ = std::move(next);
    if (loaded != pools_.end()) pools_.erase(loaded);
    // Checkpoint entries the WAL already freed would otherwise look like
    // allocations of a missing pool.
    bool dropped = false;
    for (auto it = allocations_.begin(); it != allocations_.end();)
    {
        if (it->second.poolId != poolId) { ++it; continue; }
        it = allocations_.erase(it);
        dropped = true;
    }
    if (dropped)
    {
        clientAllocCounts_.clear();
        for (const auto &[id, a] : allocations_) ++clientAllocCounts_[a.clientId];
    }
    failedDevices_.erase(path);
    refreshUnavailableLocked();
    logf(LogLevel::Warn, "admin: excluded %s (pool %u); it is no longer used", path.c_str(),
         poolId);

    appendf(out, "excluded %s (pool %u)\n", path.c_str(), poolId);
    if (checkpointBlock_.empty() && checkpointLocked() == 0)
        out += "checkpoint=written\n";
    else
        out += "checkpoint=deferred reason=" +
               (checkpointBlock_.empty() ? std::string("write failed") : checkpointBlock_) + "\n";
    if (excludedSyncRc_ != 0)
    {
        out += unsyncedExclusionNote(stateDir_, excludedSyncRc_, name);
        return kAdminIncomplete;
    }
    return 0;
}

int PoolManager::adminInclude(const std::string &device, bool reinit, std::string &out)
{
    std::string name;
    uint32_t poolId = 0;
    if (!normalizeDaxName(device, name) || !parseRegionIndexFromDaxName(name, poolId))
    {
        out = "invalid device name: " + device + "\n";
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    const std::string path = "/dev/" + name;
    for (const auto &p : pools_)
        if (p.devPath == path || p.poolId == poolId)
        {
            out = "pool " + std::to_string(p.poolId) + " is already served by " + p.devPath + "\n";
            return -EINVAL;
        }
    int rc = checkExcludedFileLocked(out);
    if (rc != 0) return rc;
    std::vector<DeviceInfo> devices;
    rc = scanDevices(devices);
    if (rc != 0)
    {
        out = "device scan failed: " + std::string(std::strerror(-rc)) + "\n";
        return rc;
    }
    const DeviceInfo *info = nullptr;
    for (const auto &dev : devices)
        if (dev.devPath == path) info = &dev;
    if (!info)
    {
        out = path + " was not found; is it bound to device_dax?\n";
        return -ENODEV;
    }

    // An excluded pool holds no allocations (checked by exclude and at
    // startup), so it comes back as an empty pool. Its metadata predates the
    // exclusion and would no longer match: keep it aside and write a fresh one
    // before the list says the device is in use, so a restart at any point
    // finds consistent files.
    const bool wasExcluded = excluded_.count(name) != 0;
    const std::string meta = stateDir_ + "/pool_" + std::to_string(poolId) + ".meta";
    std::string archived;
    if (wasExcluded || reinit)
    {
        // Never replace an earlier archive (the state dir lock makes this
        // check race-free).
        const std::string base = meta + (reinit ? ".reinit-" : ".include-") +
                                 std::to_string(static_cast<long long>(std::time(nullptr)));
        struct stat st{};
        archived = base;
        for (int n = 1; ::lstat(archived.c_str(), &st) == 0; ++n)
            archived = base + "." + std::to_string(n);
        if (::rename(meta.c_str(), archived.c_str()) != 0)
        {
            if (errno != ENOENT)
            {
                rc = -errno;
                out = "cannot archive " + meta + ": " + std::strerror(-rc) + "\n";
                return rc;
            }
            archived.clear();
        }
    }
    bool metaWritten = false, listSaved = false, headerWritten = false;
    auto rollback = [&](int error, const std::string &why) {
        reinit_.erase(name);
        out = path + ": " + why + " (" + std::strerror(-error) + ")\n";
        if (wasExcluded)
        {
            excluded_.insert(name);
            int syncRc = 0;
            const int saveRc =
                listSaved ? saveExcludedDevices(stateDir_, excluded_, syncRc) : 0;
            if (saveRc != 0)
                out += "WARNING: could not record the exclusion again (" +
                       std::string(std::strerror(-saveRc)) +
                       "); the RM may register this device when it restarts\n";
            else if (listSaved)
            {
                excludedSyncRc_ = syncRc;
                if (syncRc != 0) out += unsyncedExclusionNote(stateDir_, syncRc, name);
            }
        }
        if (headerWritten)
        {
            // The old metadata described the device before its new header;
            // never put it back under that header.
            out += "a new device header was already written";
            out += archived.empty() ? "\n" : "; previous metadata kept at " + archived + "\n";
        }
        else if (!archived.empty())
            ::rename(archived.c_str(), meta.c_str());
        else if (metaWritten)
            ::unlink(meta.c_str());
        refreshUnavailableLocked();
        return error;
    };

    excluded_.erase(name);
    if (reinit) reinit_.insert(name);
    failedDevices_.erase(path); // Report only this attempt's failure.
    PoolState pool;
    rc = buildPoolFromDevice(info->poolId, info->devPath, info->type, pool);
    if (rc != 0)
    {
        auto failure = failedDevices_.find(path);
        rollback(rc, failure != failedDevices_.end() ? failure->second.first : "cannot read device");
        if (!reinit) out += "use --reinit to discard its contents and start an empty pool\n";
        return rc;
    }
    // Header first: until the list is saved the device stays excluded, so a
    // crash at any point below leaves either an excluded device or a complete
    // one, never a listed device without a header.
    const bool writesHeader = pool.needsHeaderInit;
    StagedRecovery recovery;
    recovery.pools.push_back(std::move(pool));
    if ((rc = stagePoolsLocked(recovery)) != 0)
    {
        auto failure = failedDevices_.find(path);
        rollback(rc, failure != failedDevices_.end() ? failure->second.first : "recovery failed");
        if (!reinit) out += "use --reinit to discard its contents and start an empty pool\n";
        return rc;
    }
    headerWritten = writesHeader;
    // Fresh metadata reconciles a global checkpoint that may still list
    // regions the WAL freed. Without one there is nothing to reconcile, and
    // pool metadata without a global checkpoint would make startup refuse.
    if ((wasExcluded || reinit) && hasGlobalCheckpoint_)
    {
        if ((rc = metadata_->save(recovery.pools.front())) != 0)
            return rollback(rc, "cannot write pool metadata");
        metaWritten = true;
    }
    if (wasExcluded)
    {
        int syncRc = 0;
        if ((rc = saveExcludedDevices(stateDir_, excluded_, syncRc)) != 0)
            return rollback(rc, "cannot save excluded device list");
        listSaved = true;
        excludedSyncRc_ = syncRc;
    }
    publishPoolsLocked(recovery);
    reinit_.erase(name);

    const PoolState *added = findPoolById(poolId);
    logf(LogLevel::Warn, "admin: included %s (pool %u)%s", path.c_str(), poolId,
         reinit ? " with a fresh header" : "");
    appendf(out, "included %s (pool %u) uuid=%s\n", path.c_str(), poolId,
            added ? added->deviceUuid.c_str() : "");
    if (!archived.empty()) out += "previous_metadata=" + archived + "\n";
    if (checkpointBlock_.empty() && checkpointLocked() == 0)
        out += "checkpoint=written\n";
    else
        out += "checkpoint=deferred reason=" +
               (checkpointBlock_.empty() ? std::string("write failed") : checkpointBlock_) + "\n";
    // Losing this list only keeps the device excluded after a power loss, so
    // the include stands; still say so.
    if (wasExcluded && excludedSyncRc_ != 0)
        out += "WARNING: the excluded device list is not durable yet (syncing " + stateDir_ +
               " failed: " + std::strerror(-excludedSyncRc_) + "); after a power loss the RM " +
               "may find " + path + " excluded again\n";
    return 0;
}

int PoolManager::adminRelease(uint64_t regionId, bool confirmed, std::string &out)
{
    std::lock_guard<std::mutex> lock(mu_);
    auto it = allocations_.find(regionId);
    if (it == allocations_.end())
    {
        out = "no allocation with region " + std::to_string(regionId) + "\n";
        return -ENOENT;
    }
    const Allocation a = it->second;
    if (!findPoolById(a.poolId))
    {
        out = "pool " + std::to_string(a.poolId) +
              " has no device; its allocations cannot be released here\n";
        return -ENODEV;
    }
    if (isLocalClient(a.clientId) && localOwnerAlive(a.clientId))
    {
        out = std::string("owner ") + a.clientId + " is still running\n";
        return -EBUSY;
    }
    if (!confirmed)
    {
        out = std::string("verify that owner ") + a.clientId +
              " has exited and unmapped the region, then pass --confirm\n";
        return -EPERM;
    }
    const int rc = doFreeAllocation(regionId);
    if (rc != 0)
    {
        out = "release failed: " + std::string(std::strerror(-rc)) + "\n";
        return rc;
    }
    logf(LogLevel::Warn, "admin: released region %llu (%llu bytes, pool %u) owned by %s",
         static_cast<unsigned long long>(regionId),
         static_cast<unsigned long long>(a.allocLength), a.poolId, a.clientId);
    appendf(out, "released region=%llu size=%llu client=%s\n",
            static_cast<unsigned long long>(regionId),
            static_cast<unsigned long long>(a.allocLength), a.clientId);
    return 0;
}

int PoolManager::adminWipe(const std::string &device, bool confirmed, std::string &out,
                           const std::function<bool()> &cancelled)
{
    std::string name;
    if (!normalizeDaxName(device, name))
    {
        out = "invalid device name: " + device + "\n";
        return -EINVAL;
    }
    const std::string path = "/dev/" + name;
    uint64_t size = 0;
    uint64_t align = alignBytes_;
    {
        std::lock_guard<std::mutex> lock(mu_);
        int rc = checkExcludedFileLocked(out);
        if (rc != 0) return rc;
        if (!excluded_.count(name))
        {
            out = path + " is not excluded; exclude it first\n";
            return -EINVAL;
        }
        // Wiping prepares a handover, which must not start while a power loss
        // could still return the device to the RM.
        if (excludedSyncRc_ != 0) excludedSyncRc_ = syncExcludedDevices(stateDir_);
        if (excludedSyncRc_ != 0)
        {
            out = path + " is excluded, but " +
                  unsyncedExclusionNote(stateDir_, excludedSyncRc_, name);
            return excludedSyncRc_;
        }
        if (!confirmed)
        {
            out = "this zeroes all of " + path +
                  ", header included; verify that no host maps it, then pass --confirm\n";
            return -EPERM;
        }
        rc = getDeviceSize(path, size);
        if (rc != 0 || size == 0)
        {
            rc = rc != 0 ? rc : -ENODEV;
            out = "cannot read the size of " + path + " (is it bound to device_dax?): " +
                  std::strerror(-rc) + "\n";
            return rc;
        }
        uint64_t devAlign = 0;
        if (readDaxAlignBytes(name, devAlign) && devAlign != 0) align = devAlign;
    }

    // Outside the lock: an excluded device is unused by the RM, and admin
    // commands run one at a time, so it cannot be included meanwhile.
    logf(LogLevel::Warn, "admin: wiping %s (%llu bytes)", path.c_str(),
         static_cast<unsigned long long>(size));
    const auto started = SteadyClock::now();
    const uint64_t chunk = std::max<uint64_t>(align, (1ULL << 30) / align * align);
    const int rc = wipeDeviceIsolated(path, size, chunk, cancelled);
    const double seconds =
        std::chrono::duration<double>(SteadyClock::now() - started).count();
    if (rc != 0)
    {
        logf(LogLevel::Error, "admin: wipe of %s stopped after %.1fs: %s", path.c_str(),
             seconds, std::strerror(-rc));
        out = "wipe of " + path + " stopped: " + std::strerror(-rc) +
              "; the device may be partly zeroed\n";
        return rc;
    }
    logf(LogLevel::Warn, "admin: wiped %s in %.1fs", path.c_str(), seconds);
    appendf(out, "wiped %s bytes=%llu seconds=%.1f\n", path.c_str(),
            static_cast<unsigned long long>(size), seconds);
    return 0;
}

}  // namespace maru
