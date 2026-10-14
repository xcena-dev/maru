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
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <unordered_set>

#include "device_header.h"
#include "metadata.h"
#include "pool_validation.h"
#include "util.h"
#include "wal.h"

#ifdef __linux__
#include <linux/fs.h>
#endif

namespace maru
{

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
        if (hrc == -ENODATA)
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

int PoolManager::recoverPools(std::vector<PoolState> &pools,
                              std::map<uint64_t, Allocation> &allocations,
                              uint64_t &nextRegionId, std::string &unavailablePools)
{
    std::set<uint32_t> referenced;
    int rc = wal_->referencedPools(referenced);
    if (rc != 0 && rc != -ENOENT) return rc;

    // Validate saved allocations before replay, even if a later FREE could
    // hide them. Never initialize a header over any existing pool history.
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
        if (pool.needsHeaderInit &&
            (pool.hasCheckpoint || hasAllocations || referenced.count(pool.poolId)))
            return poolBuildFailed(pool.devPath, "missing header with saved pool history", -EUCLEAN);
    }

    rc = wal_->replay(pools, allocations, nextRegionId);
    if (rc != 0 && rc != -ENOENT) return rc;
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
        logf(LogLevel::Info, "Auto-initialized device header on %s: UUID=%s",
             pool.devPath.c_str(), pool.deviceUuid.c_str());
    }

    // Missing devices must retain their WAL, including records not imported
    // into allocations yet. Keep current pools in the coverage check on rescan.
    std::set<uint32_t> available;
    for (const auto &p : pools_) available.insert(p.poolId);
    for (const auto &p : pools) available.insert(p.poolId);
    std::set<uint32_t> missing;
    for (uint32_t id : referenced)
        if (!available.count(id)) missing.insert(id);
    for (const auto &[id, a] : allocations)
        if (!available.count(a.poolId)) missing.insert(a.poolId);
    unavailablePools.clear();
    for (uint32_t id : missing)
        unavailablePools += (unavailablePools.empty() ? "" : ",") + std::to_string(id);
    if (!unavailablePools.empty())
        unavailablePools = "no device for pool(s) " + unavailablePools +
                           " referenced by WAL/checkpoint";
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
        PoolState pool;
        rc = buildPoolFromDevice(dev.poolId, dev.devPath, dev.type, pool);
        if (rc == -EUCLEAN) return rc;
        if (rc != 0) continue; // Transient device errors remain retryable.
        if (pool.hasCheckpoint && globalRc == -ENOENT)
            return poolBuildFailed(pool.devPath, "missing global checkpoint", -EUCLEAN);
        stagedPools.push_back(std::move(pool));
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
        if (exists) continue;
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

    auto stagedAllocations = allocations_;
    uint64_t stagedNextRegionId = nextRegionId_;
    std::string unavailable;
    rc = recoverPools(stagedPools, stagedAllocations, stagedNextRegionId, unavailable);
    if (rc != 0)
    {
        setCheckpointBlock("rescan recovery failed (rc=" + std::to_string(rc) + ")");
        return rc;
    }
    const size_t firstNew = pools_.size();
    for (auto &pool : stagedPools) pools_.push_back(std::move(pool));
    allocations_ = std::move(stagedAllocations);
    nextRegionId_ = stagedNextRegionId;
    clientAllocCounts_.clear();
    for (auto &[id, a] : allocations_)
    {
        a.handle.authToken = computeAuthToken(a.handle, a.nonce, std::string(a.clientId));
        ++clientAllocCounts_[a.clientId];
    }
    unavailablePools_ = std::move(unavailable);
    for (size_t i = firstNew; i < pools_.size(); ++i) removeOrphanFsDaxFiles(pools_[i]);
    setCheckpointBlock(unavailablePools_);
    return 0;
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

}  // namespace maru
