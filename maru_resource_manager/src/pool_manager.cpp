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
#include <unordered_set>

#include "device_header.h"
#include "metadata.h"
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

static bool getRegionIndexForDax(const std::string &devName,
                                 uint32_t &outIndex)
{
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
            uint32_t regionId = 0;
            if (!getRegionIndexForDax(name, regionId))
            {
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

int PoolManager::loadPoolFromDevice(uint32_t poolId, const std::string &path,
                                    DaxType type)
{
    uint64_t size = 0;
    int rc = getDeviceSize(path, size);
    if (rc != 0 || size == 0)
    {
        logf(LogLevel::Error, "maru-resource-manager: failed to get size for %s (%d)", path.c_str(), rc);
        return rc != 0 ? rc : -EINVAL;
    }

    // DEV_DAX: read or auto-initialize device UUID header
    uint64_t dataOffset = 0;
    std::string deviceUuid;
    if (type == DaxType::DEV_DAX)
    {
        DeviceHeader hdr{};
        int hrc = readDeviceHeader(path, hdr);
        if (hrc == -ENODATA)
        {
            // No valid header — auto-initialize
            initDeviceHeader(hdr);
            hrc = writeDeviceHeader(path, hdr);
            if (hrc != 0)
            {
                logf(LogLevel::Error,
                     "Failed to write device header to %s (%d)",
                     path.c_str(), hrc);
                return hrc;
            }
            logf(LogLevel::Info,
                 "Auto-initialized device header on %s: UUID=%s",
                 path.c_str(), uuidToString(hdr.uuid).c_str());
        }
        else if (hrc != 0)
        {
            logf(LogLevel::Error,
                 "Failed to read device header from %s (%d)",
                 path.c_str(), hrc);
            return hrc;
        }
        else
        {
            logf(LogLevel::Info,
                 "Device %s: UUID=%s",
                 path.c_str(), uuidToString(hdr.uuid).c_str());
        }
        deviceUuid = uuidToString(hdr.uuid);
        dataOffset = kDeviceHeaderSize;
    }

    PoolState pool{};
    pool.poolId = poolId;
    pool.devPath = path;
    pool.deviceUuid = deviceUuid;
    pool.totalSize = size - dataOffset;
    pool.freeSize = size - dataOffset;
    pool.alignBytes = alignBytes_;
    pool.type = type;
    if (type == DaxType::DEV_DAX)
    {
        uint64_t devAlign = 0;
        if (readDaxAlignBytes(baseName(path), devAlign))
        {
            pool.alignBytes = devAlign;
        }
    }
    else if (type == DaxType::FS_DAX)
    {
        uint64_t blksz = 0;
        if (getBlockLogicalBlockSize(path, blksz) && blksz > 0)
        {
            if (blksz > pool.alignBytes)
            {
                pool.alignBytes = blksz;
            }
        }
    }
    pool.freeList.push_back(Extent{dataOffset, size - dataOffset});

    PoolState loaded = pool;
    rc = metadata_->load(poolId, loaded);
    if (rc == 0)
    {
        loaded.devPath = path;
        loaded.totalSize = size;
        loaded.alignBytes = pool.alignBytes;
        pool = loaded;
    }

    // Auto-register own node mapping for DEV_DAX
    if (!pool.deviceUuid.empty())
    {
        nodeMappings_[pool.deviceUuid][localHostname()] = pool.devPath;
    }

    pools_.push_back(pool);
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

int PoolManager::loadPoolsLocked()
{
    std::vector<DeviceInfo> devices;
    int rc = scanDevices(devices);
    if (rc != 0)
    {
        return rc;
    }

    pools_.clear();
    allocations_.clear();
    nextRegionId_ = 1;

    for (const auto &dev : devices)
    {
        rc = loadPoolFromDevice(dev.poolId, dev.devPath, dev.type);
        if (rc != 0)
        {
            logf(LogLevel::Warn, "maru-resource-manager: failed to load pool %u from %s: %d", dev.poolId, dev.devPath.c_str(), rc);
        }
    }

    metadata_->loadGlobal(allocations_, nextRegionId_);

    int walRc = wal_->replay(pools_, allocations_, nextRegionId_);
    if (walRc != 0 && walRc != -ENOENT)
    {
        return walRc;
    }

    // Recompute auth tokens for all restored allocations.
    // Tokens now include client_id in the HMAC input; legacy tokens
    // (computed without client_id) would fail verification after this change.
    for (auto &[regionId, alloc] : allocations_)
    {
        alloc.handle.authToken = computeAuthToken(
            alloc.handle, alloc.nonce, std::string(alloc.clientId));
    }

    for (auto &pool : pools_)
    {
        recomputeFreeSize(pool);
    }

    return 0;
}

int PoolManager::rescanDevicesLocked()
{
    std::vector<DeviceInfo> devices;
    int rc = scanDevices(devices);
    if (rc != 0)
    {
        return rc;
    }

    size_t oldSize = pools_.size();
    for (const auto &dev : devices)
    {
        bool exists = false;
        for (const auto &pool : pools_)
        {
            if (pool.poolId == dev.poolId || pool.devPath == dev.devPath)
            {
                exists = true;
                break;
            }
        }
        if (exists)
        {
            continue;
        }
        rc = loadPoolFromDevice(dev.poolId, dev.devPath, dev.type);
        if (rc != 0)
        {
            return rc;
        }
    }

    if (pools_.size() == oldSize)
    {
        return 0;
    }

    std::vector<PoolState> newPools(
        pools_.begin() + static_cast<std::ptrdiff_t>(oldSize),
        pools_.end());
    int walRc = wal_->replay(newPools, allocations_, nextRegionId_);
    if (walRc != 0 && walRc != -ENOENT)
    {
        return walRc;
    }

    for (size_t i = 0; i < newPools.size(); ++i)
    {
        recomputeFreeSize(newPools[i]);
        pools_[oldSize + i] = std::move(newPools[i]);
    }

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
    uint64_t alignedSize = alignUp(size, pool.alignBytes);
    for (size_t i = 0; i < pool.freeList.size(); ++i)
    {
        Extent ex = pool.freeList[i];
        uint64_t aligned = alignUp(ex.offset, pool.alignBytes);
        uint64_t end = aligned + alignedSize;
        if (end > ex.offset + ex.length)
        {
            continue;
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

        uint64_t regionId = nextRegionId_++;

        if (pool.type == DaxType::FS_DAX)
        {
            if (createFsDaxFile(pool.devPath, regionId, alignedSize) != 0)
            {
                return false;
            }
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

PoolState *PoolManager::findPoolByUuid(const std::string &uuid)
{
    for (auto &pool : pools_)
    {
        if (pool.deviceUuid == uuid)
        {
            return &pool;
        }
    }
    return nullptr;
}

int PoolManager::registerNode(const std::string &nodeId,
                              const std::vector<DeviceMapping> &mappings)
{
    std::lock_guard<std::mutex> lock(mu_);

    if (registeredNodes_.count(nodeId))
    {
        logf(LogLevel::Debug,
             "[NODE_REGISTER] node=%s already registered, skipping",
             nodeId.c_str());
        return 0;
    }

    int matched = 0;
    for (const auto &m : mappings)
    {
        nodeMappings_[m.uuid][nodeId] = m.localDaxPath;
        if (findPoolByUuid(m.uuid) != nullptr)
        {
            ++matched;
        }
    }
    registeredNodes_.insert(nodeId);
    logf(LogLevel::Info,
         "[NODE_REGISTER] node=%s, devices=%zu, matched=%d",
         nodeId.c_str(), mappings.size(), matched);
    return matched;
}

std::string PoolManager::resolvePathForClient(const std::string &deviceUuid,
                                              const std::string &clientId)
{
    // FS_DAX or no UUID: no resolution needed
    if (deviceUuid.empty())
    {
        return "";
    }

    std::string hostname;
    auto pos = clientId.rfind(':');
    if (pos != std::string::npos)
    {
        hostname = clientId.substr(0, pos);
    }
    else
    {
        hostname = clientId;
    }

    // Local client: return RM's own devPath
    if (hostname == localHostname())
    {
        auto *pool = findPoolByUuid(deviceUuid);
        return pool ? pool->devPath : "";
    }

    // Remote client: lookup node mapping table
    auto it = nodeMappings_.find(deviceUuid);
    if (it != nodeMappings_.end())
    {
        auto nodeIt = it->second.find(hostname);
        if (nodeIt != it->second.end())
        {
            return nodeIt->second;
        }
    }
    return "";
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

int PoolManager::alloc(uint64_t size, const std::string &clientId, Handle &out,
                       std::string &devPath, const std::string &daxPath,
                       uint64_t &requestedSizeOut)
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

    Allocation alloc{};
    PoolState *selectedPool = nullptr;

    if (!daxPath.empty())
    {
        selectedPool = findPoolByPath(daxPath);
        if (!selectedPool)
        {
            return -ENOENT;
        }
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

    requestedSizeOut = alloc.requestedSize;
    out = alloc.handle;
    wal_->appendAlloc(alloc);
    if (++opCount_ % checkpointInterval_ == 0)
    {
        wal_->checkpoint(pools_, *metadata_, allocations_, nextRegionId_);
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
    std::unordered_set<std::string> expiredClients;
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
        if (doFreeAllocation(regionId) == 0)
        {
            ++reapedCount;
        }
    }
}

void PoolManager::checkpoint()
{
    std::lock_guard<std::mutex> lock(mu_);
    wal_->checkpoint(pools_, *metadata_, allocations_, nextRegionId_);
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

    wal_->appendFree(regionId);
    if (++opCount_ % checkpointInterval_ == 0)
    {
        wal_->checkpoint(pools_, *metadata_, allocations_, nextRegionId_);
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
