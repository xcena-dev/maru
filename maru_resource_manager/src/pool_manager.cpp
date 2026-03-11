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

#include "metadata.h"
#include "util.h"
#include "wal.h"

#ifdef __linux__
#include <linux/fs.h>
#endif

namespace maru
{

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
    return v + (align - rem);
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

PoolManager::PoolManager()
{
    metadata_ = std::make_unique<MetadataStore>(defaultStateDir());
    wal_ = std::make_unique<WalStore>(defaultStateDir());
}

PoolManager::~PoolManager() = default;

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

    PoolState pool{};
    pool.poolId = poolId;
    pool.devPath = path;
    pool.totalSize = size;
    pool.freeSize = size;
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
    pool.freeList.push_back(Extent{0, size});

    PoolState loaded = pool;
    rc = metadata_->load(poolId, loaded);
    if (rc == 0)
    {
        loaded.devPath = path;
        loaded.totalSize = size;
        loaded.alignBytes = pool.alignBytes;
        pool = loaded;
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

int PoolManager::alloc(uint64_t size, uint64_t ownerId, Handle &out,
                       std::string &devPath, uint32_t poolId,
                       uint64_t &requestedSizeOut)
{
    std::lock_guard<std::mutex> lock(mu_);
    if (pools_.empty())
    {
        return -ENODEV;
    }

    Allocation alloc{};
    PoolState *selectedPool = nullptr;

    if (poolId != kAnyPoolId)
    {
        for (auto &pool : pools_)
        {
            if (pool.poolId != poolId)
            {
                continue;
            }
            if (!allocateFromPool(pool, size, alloc))
            {
                return -ENOMEM;
            }
            alloc.ownerId = ownerId;
            pool.freeSize -= alloc.allocLength;
            selectedPool = &pool;
            break;
        }
        if (!selectedPool)
        {
            return -ENOENT;
        }
    }
    else
    {
        for (auto &pool : pools_)
        {
            if (allocateFromPool(pool, size, alloc))
            {
                alloc.ownerId = ownerId;
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

    alloc.handle.authToken = computeAuthToken(alloc.handle, alloc.nonce);

    // Cache owner PID start time for reaper PID-reuse detection
    if (ownerId != 0)
    {
        pid_t pid = static_cast<pid_t>(ownerId);
        ++pidAllocCounts_[pid];
        if (pidStartTimes_.find(pid) == pidStartTimes_.end())
        {
            uint64_t st = getPidStartTime(pid);
            if (st != 0)
            {
                pidStartTimes_[pid] = st;
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

int PoolManager::free(const Handle &handle, uint64_t ownerId)
{
    std::lock_guard<std::mutex> lock(mu_);

    auto globalIt = allocations_.find(handle.regionId);
    if (globalIt == allocations_.end())
    {
        return -ENOENT;
    }

    const Allocation &alloc = globalIt->second;
    if (ownerId != 0 && alloc.ownerId != ownerId)
    {
        return -EPERM;
    }

    PoolState *targetPool = findPoolById(alloc.poolId);
    if (!targetPool)
    {
        return -ENOENT;
    }

    if (targetPool->type == DaxType::FS_DAX)
    {
        deleteFsDaxFile(targetPool->devPath, handle.regionId);
    }

    insertExtentSorted(*targetPool, alloc.realOffset, alloc.allocLength);
    targetPool->freeSize += alloc.allocLength;

    // Update PID refcount
    if (alloc.ownerId != 0)
    {
        pid_t pid = static_cast<pid_t>(alloc.ownerId);
        auto countIt = pidAllocCounts_.find(pid);
        if (countIt != pidAllocCounts_.end())
        {
            if (--countIt->second == 0)
            {
                pidAllocCounts_.erase(countIt);
                pidStartTimes_.erase(pid);
            }
        }
    }

    allocations_.erase(globalIt);

    wal_->appendFree(handle.regionId);
    if (++opCount_ % checkpointInterval_ == 0)
    {
        wal_->checkpoint(pools_, *metadata_, allocations_, nextRegionId_);
    }
    return 0;
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

    std::vector<uint64_t> toFree;
    for (const auto &kv : allocations_)
    {
        uint64_t ownerId = kv.second.ownerId;
        if (ownerId == 0)
        {
            continue;
        }
        pid_t pid = static_cast<pid_t>(ownerId);
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
        auto globalIt = allocations_.find(regionId);
        if (globalIt == allocations_.end())
        {
            continue;
        }

        const Allocation &alloc = globalIt->second;

        PoolState *targetPool = findPoolById(alloc.poolId);
        if (!targetPool)
        {
            continue;
        }

        if (targetPool->type == DaxType::FS_DAX)
        {
            deleteFsDaxFile(targetPool->devPath, regionId);
        }

        insertExtentSorted(*targetPool, alloc.realOffset, alloc.allocLength);
        targetPool->freeSize += alloc.allocLength;

        // Update PID refcount — O(1) instead of scanning all allocations
        if (alloc.ownerId != 0)
        {
            pid_t pid = static_cast<pid_t>(alloc.ownerId);
            auto countIt = pidAllocCounts_.find(pid);
            if (countIt != pidAllocCounts_.end())
            {
                if (--countIt->second == 0)
                {
                    pidAllocCounts_.erase(countIt);
                    pidStartTimes_.erase(pid);
                }
            }
        }

        allocations_.erase(globalIt);

        wal_->appendFree(regionId);
        if (++opCount_ % checkpointInterval_ == 0)
        {
            wal_->checkpoint(pools_, *metadata_, allocations_, nextRegionId_);
        }
        ++reapedCount;
    }
}

bool PoolManager::verifyAuthToken(const Handle &handle)
{
    std::lock_guard<std::mutex> lock(mu_);

    auto it = allocations_.find(handle.regionId);
    if (it == allocations_.end())
    {
        return false;
    }

    uint64_t expectedToken = computeAuthToken(handle, it->second.nonce);
    return handle.authToken == expectedToken;
}

}  // namespace maru
