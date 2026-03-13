#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "log.h"
#include "types.h"

namespace maru
{

struct Extent
{
    uint64_t offset;
    uint64_t length;
};

struct Allocation
{
    Handle handle;
    uint64_t ownerId;
    uint64_t requestedSize;
    uint64_t allocLength;
    uint64_t nonce;       // Server-side nonce for auth token computation
    uint32_t poolId;      // Internal: pool reference for resource manager use
    uint64_t realOffset;  // Internal: actual offset within pool
};

struct PoolState
{
    uint32_t poolId;
    std::string devPath;
    uint64_t totalSize;
    uint64_t freeSize;
    uint64_t alignBytes;
    DaxType type;
    std::vector<Extent> freeList;
};

class MetadataStore;
class WalStore;

class PoolManager
{
public:
    explicit PoolManager(const std::string &stateDir);
    ~PoolManager();

    const std::string &stateDir() const { return stateDir_; }
    uint32_t allocationCount() const;
    uint32_t registeredServerCount() const;
    void registerServer(pid_t pid);
    void unregisterServer(pid_t pid);

    int loadPools();
    int rescanDevices();
    int alloc(uint64_t size, uint64_t ownerId, Handle &out, std::string &devPath,
              uint32_t poolId, uint64_t &requestedSizeOut);
    int free(const Handle &handle, uint64_t ownerId);

    void getStats(std::vector<PoolState> &out);
    int getPathForHandle(const Handle &handle, std::string &outPath);
    bool hasExistingAllocations();
    bool verifyAuthToken(const Handle &handle);

    void reapExpired(uint64_t &reapedCount);
    void checkpoint();

private:
    struct DeviceInfo
    {
        uint32_t poolId;
        std::string devPath;
        DaxType type;
    };

    int scanDevices(std::vector<DeviceInfo> &outDevices);
    int loadPoolFromDevice(uint32_t poolId, const std::string &path,
                           DaxType type);
    int getDeviceSize(const std::string &path, uint64_t &sizeOut);
    int loadPoolsLocked();
    int rescanDevicesLocked();
    void recomputeFreeSize(PoolState &pool);

    void coalesceFreeList(PoolState &pool);
    void insertExtentSorted(PoolState &pool, uint64_t offset, uint64_t length);
    bool allocateFromPool(PoolState &pool, uint64_t size, Allocation &outAlloc);
    PoolState *findPoolById(uint32_t poolId);

    mutable std::mutex mu_;
    std::string stateDir_;
    std::vector<PoolState> pools_;
    uint64_t opCount_{0};
    uint64_t checkpointInterval_{100};

    std::unique_ptr<MetadataStore> metadata_;
    std::unique_ptr<WalStore> wal_;
    uint64_t alignBytes_{2ULL << 20};  // 2 MiB

    // Global allocation tracking by regionId
    std::map<uint64_t, Allocation> allocations_;
    uint64_t nextRegionId_{1};

    // PID start times for reaper PID-reuse detection (in-memory only)
    std::map<pid_t, uint64_t> pidStartTimes_;
    // PID allocation refcount for O(1) reaper cleanup
    std::map<pid_t, uint32_t> pidAllocCounts_;

    // Registered server PIDs → start time (in-memory only, not persisted)
    std::map<pid_t, uint64_t> registeredServers_;
};

}  // namespace maru
