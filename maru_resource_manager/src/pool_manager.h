#pragma once

#include <chrono>
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

/// Max client_id length for fixed-size serialization (hostname:pid).
static constexpr size_t kMaxClientIdLen = 128;

struct Allocation
{
    Handle handle;
    char clientId[kMaxClientIdLen];  // "hostname:pid" — null-terminated
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
    std::string deviceUuid;  // UUID from device header (DEV_DAX only, empty for FS_DAX)
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
    explicit PoolManager(const std::string &stateDir, int gracePeriodSec = 30);
    ~PoolManager();

    const std::string &stateDir() const { return stateDir_; }
    int gracePeriodSec() const { return gracePeriodSec_; }
    uint32_t allocationCount() const;

    int loadPools();
    int rescanDevices();
    int alloc(uint64_t size, const std::string &clientId, Handle &out,
              std::string &devPath, std::string &deviceUuid,
              const std::string &daxPath, uint64_t &requestedSizeOut);

    /// Atomically verify auth token and free. Returns -EACCES on bad token.
    int verifyAndFree(const Handle &handle, const std::string &clientId);
    /// Atomically verify auth token and get device path. Returns -EACCES on bad token.
    int verifyAndGetPath(const Handle &handle, std::string &outPath);

    void getStats(std::vector<PoolState> &out);
    int getPathForHandle(const Handle &handle, std::string &outPath);
    bool hasExistingAllocations();

    /// Get device UUID for a given region. Returns empty string if not found.
    std::string getDeviceUuidForRegion(uint64_t regionId);

    void reapExpired(uint64_t &reapedCount);
    void checkpoint();

    /// Notify that a client has disconnected. Starts the grace period timer.
    void clientDisconnected(const std::string &clientId);
    /// Notify that a client has reconnected. Cancels the grace period timer.
    void clientReconnected(const std::string &clientId);

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
    PoolState *findPoolByPath(const std::string &devPath);
    PoolState *findPoolForRegion(uint64_t regionId);
    /// Free without auth token verification (internal use only).
    int free(const Handle &handle, const std::string &clientId);
    /// Core deallocation logic shared by free/verifyAndFree/reapExpired.
    /// Caller MUST hold mu_.
    int doFreeAllocation(uint64_t regionId);

    mutable std::mutex mu_;
    std::string stateDir_;
    int gracePeriodSec_;
    std::vector<PoolState> pools_;
    uint64_t opCount_{0};
    uint64_t checkpointInterval_{100};

    std::unique_ptr<MetadataStore> metadata_;
    std::unique_ptr<WalStore> wal_;
    uint64_t alignBytes_{2ULL << 20};  // 2 MiB

    // Global allocation tracking by regionId
    std::map<uint64_t, Allocation> allocations_;
    uint64_t nextRegionId_{1};

    // client_id → {pid, start_time} for reaper PID-reuse detection (in-memory only)
    // Only tracked for local clients (same hostname)
    std::map<pid_t, uint64_t> pidStartTimes_;
    // client_id allocation refcount for O(1) reaper cleanup
    std::map<std::string, uint32_t> clientAllocCounts_;

    // Disconnected remote clients pending reap after grace period.
    // Maps client_id -> time of disconnection.
    using SteadyClock = std::chrono::steady_clock;
    std::map<std::string, SteadyClock::time_point> disconnectedClients_;

};

}  // namespace maru
