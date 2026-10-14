#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
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
    // Runtime bounds from the device, never trusted from checkpoint metadata.
    uint64_t dataBegin{0};
    uint64_t dataEnd{0};
    bool hasCheckpoint{false};
    bool needsHeaderInit{false};
};

class MetadataStore;
class WalStore;

/// Extract the dax_region index from a dev_dax device name ("dax3.7" -> 3).
///
/// The kernel names every dev_dax "dax<dax_region id>.<dev_dax id>", so this is
/// authoritative for every provider (pmem, CXL region, hmem) and, unlike parsing
/// the sysfs link target, does not depend on the device hierarchy layout.
/// Exposed for testing.
bool parseRegionIndexFromDaxName(const std::string &devName, uint32_t &outIndex);

/// True only for a sysfs DAX device bound to device_dax (not kmem/unbound).
bool isDeviceDaxBound(const std::string &sysfsDevicePath);

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
    /// Lightweight check: returns true if at least one pool is loaded.
    bool hasPools() const;
    int getPathForHandle(const Handle &handle, std::string &outPath);
    bool hasExistingAllocations();

    /// Get device UUID for a given region. Returns empty string if not found.
    std::string getDeviceUuidForRegion(uint64_t regionId);

    void reapExpired(uint64_t &reapedCount);
    int checkpoint();

    /// Notify that a client has disconnected. Starts the grace period timer.
    void clientDisconnected(const std::string &clientId);
    /// Notify that a client has reconnected. Cancels the grace period timer.
    void clientReconnected(const std::string &clientId);

private:
    friend class PoolManagerTestPeer;
    struct DeviceInfo
    {
        uint32_t poolId;
        std::string devPath;
        DaxType type;
    };

    int scanDevices(std::vector<DeviceInfo> &outDevices);
    // Read-only builder. Header initialization is deferred until recovery passes.
    int buildPoolFromDevice(uint32_t poolId, const std::string &path,
                            DaxType type, PoolState &out);
    int poolBuildFailed(const std::string &path, const char *stage, int error);
    // Suppress only identical failures; every scan still retries the device.
    std::map<std::string, std::pair<std::string, int>> failedDevices_;
    int getDeviceSize(const std::string &path, uint64_t &sizeOut);
    int loadPoolsLocked();
    int rescanDevicesLocked();
    int recoverPools(std::vector<PoolState> &pools,
                     std::map<uint64_t, Allocation> &allocations,
                     uint64_t &nextRegionId, std::string &unavailablePools);
    int checkpointLocked();
    void setCheckpointBlock(std::string reason);
    void removeOrphanFsDaxFiles(const PoolState &pool);
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
    // Keep WAL records for unavailable/rejected pools; never checkpoint a
    // partial recovery or discard the evidence needed for a later retry.
    // Empty when checkpoints are allowed, otherwise why they are paused.
    std::string checkpointBlock_{"recovery has not completed"};
    // From the last successful recovery; only a rescan that adds pools can
    // change it, because new WAL records and allocations use live pools.
    std::string unavailablePools_;
    std::chrono::steady_clock::time_point nextSkippedCheckpointLog_{};
    bool hasGlobalCheckpoint_{false};

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
    // Expired clients whose free could not be logged; retried every pass.
    std::set<std::string> reapRetries_;

    // Rate-limited: the reaper retries failed frees every second.
    void logWalAppendFailure(const char *operation, int error);
    SteadyClock::time_point nextWalFailureLog_{};

};

}  // namespace maru
