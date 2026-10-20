#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
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
};

class MetadataStore;
class WalStore;
struct Ledger;
struct PoolRecord;
struct DeviceProbe;
struct DevicePlan;
class DeviceSlots;

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
    using SteadyClock = std::chrono::steady_clock;
    using SteadyClockPoint = SteadyClock::time_point;

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

    /// maru-rm-ctl commands. Each returns 0 or -errno and fills `out` with
    /// key=value lines, or with the reason on failure.
    int adminStatus(std::string &out);
    int adminAllocations(const std::string &device, std::string &out);
    /// Returned by adminExclude when the device is excluded (the RM no longer
    /// uses it) but the list is not on disk yet, so a power loss could undo it.
    static constexpr int kAdminIncomplete = 1;
    /// Stop using a device for good (e.g. to hand it to ROOF). Refused while
    /// any allocation on it is live. kAdminIncomplete until the list is on
    /// disk; calling it again retries the sync.
    int adminExclude(const std::string &device, std::string &out);
    /// How `include` may treat the device's identity.
    enum class IncludeMode
    {
        Judge,          // The normal rules (an excluded pool must have a record)
        Adopt,          // Accept the Maru header of an excluded device without a record
        Reinit,         // Start the pool over with a new UUID, even over a foreign header
        RestoreHeader,  // Write the recorded UUID back when the identity tags match
    };
    /// Use an excluded, refused or skipped device again.
    int adminInclude(const std::string &device, IncludeMode mode, std::string &out);
    /// Return one allocation whose owner the caller verified has exited.
    /// `force` skips the running-owner check (a PID reused after a reboot).
    int adminRelease(uint64_t regionId, bool confirmed, bool force, std::string &out);
    /// Return every allocation of a device's pool, without opening the device.
    int adminDiscard(const std::string &device, bool confirmed, bool force,
                     std::string &out);
    /// Zero all of an excluded device, header included. Runs without holding
    /// the pool lock; stops between chunks once `cancel` is set.
    int adminWipe(const std::string &device, bool confirmed, std::string &out,
                  const std::function<bool()> &cancelled = {});

private:
    friend class PoolManagerTestPeer;
    struct DeviceInfo
    {
        uint32_t poolId;
        std::string devPath;
        DaxType type;
    };

    int scanDevices(std::vector<DeviceInfo> &outDevices);
    // Read-only facts about one device (sysfs, header, identity tags). Runs
    // without mu_; failures are logged once per change of reason.
    int probeDevice(const DeviceInfo &dev, DeviceProbe &out);
    // Log a probe failure once per change, or the device's recovery.
    int reportProbe(const DeviceProbe &probe);
    int poolBuildFailed(const std::string &path, const char *stage, int error);
    // Suppress only identical failures; every scan still retries the device.
    std::map<std::string, std::pair<std::string, int>> failedDevices_;
    int getDeviceSize(const std::string &path, uint64_t &sizeOut);
    int loadPoolsLocked(std::unique_lock<std::mutex> &lock);

    // Recovery (pool_recovery.cpp)
    struct DecideContext;
    void decide(const DeviceProbe &probe, const PoolRecord &record,
                const DecideContext &ctx, DevicePlan &plan) const;
    // A local owner whose process is gone (or ran in an earlier boot).
    bool ownerGone(const Allocation &a) const;
    std::vector<Allocation> poolAllocations(uint32_t poolId) const;
    // Probe devices that are neither served nor excluded and decide what to
    // do with each. With a `target` (include), `mode` and `wasExcluded` apply
    // to it alone, and the caller acts only on its plan. Device and
    // state-file reads run with `lock` released.
    std::vector<DevicePlan> probeAndDecide(std::unique_lock<std::mutex> &lock,
                                           const std::vector<DeviceInfo> &devices,
                                           IncludeMode mode, bool wasExcluded, bool startup,
                                           const std::string &target = "");
    // Carry out plans: ledger, record and device writes, then publish. Device
    // writes run with `lock` released. Returns the first write error.
    int executePlans(std::unique_lock<std::mutex> &lock, std::vector<DevicePlan> &plans);
    void publishPlan(DevicePlan &plan);
    void markUnserved(const DevicePlan &plan);
    // Free list = data range minus the pool's allocations in the ledger.
    void rebuildFreeList(PoolState &pool) const;
    void loadBootMarker();
    int finishBootMarker();
    void recountClients();

    int checkpointLocked();
    void setCheckpointBlock(std::string reason);
    void removeOrphanFsDaxFiles(const PoolState &pool);
    bool isExcluded(const std::string &devPath) const;
    int checkExcludedFileLocked(std::string &out);

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
    // Empty when checkpoints are allowed; set until recovery completes.
    std::string checkpointBlock_{"recovery has not completed"};
    std::chrono::steady_clock::time_point nextSkippedCheckpointLog_{};

    // Devices that are present but not served, with the reason (status).
    enum class DeviceState { Refused, Skipped, Busy };
    struct UnservedDevice
    {
        uint32_t poolId;
        DaxType type;
        DeviceState state;
        std::string reason;
        size_t live{0};
        size_t ownersAlive{0};
    };
    std::map<std::string, UnservedDevice> unserved_;
    // One operation per device at a time; shared with detached I/O threads.
    std::shared_ptr<DeviceSlots> slots_;
    // Local owners recorded in an earlier boot are gone (startup only).
    bool previousBoot_{false};
    // Write retries after a failed device or record write.
    std::map<std::string, std::pair<SteadyClockPoint, int>> retryAfter_;
    // Resets that wrote an archive copy in this run (no copy on retry).
    std::set<uint32_t> archivedResets_;
    std::string bootIdPath_{"/proc/sys/kernel/random/boot_id"};
    std::string bootIdNow_;
    int deviceIoTimeoutMs_{10000};
    // Devices handed to another owner (daxN.M): never opened or registered.
    std::set<std::string> excluded_;
    // -errno while the last list renamed into place has not been synced: a
    // power loss could bring the previous list back, so no excluded device
    // may be handed over yet. 0 once the list is durable.
    int excludedSyncRc_{0};

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
    std::map<std::string, SteadyClock::time_point> disconnectedClients_;
    // Expired clients whose free could not be logged; retried every pass.
    std::set<std::string> reapRetries_;

    // Rate-limited: the reaper retries failed frees every second.
    void logWalAppendFailure(const char *operation, int error);
    SteadyClock::time_point nextWalFailureLog_{};

};

}  // namespace maru
