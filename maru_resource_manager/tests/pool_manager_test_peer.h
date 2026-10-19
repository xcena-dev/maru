// The test-side friend of PoolManager (pool_manager.h). Every test binary
// includes this one definition, so no two of them can disagree.
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "pool_manager.h"
#include "device_exclusion.h"
#include "pool_recovery.h"

namespace maru {
class PoolManagerTestPeer {
public:
    static int build(PoolManager &pm, const std::string &path, DaxType type) {
        DeviceProbe probe;
        return pm.probeDevice(PoolManager::DeviceInfo{0, path, type}, probe);
    }
    static void scan(PoolManager &pm) {
        std::vector<PoolManager::DeviceInfo> devices;
        pm.scanDevices(devices);
    }
    static void inject(PoolManager &pm, PoolState pool, const Allocation &a) {
        pm.pools_.push_back(std::move(pool));
        if (a.allocLength) pm.allocations_[a.handle.regionId] = a;
    }
    static std::string checkpointBlock(const PoolManager &pm) {
        return pm.checkpointBlock_;
    }
    // FS_DAX discovery reads /proc/mounts, so tests drive the sweep directly.
    static void removeOrphanFsDaxFiles(PoolManager &pm, uint64_t nextRegionId) {
        pm.nextRegionId_ = nextRegionId;
        for (const auto &pool : pm.pools_) pm.removeOrphanFsDaxFiles(pool);
    }
    static void setBootIdPath(PoolManager &pm, const std::string &path) {
        pm.bootIdPath_ = path;
    }
    static void setDeviceIoTimeout(PoolManager &pm, int ms) { pm.deviceIoTimeoutMs_ = ms; }
    // "refused"/"skipped"/"busy" and the reason, or "" if the device is not
    // listed as unserved.
    static std::string unserved(const PoolManager &pm, const std::string &path) {
        auto it = pm.unserved_.find(path);
        if (it == pm.unserved_.end()) return "";
        static const char *names[] = {"refused", "skipped", "busy"};
        return std::string(names[static_cast<int>(it->second.state)]) + ": " + it->second.reason;
    }
    static bool slotHeld(PoolManager &pm, const std::string &path) {
        return pm.slots_->isHeld(path);
    }
    // Reread excluded_devices, as a restart would.
    static void reloadExcluded(PoolManager &pm) {
        std::lock_guard<std::mutex> lock(pm.mu_);
        pm.excluded_.clear();
        loadExcludedDevices(pm.stateDir_, pm.excluded_);
    }
    // An allocation in the ledger whose pool is not served.
    static void addAllocation(PoolManager &pm, const Allocation &a) {
        pm.allocations_[a.handle.regionId] = a;
        ++pm.clientAllocCounts_[a.clientId];
    }
    static void removeAllocation(PoolManager &pm, uint64_t regionId) {
        pm.allocations_.erase(regionId);
    }
};
} // namespace maru
