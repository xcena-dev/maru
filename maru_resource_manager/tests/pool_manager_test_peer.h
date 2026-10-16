// The test-side friend of PoolManager (pool_manager.h). Every test binary
// includes this one definition, so no two of them can disagree.
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "pool_manager.h"

namespace maru {
class PoolManagerTestPeer {
public:
    static int build(PoolManager &pm, const std::string &path, DaxType type) {
        PoolState pool{};
        return pm.buildPoolFromDevice(0, path, type, pool);
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
    static void removeOrphanFsDaxFiles(PoolManager &pm, uint64_t nextRegionId,
                                       std::string unavailable = "") {
        pm.nextRegionId_ = nextRegionId;
        pm.unavailablePools_ = std::move(unavailable);
        for (const auto &pool : pm.pools_) pm.removeOrphanFsDaxFiles(pool);
    }
};
} // namespace maru
