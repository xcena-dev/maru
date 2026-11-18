// Node ownership: a StorageService owns CXL regions as its node (node_id),
// not as its process, so a restarted run takes them over with NODE_CLAIM.
// The node table (current run and bound host, ended runs, unfinished
// releases) is persisted in <stateDir>/nodes (node_owners.cpp); the regions
// are ordinary ledger allocations whose owner ID is nodeOwnerId(node_id).
// They are freed by the current run, by the lease (no CLAIM, ALLOC or RENEW
// for nodeLease_), by maru-rm-ctl, or by a reboot of this host for nodes
// bound to it. A run that ended is never the node's run again.
#include <algorithm>
#include <cerrno>
#include <cstring>

#include "log.h"
#include "node_owners.h"
#include "pool_manager.h"
#include "pool_recovery.h"
#include "region_holds.h"
#include "util.h"

namespace maru
{

uint64_t PoolManager::nodeLeaseMs() const
{
    return static_cast<uint64_t>(nodeLease_.count());
}

std::string PoolManager::rmId() const
{
    std::lock_guard<std::mutex> lock(mu_);
    return rmId_;
}

PoolManager::NodeState *PoolManager::currentNode(const std::string &nodeId,
                                                 const std::string &epoch)
{
    auto it = nodes_.find(nodeId);
    if (it == nodes_.end() || it->second.fenced || it->second.epoch.empty() ||
        it->second.epoch != epoch)
        return nullptr;
    return &it->second;
}

bool PoolManager::nodeOnThisHost(const char *clientId) const
{
    auto it = nodes_.find(nodeIdOfOwner(clientId));
    return it != nodes_.end() && isLocalHostName(it->second.host);
}

std::vector<std::string> PoolManager::endedRunsOf(const std::string &nodeId) const
{
    auto it = endedRuns_.find(nodeId);
    return it == endedRuns_.end() ? std::vector<std::string>{} : it->second;
}

void PoolManager::restoreEndedLocked(const std::string &nodeId, std::vector<std::string> runs)
{
    if (runs.empty()) endedRuns_.erase(nodeId);
    else endedRuns_[nodeId] = std::move(runs);
}

void PoolManager::endRunLocked(const std::string &nodeId, const std::string &epoch)
{
    auto &runs = endedRuns_[nodeId];
    if (std::find(runs.begin(), runs.end(), epoch) == runs.end()) runs.push_back(epoch);
    if (runs.size() > kMaxEndedRuns) runs.erase(runs.begin());
}

bool PoolManager::runEndedLocked(const std::string &nodeId, const std::string &epoch) const
{
    auto it = endedRuns_.find(nodeId);
    return it != endedRuns_.end() &&
           std::find(it->second.begin(), it->second.end(), epoch) != it->second.end();
}

int PoolManager::saveNodesLocked(int &syncRc)
{
    NodeOwners table, releasing;
    for (const auto &[id, node] : nodes_)
    {
        if (node.releasing) releasing[id] = NodeOwner{node.epoch, node.host};
        else if (!node.epoch.empty()) table[id] = NodeOwner{node.epoch, node.host};
    }
    const int rc = saveNodeOwners(stateDir_, table, releasing, endedRuns_, syncRc);
    if (rc == 0) nodesSyncRc_ = syncRc;
    if (rc != 0)
        logf(LogLevel::Error, "cannot save the node table in %s/nodes: %s", stateDir_.c_str(),
             std::strerror(-rc));
    else if (syncRc != 0)
        logf(LogLevel::Error, "node table saved in %s/nodes but the directory was not synced: %s",
             stateDir_.c_str(), std::strerror(-syncRc));
    return rc;
}

int PoolManager::confirmNodesDurableLocked()
{
    if (nodesSyncRc_ == 0) return 0;
    nodesSyncRc_ = syncDirectory(stateDir_);
    return nodesSyncRc_;
}

int PoolManager::loadNodesLocked()
{
    NodeOwners table, releasing;
    const int rc = loadNodeOwners(stateDir_, table, releasing, endedRuns_);
    if (rc != 0)
    {
        logf(LogLevel::Error, "cannot read the node table in %s/nodes: %s", stateDir_.c_str(),
             std::strerror(-rc));
        return rc;
    }
    // Timers do not persist: every lease starts now.
    const auto now = SteadyClock::now();
    nodes_.clear();
    for (const auto &[id, owner] : table) nodes_[id] = NodeState{owner.epoch, owner.host, now};
    for (const auto &[id, owner] : releasing)
        nodes_[id] = NodeState{owner.epoch, owner.host, now, true, true};
    size_t orphans = 0;
    for (const auto &[regionId, a] : allocations_)
    {
        const std::string nodeId = nodeIdOfOwner(a.clientId);
        if (!nodeId.empty() && !isDraining(regionId) &&
            nodes_.emplace(nodeId, NodeState{"", "", now}).second)
            ++orphans;
    }
    if (orphans)
        logf(LogLevel::Warn,
             "%zu node(s) own regions but have no record in %s/nodes; any host may claim "
             "them, else their lease frees them",
             orphans, stateDir_.c_str());
    if (!releasing.empty())
        logf(LogLevel::Warn, "%zu node release(s) cut short before this start", releasing.size());
    return 0;
}

int PoolManager::releaseNodeLocked(const std::string &nodeId, uint32_t &released)
{
    released = 0;
    auto it = nodes_.find(nodeId);
    if (it != nodes_.end() && !it->second.epoch.empty() && !it->second.releasing)
    {
        // The run ends on disk ("releasing") before a region is freed: a
        // restarted RM neither brings it back nor gives it its freed regions,
        // and finishes the release. The node stays bound to its host.
        const NodeState before = it->second;
        std::vector<std::string> endedBefore = endedRunsOf(nodeId);
        endRunLocked(nodeId, before.epoch);
        it->second.fenced = it->second.releasing = true;
        int syncRc = 0;
        const int rc = saveNodesLocked(syncRc);
        if (rc != 0)
        {
            it->second = before;
            restoreEndedLocked(nodeId, std::move(endedBefore));
            return rc;
        }
    }
    // A power loss must not undo the end of the run once a region is freed.
    int rc = confirmNodesDurableLocked();
    if (rc != 0) return rc;
    const std::string owner = nodeOwnerId(nodeId);
    std::vector<uint64_t> regions;
    for (const auto &[id, a] : allocations_)
        if (owner == a.clientId && !isDraining(id)) regions.push_back(id);
    for (uint64_t id : regions)
    {
        // A held region drains, as for FREE.
        rc = doFreeAllocation(id);
        if (rc != 0) return rc;
        ++released;
    }
    if (it == nodes_.end()) return 0;
    const NodeState node = it->second;
    nodes_.erase(it);
    if (node.releasing)
    {
        // Drop the record; if that fails the next pass does, freeing nothing.
        int syncRc = 0;
        if (saveNodesLocked(syncRc) != 0) nodes_[nodeId] = node;
    }
    return 0;
}

void PoolManager::finishReleasesLocked()
{
    std::vector<std::string> pending;
    for (const auto &[id, node] : nodes_)
        if (node.releasing) pending.push_back(id);
    for (const auto &id : pending)
    {
        uint32_t released = 0;
        const int rc = releaseNodeLocked(id, released);
        if (rc == 0)
            logf(LogLevel::Info, "node %s: release finished, %u region(s) released", id.c_str(),
                 released);
        else
            logf(LogLevel::Warn, "node %s: release not finished (%s); the reaper retries",
                 id.c_str(), std::strerror(-rc));
    }
}

void PoolManager::expireNodeLeasesLocked()
{
    const auto now = SteadyClock::now();
    std::vector<std::string> expired;
    for (auto &[id, node] : nodes_)
    {
        // A fenced node is a release to finish, at once.
        if (!node.fenced && now - node.seen < nodeLease_) continue;
        if (!node.fenced)
            logf(LogLevel::Warn, "node %s (run %s) did not renew for %llds; releasing its regions",
                 id.c_str(), node.epoch.empty() ? "-" : node.epoch.c_str(),
                 static_cast<long long>(
                     std::chrono::duration_cast<std::chrono::seconds>(nodeLease_).count()));
        // Its run is over from now on, even if a release below fails.
        node.fenced = true;
        expired.push_back(id);
    }
    for (const auto &id : expired)
    {
        uint32_t released = 0;
        const int rc = releaseNodeLocked(id, released);
        if (rc == 0)
            logf(LogLevel::Warn, "node %s forgotten: %u region(s) released", id.c_str(), released);
        else if (released)
            logf(LogLevel::Warn, "node %s: %u region(s) released, the rest retried", id.c_str(),
                 released);
    }
}

int PoolManager::nodeClaim(const std::string &nodeId, const std::string &epoch,
                           const std::string &host, std::vector<NodeRegion> &regions)
{
    regions.clear();
    if (!validNodeId(nodeId) || !validHoldName(epoch) || !validHoldName(host))
    {
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(nodeId);
    // An ended run is not the node's run again, from any host, even when its
    // claim comes back after the RM restarted; nor is a run whose lease ran
    // out before its end could be written. -ESTALE: the run must not retry.
    if (runEndedLocked(nodeId, epoch) ||
        (it != nodes_.end() && it->second.fenced && it->second.epoch == epoch))
    {
        logf(LogLevel::Warn, "node %s: claim by run %s refused: the run has ended",
             nodeId.c_str(), epoch.c_str());
        return -ESTALE;
    }
    if (it != nodes_.end() && !it->second.host.empty() && it->second.host != host)
    {
        logf(LogLevel::Warn, "node %s claimed from %s, but it is bound to %s", nodeId.c_str(),
             host.c_str(), it->second.host.c_str());
        return -EEXIST;
    }
    if (it != nodes_.end() && it->second.fenced)
    {
        // A new run never gets regions that are being freed: finish first.
        uint32_t released = 0;
        const int rc = releaseNodeLocked(nodeId, released);
        if (rc != 0) return rc;
        it = nodes_.find(nodeId);
    }
    if (it != nodes_.end() && it->second.epoch == epoch)
    {
        // The same run again: succeed only once its record is durable.
        const int rc = confirmNodesDurableLocked();
        if (rc != 0) return rc;
        it->second.seen = SteadyClock::now();
    }
    else
    {
        const bool created = it == nodes_.end();
        NodeState &node = nodes_[nodeId];
        const NodeState before = node;
        std::vector<std::string> endedBefore = endedRunsOf(nodeId);
        // A releasing node was finished above (its record may remain).
        if (!before.epoch.empty() && !before.releasing) endRunLocked(nodeId, before.epoch);
        node = NodeState{epoch, host, SteadyClock::now()};
        int syncRc = 0;
        const int rc = saveNodesLocked(syncRc);
        if (rc != 0)
        {
            if (created) nodes_.erase(nodeId);
            else nodes_[nodeId] = before;
            restoreEndedLocked(nodeId, std::move(endedBefore));
            return rc;
        }
        logf(LogLevel::Info, "node %s claimed by run %s from %s (previous run %s)", nodeId.c_str(),
             epoch.c_str(), host.c_str(), before.epoch.empty() ? "-" : before.epoch.c_str());
        // In effect (the file has it), but a power loss could undo it: the
        // run retries the claim.
        if (syncRc != 0) return syncRc;
    }
    // Taken with the epoch switch: a hold of an earlier run is listed here,
    // or is refused (-ESTALE) from now on.
    regions = nodeRegionsLocked(nodeId);
    return 0;
}

std::vector<PoolManager::NodeRegion> PoolManager::nodeRegionsLocked(const std::string &nodeId)
{
    std::vector<NodeRegion> regions;
    const std::string owner = nodeOwnerId(nodeId);
    for (const auto &[id, a] : allocations_)
    {
        if (owner != a.clientId || isDraining(id)) continue;
        NodeRegion r{a.handle, a.requestedSize, "", "", {}};
        // Unserved: its device is not confirmed, so the run gets no path.
        if (const PoolState *pool = findPoolById(a.poolId))
        {
            r.devPath = regionPath(*pool, id);
            r.deviceUuid = pool->deviceUuid;
        }
        auto held = holds_.find(id);
        if (held != holds_.end()) r.holders.assign(held->second.begin(), held->second.end());
        regions.push_back(r);
    }
    return regions;
}

int PoolManager::nodeAlloc(const std::string &nodeId, const std::string &epoch, uint64_t size,
                           const std::string &daxPath, NodeRegion &out)
{
    if (!validNodeId(nodeId) || !validHoldName(epoch))
    {
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    NodeState *node = currentNode(nodeId, epoch);
    if (!node)
    {
        return -ESTALE;
    }
    node->seen = SteadyClock::now();
    return allocLocked(size, nodeOwnerId(nodeId), out.handle, out.devPath, out.deviceUuid, daxPath,
                       out.requestedSize);
}

int PoolManager::nodeFree(const Handle &handle, const std::string &nodeId,
                          const std::string &epoch)
{
    if (!validNodeId(nodeId) || !validHoldName(epoch))
    {
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (!currentNode(nodeId, epoch))
    {
        return -ESTALE;
    }
    auto it = allocations_.find(handle.regionId);
    if (it == allocations_.end())
    {
        return -ENOENT;
    }
    const Allocation &a = it->second;
    if (handle.authToken != computeAuthToken(handle, a.nonce, std::string(a.clientId)))
    {
        return -EACCES;
    }
    if (nodeOwnerId(nodeId) != a.clientId)
    {
        return -EPERM;
    }
    return doFreeAllocation(handle.regionId);
}

int PoolManager::nodeHold(const Handle &handle, const std::string &nodeId,
                          const std::string &holder, const HoldRecord &who, std::string &outPath)
{
    if (!validNodeId(nodeId) || !validHoldName(who.nodeEpoch) || !validHoldName(holder) ||
        !validHoldName(who.host) ||
        (!who.bootId.empty() && (!validHoldName(who.bootId) || who.bootId == "-")))
    {
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = allocations_.find(handle.regionId);
    if (it == allocations_.end())
    {
        return -ENOENT;
    }
    const Allocation &a = it->second;
    if (handle.authToken != computeAuthToken(handle, a.nonce, std::string(a.clientId)))
    {
        return -EACCES;
    }
    if (nodeOwnerId(nodeId) != a.clientId)
    {
        return -EPERM;
    }
    if (!currentNode(nodeId, who.nodeEpoch))
    {
        return -ESTALE;
    }
    const PoolState *pool = findPoolById(a.poolId);
    if (isDraining(handle.regionId) || !pool)
    {
        return -ENOENT;
    }
    HoldRecord hold = who;
    hold.instance = holder;
    auto &holders = holds_[handle.regionId];
    auto previous = holders.find(holder);
    if (previous != holders.end() && previous->second == hold)
    {
        // The same worker again: succeed only once its hold is durable.
        const int rc = confirmHoldsDurableLocked();
        if (rc == 0) outPath = regionPath(*pool, handle.regionId);
        return rc;
    }
    const bool replacing = previous != holders.end();
    const HoldRecord replaced = replacing ? previous->second : HoldRecord{};
    holders[holder] = hold;
    int syncRc = 0;
    const int rc = saveHoldsLocked(syncRc);
    if (rc != 0)
    {
        if (replacing) holders[holder] = replaced;
        else holders.erase(holder);
        if (holders.empty()) holds_.erase(handle.regionId);
        return rc;
    }
    logf(LogLevel::Debug, "region %llu held by worker %s (node %s run %s, pid %u)",
         static_cast<unsigned long long>(handle.regionId), holder.c_str(), nodeId.c_str(),
         who.nodeEpoch.c_str(), who.pid);
    // Kept (the file has it), but the worker must not map the region until
    // a retry finds it durable.
    if (syncRc != 0) return syncRc;
    outPath = regionPath(*pool, handle.regionId);
    return 0;
}

int PoolManager::nodeRenew(const std::string &nodeId, const std::string &epoch)
{
    if (!validNodeId(nodeId) || !validHoldName(epoch))
    {
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    const int status = runStatusLocked(nodeId, epoch);
    if (status == 0) currentNode(nodeId, epoch)->seen = SteadyClock::now();
    return status;
}

int PoolManager::nodeRunStatus(const std::string &nodeId, const std::string &epoch)
{
    if (!validNodeId(nodeId) || !validHoldName(epoch))
    {
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    return runStatusLocked(nodeId, epoch);
}

int PoolManager::runStatusLocked(const std::string &nodeId, const std::string &epoch)
{
    if (currentNode(nodeId, epoch)) return 0;
    // The node's last run, whose lease ran out or that was released:
    // -EKEYEXPIRED; the StorageService starts a new run and claims again.
    // A run that a later run replaced: -ESTALE.
    auto it = nodes_.find(nodeId);
    if (it != nodes_.end() && !it->second.epoch.empty())
        return it->second.epoch == epoch ? -EKEYEXPIRED : -ESTALE;
    auto ended = endedRuns_.find(nodeId);
    if (ended != endedRuns_.end() && !ended->second.empty() && ended->second.back() != epoch)
        return -ESTALE;
    return -EKEYEXPIRED;
}

int PoolManager::nodeHolds(const std::string &nodeId, const std::string &epoch,
                           std::vector<NodeRegion> &regions)
{
    regions.clear();
    if (!validNodeId(nodeId) || !validHoldName(epoch))
    {
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (!currentNode(nodeId, epoch))
    {
        return -ESTALE;
    }
    regions = nodeRegionsLocked(nodeId);
    return 0;
}

int PoolManager::nodeRelease(const std::string &nodeId, const std::string &epoch,
                             uint32_t &released)
{
    released = 0;
    if (!validNodeId(nodeId) || !validHoldName(epoch))
    {
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    // The current run, or the same run again whose release did not finish:
    // it continues.
    auto it = nodes_.find(nodeId);
    if (!currentNode(nodeId, epoch) &&
        (it == nodes_.end() || !it->second.fenced || it->second.epoch != epoch))
    {
        return -ESTALE;
    }
    const int rc = releaseNodeLocked(nodeId, released);
    if (rc == 0)
        logf(LogLevel::Info, "node %s released by run %s: %u region(s)", nodeId.c_str(),
             epoch.c_str(), released);
    return rc;
}

int PoolManager::adminReleaseNode(const std::string &nodeId, bool confirmed, std::string &out)
{
    if (!validNodeId(nodeId))
    {
        out = "invalid node ID: " + nodeId + "\n";
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(nodeId);
    if (it == nodes_.end())
    {
        out = "node " + nodeId + " is not known\n";
        return -ENOENT;
    }
    const NodeState node = it->second;
    std::string list = "node=" + nodeId + " epoch=" + (node.epoch.empty() ? "-" : node.epoch) +
                       " host=" + (node.host.empty() ? "-" : node.host) +
                       (node.releasing ? " releasing" : "") + "\n";
    const std::string owner = nodeOwnerId(nodeId);
    for (const auto &[id, a] : allocations_)
        if (owner == a.clientId && !isDraining(id))
            list += "region=" + std::to_string(id) + " pool=" + std::to_string(a.poolId) +
                    " size=" + std::to_string(a.allocLength) + holdState(id) + "\n";
    if (!confirmed)
    {
        out = list + "this frees the regions above and forgets the node. Verify that its "
                     "StorageService is stopped and no worker maps them, then pass --confirm\n";
        return -EPERM;
    }
    uint32_t released = 0;
    const int rc = releaseNodeLocked(nodeId, released);
    if (rc != 0)
    {
        out = list + "released " + std::to_string(released) + " region(s), then failed: " +
              std::strerror(-rc) + "; run this again\n";
        return rc;
    }
    logf(LogLevel::Warn, "admin: released node %s (run %s, host %s): %u region(s)",
         nodeId.c_str(), node.epoch.empty() ? "-" : node.epoch.c_str(),
         node.host.empty() ? "-" : node.host.c_str(), released);
    out = list + "released node=" + nodeId + " regions=" + std::to_string(released) + "\n";
    return 0;
}

}  // namespace maru
