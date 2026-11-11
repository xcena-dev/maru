// Node ownership: a StorageService owns CXL regions as its node (node_id),
// not as its process, so a restarted run takes them over with NODE_CLAIM.
// The node table (current run, bound host) is persisted in <stateDir>/nodes
// (node_owners.cpp); the regions are ordinary ledger allocations whose owner
// ID is nodeOwnerId(node_id). They are freed by the current run, by the lease
// (no CLAIM, ALLOC or RENEW for nodeLease_), by maru-rm-ctl, or by a reboot
// of this host for nodes bound to it.
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

int PoolManager::saveNodesLocked(int &syncRc)
{
    NodeOwners table;
    for (const auto &[id, node] : nodes_)
        if (!node.epoch.empty()) table[id] = NodeOwner{node.epoch, node.host};
    const int rc = saveNodeOwners(stateDir_, table, syncRc);
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
    NodeOwners table;
    const int rc = loadNodeOwners(stateDir_, table);
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
    return 0;
}

int PoolManager::releaseNodeLocked(const std::string &nodeId, uint32_t &released)
{
    released = 0;
    const std::string owner = nodeOwnerId(nodeId);
    std::vector<uint64_t> regions;
    for (const auto &[id, a] : allocations_)
        if (owner == a.clientId && !isDraining(id)) regions.push_back(id);
    for (uint64_t id : regions)
    {
        // A held region drains, as for FREE.
        const int rc = doFreeAllocation(id);
        if (rc != 0) return rc;
        ++released;
    }
    auto it = nodes_.find(nodeId);
    if (it == nodes_.end()) return 0;
    const NodeState before = it->second;
    nodes_.erase(it);
    // Forgetting stands once the file has it; a power loss only brings back
    // a node without regions.
    int syncRc = 0;
    const int rc = saveNodesLocked(syncRc);
    if (rc != 0) nodes_[nodeId] = before;
    return rc;
}

void PoolManager::expireNodeLeasesLocked()
{
    const auto now = SteadyClock::now();
    std::vector<std::string> expired;
    for (auto &[id, node] : nodes_)
    {
        if (now - node.seen < nodeLease_) continue;
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
    if (it != nodes_.end() && !it->second.host.empty() && it->second.host != host)
    {
        logf(LogLevel::Warn, "node %s claimed from %s, but it is bound to %s", nodeId.c_str(),
             host.c_str(), it->second.host.c_str());
        return -EEXIST;
    }
    if (it != nodes_.end() && it->second.epoch == epoch)
    {
        // The same run again: succeed only once its record is durable, and
        // leave a fenced run fenced until then.
        const int rc = confirmNodesDurableLocked();
        if (rc != 0) return rc;
        it->second.fenced = false;
        it->second.seen = SteadyClock::now();
    }
    else
    {
        const bool created = it == nodes_.end();
        NodeState &node = nodes_[nodeId];
        const NodeState before = node;
        node = NodeState{epoch, host, SteadyClock::now()};
        int syncRc = 0;
        const int rc = saveNodesLocked(syncRc);
        if (rc != 0)
        {
            if (created) nodes_.erase(nodeId);
            else nodes_[nodeId] = before;
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
    NodeState *node = currentNode(nodeId, epoch);
    if (!node)
    {
        // Lease expired: fenced (the lease ran out), or the node record is
        // gone (NODE_RELEASE or an admin release-node), and no other run
        // claimed the node since. The run is over; the StorageService starts
        // a new run and claims again. Another run's claim: -ESTALE.
        auto it = nodes_.find(nodeId);
        const bool fenced = it == nodes_.end() || it->second.epoch.empty() ||
                            it->second.epoch == epoch;
        return fenced ? -EKEYEXPIRED : -ESTALE;
    }
    node->seen = SteadyClock::now();
    return 0;
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
    if (!currentNode(nodeId, epoch))
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
                       " host=" + (node.host.empty() ? "-" : node.host) + "\n";
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
