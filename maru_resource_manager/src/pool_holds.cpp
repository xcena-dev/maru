// Region holds: a remote CXL agent holds each region it registers with its
// NIC, and the RM keeps a freed region's range out of use until every holder
// has unheld it, so a late RDMA WRITE through that registration cannot land in
// the range's next owner. Holds are persisted in <stateDir>/holds
// (region_holds.cpp); the WAL and free list change only when a region is
// released for real.
#include <cerrno>
#include <cstring>

#include "log.h"
#include "pool_manager.h"
#include "region_holds.h"
#include "util.h"

namespace maru
{

std::string PoolManager::holdState(uint64_t regionId) const
{
    std::string state;
    auto it = holds_.find(regionId);
    if (it != holds_.end())
    {
        state = " held_by=";
        for (const auto &[holder, instance] : it->second)
            state += (state.size() > 9 ? "," : "") + holder;
    }
    if (isDraining(regionId)) state += " draining";
    return state;
}

int PoolManager::saveHoldsLocked(int &syncRc)
{
    const int rc = saveRegionHolds(stateDir_, holds_, draining_, syncRc);
    if (rc == 0) holdsSyncRc_ = syncRc;
    if (rc != 0)
        logf(LogLevel::Error, "cannot save region holds in %s/holds: %s", stateDir_.c_str(),
             std::strerror(-rc));
    else if (syncRc != 0)
        logf(LogLevel::Error,
             "region holds saved in %s/holds but the directory was not synced: %s",
             stateDir_.c_str(), std::strerror(-syncRc));
    return rc;
}

int PoolManager::confirmHoldsDurableLocked()
{
    if (holdsSyncRc_ == 0) return 0;
    holdsSyncRc_ = syncDirectory(stateDir_);
    if (holdsSyncRc_ != 0)
        logf(LogLevel::Error, "region holds in %s/holds are still not synced: %s",
             stateDir_.c_str(), std::strerror(-holdsSyncRc_));
    return holdsSyncRc_;
}

int PoolManager::loadHoldsLocked(size_t &stale)
{
    stale = 0;
    const int rc = loadRegionHolds(stateDir_, holds_, draining_);
    if (rc != 0)
    {
        logf(LogLevel::Error, "cannot read region holds in %s/holds: %s", stateDir_.c_str(),
             std::strerror(-rc));
        return rc;
    }
    // Region IDs are never reused, so a record of a region the ledger no
    // longer has (released while the file could not be saved) is stale.
    for (auto it = holds_.begin(); it != holds_.end();)
    {
        if (allocations_.count(it->first))
        {
            ++it;
            continue;
        }
        stale += it->second.size();
        it = holds_.erase(it);
    }
    for (auto it = draining_.begin(); it != draining_.end();)
    {
        if (allocations_.count(*it))
        {
            ++it;
            continue;
        }
        ++stale;
        it = draining_.erase(it);
    }
    if (stale)
        logf(LogLevel::Info, "dropped %zu hold record(s) of regions no longer allocated", stale);
    return 0;
}

size_t PoolManager::finishDrainingLocked(bool save)
{
    size_t freed = 0;
    const std::set<uint64_t> drained = draining_;
    for (uint64_t regionId : drained)
    {
        if (isHeld(regionId)) continue;
        // On failure (e.g. the WAL append) it stays draining; the reaper retries.
        const int rc = releaseAllocation(regionId);
        if (rc == -ENOENT)
        {
            save |= draining_.erase(regionId) != 0;
        }
        else if (rc == 0)
        {
            ++freed;
            logf(LogLevel::Info, "region %llu released: its last hold was dropped",
                 static_cast<unsigned long long>(regionId));
        }
    }
    // The freed regions' records are stale now, and dropped at the next start
    // if this save fails.
    int syncRc = 0;
    if (freed || save) saveHoldsLocked(syncRc);
    return freed;
}

int PoolManager::hold(const Handle &handle, const std::string &holder,
                      const std::string &instance, std::string &outPath)
{
    if (!validHoldName(holder) || !validHoldName(instance))
    {
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    // The caller proves it was given the handle, as for GET_ACCESS.
    const int rc = verifyAndGetPathLocked(handle, outPath);
    if (rc != 0)
    {
        return rc;
    }
    auto &holders = holds_[handle.regionId];
    auto previous = holders.find(holder);
    if (previous != holders.end() && previous->second == instance)
    {
        // A retry after an unsynced save: succeed only once it is durable.
        return confirmHoldsDurableLocked();
    }
    // A newer instance of the holder replaces the old one: one agent per node.
    const std::string replaced = previous != holders.end() ? previous->second : "";
    holders[holder] = instance;
    int syncRc = 0;
    const int saveRc = saveHoldsLocked(syncRc);
    if (saveRc != 0)
    {
        if (replaced.empty()) holders.erase(holder);
        else holders[holder] = replaced;
        if (holders.empty()) holds_.erase(handle.regionId);
        return saveRc;
    }
    logf(LogLevel::Debug, "region %llu held by %s (instance %s)",
         static_cast<unsigned long long>(handle.regionId), holder.c_str(), instance.c_str());
    // Kept in memory (the file has it), but not durable: the agent must not
    // register the region yet, and retries.
    return syncRc;
}

int PoolManager::unhold(const Handle &handle, const std::string &holder,
                        const std::string &instance)
{
    if (!validHoldName(holder) || !validHoldName(instance))
    {
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    const uint64_t regionId = handle.regionId;
    // A draining region is still allocated, so its token can be checked.
    auto alloc = allocations_.find(regionId);
    if (alloc == allocations_.end())
    {
        return 0;
    }
    if (handle.authToken !=
        computeAuthToken(handle, alloc->second.nonce, std::string(alloc->second.clientId)))
    {
        return -EACCES;
    }
    auto it = holds_.find(regionId);
    if (it == holds_.end())
    {
        return 0;
    }
    // Another instance's hold is that process's to drop (or resetHolds').
    auto held = it->second.find(holder);
    if (held == it->second.end() || held->second != instance)
    {
        return 0;
    }
    it->second.erase(held);
    const bool last = it->second.empty();
    if (last) holds_.erase(it);
    // An unsynced save stands: a power loss only brings the hold back.
    int syncRc = 0;
    const int rc = saveHoldsLocked(syncRc);
    if (rc != 0)
    {
        holds_[regionId][holder] = instance;
        return rc;
    }
    logf(LogLevel::Debug, "region %llu unheld by %s (instance %s)",
         static_cast<unsigned long long>(regionId), holder.c_str(), instance.c_str());
    if (last && isDraining(regionId)) finishDrainingLocked(false);
    return 0;
}

int PoolManager::resetHolds(const std::string &holder, const std::string &instance,
                            uint32_t &dropped)
{
    dropped = 0;
    if (!validHoldName(holder) || !validHoldName(instance))
    {
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    const auto before = holds_;
    for (auto it = holds_.begin(); it != holds_.end();)
    {
        auto held = it->second.find(holder);
        if (held != it->second.end() && held->second != instance)
        {
            it->second.erase(held);
            ++dropped;
        }
        it = it->second.empty() ? holds_.erase(it) : std::next(it);
    }
    if (dropped == 0)
    {
        return 0;
    }
    // An unsynced save stands: a power loss only brings the holds back.
    int syncRc = 0;
    const int rc = saveHoldsLocked(syncRc);
    if (rc != 0)
    {
        holds_ = before;
        dropped = 0;
        return rc;
    }
    logf(LogLevel::Info, "dropped %u hold(s) of %s left by an earlier instance", dropped,
         holder.c_str());
    finishDrainingLocked(false);
    return 0;
}

int PoolManager::adminDropHolds(const std::string &holder, bool confirmed, std::string &out)
{
    if (!validHoldName(holder))
    {
        out = "invalid holder: " + holder + "\n";
        return -EINVAL;
    }
    std::lock_guard<std::mutex> lock(mu_);
    std::string list;
    size_t count = 0;
    for (const auto &[regionId, holders] : holds_)
    {
        auto held = holders.find(holder);
        if (held == holders.end()) continue;
        list += "region=" + std::to_string(regionId) + " holder=" + holder +
                " instance=" + held->second + (isDraining(regionId) ? " draining" : "") + "\n";
        ++count;
    }
    if (count == 0)
    {
        out = "holder " + holder + " holds no region\n";
        return 0;
    }
    if (!confirmed)
    {
        out = list + "these regions stay out of use while held. Verify that the agent of node " +
              holder + " is gone for good (its NIC registrations with it), then pass --confirm\n";
        return -EPERM;
    }
    const auto before = holds_;
    for (auto it = holds_.begin(); it != holds_.end();)
    {
        it->second.erase(holder);
        it = it->second.empty() ? holds_.erase(it) : std::next(it);
    }
    int syncRc = 0;
    const int rc = saveHoldsLocked(syncRc);
    if (rc != 0)
    {
        holds_ = before;
        out = "cannot save the holds: " + std::string(std::strerror(-rc)) + "\n";
        return rc;
    }
    const size_t freed = finishDrainingLocked(false);
    logf(LogLevel::Warn, "admin: dropped %zu hold(s) of %s; %zu draining region(s) released",
         count, holder.c_str(), freed);
    out = list + "dropped holder=" + holder + " holds=" + std::to_string(count) +
          " released=" + std::to_string(freed) + "\n";
    // Dropping stands; a power loss only brings the holds back.
    if (syncRc != 0)
        out += "WARNING: syncing " + stateDir_ + " failed (" + std::strerror(-syncRc) +
               "); after a power loss the holds may return: run this again\n";
    return 0;
}

}  // namespace maru
