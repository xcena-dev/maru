// Recovery: decide, per device, whether its pool is served as recorded,
// started over (reset), refused or skipped, and carry that out. The
// allocation ledger (global.meta + WAL) is the only authority; free lists are
// derived from it and the device range. See docs/source/design_doc/
// maru_resource_manager.md, "Persistence & Recovery".
#include "pool_recovery.h"

#include <signal.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <set>

#include "device_exclusion.h"
#include "log.h"
#include "metadata.h"
#include "pool_validation.h"
#include "server_config.h"
#include "util.h"
#include "wal.h"

namespace maru
{

namespace
{

Uuid toUuid(const uint8_t *bytes)
{
    Uuid u{};
    std::memcpy(u.data(), bytes, u.size());
    return u;
}

bool parseUuid(const std::string &text, Uuid &out)
{
    if (text.size() != 36) return false;
    size_t n = 0;
    for (size_t i = 0; i < text.size(); ++i)
    {
        if (i == 8 || i == 13 || i == 18 || i == 23)
        {
            if (text[i] != '-') return false;
            continue;
        }
        const char c = text[i];
        int v = c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10
                                                 : c >= 'A' && c <= 'F' ? c - 'A' + 10
                                                                        : -1;
        if (v < 0) return false;
        if (n % 2 == 0) out[n / 2] = static_cast<uint8_t>(v << 4);
        else out[n / 2] |= static_cast<uint8_t>(v);
        ++n;
    }
    return n == 32;
}

std::string uuidText(const Uuid &u) { return uuidToString(u.data()); }

PoolState layoutOf(const DeviceProbe &p)
{
    PoolState s{};
    s.poolId = p.poolId;
    s.devPath = p.devPath;
    s.type = p.type;
    s.alignBytes = p.align;
    s.dataBegin = p.dataBegin;
    s.dataEnd = p.dataEnd;
    s.totalSize = p.dataEnd - p.dataBegin;
    s.freeSize = s.totalSize;
    s.freeList = {{s.dataBegin, s.totalSize}};
    return s;
}

uint64_t sumOf(const std::vector<Extent> &list)
{
    uint64_t sum = 0;
    for (const auto &e : list) sum += e.length;
    return sum;
}

// Every allocation lies in the data range at a valid offset, none overlap.
bool allocationsFit(const PoolState &layout, const std::vector<Allocation> &allocs)
{
    std::vector<Extent> used;
    for (const auto &a : allocs)
    {
        if (!validAllocation(layout, a)) return false;
        used.push_back({a.realOffset, a.allocLength});
    }
    std::sort(used.begin(), used.end(),
              [](const Extent &a, const Extent &b) { return a.offset < b.offset; });
    for (size_t i = 1; i < used.size(); ++i)
        if (used[i - 1].offset + used[i - 1].length > used[i].offset) return false;
    return true;
}

}  // namespace

bool DeviceProbe::sameIdentity(const DeviceProbe &o) const
{
    return rc == o.rc && size == o.size && align == o.align && header == o.header &&
           headerUuid == o.headerUuid && tags == o.tags &&
           (tags != TagState::Consistent || tagUuid == o.tagUuid);
}

struct PoolManager::DecideContext
{
    // include: the device the mode applies to (the others are judged too).
    std::string target;
    IncludeMode mode{IncludeMode::Judge};
    bool wasExcluded{false};
    bool startup{false};
    // Header UUID -> devices carrying it (this pass and served pools).
    std::map<Uuid, std::vector<std::string>> headerPaths;
    // Version 7 record UUID -> pool.
    std::map<Uuid, uint32_t> recordPools;
    // Devices per pool ID in this scan, excluded ones not counted.
    std::map<uint32_t, int> poolIdDevices;
    std::set<uint32_t> servedPools;
};

std::vector<Allocation> PoolManager::poolAllocations(uint32_t poolId) const
{
    std::vector<Allocation> out;
    for (const auto &[id, a] : allocations_)
        if (a.poolId == poolId) out.push_back(a);
    return out;
}

void PoolManager::decide(const DeviceProbe &probe, const PoolRecord &record,
                         const DecideContext &ctx, DevicePlan &plan) const
{
    plan = DevicePlan{};
    plan.probe = probe;
    const bool target = probe.devPath == ctx.target;
    const IncludeMode mode = target ? ctx.mode : IncludeMode::Judge;
    const bool wasExcluded = target && ctx.wasExcluded;
    auto refuse = [&](std::string why) {
        plan.action = DevicePlan::Action::Refuse;
        plan.reason = std::move(why);
    };
    auto skip = [&](std::string why) {
        plan.action = DevicePlan::Action::Skip;
        plan.reason = std::move(why);
    };
    if (probe.rc != 0)
        return skip(probe.stage + ": " + std::strerror(-probe.rc));
    if (record.kind == PoolRecord::Kind::IoError)
        return skip(std::string("cannot read pool record: ") + std::strerror(-record.error));

    const uint32_t poolId = probe.poolId;
    const bool dev = probe.type == DaxType::DEV_DAX;
    const auto live = poolAllocations(poolId);
    std::vector<uint64_t> gone;
    size_t alive = 0;
    // A held or draining region may still be written by a remote agent.
    for (const auto &a : live)
    {
        const uint64_t id = a.handle.regionId;
        if (ownerGone(a) && !isHeld(id) && !isDraining(id)) gone.push_back(id);
        else ++alive;
    }
    plan.live = live.size();
    plan.ownersAlive = alive;

    if (probe.dataEnd <= probe.dataBegin)
        return refuse("device layout: size " + std::to_string(probe.size) +
                      " is not larger than its alignment " + std::to_string(probe.align));
    const auto devices = ctx.poolIdDevices.find(poolId);
    if ((devices != ctx.poolIdDevices.end() && devices->second > 1) ||
        (!ctx.startup && ctx.servedPools.count(poolId)))
        return refuse("pool ID " + std::to_string(poolId) +
                      " collision: another device has the same region number");

    const PoolState layout = layoutOf(probe);
    auto otherDeviceHas = [&](const Uuid &u) -> std::string {
        auto it = ctx.headerPaths.find(u);
        if (it == ctx.headerPaths.end()) return "";
        for (const auto &path : it->second)
            if (path != probe.devPath) return path;
        return "";
    };
    auto claimedByOtherPool = [&](const Uuid &u) -> int64_t {
        auto it = ctx.recordPools.find(u);
        if (it == ctx.recordPools.end() || it->second == poolId) return -1;
        return static_cast<int64_t>(it->second);
    };

    if (mode == IncludeMode::Reinit)
    {
        // A header recorded for pool M means this device holds M's data.
        if (dev && probe.header == DeviceProbe::Header::Ours)
        {
            const int64_t m = claimedByOtherPool(probe.headerUuid);
            if (m >= 0)
            {
                size_t mAlive = 0;
                for (const auto &a : poolAllocations(static_cast<uint32_t>(m)))
                    if (!ownerGone(a) || isHeld(a.handle.regionId) ||
                        isDraining(a.handle.regionId))
                        ++mAlive;
                if (mAlive)
                    return refuse("the device holds pool " + std::to_string(m) + "'s data with " +
                                  std::to_string(mAlive) + " live allocation(s); discard pool " +
                                  std::to_string(m) + " first");
            }
        }
        if (alive)
            return refuse(std::to_string(alive) +
                          " allocation(s) may still be in use; 'maru-rm-ctl discard' them first");
        plan.action = DevicePlan::Action::Reset;
        plan.reason = "reinitialized by maru-rm-ctl";
        plan.newUuid = dev;
        plan.writeHeader = plan.writeTags = dev;
        plan.writeRecord = true;
        plan.archive = record.kind != PoolRecord::Kind::Absent;
        plan.frees = gone;
        return;
    }

    auto header = probe.header;
    Uuid headerUuid = probe.headerUuid;
    if (mode == IncludeMode::RestoreHeader)
    {
        if (!dev || record.kind != PoolRecord::Kind::V7)
            return refuse("--restore-header needs a version 7 pool record");
        if (header == DeviceProbe::Header::Ours)
        {
            const int64_t m = claimedByOtherPool(headerUuid);
            if (m >= 0)
                return refuse("the header holds pool " + std::to_string(m) + "'s UUID");
        }
        else if (header != DeviceProbe::Header::None)
        {
            return refuse("the device does not hold a Maru header");
        }
        const std::string elsewhere = otherDeviceHas(record.uuid);
        if (!elsewhere.empty())
            return refuse("UUID " + uuidText(record.uuid) + " is on " + elsewhere);
        if (probe.tags != TagState::Consistent || probe.tagUuid != record.uuid)
            return refuse("identity tags do not match UUID " + uuidText(record.uuid) +
                          "; the device contents changed");
        header = DeviceProbe::Header::Ours;
        headerUuid = record.uuid;
        plan.writeHeader = true;
    }

    if (dev && header == DeviceProbe::Header::Roof)
        return skip("ROOF filesystem on device, not registering it");
    if (dev && header == DeviceProbe::Header::Ours)
    {
        const std::string elsewhere = otherDeviceHas(headerUuid);
        if (!elsewhere.empty())
            return refuse("UUID " + uuidText(headerUuid) + " is also on " + elsewhere);
    }
    if (dev && header == DeviceProbe::Header::BadVersion)
        return refuse("unknown Maru header version");
    if (record.kind == PoolRecord::Kind::Newer)
        return refuse("pool record written by a newer resource manager");

    enum class Identity { New, Same, Lost } identity = Identity::Same;
    if (dev && header == DeviceProbe::Header::Ours)
    {
        if (record.kind == PoolRecord::Kind::V7 && record.uuid != headerUuid)
        {
            const int64_t m = claimedByOtherPool(headerUuid);
            if (m >= 0)
                return refuse("header UUID " + uuidText(headerUuid) + " belongs to pool " +
                              std::to_string(m) + " (dax numbering changed?)");
            return refuse("header UUID " + uuidText(headerUuid) + " differs from recorded " +
                          uuidText(record.uuid) +
                          " (if an include --reinit was interrupted, run it again)");
        }
        if (record.kind != PoolRecord::Kind::V7)
        {
            const int64_t m = claimedByOtherPool(headerUuid);
            if (m >= 0)
                return refuse("header UUID " + uuidText(headerUuid) + " belongs to pool " +
                              std::to_string(m) + " (dax numbering changed?)");
            if (wasExcluded && mode != IncludeMode::Adopt)
                return refuse("the excluded device has a Maru header but no identity record; "
                              "include it with --adopt to accept the header, or --reinit");
        }
    }
    else if (dev)
    {
        if (record.kind == PoolRecord::Kind::V7)
        {
            const std::string elsewhere = otherDeviceHas(record.uuid);
            if (!elsewhere.empty())
                return refuse("recorded UUID " + uuidText(record.uuid) + " is on " + elsewhere);
        }
        identity = record.kind != PoolRecord::Kind::Absent || !live.empty() ? Identity::Lost
                                                                            : Identity::New;
    }
    if (identity == Identity::New)
    {
        plan.action = DevicePlan::Action::Serve;
        plan.reason = "new device";
        plan.newUuid = true;
        plan.writeHeader = plan.writeTags = plan.writeRecord = true;
        return;
    }
    if (identity == Identity::Lost)
    {
        if (alive)
            return refuse("device header lost while " + std::to_string(alive) +
                          " allocation(s) may still be in use");
        plan.action = DevicePlan::Action::Reset;
        plan.reason = "device header lost";
        plan.newUuid = true;
        plan.writeHeader = plan.writeTags = plan.writeRecord = true;
        plan.archive = record.kind != PoolRecord::Kind::Absent;
        plan.frees = gone;
        return;
    }

    if (dev) plan.uuid = headerUuid;
    bool confirmed = false;
    std::string why;
    switch (record.kind)
    {
    case PoolRecord::Kind::V7:
        if (record.type != probe.type || record.dataBegin != probe.dataBegin ||
            record.alignBytes != probe.align ||
            (dev && record.dataEnd != probe.dataEnd))
        {
            why = "device layout changed (recorded size " + std::to_string(record.dataEnd) +
                  ", alignment " + std::to_string(record.alignBytes) + "; device size " +
                  std::to_string(probe.dataEnd) + ", alignment " + std::to_string(probe.align) + ")";
        }
        else if (dev && (probe.tags != TagState::Consistent || probe.tagUuid != headerUuid) &&
                 !plan.writeHeader)
        {
            why = "identity tags do not match: the device mapping or contents changed";
        }
        else
        {
            confirmed = true;
        }
        break;
    case PoolRecord::Kind::V6:  // No identity or layout recorded: as if absent.
    case PoolRecord::Kind::Absent:
        confirmed = true;
        break;
    default:
        why = "pool record is damaged";
        break;
    }
    const bool fits = allocationsFit(layout, live);
    if (confirmed && fits)
    {
        plan.action = DevicePlan::Action::Serve;
        plan.reason = record.kind == PoolRecord::Kind::V7 ? "recovered" : "recovered and recorded";
        plan.writeTags = dev && record.kind != PoolRecord::Kind::V7;
        plan.writeRecord = record.kind != PoolRecord::Kind::V7 ||
                           (!dev && record.dataEnd != probe.dataEnd);
        return;
    }
    if (confirmed) why = "allocations do not fit the device range";
    if (alive)
        return refuse(why + "; " + std::to_string(alive) + " allocation(s) may still be in use");
    plan.action = DevicePlan::Action::Reset;
    plan.reason = why;
    plan.writeTags = dev;
    plan.writeRecord = true;
    plan.archive = record.kind != PoolRecord::Kind::Absent;
    plan.frees = gone;
}

std::vector<DevicePlan> PoolManager::probeAndDecide(std::unique_lock<std::mutex> &lock,
                                                    const std::vector<DeviceInfo> &devices,
                                                    IncludeMode mode, bool wasExcluded,
                                                    bool startup, const std::string &target)
{
    DecideContext ctx;
    ctx.target = target;
    ctx.mode = mode;
    ctx.wasExcluded = wasExcluded;
    ctx.startup = startup;
    std::vector<DeviceInfo> candidates;
    const auto now = SteadyClock::now();
    for (const auto &dev : devices)
    {
        if (isExcluded(dev.devPath)) continue;
        ++ctx.poolIdDevices[dev.poolId];
        if (findPoolByPath(dev.devPath)) continue;
        // include reads every device, so that it sees the duplicates a scan
        // would, but its caller acts only on the target. A rescan leaves a
        // device alone while it waits to retry a write.
        const bool other = !target.empty() && dev.devPath != target;
        auto retry = retryAfter_.find(dev.devPath);
        if (target.empty() && retry != retryAfter_.end() && now < retry->second.first)
            continue;
        if (!slots_->tryAcquire(dev.devPath))
        {
            if (other) continue;
            auto &u = unserved_[dev.devPath];
            if (u.state != DeviceState::Busy)
                logf(LogLevel::Warn, "%s is busy (an operation on it has not finished)",
                     dev.devPath.c_str());
            u = UnservedDevice{dev.poolId, dev.type, DeviceState::Busy,
                               "an operation on the device has not finished", 0, 0};
            continue;
        }
        candidates.push_back(dev);
    }
    for (const auto &p : pools_)
    {
        ctx.servedPools.insert(p.poolId);
        Uuid u{};
        if (p.type == DaxType::DEV_DAX && parseUuid(p.deviceUuid, u))
            ctx.headerPaths[u].push_back(p.devPath);
    }
    const uint64_t defaultAlign = alignBytes_;
    const int timeoutMs = deviceIoTimeoutMs_;
    auto slots = slots_;

    // Device and state-file reads, without the lock. Every device is read at
    // once, so devices that do not answer cost one timeout in total.
    lock.unlock();
    std::vector<std::unique_ptr<DeviceIo<DeviceProbe>>> reads;
    for (const auto &dev : candidates)
        reads.push_back(std::make_unique<DeviceIo<DeviceProbe>>(
            slots, dev.devPath, false, [dev, defaultAlign] {
                return probeDeviceIo(dev.poolId, dev.devPath, dev.type, defaultAlign);
            }));
    const auto deadline = SteadyClock::now() + std::chrono::milliseconds(timeoutMs);
    std::vector<DeviceProbe> probes(candidates.size());
    std::vector<bool> answered(candidates.size(), false);
    for (size_t i = 0; i < candidates.size(); ++i) answered[i] = reads[i]->wait(deadline, probes[i]);
    std::vector<uint32_t> ids;
    const int listRc = metadata_->listRecordIds(ids);
    std::map<uint32_t, PoolRecord> records;
    for (uint32_t id : ids) metadata_->loadRecord(id, records[id]);
    lock.lock();

    for (const auto &[id, rec] : records)
        if (rec.kind == PoolRecord::Kind::V7) ctx.recordPools[rec.uuid] = id;
    for (size_t i = 0; i < candidates.size(); ++i)
        if (answered[i] && probes[i].rc == 0 && probes[i].header == DeviceProbe::Header::Ours)
            ctx.headerPaths[probes[i].headerUuid].push_back(probes[i].devPath);

    std::vector<DevicePlan> plans(candidates.size());
    for (size_t i = 0; i < candidates.size(); ++i)
    {
        DevicePlan &plan = plans[i];
        if (!answered[i])
        {
            plan.probe.poolId = candidates[i].poolId;
            plan.probe.devPath = candidates[i].devPath;
            plan.probe.type = candidates[i].type;
            plan.action = DevicePlan::Action::Skip;
            plan.timedOut = true;
            plan.reason = "device not responding";
            continue;
        }
        reportProbe(probes[i]);
        if (listRc != 0)
        {
            // Records that cannot be listed are unknown, not absent.
            plan.probe = probes[i];
            plan.action = DevicePlan::Action::Skip;
            plan.reason = "cannot list pool records in " + stateDir_ + ": " +
                          std::strerror(-listRc);
            continue;
        }
        PoolRecord none;
        auto rec = records.find(candidates[i].poolId);
        decide(probes[i], rec != records.end() ? rec->second : none, ctx, plan);
    }
    return plans;
}

void PoolManager::markUnserved(const DevicePlan &plan)
{
    const DeviceState state = plan.timedOut ? DeviceState::Busy
                              : plan.action == DevicePlan::Action::Refuse ? DeviceState::Refused
                                                                          : DeviceState::Skipped;
    auto it = unserved_.find(plan.probe.devPath);
    const bool changed = it == unserved_.end() || it->second.state != state ||
                         it->second.reason != plan.reason;
    unserved_[plan.probe.devPath] = UnservedDevice{plan.probe.poolId, plan.probe.type, state,
                                                   plan.reason, plan.live, plan.ownersAlive};
    if (!changed) return;
    if (state == DeviceState::Refused)
        logf(LogLevel::Error,
             "%s (pool %u) refused: %s; %zu allocation(s) recorded, %zu may still be in use "
             "(maru-rm-ctl allocations %s; include, include --reinit or discard)",
             plan.probe.devPath.c_str(), plan.probe.poolId, plan.reason.c_str(), plan.live,
             plan.ownersAlive, baseNameOf(plan.probe.devPath).c_str());
    else
        logf(LogLevel::Warn, "%s (pool %u) %s: %s", plan.probe.devPath.c_str(),
             plan.probe.poolId, state == DeviceState::Busy ? "busy" : "skipped",
             plan.reason.c_str());
}

void PoolManager::rebuildFreeList(PoolState &pool) const
{
    std::vector<Extent> used;
    for (const auto &[id, a] : allocations_)
        if (a.poolId == pool.poolId) used.push_back({a.realOffset, a.allocLength});
    std::sort(used.begin(), used.end(),
              [](const Extent &a, const Extent &b) { return a.offset < b.offset; });
    pool.freeList.clear();
    uint64_t cursor = pool.dataBegin;
    for (const auto &e : used)
    {
        if (e.offset > cursor) pool.freeList.push_back({cursor, e.offset - cursor});
        cursor = std::max(cursor, e.offset + e.length);
    }
    if (cursor < pool.dataEnd) pool.freeList.push_back({cursor, pool.dataEnd - cursor});
    pool.freeSize = sumOf(pool.freeList);
}

void PoolManager::publishPlan(DevicePlan &plan)
{
    PoolState pool = layoutOf(plan.probe);
    if (pool.type == DaxType::DEV_DAX) pool.deviceUuid = uuidText(plan.uuid);
    rebuildFreeList(pool);
    pools_.push_back(pool);
    unserved_.erase(pool.devPath);
    retryAfter_.erase(pool.devPath);
    archivedResets_.erase(pool.poolId);
    if (plan.action == DevicePlan::Action::Reset)
        logf(LogLevel::Warn,
             "%s (pool %u) started over: %s; %zu allocation(s) of exited owners released, "
             "UUID=%s%s",
             pool.devPath.c_str(), pool.poolId, plan.reason.c_str(), plan.frees.size(),
             pool.deviceUuid.c_str(), plan.archived.empty() ? "" : (", previous record kept at " + plan.archived).c_str());
    else
        logf(LogLevel::Info, "%s (pool %u) %s: total=%llu free=%llu UUID=%s", pool.devPath.c_str(),
             pool.poolId, plan.reason.c_str(), static_cast<unsigned long long>(pool.totalSize),
             static_cast<unsigned long long>(pool.freeSize), pool.deviceUuid.c_str());
    removeOrphanFsDaxFiles(pools_.back());
}

int PoolManager::executePlans(std::unique_lock<std::mutex> &lock, std::vector<DevicePlan> &plans)
{
    int firstError = 0;
    const int timeoutMs = deviceIoTimeoutMs_;
    auto slots = slots_;
    for (auto &plan : plans)
    {
        const std::string path = plan.probe.devPath;
        auto fail = [&](const std::string &why, int rc) {
            plan.action = DevicePlan::Action::Refuse;
            plan.reason = why + ": " + std::strerror(-rc);
            if (!firstError) firstError = rc;
            auto &retry = retryAfter_[path];
            retry.second = retry.second ? std::min(retry.second * 2, 600) : 10;
            retry.first = SteadyClock::now() + std::chrono::seconds(retry.second);
            markUnserved(plan);
        };
        // Another operation (include, rescan) may have served this pool ID,
        // or this UUID, from another device while the lock was released.
        auto servedElsewhere = [&]() {
            const PoolState *other = findPoolById(plan.probe.poolId);
            std::string why;
            if (other)
                why = "pool ID " + std::to_string(plan.probe.poolId) +
                      " collision: already served by " + other->devPath;
            const std::string uuid = uuidText(plan.uuid);
            for (const auto &p : pools_)
                if (!plan.newUuid && plan.probe.type == DaxType::DEV_DAX &&
                    p.type == DaxType::DEV_DAX && p.deviceUuid == uuid)
                    why = "UUID " + uuid + " is also on " + p.devPath;
            if (why.empty()) return false;
            plan.action = DevicePlan::Action::Refuse;
            plan.reason = why;
            markUnserved(plan);
            slots->release(path);
            return true;
        };
        if (plan.action == DevicePlan::Action::Refuse || plan.action == DevicePlan::Action::Skip)
        {
            markUnserved(plan);
            if (!plan.timedOut) slots->release(path);
            continue;
        }
        if (servedElsewhere()) continue;
        // Exited owners' allocations leave the ledger before anything else.
        if (plan.action == DevicePlan::Action::Reset)
        {
            // The lock was released since decide() (an earlier plan's device
            // I/O): a region held meanwhile must not be released, and the
            // pool must not start over under it.
            std::vector<uint64_t> frees;
            bool held = false;
            for (uint64_t id : plan.frees)
            {
                if (!allocations_.count(id)) continue;
                held |= isHeld(id) || isDraining(id);
                frees.push_back(id);
            }
            if (held)
            {
                plan.action = DevicePlan::Action::Skip;
                plan.reason = "an allocation was held while the device was being recovered; "
                              "judged again at the next scan";
                markUnserved(plan);
                slots->release(path);
                continue;
            }
            if (!frees.empty())
            {
                bool written = false;
                const int rc = wal_->appendFrees(frees, &written);
                if (rc != 0 && !written)
                {
                    fail("cannot log the release of exited owners' allocations", rc);
                    slots->release(path);
                    continue;
                }
                for (uint64_t id : frees) allocations_.erase(id);
                recountClients();
            }
        }
        // Read the device again: something outside the RM may have changed it.
        DeviceProbe again;
        const DeviceProbe first = plan.probe;
        const uint64_t defaultAlign = alignBytes_;
        lock.unlock();
        bool answered = runDeviceIo<DeviceProbe>(
            slots, path, false, timeoutMs,
            [first, defaultAlign] {
                return probeDeviceIo(first.poolId, first.devPath, first.type, defaultAlign);
            },
            again);
        lock.lock();
        if (!answered)
        {
            plan.action = DevicePlan::Action::Skip;
            plan.timedOut = true;
            plan.reason = "device not responding";
            markUnserved(plan);
            continue;
        }
        if (!again.sameIdentity(first))
        {
            plan.action = DevicePlan::Action::Skip;
            plan.reason = "device changed while being recovered; judged again at the next scan";
            markUnserved(plan);
            slots->release(path);
            continue;
        }
        if (servedElsewhere()) continue;
        DeviceHeader header{};
        if (plan.newUuid)
        {
            initDeviceHeader(header);
            plan.uuid = toUuid(header.uuid);
        }
        else if (plan.writeHeader)
        {
            std::memcpy(header.magic, kDeviceHeaderMagic, sizeof(header.magic));
            header.version = kDeviceHeaderVersion;
            std::memcpy(header.uuid, plan.uuid.data(), plan.uuid.size());
        }
        // A reset records the new layout first: if the device writes do not
        // finish, the record and the device disagree, so the device is never
        // served under the old record. A lost header then starts over again
        // (its allocations were released above); an old header left by an
        // interrupted --reinit is refused until --reinit runs again.
        const bool recordFirst = plan.action == DevicePlan::Action::Reset;
        auto writeRecord = [&]() -> int {
            if (plan.archive && !archivedResets_.count(plan.probe.poolId))
            {
                const int rc = metadata_->archiveRecord(plan.probe.poolId, "reset", plan.archived);
                if (rc != 0) return rc;
                archivedResets_.insert(plan.probe.poolId);
            }
            return metadata_->saveRecord(plan.probe.poolId, plan.probe.type, plan.uuid,
                                         plan.probe.dataBegin, plan.probe.dataEnd,
                                         plan.probe.align);
        };
        if (recordFirst && plan.writeRecord)
        {
            const int rc = writeRecord();
            if (rc != 0)
            {
                fail("cannot write the pool record", rc);
                slots->release(path);
                continue;
            }
        }
        if (plan.writeHeader || plan.writeTags)
        {
            const bool withHeader = plan.writeHeader || plan.newUuid;
            const Uuid uuid = plan.uuid;
            const uint64_t mapSize = plan.probe.align;
            if (plan.writeTags && deviceTagAreaEnd(mapSize) < kMaxInterleavePeriod)
                logf(LogLevel::Warn,
                     "%s: alignment %llu leaves room for identity tags below one interleave "
                     "period (%llu bytes); a changed interleave member may go undetected",
                     path.c_str(), static_cast<unsigned long long>(mapSize),
                     static_cast<unsigned long long>(kMaxInterleavePeriod));
            int wrc = 0;
            lock.unlock();
            answered = runDeviceIo<int>(
                slots, path, true, timeoutMs,
                [path, mapSize, withHeader, header, uuid] {
                    return writeDeviceIdentity(path, mapSize, withHeader ? &header : nullptr,
                                               uuid.data());
                },
                wrc);
            lock.lock();
            if (!answered)
            {
                plan.action = DevicePlan::Action::Skip;
                plan.timedOut = true;
                plan.reason = "device not responding";
                markUnserved(plan);
                continue;
            }
            if (wrc != 0)
            {
                fail("cannot write the device identity", wrc);
                slots->release(path);
                continue;
            }
            if (servedElsewhere()) continue;
        }
        if (!recordFirst && plan.writeRecord)
        {
            const int rc = writeRecord();
            if (rc != 0)
            {
                fail("cannot write the pool record", rc);
                slots->release(path);
                continue;
            }
        }
        publishPlan(plan);
        slots->release(path);
    }
    return firstError;
}

void PoolManager::loadBootMarker()
{
    const std::string marker = readBootMarker(stateDir_);
    std::string now;
    std::ifstream in(bootIdPath_);
    std::getline(in, now);
    Uuid parsed{};
    bootIdNow_ = parseUuid(now, parsed) ? now : "";
    previousBoot_ = !bootIdNow_.empty() && parseUuid(marker, parsed) && marker != bootIdNow_;
}

int PoolManager::finishBootMarker()
{
    if (previousBoot_)
    {
        // Every local owner in the ledger ran in an earlier boot. A region a
        // remote agent holds drains instead: the agent outlived the reboot.
        std::vector<uint64_t> frees;
        size_t drains = 0;
        for (const auto &[id, a] : allocations_)
        {
            if (!isLocalClientId(a.clientId)) continue;
            if (!isHeld(id) && !isDraining(id)) frees.push_back(id);
            else if (draining_.insert(id).second) ++drains;
        }
        if (drains)
        {
            int syncRc = 0;
            const int rc = saveHoldsLocked(syncRc);
            if (rc != 0 || syncRc != 0) return rc != 0 ? rc : syncRc;
            logf(LogLevel::Warn,
                 "%zu held allocation(s) of local processes from an earlier boot drain until "
                 "unheld",
                 drains);
        }
        if (!frees.empty())
        {
            bool written = false;
            const int rc = wal_->appendFrees(frees, &written);
            if (rc != 0 && !written)
            {
                logf(LogLevel::Error, "cannot log the release of earlier-boot allocations: %d", rc);
                return rc;
            }
            for (uint64_t id : frees) allocations_.erase(id);
            logf(LogLevel::Warn,
                 "released %zu allocation(s) of local processes from an earlier boot",
                 frees.size());
        }
        previousBoot_ = false;
    }
    const int rc = writeBootMarker(stateDir_, bootIdNow_);
    if (rc != 0)
        logf(LogLevel::Error, "cannot record the boot ID in %s/rm.conf: %s", stateDir_.c_str(),
             std::strerror(-rc));
    return rc;
}

int PoolManager::loadPoolsLocked(std::unique_lock<std::mutex> &lock)
{
    setCheckpointBlock("recovery has not completed");
    // Before any device is opened: an unreadable list must not mean "none".
    const int excludeRc = loadExcludedDevices(stateDir_, excluded_);
    if (excludeRc != 0)
    {
        logf(LogLevel::Error, "cannot read excluded device list in %s: %d (%s)",
             stateDir_.c_str(), excludeRc, std::strerror(-excludeRc));
        return excludeRc;
    }
    for (const auto &name : excluded_)
        logf(LogLevel::Info, "device /dev/%s is excluded (maru-rm-ctl include to use it)",
             name.c_str());
    // A previous RM may have renamed the list into place without syncing it,
    // and that state lived only in its memory. Confirm it again so no
    // excluded device is handed over while a power loss could still undo it.
    excludedSyncRc_ = excluded_.empty() ? 0 : syncExcludedDevices(stateDir_);
    if (excludedSyncRc_ != 0)
        logf(LogLevel::Warn, "the excluded device list in %s is not durable yet: %s",
             stateDir_.c_str(), std::strerror(-excludedSyncRc_));

    std::map<uint64_t, Allocation> global;
    uint64_t globalNext = 1;
    const int globalRc = metadata_->loadGlobal(global, globalNext);
    const bool globalMissing = globalRc == -ENOENT;
    if (globalRc != 0 && !globalMissing)
    {
        logf(LogLevel::Error, "cannot restore global metadata: %d", globalRc);
        return globalRc;
    }
    if (globalMissing)
    {
        global.clear();
        globalNext = 1;
        // Pool records without global.meta: accept only a log that reaches
        // back to region 1 (a crash in the first checkpoint). Otherwise
        // global.meta may have been deleted with allocations in it.
        std::vector<uint32_t> ids;
        const int listRc = metadata_->listRecordIds(ids);
        if (listRc != 0)
        {
            logf(LogLevel::Error, "cannot list pool records in %s: %s", stateDir_.c_str(),
                 std::strerror(-listRc));
            return listRc;
        }
        if (!ids.empty() && !wal_->startsAtFirstRegion())
        {
            logf(LogLevel::Error,
                 "%s has pool records but no global.meta, and the WAL does not reach back to "
                 "region 1: allocation records may be lost. If global.meta held no allocations, "
                 "move pool_*.meta aside and restart; otherwise restore global.meta from a backup",
                 stateDir_.c_str());
            return -EUCLEAN;
        }
    }
    Ledger ledger;
    int rc = wal_->buildLedger(global, globalNext, ledger);
    if (rc != 0)
    {
        logf(LogLevel::Error, "cannot read WAL: %d", rc);
        return rc;
    }
    allocations_ = std::move(ledger.live);
    nextRegionId_ = ledger.nextRegionId;
    // Before any decision: a held allocation is never taken for an exited one.
    size_t staleHolds = 0;
    rc = loadHoldsLocked(staleHolds);
    if (rc != 0) return rc;

    loadBootMarker();
    std::vector<DeviceInfo> devices;
    rc = scanDevices(devices);
    if (rc != 0) return rc;
    auto plans = probeAndDecide(lock, devices, IncludeMode::Judge, false, true);

    // Every complete record passed; never append after an incomplete one.
    rc = wal_->discardTornTail();
    if (rc != 0)
    {
        logf(LogLevel::Error, "cannot discard incomplete WAL tail: %d", rc);
        return rc;
    }
    rc = finishBootMarker();
    if (rc != 0) return rc;
    // Initialize the persisted secret before recomputing restored tokens.
    rc = initSecret(stateDir_, !allocations_.empty());
    if (rc != 0) return rc;
    for (auto &[regionId, alloc] : allocations_)
        alloc.handle.authToken =
            computeAuthToken(alloc.handle, alloc.nonce, std::string(alloc.clientId));
    recountClients();

    // Create global.meta before any pool record, so a record never exists
    // without it. It is empty: the WAL is kept, and replaying it onto an
    // empty checkpoint gives the same ledger as replaying it onto none.
    if (globalMissing)
    {
        rc = metadata_->saveGlobal({}, 1);
        if (rc != 0)
        {
            logf(LogLevel::Error, "cannot create global.meta: %d", rc);
            return rc;
        }
        // Pool records are written with a directory sync, which also makes
        // this rename durable before any of them.
        if ((rc = syncDirectory(stateDir_)) != 0)
            logf(LogLevel::Warn, "cannot sync %s after creating global.meta: %d",
                 stateDir_.c_str(), rc);
    }
    executePlans(lock, plans);
    // The ledger holds every pool's allocations, served or not.
    setCheckpointBlock("");
    // Holds dropped while the RM was down (or a release that failed) leave
    // draining regions without holds.
    finishDrainingLocked(staleHolds != 0);
    return 0;
}

int PoolManager::loadPools()
{
    std::unique_lock<std::mutex> lock(mu_);
    return loadPoolsLocked(lock);
}

int PoolManager::rescanDevices()
{
    std::unique_lock<std::mutex> lock(mu_);
    std::vector<DeviceInfo> devices;
    const int rc = scanDevices(devices);
    if (rc != 0) return rc;
    // Forget the state of devices that are gone.
    std::set<std::string> present;
    for (const auto &dev : devices) present.insert(dev.devPath);
    for (auto it = unserved_.begin(); it != unserved_.end();)
        it = present.count(it->first) ? std::next(it) : unserved_.erase(it);
    bool candidate = false;
    for (const auto &dev : devices)
        if (!isExcluded(dev.devPath) && !findPoolByPath(dev.devPath)) candidate = true;
    if (!candidate) return 0;
    auto plans = probeAndDecide(lock, devices, IncludeMode::Judge, false, false);
    executePlans(lock, plans);
    return 0;
}

}  // namespace maru
