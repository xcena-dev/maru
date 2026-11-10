#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>

namespace maru {

/// One hold. A remote agent's HOLD has a holder (its node ID) and an
/// instance (its epoch) only. A worker's NODE_HOLD (instance == holder, a
/// worker incarnation) also records who it is, so that the RM can tell when
/// that process is gone; empty or zero fields are unknown.
struct HoldRecord {
    std::string instance;
    std::string nodeEpoch;  // The node's run the hold was made under
    std::string host;
    std::string bootId;
    uint32_t pid{0};
    uint64_t pidStart{0};  // Field 22 of /proc/<pid>/stat
    uint64_t pidNs{0};     // Inode of /proc/<pid>/ns/pid

    bool worker() const { return !nodeEpoch.empty(); }
    bool operator==(const HoldRecord &o) const {
        return instance == o.instance && nodeEpoch == o.nodeEpoch && host == o.host &&
               bootId == o.bootId && pid == o.pid && pidStart == o.pidStart && pidNs == o.pidNs;
    }
};

/// Region ID -> holder -> hold. A remote agent's holder is its node ID (one
/// agent per node); its instance changes every time that agent starts.
using RegionHolds = std::map<uint64_t, std::map<std::string, HoldRecord>>;

/// Holder and instance names: 1..128 printable ASCII bytes, no spaces.
bool validHoldName(const std::string &name);

/// Holds and draining regions (freed while held), persisted in
/// <stateDir>/holds as "hold <region> <holder> <instance>" lines, with
/// " <node_epoch> <host> <boot_id|-> <pid> <pid_start> <pid_ns>" appended for
/// a worker's hold, and "draining <region>" lines. A missing file means none;
/// a malformed one is -EINVAL, never an empty set: losing a hold lets a late
/// remote write land in the region's next owner.
int loadRegionHolds(const std::string &stateDir, RegionHolds &holds,
                    std::set<uint64_t> &draining);
/// Replace the file (temporary file, fsync, rename). Returns 0 once the new
/// records are in place, or -errno with the old file untouched. On success,
/// `syncRc` is 0 once the state directory is synced, else -errno: a power
/// loss can bring the old file back until a later save syncs.
int saveRegionHolds(const std::string &stateDir, const RegionHolds &holds,
                    const std::set<uint64_t> &draining, int &syncRc);

} // namespace maru
