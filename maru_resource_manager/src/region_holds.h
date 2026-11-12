#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>

namespace maru {

/// Region ID -> holder -> instance. A holder is a remote agent's node ID (one
/// agent per node); its instance changes every time that agent starts.
using RegionHolds = std::map<uint64_t, std::map<std::string, std::string>>;

/// Holder and instance names: 1..128 printable ASCII bytes, no spaces.
bool validHoldName(const std::string &name);

/// Holds and draining regions (freed while held), persisted in
/// <stateDir>/holds as "hold <region> <holder> <instance>" and
/// "draining <region>" lines. A missing file means none; a malformed one is
/// -EINVAL, never an empty set: losing a hold lets a late remote write land
/// in the region's next owner.
int loadRegionHolds(const std::string &stateDir, RegionHolds &holds,
                    std::set<uint64_t> &draining);
/// Replace the file (temporary file, fsync, rename). Returns 0 once the new
/// records are in place, or -errno with the old file untouched. On success,
/// `syncRc` is 0 once the state directory is synced, else -errno: a power
/// loss can bring the old file back until a later save syncs.
int saveRegionHolds(const std::string &stateDir, const RegionHolds &holds,
                    const std::set<uint64_t> &draining, int &syncRc);

} // namespace maru
