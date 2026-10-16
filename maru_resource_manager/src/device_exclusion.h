#pragma once

#include <set>
#include <string>

namespace maru {

/// Normalize "daxN.M" or "/dev/daxN.M" to "daxN.M". False for anything else.
bool normalizeDaxName(const std::string &in, std::string &out);

/// Devices the RM must never open (e.g. handed to ROOF), persisted in
/// <stateDir>/excluded_devices and managed by maru-rm-ctl. A missing file
/// means none; a malformed one is -EINVAL, never an empty list.
int loadExcludedDevices(const std::string &stateDir, std::set<std::string> &out);
/// Replace the list. Returns 0 once the new list is in effect (renamed into
/// place), or -errno with the old list untouched. On success, `syncRc` says
/// whether the rename is durable: 0 once the state directory is synced, else
/// -errno, and a power loss can bring the old list back until
/// syncExcludedDevices() succeeds.
int saveExcludedDevices(const std::string &stateDir,
                        const std::set<std::string> &names, int &syncRc);
/// Sync the state directory so the list last renamed into place survives a
/// power loss. Returns 0 or -errno.
int syncExcludedDevices(const std::string &stateDir);

} // namespace maru
