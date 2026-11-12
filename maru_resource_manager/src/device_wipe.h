#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>

namespace maru {

/// Zero [0, size) of a DEV_DAX device through MAP_SHARED mappings of `chunk`
/// bytes (a multiple of the device alignment), with stores that bypass this
/// host's cache. Checks `cancel` before each chunk and returns -ECANCELED,
/// leaving the device partly zeroed.
int wipeDevice(const std::string &devPath, uint64_t size, uint64_t chunk,
               const std::atomic<bool> *cancel = nullptr);

/// wipeDevice in a child process, so a fault on the mapping cannot take the
/// caller down: unbinding a device-dax device mid-wipe turns the next store
/// into SIGBUS (reported here as -EIO). Kills the child as soon as
/// `cancelled` returns true (-ECANCELED, device partly zeroed).
int wipeDeviceIsolated(const std::string &devPath, uint64_t size, uint64_t chunk,
                       const std::function<bool()> &cancelled = {});

} // namespace maru
