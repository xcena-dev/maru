// Copyright 2026 XCENA Inc.
// Device UUID header for multi-node CXL device identification.

#pragma once

#include <cstdint>
#include <string>

namespace maru {

static constexpr char kDeviceHeaderMagic[8] = "MARUDEV";
static constexpr uint32_t kDeviceHeaderVersion = 1;
static constexpr size_t kDeviceHeaderSize = 32;

struct DeviceHeader {
    char magic[8];        // "MARUDEV\0"
    uint32_t version;     // 1
    uint8_t uuid[16];     // UUID v4 (128-bit, RFC 4122)
    uint32_t reserved;
};

static_assert(sizeof(DeviceHeader) == kDeviceHeaderSize,
              "DeviceHeader must be 32 bytes");

/// Read header from a DEV_DAX device via pread.
/// Returns 0 on success, -ENODATA if magic doesn't match (not initialized),
/// or -errno on I/O error.
int readDeviceHeader(const std::string &devPath, DeviceHeader &out);

/// Write header to a DEV_DAX device via pwrite.
/// Returns 0 on success, -errno on error.
int writeDeviceHeader(const std::string &devPath, const DeviceHeader &hdr);

/// Generate a new UUID v4 and populate all header fields.
void initDeviceHeader(DeviceHeader &hdr);

/// Format UUID bytes as "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
std::string uuidToString(const uint8_t uuid[16]);

} // namespace maru
