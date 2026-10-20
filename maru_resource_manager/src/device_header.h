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

/// Read header from a DEV_DAX device via mmap.
/// mapSize must be >= device alignment (typically 2MB for DEV_DAX).
/// Returns 0 on success, -ENODATA if magic doesn't match (not initialized),
/// or -errno on I/O error.
int readDeviceHeader(const std::string &devPath, DeviceHeader &out,
                     uint64_t mapSize);

/// Write header to a DEV_DAX device via mmap.
/// mapSize must be >= device alignment (typically 2MB for DEV_DAX).
/// Returns 0 on success, -errno on error.
int writeDeviceHeader(const std::string &devPath, const DeviceHeader &hdr,
                      uint64_t mapSize);

/// Generate a new UUID v4 and populate all header fields.
void initDeviceHeader(DeviceHeader &hdr);

/// Identity tags fill the reserved area after the header, one every
/// kDeviceTagStride bytes up to min(mapSize, kDeviceTagAreaLimit). Each holds
/// the device UUID and its own offset. A region whose interleave members
/// changed (swapped, reordered, moved, or one of them reset) no longer has a
/// consistent set, even when its header and size are unchanged: one
/// interleave period is at most 16 KiB x 16 ways, well inside the area.
static constexpr char kDeviceTagMagic[8] = "MARUTAG";
static constexpr uint64_t kDeviceTagStride = 256;
static constexpr uint64_t kDeviceTagAreaLimit = 2ULL << 20;
/// The largest interleave period (16 KiB x 16 ways). A smaller tag area
/// (alignment below this) checks only some interleave members.
static constexpr uint64_t kMaxInterleavePeriod = 256ULL << 10;

struct DeviceTag {
    char magic[8];    // "MARUTAG\0"
    uint8_t uuid[16];
    uint64_t offset;  // Device offset of this tag
};

static_assert(sizeof(DeviceTag) == 32, "DeviceTag must be 32 bytes");

enum class TagState {
    Absent,        // No tag anywhere in the area
    Consistent,    // Every tag present, one UUID, each at its own offset
    Inconsistent,  // Anything else
};

struct DeviceIdentity {
    int headerRc{0};  // 0, or -ENODATA if the magic does not match
    DeviceHeader header{};
    TagState tags{TagState::Absent};
    uint8_t tagUuid[16]{};  // Valid when tags == Consistent
};

/// End of the tag area for a mapping of mapSize bytes.
uint64_t deviceTagAreaEnd(uint64_t mapSize);

/// Read the header and the identity tags through one mapping of mapSize bytes.
/// Returns 0 (header and tag state in out) or -errno if the device cannot be
/// mapped.
int readDeviceIdentity(const std::string &devPath, uint64_t mapSize,
                       DeviceIdentity &out);

/// Write identity tags for uuid and, when hdr is not null, the header, through
/// one mapping of mapSize bytes. Returns 0 or -errno.
int writeDeviceIdentity(const std::string &devPath, uint64_t mapSize,
                        const DeviceHeader *hdr, const uint8_t uuid[16]);

/// Format UUID bytes as "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
std::string uuidToString(const uint8_t uuid[16]);

} // namespace maru
