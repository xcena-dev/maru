#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace maru {

/** @brief DAX device/allocation type. */
enum class DaxType : uint32_t {
  DEV_DAX = 0, ///< Character device (/dev/daxX.Y)
  FS_DAX = 1,  ///< File-based DAX (mounted filesystem)
  MARUFS = 2,  ///< marufs kernel filesystem (CXL shared memory)
};

/** @brief Handle for an allocation within a pool-backed DAX device. */
struct Handle {
  /** @brief Global unique region identifier. */
  uint64_t regionId;
  /** @brief mmap offset (DEV_DAX: real offset, FS_DAX: 0). */
  uint64_t offset;
  /** @brief Allocation length in bytes (aligned size). */
  uint64_t length;
  /** @brief Authorization token for FD requests. */
  uint64_t authToken;
};

/** @brief Pool statistics and attributes. */
struct PoolInfo {
  /** @brief Pool identifier. */
  uint32_t poolId;
  /** @brief DAX device type (DEV_DAX, FS_DAX, or MARUFS). */
  DaxType type;
  /** @brief Total pool size in bytes. */
  uint64_t totalSize;
  /** @brief Free size in bytes. */
  uint64_t freeSize;
  /** @brief Alignment size in bytes used by the pool. */
  uint64_t alignBytes;
};

// Wire-format size assertions — must match Python maru_shm.types
static_assert(sizeof(Handle) == 32, "Handle must be 32 bytes");
static_assert(sizeof(PoolInfo) == 32, "PoolInfo must be 32 bytes");

} // namespace maru
