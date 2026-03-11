#pragma once

#include <cstdint>

#include "types.h"

namespace maru {

static constexpr uint32_t kMagic = 0x4D415255; // 'MARU'
static constexpr uint16_t kVersion = 1;

/// Memory access type — LOCAL for fd-based mmap, REMOTE for future multi-node.
enum class AccessType : uint32_t {
  LOCAL  = 0,   // fd via SCM_RIGHTS + mmap (current)
  REMOTE = 1,   // reserved: RDMA or software-based remote memory access
};

enum class MsgType : uint16_t {
  ALLOC_REQ = 1,
  ALLOC_RESP = 2,
  FREE_REQ = 3,
  FREE_RESP = 4,
  STATS_REQ = 5,
  STATS_RESP = 6,
  GET_FD_REQ = 9,
  GET_FD_RESP = 10,
  ERROR_RESP = 255
};

struct MsgHeader {
  uint32_t magic;
  uint16_t version;
  uint16_t type;
  uint32_t payloadLen;
};

struct AllocReq {
  uint64_t size;
  uint32_t poolId;
  uint32_t reserved;
};

struct AllocResp {
  int32_t status;
  uint32_t accessType{0}; // AccessType: 0=LOCAL (fd via SCM_RIGHTS), 1=REMOTE
  Handle handle;
  uint64_t requestedSize;
};

struct FreeReq {
  Handle handle;
};

struct FreeResp {
  int32_t status;
};

struct GetFdReq {
  Handle handle;
};

struct GetFdResp {
  int32_t status;
};

struct StatsRespHeader {
  uint32_t numPools;
};

struct ErrorResp {
  int32_t status;
  uint32_t msgLen;
};

// Wire-format size assertions — must match Python maru_shm.ipc
static_assert(sizeof(MsgHeader) == 12, "MsgHeader must be 12 bytes");
static_assert(sizeof(AllocReq) == 16, "AllocReq must be 16 bytes");
static_assert(sizeof(AllocResp) == 48, "AllocResp must be 48 bytes");
static_assert(sizeof(FreeReq) == 32, "FreeReq must be 32 bytes (Handle)");
static_assert(sizeof(FreeResp) == 4, "FreeResp must be 4 bytes");
static_assert(sizeof(GetFdReq) == 32, "GetFdReq must be 32 bytes (Handle)");
static_assert(sizeof(GetFdResp) == 4, "GetFdResp must be 4 bytes");
static_assert(sizeof(StatsRespHeader) == 4, "StatsRespHeader must be 4 bytes");
static_assert(sizeof(ErrorResp) == 8, "ErrorResp must be 8 bytes");

} // namespace maru
