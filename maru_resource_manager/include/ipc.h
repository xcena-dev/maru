#pragma once

#include <cstdint>

#include "types.h"

namespace maru {

static constexpr uint32_t kMagic = 0x4D415255; // 'MARU'
static constexpr uint16_t kVersion = 2;

/// Memory access type — LOCAL for device path mmap, REMOTE for future multi-node.
enum class AccessType : uint32_t {
  LOCAL  = 0,   // client opens device path directly + mmap (current)
  REMOTE = 1,   // reserved: RDMA or software-based remote memory access
};

enum class MsgType : uint16_t {
  ALLOC_REQ = 1,
  ALLOC_RESP = 2,
  FREE_REQ = 3,
  FREE_RESP = 4,
  STATS_REQ = 5,
  STATS_RESP = 6,
  GET_ACCESS_REQ = 9,
  GET_ACCESS_RESP = 10,
  NODE_REGISTER_REQ = 11,
  NODE_REGISTER_RESP = 12,
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
  uint32_t daxPathLen;
  uint32_t reserved;
};

struct AllocResp {
  int32_t status;
  uint32_t accessType{0}; // AccessType: 0=LOCAL (device path), 1=REMOTE
  Handle handle;
  uint64_t requestedSize;
};

struct FreeReq {
  Handle handle;
};

struct FreeResp {
  int32_t status;
};

struct GetAccessReq {
  Handle handle;
};

struct GetAccessResp {
  int32_t status;
  uint32_t pathLen;
  // Followed by: path bytes (pathLen), offset(u64), length(u64)
};

struct StatsRespHeader {
  uint32_t numPools;
};

struct NodeRegisterResp {
  int32_t status;
  uint32_t matched;
  uint32_t total;
};

struct ErrorResp {
  int32_t status;
  uint32_t msgLen;
};

// Wire format uses little-endian byte order. On x86_64 this matches native
// layout, so structs can be memcpy'd directly. If porting to a big-endian
// architecture, all multi-byte fields must be byte-swapped before send/recv.
//
// Wire-format size assertions — must match Python maru_shm.ipc
static_assert(sizeof(MsgHeader) == 12, "MsgHeader must be 12 bytes");
static_assert(sizeof(AllocReq) == 16, "AllocReq must be 16 bytes");
static_assert(sizeof(AllocResp) == 48, "AllocResp must be 48 bytes");
static_assert(sizeof(FreeReq) == 32, "FreeReq must be 32 bytes (Handle)");
static_assert(sizeof(FreeResp) == 4, "FreeResp must be 4 bytes");
static_assert(sizeof(GetAccessReq) == 32, "GetAccessReq must be 32 bytes (Handle)");
static_assert(sizeof(GetAccessResp) == 8, "GetAccessResp must be 8 bytes (fixed part)");
static_assert(sizeof(StatsRespHeader) == 4, "StatsRespHeader must be 4 bytes");
static_assert(sizeof(NodeRegisterResp) == 12, "NodeRegisterResp must be 12 bytes");
static_assert(sizeof(ErrorResp) == 8, "ErrorResp must be 8 bytes");

} // namespace maru
