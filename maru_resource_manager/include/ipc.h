#pragma once

#include <cstdint>

#include "types.h"

namespace maru {

static constexpr uint32_t kMagic = 0x4D415255; // 'MARU'
static constexpr uint16_t kVersion = 1;

enum class MsgType : uint16_t {
  ALLOC_REQ = 1,
  ALLOC_RESP = 2,
  FREE_REQ = 3,
  FREE_RESP = 4,
  STATS_REQ = 5,
  STATS_RESP = 6,
  GET_FD_REQ = 9,
  GET_FD_RESP = 10,
  // marufs permission ioctls (region_id identifies the target)
  PERM_GRANT_REQ = 11,
  PERM_GRANT_RESP = 12,
  PERM_REVOKE_REQ = 13,
  PERM_REVOKE_RESP = 14,
  PERM_SET_DEFAULT_REQ = 15,
  PERM_SET_DEFAULT_RESP = 16,
  CHOWN_REQ = 17,
  CHOWN_RESP = 18,
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
  uint32_t poolType;
};

struct AllocResp {
  int32_t status;
  uint32_t _pad{0}; // explicit padding for Handle alignment
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

// marufs permission request (matches kernel marufs_perm_req layout)
struct PermGrantReq {
  uint64_t regionId;   // target region
  uint32_t nodeId;     // grantee node
  uint32_t pid;        // grantee PID
  uint32_t perms;      // permission flags (PERM_READ|PERM_WRITE|...)
  uint32_t reserved;
};

struct PermRevokeReq {
  uint64_t regionId;   // target region
  uint32_t nodeId;     // revokee node
  uint32_t pid;        // revokee PID
};

struct PermSetDefaultReq {
  uint64_t regionId;   // target region
  uint32_t perms;      // default permission flags
  uint32_t reserved;
};

struct ChownReq {
  uint64_t regionId;   // target region
};

struct PermResp {
  int32_t status;
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
