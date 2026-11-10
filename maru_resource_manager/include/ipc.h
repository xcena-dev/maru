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
  // Region holds of remote agents. An RM without them answers -ENOSYS.
  HOLD_REQ = 11,
  HOLD_RESP = 12,
  UNHOLD_REQ = 13,
  UNHOLD_RESP = 14,
  RESET_HOLDS_REQ = 15,
  RESET_HOLDS_RESP = 16,
  // Node ownership (a StorageService owns regions as its node). An RM
  // without it answers -ENOSYS.
  NODE_CLAIM_REQ = 17,
  NODE_CLAIM_RESP = 18,
  NODE_ALLOC_REQ = 19,
  NODE_ALLOC_RESP = 20,
  NODE_FREE_REQ = 21,
  NODE_FREE_RESP = 22,
  NODE_RENEW_REQ = 23,
  NODE_RENEW_RESP = 24,
  NODE_RELEASE_REQ = 25,
  NODE_RELEASE_RESP = 26,
  NODE_HOLD_REQ = 27,
  NODE_HOLD_RESP = 28,
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

// A hold keeps a region's range out of use, even after its owner frees it,
// until every holder has unheld it. Each request below is followed by the
// holder bytes, the instance bytes, then client_id and request id as
// GET_ACCESS_REQ appends them.

/// Answered as GET_ACCESS (HOLD_RESP carries a GetAccessResp payload).
struct HoldReq {
  Handle handle;
  uint32_t holderLen;
  uint32_t instanceLen;
};

/// The handle proves the caller was given the region, as for HOLD_REQ.
struct UnholdReq {
  Handle handle;
  uint32_t holderLen;
  uint32_t instanceLen;
};

struct UnholdResp {
  int32_t status;
};

/// Drop every hold of the holder made by another instance. It carries no
/// handle: the RM trusts its network for it, as for ALLOC_REQ.
struct ResetHoldsReq {
  uint32_t holderLen;
  uint32_t instanceLen;
};

struct ResetHoldsResp {
  int32_t status;
  uint32_t dropped;
};

// Node requests. Each is followed by the node ID bytes, the epoch bytes (and
// for NODE_ALLOC the DAX path bytes), then client_id and request id as
// GET_ACCESS_REQ appends them. client_id is required: its host part binds the
// node, and with the request id it keys the retransmit cache.

/// NODE_CLAIM, NODE_RENEW and NODE_RELEASE.
struct NodeReq {
  uint32_t nodeIdLen;
  uint32_t epochLen;
};

/// Answered with an AllocResp payload (NODE_ALLOC_RESP), as ALLOC.
struct NodeAllocReq {
  uint64_t size;
  uint32_t nodeIdLen;
  uint32_t epochLen;
  uint32_t daxPathLen;
  uint32_t reserved;
};

/// Answered with a FreeResp payload (NODE_FREE_RESP).
struct NodeFreeReq {
  Handle handle;
  uint32_t nodeIdLen;
  uint32_t epochLen;
};

/// A worker maps a node's region. Followed by the node ID, epoch, holder
/// (the worker incarnation) and boot ID bytes. pidStart is field 22 of
/// /proc/<pid>/stat, pidNs the inode of /proc/<pid>/ns/pid. Answered with a
/// GetAccessResp payload (NODE_HOLD_RESP), as HOLD.
struct NodeHoldReq {
  Handle handle;
  uint32_t nodeIdLen;
  uint32_t epochLen;
  uint32_t holderLen;
  uint32_t bootIdLen;
  uint32_t pid;
  uint32_t reserved;
  uint64_t pidStart;
  uint64_t pidNs;
};

/// Followed by the RM ID bytes, then numRegions NodeRegionInfo entries.
struct NodeClaimResp {
  int32_t status;
  uint32_t rmIdLen;
  uint64_t leaseMs;
  uint32_t numRegions;
  uint32_t reserved;
};

/// Followed by the device path bytes, the device UUID bytes (both empty
/// while the region's pool is not served), then numHolders NodeHolderInfo.
struct NodeRegionInfo {
  Handle handle;
  uint64_t requestedSize;
  uint32_t pathLen;
  uint32_t uuidLen;
  uint32_t numHolders;
  uint32_t reserved;
};

/// One hold of the region. Followed by the holder, node epoch, host and boot
/// ID bytes; a remote agent's hold has only the holder.
struct NodeHolderInfo {
  uint32_t holderLen;
  uint32_t epochLen;
  uint32_t hostLen;
  uint32_t bootIdLen;
  uint32_t pid;
  uint32_t reserved;
  uint64_t pidStart;
  uint64_t pidNs;
};

struct NodeRenewResp {
  int32_t status;
  uint32_t reserved;
  uint64_t leaseMs;
};

struct NodeReleaseResp {
  int32_t status;
  uint32_t released;
};

struct StatsRespHeader {
  uint32_t numPools;
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
static_assert(sizeof(HoldReq) == 40, "HoldReq must be 40 bytes (fixed part)");
static_assert(sizeof(UnholdReq) == 40, "UnholdReq must be 40 bytes (fixed part)");
static_assert(sizeof(UnholdResp) == 4, "UnholdResp must be 4 bytes");
static_assert(sizeof(ResetHoldsReq) == 8, "ResetHoldsReq must be 8 bytes (fixed part)");
static_assert(sizeof(ResetHoldsResp) == 8, "ResetHoldsResp must be 8 bytes");
static_assert(sizeof(NodeReq) == 8, "NodeReq must be 8 bytes (fixed part)");
static_assert(sizeof(NodeAllocReq) == 24, "NodeAllocReq must be 24 bytes (fixed part)");
static_assert(sizeof(NodeFreeReq) == 40, "NodeFreeReq must be 40 bytes (fixed part)");
static_assert(sizeof(NodeClaimResp) == 24, "NodeClaimResp must be 24 bytes (fixed part)");
static_assert(sizeof(NodeHoldReq) == 72, "NodeHoldReq must be 72 bytes (fixed part)");
static_assert(sizeof(NodeRegionInfo) == 56, "NodeRegionInfo must be 56 bytes (fixed part)");
static_assert(sizeof(NodeHolderInfo) == 40, "NodeHolderInfo must be 40 bytes (fixed part)");
static_assert(sizeof(NodeRenewResp) == 16, "NodeRenewResp must be 16 bytes");
static_assert(sizeof(NodeReleaseResp) == 8, "NodeReleaseResp must be 8 bytes");
static_assert(sizeof(StatsRespHeader) == 4, "StatsRespHeader must be 4 bytes");
static_assert(sizeof(ErrorResp) == 8, "ErrorResp must be 8 bytes");

} // namespace maru
