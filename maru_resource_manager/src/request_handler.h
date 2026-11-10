#pragma once

#include <cstdint>
#include <string>
#include <sys/types.h>
#include <vector>

#include "ipc.h"
#include "pool_manager.h"

namespace maru {

/// Client identity extracted from the transport layer.
struct RequestContext {
    std::string client_id;  // "hostname:pid"
    std::string dax_path;  // empty = any pool
};

/// Result of handleAlloc — includes device path and UUID for client-side resolution.
struct AllocResult {
    AllocResp resp{};
    std::string devicePath;
    std::string deviceUuid;
};

/// Result of handleGetAccess — includes device path, UUID, offset, length.
struct GetAccessResult {
    int32_t status = 0;
    std::string devicePath;
    std::string deviceUuid;
    uint64_t offset = 0;
    uint64_t length = 0;
};

/// Result of handleStats — serialized payload bytes.
struct StatsResult {
    std::vector<uint8_t> payload;
};

/// Result of handleFree.
struct FreeResult {
    FreeResp resp{};
};

/// Result of makeError.
struct ErrorResult {
    int32_t status;
    std::string message;
};

/// Business logic handler — transport-independent.
///
/// Extracted from UdsServer::handleClient() so that future transports
/// (e.g. ZmqServer for multi-node) can reuse the same logic.
class RequestHandler {
public:
    explicit RequestHandler(PoolManager &pm);

    AllocResult handleAlloc(const AllocReq &req, const RequestContext &ctx);
    FreeResult handleFree(const FreeReq &req, const RequestContext &ctx);
    GetAccessResult handleGetAccess(const GetAccessReq &req, const RequestContext &ctx);
    StatsResult handleStats();
    /// Region holds of a remote agent: holder = node ID, instance = epoch.
    GetAccessResult handleHold(const HoldReq &req, const std::string &holder,
                               const std::string &instance);
    UnholdResp handleUnhold(const UnholdReq &req, const std::string &holder,
                            const std::string &instance);
    ResetHoldsResp handleResetHolds(const std::string &holder, const std::string &instance);
    /// Node ownership. clientId is the connection's "hostname:pid"; only its
    /// host part is used (NODE_CLAIM binds the node to it).
    std::vector<uint8_t> handleNodeClaim(const std::string &nodeId, const std::string &epoch,
                                         const std::string &clientId);
    AllocResult handleNodeAlloc(const NodeAllocReq &req, const std::string &nodeId,
                                const std::string &epoch, const std::string &daxPath);
    FreeResp handleNodeFree(const NodeFreeReq &req, const std::string &nodeId,
                            const std::string &epoch);
    NodeRenewResp handleNodeRenew(const std::string &nodeId, const std::string &epoch);
    NodeReleaseResp handleNodeRelease(const std::string &nodeId, const std::string &epoch);
    GetAccessResult handleNodeHold(const NodeHoldReq &req, const std::string &nodeId,
                                   const std::string &epoch, const std::string &holder,
                                   const std::string &bootId, const std::string &clientId);

private:
    PoolManager &pm_;
};

}  // namespace maru
