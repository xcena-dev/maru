#pragma once

#include <cstdint>
#include <string>
#include <sys/types.h>
#include <vector>

#include "ipc.h"
#include "pool_manager.h"

namespace maru {

/// Credentials extracted from the UDS transport layer.
struct RequestContext {
    pid_t pid;
    uid_t uid;
};

/// Result of handleAlloc — includes daxFd for SCM_RIGHTS passing.
struct AllocResult {
    AllocResp resp{};
    int daxFd = -1;  // >= 0 means transport should send via SCM_RIGHTS
};

/// Result of handleGetFd — includes daxFd for SCM_RIGHTS passing.
struct GetFdResult {
    GetFdResp resp{};
    int daxFd = -1;
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
    GetFdResult handleGetFd(const GetFdReq &req, const RequestContext &ctx);
    StatsResult handleStats();

private:
    PoolManager &pm_;
};

}  // namespace maru
