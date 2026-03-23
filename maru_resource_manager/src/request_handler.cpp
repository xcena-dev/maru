#include "request_handler.h"

#include <cstring>

#include "log.h"
#include "util.h"

namespace maru {

RequestHandler::RequestHandler(PoolManager &pm) : pm_(pm) {}

AllocResult RequestHandler::handleAlloc(const AllocReq &req,
                                        const RequestContext &ctx) {
    AllocResult result;
    Handle handle{};
    std::string devPath;
    uint64_t requestedSize = 0;
    int32_t status =
        pm_.alloc(req.size, ctx.pid, handle, devPath, req.poolId, requestedSize);

    result.resp.status = status;
    result.resp.handle = handle;
    result.resp.accessType = static_cast<uint32_t>(AccessType::LOCAL);
    result.resp.requestedSize = requestedSize;
    result.devicePath = devPath;

    return result;
}

FreeResult RequestHandler::handleFree(const FreeReq &req,
                                      const RequestContext &ctx) {
    FreeResult result;

    if (!pm_.verifyAuthToken(req.handle)) {
        result.resp.status = -EACCES;
        return result;
    }

    uint64_t ownerId = (ctx.uid == 0) ? 0 : static_cast<uint64_t>(ctx.pid);
    result.resp.status = pm_.free(req.handle, ownerId);
    return result;
}

GetAccessResult RequestHandler::handleGetAccess(const GetAccessReq &req,
                                                const RequestContext &ctx) {
    (void)ctx;
    GetAccessResult result;

    if (!pm_.verifyAuthToken(req.handle)) {
        result.status = -EACCES;
        return result;
    }

    std::string pathToOpen;
    int status = pm_.getPathForHandle(req.handle, pathToOpen);

    if (status == 0) {
        result.devicePath = pathToOpen;
        result.offset = req.handle.offset;
        result.length = req.handle.length;
    } else {
        result.status = status;
    }

    return result;
}

StatsResult RequestHandler::handleStats() {
    StatsResult result;

    std::vector<PoolState> pools;
    pm_.getStats(pools);

    StatsRespHeader sh{};
    sh.numPools = pools.size();

    result.payload.resize(sizeof(sh));
    std::memcpy(result.payload.data(), &sh, sizeof(sh));

    for (const auto &p : pools) {
        PoolInfo pi{};
        pi.poolId = p.poolId;
        pi.type = p.type;
        pi.totalSize = p.totalSize;
        pi.freeSize = p.freeSize;
        pi.alignBytes = p.alignBytes;

        size_t old = result.payload.size();
        result.payload.resize(old + sizeof(pi));
        std::memcpy(result.payload.data() + old, &pi, sizeof(pi));
    }

    return result;
}

RegisterServerResult RequestHandler::handleRegisterServer(
    const RequestContext &ctx) {
    RegisterServerResult result;
    pm_.registerServer(ctx.pid);
    result.resp.status = 0;
    return result;
}

UnregisterServerResult RequestHandler::handleUnregisterServer(
    const RequestContext &ctx) {
    UnregisterServerResult result;
    pm_.unregisterServer(ctx.pid);
    result.resp.status = 0;
    return result;
}

}  // namespace maru
