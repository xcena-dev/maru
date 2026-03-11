#include "request_handler.h"

#include <fcntl.h>
#include <unistd.h>

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

    if (status == 0) {
        result.daxFd = ::open(devPath.c_str(), O_RDWR | O_CLOEXEC);
        if (result.daxFd < 0) {
            pm_.free(handle, 0);
            result.resp.status = -errno;
            result.resp.handle = Handle{};
        }
    }

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

GetFdResult RequestHandler::handleGetFd(const GetFdReq &req,
                                        const RequestContext &ctx) {
    (void)ctx;  // Currently unused, may be needed for access control later
    GetFdResult result;

    if (!pm_.verifyAuthToken(req.handle)) {
        result.resp.status = -EACCES;
        return result;
    }

    std::string pathToOpen;
    int status = pm_.getPathForHandle(req.handle, pathToOpen);

    if (status == 0) {
        result.daxFd = ::open(pathToOpen.c_str(), O_RDWR | O_CLOEXEC);
        if (result.daxFd < 0) {
            result.resp.status = -errno;
        }
    } else {
        result.resp.status = status;
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

}  // namespace maru
