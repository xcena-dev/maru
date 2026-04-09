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
        pm_.alloc(req.size, ctx.client_id, handle, devPath, ctx.dax_path, requestedSize);

    result.resp.status = status;
    result.resp.handle = handle;
    result.resp.accessType = static_cast<uint32_t>(AccessType::LOCAL);
    result.resp.requestedSize = requestedSize;

    if (status == 0) {
        // Resolve device path for the client's node (multi-node support)
        std::string resolved = pm_.resolveDevicePath(handle.regionId, ctx.client_id);
        result.devicePath = resolved.empty() ? devPath : resolved;

        logf(LogLevel::Debug,
             "[ALLOC] client=%s, size=%llu, pool=%s -> region_id=%llu, path=%s",
             ctx.client_id.c_str(),
             (unsigned long long)req.size,
             ctx.dax_path.c_str(),
             (unsigned long long)handle.regionId,
             result.devicePath.c_str());
    } else {
        logf(LogLevel::Warn,
             "[ALLOC] client=%s, size=%llu, pool=%s -> FAILED (status=%d)",
             ctx.client_id.c_str(),
             (unsigned long long)req.size,
             ctx.dax_path.c_str(),
             status);
    }

    return result;
}

FreeResult RequestHandler::handleFree(const FreeReq &req,
                                      const RequestContext &ctx) {
    FreeResult result;
    result.resp.status = pm_.verifyAndFree(req.handle, ctx.client_id);

    if (result.resp.status == 0) {
        logf(LogLevel::Debug,
             "[FREE] client=%s, region_id=%llu",
             ctx.client_id.c_str(),
             (unsigned long long)req.handle.regionId);
    } else {
        logf(LogLevel::Warn,
             "[FREE] client=%s, region_id=%llu -> FAILED (status=%d)",
             ctx.client_id.c_str(),
             (unsigned long long)req.handle.regionId,
             result.resp.status);
    }

    return result;
}

GetAccessResult RequestHandler::handleGetAccess(const GetAccessReq &req,
                                                const RequestContext &ctx) {
    GetAccessResult result;

    std::string pathToOpen;
    int status = pm_.verifyAndGetPath(req.handle, pathToOpen);

    if (status == 0) {
        // Resolve device path for the client's node (multi-node support)
        std::string resolved = pm_.resolveDevicePath(req.handle.regionId, ctx.client_id);
        result.devicePath = resolved.empty() ? pathToOpen : resolved;
        result.offset = req.handle.offset;
        result.length = req.handle.length;
        logf(LogLevel::Debug,
             "[GET_ACCESS] region_id=%llu -> path=%s",
             (unsigned long long)req.handle.regionId,
             result.devicePath.c_str());
    } else {
        result.status = status;
        logf(LogLevel::Warn,
             "[GET_ACCESS] region_id=%llu -> FAILED (status=%d)",
             (unsigned long long)req.handle.regionId,
             status);
    }

    return result;
}

NodeRegisterResult RequestHandler::handleNodeRegister(
    const PoolManager::NodeList &nodes) {
    NodeRegisterResult result;
    int matched = pm_.registerNodes(nodes);
    uint32_t totalDevices = 0;
    for (const auto &[nodeId, mappings] : nodes) {
        totalDevices += static_cast<uint32_t>(mappings.size());
    }
    result.resp.status = 0;
    result.resp.matched = static_cast<uint32_t>(matched);
    result.resp.total = totalDevices;
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
        pi.devPathLen = static_cast<uint32_t>(p.devPath.size());
        pi.type = p.type;
        pi.totalSize = p.totalSize;
        pi.freeSize = p.freeSize;
        pi.alignBytes = p.alignBytes;

        size_t old = result.payload.size();
        result.payload.resize(old + sizeof(pi));
        std::memcpy(result.payload.data() + old, &pi, sizeof(pi));
        // Append device path bytes immediately after the fixed struct
        result.payload.insert(result.payload.end(), p.devPath.begin(), p.devPath.end());
    }

    return result;
}

}  // namespace maru
