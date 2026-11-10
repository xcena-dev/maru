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
    std::string deviceUuid;
    uint64_t requestedSize = 0;
    int32_t status =
        pm_.alloc(req.size, ctx.client_id, handle, devPath, deviceUuid, ctx.dax_path, requestedSize);

    result.resp.status = status;
    result.resp.handle = handle;
    result.resp.accessType = static_cast<uint32_t>(AccessType::LOCAL);
    result.resp.requestedSize = requestedSize;

    if (status == 0) {
        result.devicePath = devPath;
        result.deviceUuid = deviceUuid;

        logf(LogLevel::Debug,
             "[ALLOC] client=%s, size=%llu, pool=%s -> region_id=%llu, path=%s, uuid=%s",
             ctx.client_id.c_str(),
             (unsigned long long)req.size,
             ctx.dax_path.c_str(),
             (unsigned long long)handle.regionId,
             result.devicePath.c_str(),
             result.deviceUuid.c_str());
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
                                                const RequestContext & /*ctx*/) {
    GetAccessResult result;

    std::string pathToOpen;
    int status = pm_.verifyAndGetPath(req.handle, pathToOpen);

    if (status == 0) {
        result.devicePath = pathToOpen;
        // Note: separate lock acquisition from verifyAndGetPath, but safe because
        // only the owning client calls GET_ACCESS on its own allocations.
        result.deviceUuid = pm_.getDeviceUuidForRegion(req.handle.regionId);
        result.offset = req.handle.offset;
        result.length = req.handle.length;
        logf(LogLevel::Debug,
             "[GET_ACCESS] region_id=%llu -> path=%s, uuid=%s",
             (unsigned long long)req.handle.regionId,
             result.devicePath.c_str(),
             result.deviceUuid.c_str());
    } else {
        result.status = status;
        logf(LogLevel::Warn,
             "[GET_ACCESS] region_id=%llu -> FAILED (status=%d)",
             (unsigned long long)req.handle.regionId,
             status);
    }

    return result;
}

GetAccessResult RequestHandler::handleHold(const HoldReq &req, const std::string &holder,
                                           const std::string &instance) {
    GetAccessResult result;
    std::string pathToOpen;
    result.status = pm_.hold(req.handle, holder, instance, pathToOpen);
    if (result.status == 0) {
        result.devicePath = pathToOpen;
        result.deviceUuid = pm_.getDeviceUuidForRegion(req.handle.regionId);
        result.offset = req.handle.offset;
        result.length = req.handle.length;
    } else {
        logf(LogLevel::Warn, "[HOLD] region_id=%llu holder=%s -> FAILED (status=%d)",
             (unsigned long long)req.handle.regionId, holder.c_str(), result.status);
    }
    return result;
}

UnholdResp RequestHandler::handleUnhold(const UnholdReq &req, const std::string &holder,
                                        const std::string &instance) {
    UnholdResp resp{};
    resp.status = pm_.unhold(req.handle, holder, instance);
    if (resp.status != 0) {
        logf(LogLevel::Warn, "[UNHOLD] region_id=%llu holder=%s -> FAILED (status=%d)",
             (unsigned long long)req.handle.regionId, holder.c_str(), resp.status);
    }
    return resp;
}

ResetHoldsResp RequestHandler::handleResetHolds(const std::string &holder,
                                                const std::string &instance) {
    ResetHoldsResp resp{};
    resp.status = pm_.resetHolds(holder, instance, resp.dropped);
    if (resp.status != 0) {
        logf(LogLevel::Warn, "[RESET_HOLDS] holder=%s -> FAILED (status=%d)", holder.c_str(),
             resp.status);
    }
    return resp;
}

static void appendBytes(std::vector<uint8_t> &buf, const void *data, size_t size) {
    const auto *p = static_cast<const uint8_t *>(data);
    buf.insert(buf.end(), p, p + size);
}

std::vector<uint8_t> RequestHandler::handleNodeClaim(const std::string &nodeId,
                                                     const std::string &epoch,
                                                     const std::string &clientId) {
    const size_t colon = clientId.rfind(':');
    const std::string host = colon == std::string::npos ? "" : clientId.substr(0, colon);
    std::vector<PoolManager::NodeRegion> regions;
    NodeClaimResp resp{};
    resp.status = pm_.nodeClaim(nodeId, epoch, host, regions);
    const std::string rmId = resp.status == 0 ? pm_.rmId() : "";
    resp.rmIdLen = static_cast<uint32_t>(rmId.size());
    resp.leaseMs = pm_.nodeLeaseMs();
    resp.numRegions = static_cast<uint32_t>(regions.size());
    if (resp.status != 0) {
        logf(LogLevel::Warn, "[NODE_CLAIM] node=%s epoch=%s client=%s -> FAILED (status=%d)",
             nodeId.c_str(), epoch.c_str(), clientId.c_str(), resp.status);
    }
    std::vector<uint8_t> buf;
    appendBytes(buf, &resp, sizeof(resp));
    appendBytes(buf, rmId.data(), rmId.size());
    for (const auto &r : regions) {
        NodeRegionInfo info{};
        info.handle = r.handle;
        info.requestedSize = r.requestedSize;
        info.pathLen = static_cast<uint32_t>(r.devPath.size());
        info.uuidLen = static_cast<uint32_t>(r.deviceUuid.size());
        appendBytes(buf, &info, sizeof(info));
        appendBytes(buf, r.devPath.data(), r.devPath.size());
        appendBytes(buf, r.deviceUuid.data(), r.deviceUuid.size());
    }
    return buf;
}

AllocResult RequestHandler::handleNodeAlloc(const NodeAllocReq &req, const std::string &nodeId,
                                            const std::string &epoch,
                                            const std::string &daxPath) {
    AllocResult result;
    PoolManager::NodeRegion region{};
    result.resp.status = pm_.nodeAlloc(nodeId, epoch, req.size, daxPath, region);
    result.resp.accessType = static_cast<uint32_t>(AccessType::LOCAL);
    if (result.resp.status == 0) {
        result.resp.handle = region.handle;
        result.resp.requestedSize = region.requestedSize;
        result.devicePath = region.devPath;
        result.deviceUuid = region.deviceUuid;
        logf(LogLevel::Debug, "[NODE_ALLOC] node=%s size=%llu pool=%s -> region_id=%llu",
             nodeId.c_str(), (unsigned long long)req.size, daxPath.c_str(),
             (unsigned long long)region.handle.regionId);
    } else {
        logf(LogLevel::Warn, "[NODE_ALLOC] node=%s size=%llu pool=%s -> FAILED (status=%d)",
             nodeId.c_str(), (unsigned long long)req.size, daxPath.c_str(),
             result.resp.status);
    }
    return result;
}

FreeResp RequestHandler::handleNodeFree(const NodeFreeReq &req, const std::string &nodeId,
                                        const std::string &epoch) {
    FreeResp resp{};
    resp.status = pm_.nodeFree(req.handle, nodeId, epoch);
    if (resp.status != 0) {
        logf(LogLevel::Warn, "[NODE_FREE] node=%s region_id=%llu -> FAILED (status=%d)",
             nodeId.c_str(), (unsigned long long)req.handle.regionId, resp.status);
    }
    return resp;
}

NodeRenewResp RequestHandler::handleNodeRenew(const std::string &nodeId,
                                              const std::string &epoch) {
    NodeRenewResp resp{};
    resp.status = pm_.nodeRenew(nodeId, epoch);
    resp.leaseMs = pm_.nodeLeaseMs();
    if (resp.status != 0) {
        logf(LogLevel::Warn, "[NODE_RENEW] node=%s epoch=%s -> FAILED (status=%d)",
             nodeId.c_str(), epoch.c_str(), resp.status);
    }
    return resp;
}

NodeReleaseResp RequestHandler::handleNodeRelease(const std::string &nodeId,
                                                  const std::string &epoch) {
    NodeReleaseResp resp{};
    resp.status = pm_.nodeRelease(nodeId, epoch, resp.released);
    if (resp.status != 0) {
        logf(LogLevel::Warn, "[NODE_RELEASE] node=%s epoch=%s -> FAILED (status=%d)",
             nodeId.c_str(), epoch.c_str(), resp.status);
    }
    return resp;
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
