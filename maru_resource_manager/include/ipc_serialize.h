#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "ipc.h"
#include "util.h"

namespace maru {

/// Serialize an AllocResp + device path + device UUID into a single payload buffer.
inline std::vector<uint8_t> serializeAllocResp(const AllocResp &resp,
                                                const std::string &path,
                                                const std::string &deviceUuid = "") {
    uint32_t pathLen = path.size();
    uint16_t uuidLen = deviceUuid.size();
    size_t totalSize = sizeof(resp) + sizeof(pathLen) + pathLen
                       + sizeof(uuidLen) + uuidLen;
    std::vector<uint8_t> buf(totalSize);

    size_t off = 0;
    std::memcpy(buf.data() + off, &resp, sizeof(resp));
    off += sizeof(resp);
    std::memcpy(buf.data() + off, &pathLen, sizeof(pathLen));
    off += sizeof(pathLen);
    if (pathLen > 0) {
        std::memcpy(buf.data() + off, path.data(), pathLen);
        off += pathLen;
    }
    std::memcpy(buf.data() + off, &uuidLen, sizeof(uuidLen));
    off += sizeof(uuidLen);
    if (uuidLen > 0) {
        std::memcpy(buf.data() + off, deviceUuid.data(), uuidLen);
    }
    return buf;
}

/// Serialize a GetAccessResp + path + offset/length + UUID into a single payload buffer.
inline std::vector<uint8_t> serializeGetAccessResp(
    int32_t status, const std::string &path,
    uint64_t offset, uint64_t length,
    const std::string &deviceUuid = "") {
    uint32_t pathLen = path.size();
    uint16_t uuidLen = deviceUuid.size();
    GetAccessResp resp{};
    resp.status = status;
    resp.pathLen = pathLen;

    size_t totalSize = sizeof(resp) + pathLen + sizeof(offset) + sizeof(length)
                       + sizeof(uuidLen) + uuidLen;
    std::vector<uint8_t> buf(totalSize);

    size_t off = 0;
    std::memcpy(buf.data() + off, &resp, sizeof(resp));
    off += sizeof(resp);
    if (pathLen > 0) {
        std::memcpy(buf.data() + off, path.data(), pathLen);
        off += pathLen;
    }
    std::memcpy(buf.data() + off, &offset, sizeof(offset));
    off += sizeof(offset);
    std::memcpy(buf.data() + off, &length, sizeof(length));
    off += sizeof(length);
    std::memcpy(buf.data() + off, &uuidLen, sizeof(uuidLen));
    off += sizeof(uuidLen);
    if (uuidLen > 0) {
        std::memcpy(buf.data() + off, deviceUuid.data(), uuidLen);
    }
    return buf;
}

/// Send an error response. Returns 0 on success, -errno on write failure.
inline int sendError(int fd, int32_t status, const std::string &msg) {
    ErrorResp er{};
    er.status = status;
    er.msgLen = msg.size();

    MsgHeader hdr{};
    hdr.magic = kMagic;
    hdr.version = kVersion;
    hdr.type = static_cast<uint16_t>(MsgType::ERROR_RESP);
    hdr.payloadLen = sizeof(ErrorResp) + er.msgLen;

    int rc = writeFull(fd, &hdr, sizeof(hdr));
    if (rc != 0) return rc;
    rc = writeFull(fd, &er, sizeof(er));
    if (rc != 0) return rc;
    if (er.msgLen > 0) {
        rc = writeFull(fd, msg.data(), er.msgLen);
        if (rc != 0) return rc;
    }
    return 0;
}

/// Send a response with header + payload. Returns 0 on success, -errno on write failure.
inline int sendResp(int fd, MsgType type, const void *payload,
                    size_t payloadSize) {
    MsgHeader rh{};
    rh.magic = kMagic;
    rh.version = kVersion;
    rh.type = static_cast<uint16_t>(type);
    rh.payloadLen = payloadSize;
    int rc = writeFull(fd, &rh, sizeof(rh));
    if (rc != 0) return rc;
    if (payloadSize > 0) {
        rc = writeFull(fd, payload, payloadSize);
        if (rc != 0) return rc;
    }
    return 0;
}

}  // namespace maru
