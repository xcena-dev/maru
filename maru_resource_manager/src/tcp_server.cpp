#include "tcp_server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "ipc.h"
#include "log.h"
#include "util.h"

namespace maru {

TcpServer::TcpServer(PoolManager &pm, const std::string &host, uint16_t port,
                     int numWorkers)
    : pm_(pm), handler_(pm), host_(host), port_(port),
      numWorkers_(numWorkers) {}

TcpServer::~TcpServer() { stop(); }

int TcpServer::start() {
    int rc = initSecret(pm_.stateDir(), pm_.hasExistingAllocations());
    if (rc != 0) {
        logf(LogLevel::Error, "Failed to init secret: %d", rc);
        return rc;
    }

    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return -errno;
    }

    int enable = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (host_ == "0.0.0.0" || host_.empty()) {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) != 1) {
            ::close(fd);
            return -EINVAL;
        }
    }

    if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        int err = -errno;
        ::close(fd);
        return err;
    }
    if (::listen(fd, 64) != 0) {
        int err = -errno;
        ::close(fd);
        return err;
    }

    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    listenFd_ = fd;
    stop_ = false;

    // Start worker threads
    for (int i = 0; i < numWorkers_; ++i) {
        workers_.emplace_back(&TcpServer::workerLoop, this);
    }

    // Start accept thread
    acceptTh_ = std::thread(&TcpServer::acceptLoop, this);

    logf(LogLevel::Debug, "TcpServer: %d worker threads started", numWorkers_);
    return 0;
}

void TcpServer::stop() {
    stop_ = true;

    // Wake up accept thread
    if (listenFd_ >= 0) {
        ::shutdown(listenFd_, SHUT_RDWR);
        ::close(listenFd_);
        listenFd_ = -1;
    }
    if (acceptTh_.joinable()) {
        acceptTh_.join();
    }

    // Wake up all workers and join
    queueCv_.notify_all();
    for (auto &w : workers_) {
        if (w.joinable()) {
            w.join();
        }
    }
    workers_.clear();

    // Drain any remaining fds in the queue
    std::lock_guard<std::mutex> lk(queueMutex_);
    while (!taskQueue_.empty()) {
        ::close(taskQueue_.front());
        taskQueue_.pop();
    }
}

// =============================================================================
// Serialization helpers (static, no state)
// =============================================================================

static int sendError(int fd, int32_t status, const std::string &msg) {
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

static std::vector<uint8_t> serializeAllocResp(const AllocResp &resp,
                                                const std::string &path) {
    uint32_t pathLen = path.size();
    size_t totalSize = sizeof(resp) + sizeof(pathLen) + pathLen;
    std::vector<uint8_t> buf(totalSize);

    size_t off = 0;
    std::memcpy(buf.data() + off, &resp, sizeof(resp));
    off += sizeof(resp);
    std::memcpy(buf.data() + off, &pathLen, sizeof(pathLen));
    off += sizeof(pathLen);
    if (pathLen > 0) {
        std::memcpy(buf.data() + off, path.data(), pathLen);
    }
    return buf;
}

static std::vector<uint8_t> serializeGetAccessResp(
    int32_t status, const std::string &path,
    uint64_t offset, uint64_t length) {
    uint32_t pathLen = path.size();
    GetAccessResp resp{};
    resp.status = status;
    resp.pathLen = pathLen;

    size_t totalSize = sizeof(resp) + pathLen + sizeof(offset) + sizeof(length);
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
    return buf;
}

static void sendSimpleResp(int fd, MsgType type, const void *resp,
                            size_t respSize) {
    MsgHeader rh{};
    rh.magic = kMagic;
    rh.version = kVersion;
    rh.type = static_cast<uint16_t>(type);
    rh.payloadLen = respSize;
    writeFull(fd, &rh, sizeof(rh));
    writeFull(fd, resp, respSize);
}

/// Parse client_id string from payload after fixed-size request struct.
static std::string parseClientId(const std::vector<uint8_t> &payload,
                                  size_t fixedSize) {
    if (payload.size() <= fixedSize + 2) {
        return "";
    }
    uint16_t idLen = 0;
    std::memcpy(&idLen, payload.data() + fixedSize, sizeof(idLen));
    if (idLen == 0 || fixedSize + 2 + idLen > payload.size()) {
        return "";
    }
    return std::string(
        reinterpret_cast<const char *>(payload.data() + fixedSize + 2), idLen);
}

// =============================================================================
// Accept loop — pushes fds to task queue
// =============================================================================

void TcpServer::acceptLoop() {
    while (!stop_) {
        int cfd = ::accept(listenFd_, nullptr, nullptr);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            if (errno == EBADF || errno == ENOTSOCK) break;
            break;
        }

        int flag = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        if (activeClients_.load() >= kMaxClients) {
            logf(LogLevel::Warn,
                 "TcpServer: max clients reached (%d), rejecting", kMaxClients);
            ::close(cfd);
            continue;
        }

        {
            std::lock_guard<std::mutex> lk(queueMutex_);
            taskQueue_.push(cfd);
        }
        queueCv_.notify_one();
    }
}

// =============================================================================
// Worker loop — pops fds from task queue, handles persistent connections
// =============================================================================

void TcpServer::workerLoop() {
    while (true) {
        int cfd = -1;
        {
            std::unique_lock<std::mutex> lk(queueMutex_);
            queueCv_.wait(lk, [this] {
                return stop_ || !taskQueue_.empty();
            });
            if (stop_ && taskQueue_.empty()) {
                return;
            }
            cfd = taskQueue_.front();
            taskQueue_.pop();
        }

        handleClient(cfd);
    }
}

// =============================================================================
// Client session — loops over requests on persistent connection
// =============================================================================

void TcpServer::handleClient(int clientFd) {
    activeClients_.fetch_add(1);
    struct ClientGuard {
        std::atomic<int> &counter;
        ~ClientGuard() { counter.fetch_sub(1); }
    } guard{activeClients_};

    // Set a read timeout so workers don't block forever on idle connections
    struct timeval tv{};
    tv.tv_sec = 60;  // 60s idle timeout per connection
    tv.tv_usec = 0;
    setsockopt(clientFd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // Persistent connection: handle multiple requests until EOF or error
    while (!stop_) {
        if (!handleOneRequest(clientFd)) {
            break;
        }
    }

    ::close(clientFd);
}

bool TcpServer::handleOneRequest(int clientFd) {
    MsgHeader hdr{};
    int rc = readFull(clientFd, &hdr, sizeof(hdr));
    if (rc != 0) {
        return false;  // EOF or read error — close connection
    }

    if (hdr.magic != kMagic || hdr.version != kVersion) {
        sendError(clientFd, -EPROTO, "bad header");
        return false;
    }

    constexpr uint32_t maxPayloadLen = 8 * 1024;
    if (hdr.payloadLen > maxPayloadLen) {
        sendError(clientFd, -EMSGSIZE, "payload too large");
        return false;
    }

    std::vector<uint8_t> payload(hdr.payloadLen);
    if (hdr.payloadLen > 0) {
        rc = readFull(clientFd, payload.data(), hdr.payloadLen);
        if (rc != 0) {
            return false;
        }
    }

    MsgType type = static_cast<MsgType>(hdr.type);

    if (type == MsgType::ALLOC_REQ) {
        if (payload.size() < sizeof(AllocReq)) {
            sendError(clientFd, -EPROTO, "bad alloc req");
            return false;
        }
        AllocReq req{};
        std::memcpy(&req, payload.data(), sizeof(req));

        std::string cid = parseClientId(payload, sizeof(AllocReq));
        RequestContext ctx{cid};

        auto result = handler_.handleAlloc(req, ctx);

        auto respPayload = serializeAllocResp(result.resp, result.devicePath);
        MsgHeader rh{};
        rh.magic = kMagic;
        rh.version = kVersion;
        rh.type = static_cast<uint16_t>(MsgType::ALLOC_RESP);
        rh.payloadLen = respPayload.size();
        writeFull(clientFd, &rh, sizeof(rh));
        writeFull(clientFd, respPayload.data(), respPayload.size());
    } else if (type == MsgType::FREE_REQ) {
        if (payload.size() < sizeof(FreeReq)) {
            sendError(clientFd, -EPROTO, "bad free req");
            return false;
        }
        FreeReq req{};
        std::memcpy(&req, payload.data(), sizeof(req));

        std::string cid = parseClientId(payload, sizeof(FreeReq));
        RequestContext ctx{cid};

        auto result = handler_.handleFree(req, ctx);
        if (result.resp.status == -EACCES) {
            sendError(clientFd, -EACCES, "invalid authToken");
            return false;
        }
        sendSimpleResp(clientFd, MsgType::FREE_RESP,
                        &result.resp, sizeof(result.resp));
    } else if (type == MsgType::STATS_REQ) {
        auto result = handler_.handleStats();
        MsgHeader rh{};
        rh.magic = kMagic;
        rh.version = kVersion;
        rh.type = static_cast<uint16_t>(MsgType::STATS_RESP);
        rh.payloadLen = result.payload.size();
        writeFull(clientFd, &rh, sizeof(rh));
        if (!result.payload.empty()) {
            writeFull(clientFd, result.payload.data(), result.payload.size());
        }
    } else if (type == MsgType::GET_ACCESS_REQ) {
        if (payload.size() < sizeof(GetAccessReq)) {
            sendError(clientFd, -EPROTO, "bad get_access req");
            return false;
        }
        GetAccessReq req{};
        std::memcpy(&req, payload.data(), sizeof(req));

        RequestContext ctx{""};
        auto result = handler_.handleGetAccess(req, ctx);
        if (result.status == -EACCES) {
            sendError(clientFd, -EACCES, "invalid auth token");
            return false;
        }

        auto respPayload = serializeGetAccessResp(
            result.status, result.devicePath, result.offset, result.length);
        MsgHeader rh{};
        rh.magic = kMagic;
        rh.version = kVersion;
        rh.type = static_cast<uint16_t>(MsgType::GET_ACCESS_RESP);
        rh.payloadLen = respPayload.size();
        writeFull(clientFd, &rh, sizeof(rh));
        writeFull(clientFd, respPayload.data(), respPayload.size());
    } else if (type == MsgType::REGISTER_SERVER_REQ) {
        std::string cid = parseClientId(payload, 0);
        RequestContext ctx{cid};
        auto result = handler_.handleRegisterServer(ctx);
        sendSimpleResp(clientFd, MsgType::REGISTER_SERVER_RESP,
                        &result.resp, sizeof(result.resp));
    } else if (type == MsgType::UNREGISTER_SERVER_REQ) {
        std::string cid = parseClientId(payload, 0);
        RequestContext ctx{cid};
        auto result = handler_.handleUnregisterServer(ctx);
        sendSimpleResp(clientFd, MsgType::UNREGISTER_SERVER_RESP,
                        &result.resp, sizeof(result.resp));
    } else {
        sendError(clientFd, -ENOSYS, "unknown request");
        return false;
    }

    return true;  // Connection stays open for next request
}

}  // namespace maru
