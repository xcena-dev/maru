#include "tcp_server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "ipc.h"
#include "ipc_serialize.h"
#include "log.h"
#include "util.h"

namespace maru {

TcpServer::TcpServer(PoolManager &pm, const std::string &host, uint16_t port,
                     int numWorkers, int maxClients)
    : pm_(pm), handler_(pm), host_(host), port_(port),
      numWorkers_(numWorkers), maxClients_(maxClients) {}

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
        logf(LogLevel::Warn,
             "Binding to 0.0.0.0 — server is accessible from ALL network "
             "interfaces. Auth tokens and device paths are sent in PLAINTEXT. "
             "Use 127.0.0.1 for local-only access or an encrypted tunnel "
             "(WireGuard, SSH) for multi-node deployments.");
    } else if (host_ != "127.0.0.1" && host_ != "localhost") {
        logf(LogLevel::Warn,
             "Binding to non-loopback address '%s' — traffic including auth "
             "tokens will be sent in PLAINTEXT. Use an encrypted tunnel for "
             "production multi-node deployments.",
             host_.c_str());
    }
    if (host_ != "0.0.0.0" && !host_.empty()) {
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

    // Create epoll instance for I/O multiplexing
    epollFd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epollFd_ < 0) {
        int err = -errno;
        ::close(fd);
        return err;
    }

    // Monitor listen socket (level-triggered, persistent — not ONESHOT)
    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
        int err = -errno;
        ::close(fd);
        ::close(epollFd_);
        epollFd_ = -1;
        return err;
    }

    listenFd_ = fd;
    stop_ = false;

    // Start worker threads
    for (int i = 0; i < numWorkers_; ++i) {
        workers_.emplace_back(&TcpServer::workerLoop, this);
    }

    // Start event loop thread
    eventTh_ = std::thread(&TcpServer::eventLoop, this);

    logf(LogLevel::Debug, "TcpServer: %d worker threads started (epoll)", numWorkers_);
    return 0;
}

void TcpServer::stop() {
    stop_ = true;

    // Wake up event loop by closing listen fd
    int fd = listenFd_.exchange(-1);
    if (fd >= 0) {
        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);
    }
    if (eventTh_.joinable()) {
        eventTh_.join();
    }

    // Wake up all workers and join
    queueCv_.notify_all();
    for (auto &w : workers_) {
        if (w.joinable()) {
            w.join();
        }
    }
    workers_.clear();

    // Drain any remaining fds in the task queue (not yet picked up by workers)
    {
        std::lock_guard<std::mutex> lk(queueMutex_);
        while (!taskQueue_.empty()) {
            taskQueue_.pop();
            // fds are tracked in connectedFds_, closed below
        }
    }

    // Close all connected client fds
    {
        std::lock_guard<std::mutex> lk(fdSetMutex_);
        for (int fd : connectedFds_) {
            ::close(fd);
        }
        connectedFds_.clear();
    }
    {
        std::lock_guard<std::mutex> lk(fdClientMutex_);
        fdClientMap_.clear();
    }

    if (epollFd_ >= 0) {
        ::close(epollFd_);
        epollFd_ = -1;
    }
}

/// Parse client_id string from payload after fixed-size request struct.
/// Sets outEnd to the byte offset after client_id (for subsequent fields).
static std::string parseClientId(const std::vector<uint8_t> &payload,
                                  size_t fixedSize, size_t &outEnd) {
    outEnd = fixedSize;
    if (payload.size() <= fixedSize + 2) {
        return "";
    }
    uint16_t idLen = 0;
    std::memcpy(&idLen, payload.data() + fixedSize, sizeof(idLen));
    if (idLen == 0 || fixedSize + 2 + idLen > payload.size()) {
        return "";
    }
    // Reject oversized client_id that would be truncated in Allocation::clientId
    if (idLen >= kMaxClientIdLen) {
        return "";
    }
    const auto *start = payload.data() + fixedSize + 2;
    // Reject embedded null bytes — they cause mismatch between std::string
    // keys and null-terminated char[] stored in Allocation
    if (std::memchr(start, '\0', idLen) != nullptr) {
        return "";
    }
    outEnd = fixedSize + 2 + idLen;
    return std::string(reinterpret_cast<const char *>(start), idLen);
}

/// Parse request_id (uint64) from payload at the given offset.
static uint64_t parseRequestId(const std::vector<uint8_t> &payload,
                                size_t offset) {
    if (offset + sizeof(uint64_t) > payload.size()) {
        return 0;
    }
    uint64_t requestId = 0;
    std::memcpy(&requestId, payload.data() + offset, sizeof(requestId));
    return requestId;
}

// =============================================================================
// Event loop — epoll-based I/O multiplexing for listen + idle connections
// =============================================================================

void TcpServer::eventLoop() {
    constexpr int kMaxEvents = 64;
    struct epoll_event events[kMaxEvents];

    while (!stop_) {
        int n = epoll_wait(epollFd_, events, kMaxEvents, 500 /* ms timeout */);
        if (n < 0) {
            if (errno == EINTR) continue;
            break;  // epollFd_ closed or fatal error
        }

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;

            if (fd == listenFd_.load()) {
                // Accept all pending connections (non-blocking listen socket)
                while (true) {
                    int cfd = ::accept4(fd, nullptr, nullptr, SOCK_CLOEXEC);
                    if (cfd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        if (errno == EINTR) continue;
                        break;
                    }

                    int flag = 1;
                    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &flag,
                               sizeof(flag));

                    // Enable TCP keepalive to detect dead remote clients
                    setsockopt(cfd, SOL_SOCKET, SO_KEEPALIVE, &flag, sizeof(flag));
                    int idle = 10;  // seconds before first probe
                    setsockopt(cfd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
                    int intvl = 5;  // seconds between probes
                    setsockopt(cfd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
                    int cnt = 3;    // failed probes before disconnect
                    setsockopt(cfd, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt));

                    // Safety timeout for partial reads in workers
                    struct timeval tv{};
                    tv.tv_sec = 30;
                    tv.tv_usec = 0;
                    setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

                    if (activeClients_.load() >= maxClients_) {
                        logf(LogLevel::Warn,
                             "TcpServer: max clients reached (%d), rejecting",
                             maxClients_);
                        ::close(cfd);
                        continue;
                    }

                    activeClients_.fetch_add(1);
                    logf(LogLevel::Debug, "[CONN] new client fd=%d (active=%d)",
                         cfd, activeClients_.load());
                    {
                        std::lock_guard<std::mutex> lk(fdSetMutex_);
                        connectedFds_.insert(cfd);
                    }

                    // Add to epoll with ONESHOT: fires once, then auto-disarms
                    // until re-armed by the worker after handling a request.
                    struct epoll_event cev{};
                    cev.events = EPOLLIN | EPOLLONESHOT;
                    cev.data.fd = cfd;
                    if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, cfd, &cev) != 0) {
                        removeClient(cfd);
                    }
                }
            } else {
                // Client fd has data ready — dispatch to worker pool.
                // EPOLLONESHOT auto-disarms the fd so no duplicate dispatches.
                {
                    std::lock_guard<std::mutex> lk(queueMutex_);
                    taskQueue_.push(fd);
                }
                queueCv_.notify_one();
            }
        }
    }
}

// =============================================================================
// Worker loop — handles one request per dispatch, then returns fd to epoll
// =============================================================================

void TcpServer::workerLoop() {
    while (true) {
        int cfd = -1;
        {
            std::unique_lock<std::mutex> lk(queueMutex_);
            queueCv_.wait(lk, [this] {
                return stop_ || !taskQueue_.empty();
            });
            if (stop_) {
                return;
            }
            cfd = taskQueue_.front();
            taskQueue_.pop();
        }

        if (handleOneRequest(cfd)) {
            // Request handled — return fd to epoll for next request
            struct epoll_event ev{};
            ev.events = EPOLLIN | EPOLLONESHOT;
            ev.data.fd = cfd;
            if (epoll_ctl(epollFd_, EPOLL_CTL_MOD, cfd, &ev) != 0) {
                removeClient(cfd);
            }
        } else {
            // EOF or error — close connection
            removeClient(cfd);
        }
    }
}

void TcpServer::removeClient(int fd) {
    // Guard against double-close: only proceed if fd was in connectedFds_.
    // This prevents closing an fd that was already removed (and potentially
    // reassigned to a new connection by the kernel).
    bool removed = false;
    {
        std::lock_guard<std::mutex> lk(fdSetMutex_);
        removed = connectedFds_.erase(fd) > 0;
    }
    if (!removed) {
        return;
    }

    // Notify PoolManager of client disconnect before closing
    {
        std::lock_guard<std::mutex> lk(fdClientMutex_);
        auto it = fdClientMap_.find(fd);
        if (it != fdClientMap_.end()) {
            pm_.clientDisconnected(it->second);
            fdClientMap_.erase(it);
        }
    }

    epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
    int remaining = activeClients_.fetch_sub(1) - 1;
    logf(LogLevel::Debug, "[CONN] client disconnected fd=%d (active=%d)",
         fd, remaining);
}

bool TcpServer::trackClientId(int fd, const std::string &clientId) {
    if (clientId.empty()) return false;
    std::lock_guard<std::mutex> lk(fdClientMutex_);
    auto [it, inserted] = fdClientMap_.emplace(fd, clientId);
    if (inserted) {
        // First request on this connection — record and cancel any pending grace period
        pm_.clientReconnected(clientId);
        return true;
    }
    // Connection already has a pinned client_id — reject if different
    if (it->second != clientId) {
        logf(LogLevel::Warn,
             "fd=%d client_id mismatch: pinned='%s', received='%s' — rejected",
             fd, it->second.c_str(), clientId.c_str());
        return false;
    }
    return true;
}

// =============================================================================
// Idempotency cache — prevents duplicate alloc/free on client retry
// =============================================================================

std::string TcpServer::cacheKey(const std::string &clientId,
                                 uint64_t requestId) {
    // "hostname:pid:requestId" — unique per client per request
    return clientId + ":" + std::to_string(requestId);
}

bool TcpServer::cacheLookup(const std::string &clientId,
                             uint64_t requestId, int clientFd) {
    if (requestId == 0) return false;
    std::string key = cacheKey(clientId, requestId);
    std::lock_guard<std::mutex> lk(cacheMutex_);
    auto it = idempotencyCache_.find(key);
    if (it == idempotencyCache_.end()) return false;
    // Check TTL — expired entries are treated as cache misses
    auto age = std::chrono::duration_cast<std::chrono::seconds>(
        SteadyClock::now() - it->second.insertedAt).count();
    if (age >= kCacheTtlSec) {
        idempotencyCache_.erase(it);
        return false;
    }
    // Send cached response
    sendResp(clientFd, static_cast<MsgType>(it->second.type),
             it->second.payload.data(), it->second.payload.size());
    return true;
}

void TcpServer::cacheEvictExpired() {
    // Evict expired entries from the front (oldest first).
    // Caller must hold cacheMutex_.
    auto now = SteadyClock::now();
    while (!cacheOrder_.empty()) {
        auto it = idempotencyCache_.find(cacheOrder_.front());
        if (it == idempotencyCache_.end()) {
            cacheOrder_.pop_front();
            continue;
        }
        auto age = std::chrono::duration_cast<std::chrono::seconds>(
            now - it->second.insertedAt).count();
        if (age < kCacheTtlSec) break;  // rest are newer
        idempotencyCache_.erase(it);
        cacheOrder_.pop_front();
    }
}

void TcpServer::cacheInsert(const std::string &clientId,
                             uint64_t requestId, uint16_t type,
                             const void *payload, size_t payloadSize) {
    if (requestId == 0) return;
    std::string key = cacheKey(clientId, requestId);
    std::lock_guard<std::mutex> lk(cacheMutex_);
    // Evict expired entries first, then enforce size limit
    cacheEvictExpired();
    while (cacheOrder_.size() >= kMaxCacheEntries) {
        idempotencyCache_.erase(cacheOrder_.front());
        cacheOrder_.pop_front();
    }
    auto [it, inserted] = idempotencyCache_.emplace(key, CachedResponse{});
    it->second.type = type;
    it->second.payload.assign(static_cast<const uint8_t *>(payload),
                               static_cast<const uint8_t *>(payload) + payloadSize);
    it->second.insertedAt = SteadyClock::now();
    if (inserted) {
        cacheOrder_.push_back(key);
    }
}

// =============================================================================
// Request handling — processes a single request on the connection
// =============================================================================

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

        // Extract pool path (variable-length, before client_id)
        std::string poolPath;
        size_t poolPathEnd = sizeof(AllocReq);
        if (req.poolPathLen > 0) {
            if (poolPathEnd + req.poolPathLen > payload.size()) {
                sendError(clientFd, -EPROTO, "bad alloc req: pool path truncated");
                return false;
            }
            poolPath.assign(
                reinterpret_cast<const char*>(payload.data()) + poolPathEnd,
                req.poolPathLen);
        }
        poolPathEnd += req.poolPathLen;

        // client_id parsing now starts at poolPathEnd (was sizeof(AllocReq))
        size_t cidEnd = 0;
        std::string cid = parseClientId(payload, poolPathEnd, cidEnd);
        if (cid.empty()) {
            sendError(clientFd, -EINVAL, "missing client_id");
            return true;
        }
        if (!trackClientId(clientFd, cid)) {
            sendError(clientFd, -EPERM, "client_id mismatch on connection");
            return false;  // close connection
        }
        uint64_t requestId = parseRequestId(payload, cidEnd);

        // Check idempotency cache
        if (cacheLookup(cid, requestId, clientFd)) {
            return true;
        }

        RequestContext ctx{cid, poolPath};
        auto result = handler_.handleAlloc(req, ctx);

        auto respPayload = serializeAllocResp(result.resp, result.devicePath);
        cacheInsert(cid, requestId, static_cast<uint16_t>(MsgType::ALLOC_RESP),
                    respPayload.data(), respPayload.size());
        if (sendResp(clientFd, MsgType::ALLOC_RESP,
                     respPayload.data(), respPayload.size()) != 0) {
            return false;
        }
    } else if (type == MsgType::FREE_REQ) {
        if (payload.size() < sizeof(FreeReq)) {
            sendError(clientFd, -EPROTO, "bad free req");
            return false;
        }
        FreeReq req{};
        std::memcpy(&req, payload.data(), sizeof(req));

        size_t cidEnd = 0;
        std::string cid = parseClientId(payload, sizeof(FreeReq), cidEnd);
        if (cid.empty()) {
            sendError(clientFd, -EINVAL, "missing client_id");
            return true;
        }
        if (!trackClientId(clientFd, cid)) {
            sendError(clientFd, -EPERM, "client_id mismatch on connection");
            return false;  // close connection
        }
        uint64_t requestId = parseRequestId(payload, cidEnd);

        // Check idempotency cache
        if (cacheLookup(cid, requestId, clientFd)) {
            return true;
        }

        RequestContext ctx{cid, {}};
        auto result = handler_.handleFree(req, ctx);
        if (result.resp.status == -EACCES) {
            sendError(clientFd, -EACCES, "invalid authToken");
            return true;  // protocol error, but keep connection open
        }
        cacheInsert(cid, requestId, static_cast<uint16_t>(MsgType::FREE_RESP),
                    &result.resp, sizeof(result.resp));
        if (sendResp(clientFd, MsgType::FREE_RESP,
                     &result.resp, sizeof(result.resp)) != 0) {
            return false;
        }
    } else if (type == MsgType::STATS_REQ) {
        auto result = handler_.handleStats();
        if (sendResp(clientFd, MsgType::STATS_RESP,
                     result.payload.data(), result.payload.size()) != 0) {
            return false;
        }
    } else if (type == MsgType::GET_ACCESS_REQ) {
        if (payload.size() < sizeof(GetAccessReq)) {
            sendError(clientFd, -EPROTO, "bad get_access req");
            return false;
        }
        GetAccessReq req{};
        std::memcpy(&req, payload.data(), sizeof(req));

        size_t cidEnd = 0;
        std::string cid = parseClientId(payload, sizeof(GetAccessReq), cidEnd);
        if (!cid.empty()) {
            if (!trackClientId(clientFd, cid)) {
                sendError(clientFd, -EPERM, "client_id mismatch on connection");
                return false;
            }
        }

        RequestContext ctx{cid, {}};
        auto result = handler_.handleGetAccess(req, ctx);
        if (result.status == -EACCES) {
            sendError(clientFd, -EACCES, "invalid auth token");
            return true;  // protocol error, but keep connection open
        }

        auto respPayload = serializeGetAccessResp(
            result.status, result.devicePath, result.offset, result.length);
        if (sendResp(clientFd, MsgType::GET_ACCESS_RESP,
                     respPayload.data(), respPayload.size()) != 0) {
            return false;
        }
    } else {
        sendError(clientFd, -ENOSYS, "unknown request");
        return false;
    }

    return true;  // Connection stays open for next request
}

}  // namespace maru
