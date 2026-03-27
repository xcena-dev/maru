#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "pool_manager.h"
#include "request_handler.h"

namespace maru {

class TcpServer {
public:
    TcpServer(PoolManager &pm, const std::string &host, uint16_t port,
              int numWorkers = 32);
    ~TcpServer();

    int start();
    void stop();

private:
    /// epoll-based event loop: monitors listen socket + idle client connections.
    /// Dispatches ready connections to the worker pool.
    void eventLoop();
    void workerLoop();
    /// Handle one request on a persistent connection. Returns false on EOF/error.
    bool handleOneRequest(int clientFd);
    /// Remove a client fd from epoll, close it, and update tracking state.
    void removeClient(int fd);

    PoolManager &pm_;
    RequestHandler handler_;
    std::string host_;
    uint16_t port_;
    int numWorkers_;

    std::atomic<bool> stop_{false};
    std::atomic<int> listenFd_{-1};
    int epollFd_{-1};

    // Event loop thread (replaces dedicated accept thread)
    std::thread eventTh_;

    // Worker thread pool
    std::vector<std::thread> workers_;
    std::queue<int> taskQueue_;
    std::mutex queueMutex_;
    std::condition_variable queueCv_;

    // Track all connected client fds for graceful shutdown cleanup
    std::mutex fdSetMutex_;
    std::unordered_set<int> connectedFds_;

    // Map fd -> client_id for disconnect notification
    std::mutex fdClientMutex_;
    std::unordered_map<int, std::string> fdClientMap_;
    void trackClientId(int fd, const std::string &clientId);

    static constexpr int kMaxClients = 256;
    std::atomic<int> activeClients_{0};

    // Idempotency cache for alloc/free requests.
    // Maps request_id -> (response MsgType, serialized payload).
    // Prevents duplicate side effects when client retries after response loss.
    using SteadyClock = std::chrono::steady_clock;
    struct CachedResponse {
        uint16_t type;
        std::vector<uint8_t> payload;
        SteadyClock::time_point insertedAt;
    };
    // Cache key: "client_id:request_id" to prevent cross-client collisions
    std::mutex cacheMutex_;
    std::unordered_map<std::string, CachedResponse> idempotencyCache_;
    std::deque<std::string> cacheOrder_;  // insertion order for eviction
    static constexpr size_t kMaxCacheEntries = 1024;
    static constexpr int kCacheTtlSec = 60;
    void cacheEvictExpired();

    static std::string cacheKey(const std::string &clientId, uint64_t requestId);
    bool cacheLookup(const std::string &clientId, uint64_t requestId, int clientFd);
    void cacheInsert(const std::string &clientId, uint64_t requestId,
                     uint16_t type, const void *payload, size_t payloadSize);
};

}  // namespace maru
