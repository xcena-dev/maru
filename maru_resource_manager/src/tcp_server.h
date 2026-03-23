#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
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
    void acceptLoop();
    void workerLoop();
    void handleClient(int clientFd);
    /// Handle one request on a persistent connection. Returns false on EOF/error.
    bool handleOneRequest(int clientFd);

    PoolManager &pm_;
    RequestHandler handler_;
    std::string host_;
    uint16_t port_;
    int numWorkers_;

    std::atomic<bool> stop_{false};
    int listenFd_{-1};

    // Accept thread
    std::thread acceptTh_;

    // Worker thread pool
    std::vector<std::thread> workers_;
    std::queue<int> taskQueue_;
    std::mutex queueMutex_;
    std::condition_variable queueCv_;

    static constexpr int kMaxClients = 256;
    std::atomic<int> activeClients_{0};
};

}  // namespace maru
