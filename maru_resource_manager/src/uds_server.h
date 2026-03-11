#pragma once

#include <atomic>
#include <thread>

#include "pool_manager.h"
#include "request_handler.h"

namespace maru {

class UdsServer {
public:
  explicit UdsServer(PoolManager &pm);
  ~UdsServer();

  int start();
  void stop();

private:
  void acceptLoop();
  void handleClient(int clientFd);

  PoolManager &pm_;
  RequestHandler handler_;
  std::atomic<bool> stop_{false};
  std::thread th_;
  int listenFd_{-1};

  // Connection limit to prevent DoS via unbounded thread creation
  static constexpr int kMaxClients = 256;
  std::atomic<int> activeClients_{0};
};

} // namespace maru
