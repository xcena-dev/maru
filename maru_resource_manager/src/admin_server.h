#pragma once

#include <atomic>
#include <functional>
#include <string>
#include <thread>

namespace maru {

class PoolManager;

/// Local endpoint for maru-rm-ctl: <stateDir>/admin.sock, mode 0600, one
/// request line per connection. Kept off the TCP port, which has no
/// authentication and may listen on every interface.
class AdminServer {
public:
    AdminServer(PoolManager &pm, std::string socketPath);
    ~AdminServer();

    AdminServer(const AdminServer &) = delete;
    AdminServer &operator=(const AdminServer &) = delete;

    /// Must run before other threads start: the socket is created under a
    /// temporary process umask so it is never accessible to other users.
    int start();
    void stop();

    /// Run one request line and return the reply ("OK" or "ERR <errno> ...").
    /// Long commands (wipe) stop early once `cancelled` returns true.
    static std::string handleLine(PoolManager &pm, const std::string &line,
                                  const std::function<bool()> &cancelled = {});

private:
    void loop();
    void serve(int fd);

    PoolManager &pm_;
    std::string path_;
    int listenFd_{-1};
    bool bound_{false}; // Only then is the path ours to unlink.
    int stopPipe_[2]{-1, -1};
    std::thread thread_;
    std::atomic<bool> stopping_{false};
};

} // namespace maru
