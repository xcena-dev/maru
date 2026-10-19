#include "admin_server.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <vector>

#include "log.h"
#include "pool_manager.h"

namespace maru {

static constexpr size_t kMaxRequestBytes = 4096;
static constexpr const char *kUsage =
    "usage: status | allocations [daxN.M] | exclude daxN.M | "
    "include daxN.M [--reinit|--adopt|--restore-header] | "
    "release REGION [--confirm [--force]] | discard daxN.M [--confirm [--force]] | "
    "wipe daxN.M [--confirm]\n";

AdminServer::AdminServer(PoolManager &pm, std::string socketPath)
    : pm_(pm), path_(std::move(socketPath)) {}

AdminServer::~AdminServer() { stop(); }

int AdminServer::start() {
    sockaddr_un addr{};
    if (path_.size() >= sizeof(addr.sun_path)) return -ENAMETOOLONG;
    addr.sun_family = AF_UNIX;
    std::memcpy(addr.sun_path, path_.c_str(), path_.size() + 1);
    struct stat st{};
    if (::lstat(path_.c_str(), &st) == 0) {
        if (!S_ISSOCK(st.st_mode)) return -EEXIST; // Never remove anything else.
        // Remove only a stale socket, never one another RM still serves.
        const int probe = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
        const bool live =
            probe >= 0 && ::connect(probe, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0;
        if (probe >= 0) ::close(probe);
        if (live) return -EADDRINUSE;
        ::unlink(path_.c_str());
    }
    if (::pipe2(stopPipe_, O_CLOEXEC) != 0) return -errno;
    listenFd_ = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listenFd_ < 0) {
        const int rc = -errno;
        stop();
        return rc;
    }
    const mode_t previous = ::umask(0177);
    int rc = ::bind(listenFd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    if (rc != 0) rc = -errno;
    ::umask(previous);
    bound_ = rc == 0;
    if (rc == 0 && ::listen(listenFd_, 4) != 0) rc = -errno;
    if (rc != 0) {
        stop();
        return rc;
    }
    thread_ = std::thread(&AdminServer::loop, this);
    logf(LogLevel::Info, "admin socket: %s", path_.c_str());
    return 0;
}

void AdminServer::stop() {
    stopping_ = true; // Interrupts a running wipe between chunks.
    if (thread_.joinable()) {
        const char wake = 0;
        (void)!::write(stopPipe_[1], &wake, 1);
        thread_.join();
    }
    if (listenFd_ >= 0) {
        ::close(listenFd_);
        listenFd_ = -1;
    }
    if (bound_) {
        ::unlink(path_.c_str());
        bound_ = false;
    }
    for (int &fd : stopPipe_) {
        if (fd >= 0) ::close(fd);
        fd = -1;
    }
}

void AdminServer::loop() {
    pollfd fds[2] = {{listenFd_, POLLIN, 0}, {stopPipe_[0], POLLIN, 0}};
    while (true) {
        if (::poll(fds, 2, -1) < 0) {
            if (errno == EINTR) continue;
            logf(LogLevel::Error, "admin socket poll failed: %s", std::strerror(errno));
            return;
        }
        if (fds[1].revents != 0) return;
        if (fds[0].revents & POLLIN) {
            const int fd = ::accept4(listenFd_, nullptr, nullptr, SOCK_CLOEXEC);
            if (fd < 0) {
                if (errno != EINTR && errno != EAGAIN && errno != ECONNABORTED) {
                    // E.g. EMFILE: the connection stays queued, so back off
                    // instead of spinning on POLLIN.
                    logf(LogLevel::Warn, "admin socket accept failed: %s", std::strerror(errno));
                    ::poll(&fds[1], 1, 1000);
                }
                continue;
            }
            serve(fd); // Admin commands are rare; one at a time is enough.
            ::close(fd);
        }
    }
}

// The client closed its connection (gave up waiting, or was interrupted). A
// half-close after sending the request still counts as present.
static bool peerGone(int fd) {
    pollfd p{fd, 0, 0};
    return ::poll(&p, 1, 0) == 1 && (p.revents & (POLLHUP | POLLERR)) != 0;
}

void AdminServer::serve(int fd) {
    const timeval timeout{5, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    std::string reply;
    ucred peer{};
    socklen_t size = sizeof(peer);
    if (::getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &peer, &size) != 0 ||
        (peer.uid != 0 && peer.uid != ::geteuid())) {
        reply = "ERR " + std::to_string(EACCES) + " permission denied\n";
    } else {
        std::string line;
        char buf[512];
        while (line.find('\n') == std::string::npos && line.size() <= kMaxRequestBytes) {
            const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0) break;
            line.append(buf, static_cast<size_t>(n));
        }
        const size_t end = line.find('\n');
        if (end == std::string::npos || end > kMaxRequestBytes) {
            reply = "ERR " + std::to_string(EINVAL) + " malformed request\n" + kUsage;
        } else {
            line.resize(end);
            // Requests queued behind a long command must not run for a
            // client that already left (it may have reported a timeout).
            if (peerGone(fd)) {
                logf(LogLevel::Warn, "admin: client left before '%s' ran; skipped", line.c_str());
                return;
            }
            reply = handleLine(pm_, line, [this, fd] { return stopping_.load() || peerGone(fd); });
            logf(LogLevel::Info, "admin: '%s' -> %.*s", line.c_str(),
                 static_cast<int>(reply.find('\n')), reply.c_str());
        }
    }
    for (size_t sent = 0; sent < reply.size();) {
        const ssize_t n = ::send(fd, reply.data() + sent, reply.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) break;
        sent += static_cast<size_t>(n);
    }
}

static bool parseRegion(const std::string &text, uint64_t &out) {
    if (text.empty() || text.size() > 20) return false;
    for (char c : text)
        if (c < '0' || c > '9') return false;
    errno = 0;
    char *end = nullptr;
    const unsigned long long value = std::strtoull(text.c_str(), &end, 10);
    if (errno != 0 || *end != '\0') return false;
    out = value;
    return true;
}

std::string AdminServer::handleLine(PoolManager &pm, const std::string &line,
                                    const std::function<bool()> &cancelled) {
    std::istringstream words(line);
    std::vector<std::string> args;
    for (std::string word; words >> word;) args.push_back(word);
    const std::string command = args.empty() ? "" : args[0];

    std::string out;
    int rc = -EINVAL;
    uint64_t region = 0;
    if (command == "status" && args.size() == 1) {
        rc = pm.adminStatus(out);
    } else if (command == "allocations" && args.size() <= 2) {
        rc = pm.adminAllocations(args.size() == 2 ? args[1] : "", out);
    } else if (command == "exclude" && args.size() == 2) {
        rc = pm.adminExclude(args[1], out);
    } else if (command == "include" && (args.size() == 2 || args.size() == 3)) {
        using Mode = PoolManager::IncludeMode;
        Mode mode = Mode::Judge;
        bool valid = true;
        if (args.size() == 3) {
            if (args[2] == "--reinit") mode = Mode::Reinit;
            else if (args[2] == "--adopt") mode = Mode::Adopt;
            else if (args[2] == "--restore-header") mode = Mode::RestoreHeader;
            else valid = false;
        }
        if (valid) rc = pm.adminInclude(args[1], mode, out);
        else out = kUsage;
    } else if (command == "release" && args.size() >= 2 && args.size() <= 4 &&
               parseRegion(args[1], region) &&
               (args.size() < 3 || args[2] == "--confirm") &&
               (args.size() < 4 || args[3] == "--force")) {
        rc = pm.adminRelease(region, args.size() >= 3, args.size() == 4, out);
    } else if (command == "discard" && args.size() >= 2 && args.size() <= 4 &&
               (args.size() < 3 || args[2] == "--confirm") &&
               (args.size() < 4 || args[3] == "--force")) {
        rc = pm.adminDiscard(args[1], args.size() >= 3, args.size() == 4, out);
    } else if (command == "wipe" &&
               (args.size() == 2 || (args.size() == 3 && args[2] == "--confirm"))) {
        rc = pm.adminWipe(args[1], args.size() == 3, out, cancelled);
    } else {
        out = kUsage;
    }
    if (rc == 0) return "OK\n" + out;
    // Done, but not safe to build on yet (e.g. hand the device over).
    if (rc == PoolManager::kAdminIncomplete) return "INCOMPLETE\n" + out;
    return "ERR " + std::to_string(-rc) + " " + std::strerror(-rc) + "\n" + out;
}

} // namespace maru
