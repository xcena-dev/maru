#include "uds_server.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
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

namespace maru
{

UdsServer::UdsServer(PoolManager &pm, const std::string &socketPath)
    : pm_(pm), handler_(pm), socketPath_(socketPath)
{
}

UdsServer::~UdsServer()
{
    stop();
}

int UdsServer::start()
{
    // Initialize HMAC secret - must exist if there are recovered allocations
    int rc = initSecret(pm_.stateDir(), pm_.hasExistingAllocations());
    if (rc != 0)
    {
        logf(LogLevel::Error, "Failed to init secret: %d", rc);
        return rc;
    }

    const std::string &path = socketPath_;

    int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0)
    {
        return -errno;
    }

    int enable = 1;
    setsockopt(fd, SOL_SOCKET, SO_PASSCRED, &enable, sizeof(enable));

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    if (path.size() >= sizeof(addr.sun_path))
    {
        ::close(fd);
        return -ENAMETOOLONG;
    }
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

    ::unlink(path.c_str());
    if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0)
    {
        int err = -errno;
        ::close(fd);
        return err;
    }
    if (::chmod(path.c_str(), 0666) != 0)
    {
        logf(LogLevel::Warn, "maru-resource-manager: chmod 666 failed for %s (%d)", path.c_str(), errno);
    }
    if (::listen(fd, 64) != 0)
    {
        int err = -errno;
        ::close(fd);
        return err;
    }

    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags >= 0)
    {
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    listenFd_ = fd;
    stop_ = false;
    th_ = std::thread(&UdsServer::acceptLoop, this);
    return 0;
}

void UdsServer::stop()
{
    stop_ = true;
    if (listenFd_ >= 0)
    {
        ::shutdown(listenFd_, SHUT_RDWR);
        ::close(listenFd_);
        listenFd_ = -1;
    }
    if (th_.joinable())
    {
        th_.join();
    }
    // Wait for in-flight client handlers to finish (max 5 seconds)
    for (int i = 0; i < 100 && activeClients_.load() > 0; ++i)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

static int sendError(int fd, int32_t status, const std::string &msg)
{
    ErrorResp er{};
    er.status = status;
    er.msgLen = msg.size();

    MsgHeader hdr{};
    hdr.magic = kMagic;
    hdr.version = kVersion;
    hdr.type = static_cast<uint16_t>(MsgType::ERROR_RESP);
    hdr.payloadLen = sizeof(ErrorResp) + er.msgLen;

    int rc = writeFull(fd, &hdr, sizeof(hdr));
    if (rc != 0)
    {
        return rc;
    }
    rc = writeFull(fd, &er, sizeof(er));
    if (rc != 0)
    {
        return rc;
    }
    if (er.msgLen > 0)
    {
        rc = writeFull(fd, msg.data(), er.msgLen);
        if (rc != 0)
        {
            return rc;
        }
    }
    return 0;
}

static int readFullWithCred(int fd, void *buf, size_t len,
                            struct ucred *credOut)
{
    if (len == 0)
    {
        return 0;
    }

    struct iovec iov = {};
    iov.iov_base = buf;
    iov.iov_len = len;

    char controlBuf[CMSG_SPACE(sizeof(struct ucred))];
    std::memset(controlBuf, 0, sizeof(controlBuf));

    struct msghdr msg = {};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = controlBuf;
    msg.msg_controllen = sizeof(controlBuf);

    ssize_t n = ::recvmsg(fd, &msg, MSG_WAITALL);
    if (n < 0)
    {
        return -errno;
    }
    if (static_cast<size_t>(n) != len)
    {
        return -EIO;
    }

    if (credOut)
    {
        credOut->pid = -1;
        credOut->uid = static_cast<uid_t>(-1);
        credOut->gid = static_cast<gid_t>(-1);

        for (struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr;
             cmsg = CMSG_NXTHDR(&msg, cmsg))
        {
            if (cmsg->cmsg_level == SOL_SOCKET &&
                cmsg->cmsg_type == SCM_CREDENTIALS)
            {
                if (cmsg->cmsg_len >= CMSG_LEN(sizeof(struct ucred)))
                {
                    std::memcpy(credOut, CMSG_DATA(cmsg), sizeof(struct ucred));
                }
                break;
            }
        }
    }

    return 0;
}

static int sendAllocRespWithFd(int fd, const AllocResp &resp, int daxFd)
{
    MsgHeader rh{};
    rh.magic = kMagic;
    rh.version = kVersion;
    rh.type = static_cast<uint16_t>(MsgType::ALLOC_RESP);
    rh.payloadLen = sizeof(resp);

    int rc = writeFull(fd, &rh, sizeof(rh));
    if (rc != 0)
    {
        return rc;
    }

    struct iovec iov
    {
    };
    iov.iov_base = const_cast<AllocResp *>(&resp);
    iov.iov_len = sizeof(resp);

    char controlBuf[CMSG_SPACE(sizeof(int))];
    std::memset(controlBuf, 0, sizeof(controlBuf));

    struct msghdr msg
    {
    };
    msg.msg_name = nullptr;
    msg.msg_namelen = 0;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = nullptr;
    msg.msg_controllen = 0;

    if (daxFd >= 0)
    {
        msg.msg_control = controlBuf;
        msg.msg_controllen = sizeof(controlBuf);
        struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SCM_RIGHTS;
        cmsg->cmsg_len = CMSG_LEN(sizeof(int));
        std::memcpy(CMSG_DATA(cmsg), &daxFd, sizeof(int));
    }

    ssize_t n = ::sendmsg(fd, &msg, 0);
    if (n < 0)
    {
        return -errno;
    }
    if (static_cast<size_t>(n) != sizeof(resp))
    {
        return -EIO;
    }
    return 0;
}

static int sendGetFdRespWithFd(int fd, const GetFdResp &resp, int daxFd)
{
    MsgHeader rh{};
    rh.magic = kMagic;
    rh.version = kVersion;
    rh.type = static_cast<uint16_t>(MsgType::GET_FD_RESP);
    rh.payloadLen = sizeof(resp);

    int rc = writeFull(fd, &rh, sizeof(rh));
    if (rc != 0)
    {
        return rc;
    }

    struct iovec iov
    {
    };
    iov.iov_base = const_cast<GetFdResp *>(&resp);
    iov.iov_len = sizeof(resp);

    char controlBuf[CMSG_SPACE(sizeof(int))];
    std::memset(controlBuf, 0, sizeof(controlBuf));

    struct msghdr msg
    {
    };
    msg.msg_name = nullptr;
    msg.msg_namelen = 0;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = nullptr;
    msg.msg_controllen = 0;

    if (daxFd >= 0)
    {
        msg.msg_control = controlBuf;
        msg.msg_controllen = sizeof(controlBuf);
        struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SCM_RIGHTS;
        cmsg->cmsg_len = CMSG_LEN(sizeof(int));
        std::memcpy(CMSG_DATA(cmsg), &daxFd, sizeof(int));
    }

    ssize_t n = ::sendmsg(fd, &msg, 0);
    if (n < 0)
    {
        return -errno;
    }
    if (static_cast<size_t>(n) != sizeof(resp))
    {
        return -EIO;
    }
    return 0;
}

void UdsServer::acceptLoop()
{
    while (!stop_)
    {
        int cfd = ::accept(listenFd_, nullptr, nullptr);
        if (cfd < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            if (errno == EBADF || errno == ENOTSOCK)
            {
                break;
            }
            break;
        }
        if (activeClients_.load() >= kMaxClients)
        {
            logf(LogLevel::Warn,
                 "maru-resource-manager: max clients reached (%d), rejecting connection",
                 kMaxClients);
            ::close(cfd);
            continue;
        }
        std::thread(&UdsServer::handleClient, this, cfd).detach();
    }
}

void UdsServer::handleClient(int clientFd)
{
    activeClients_.fetch_add(1);
    // RAII guard: always decrement activeClients_ on any exit path
    struct ClientGuard
    {
        std::atomic<int> &counter;
        ~ClientGuard()
        {
            counter.fetch_sub(1);
        }
    } guard{activeClients_};

    struct ucred connCred
    {
    };
    socklen_t len = sizeof(connCred);
    if (::getsockopt(clientFd, SOL_SOCKET, SO_PEERCRED, &connCred, &len) != 0)
    {
        ::close(clientFd);
        return;
    }

    MsgHeader hdr{};
    struct ucred cred
    {
    };
    int rc = readFullWithCred(clientFd, &hdr, sizeof(hdr), &cred);
    if (rc != 0)
    {
        ::close(clientFd);
        return;
    }

    if (cred.pid != -1)
    {
        if (cred.pid != connCred.pid || cred.uid != connCred.uid)
        {
            sendError(clientFd, -EACCES, "credential changed during session");
            ::close(clientFd);
            return;
        }
    }
    else
    {
        cred = connCred;
    }

    if (hdr.magic != kMagic || hdr.version != kVersion)
    {
        sendError(clientFd, -EPROTO, "bad header");
        ::close(clientFd);
        return;
    }

    constexpr uint32_t maxPayloadLen = 1 * 1024;  // 1KB max payload
    if (hdr.payloadLen > maxPayloadLen)
    {
        sendError(clientFd, -EMSGSIZE, "payload too large");
        ::close(clientFd);
        return;
    }

    std::vector<uint8_t> payload(hdr.payloadLen);
    if (hdr.payloadLen > 0)
    {
        rc = readFull(clientFd, payload.data(), hdr.payloadLen);
        if (rc != 0)
        {
            ::close(clientFd);
            return;
        }
    }

    RequestContext ctx{cred.pid, cred.uid};
    MsgType type = static_cast<MsgType>(hdr.type);

    if (type == MsgType::ALLOC_REQ)
    {
        if (payload.size() != sizeof(AllocReq))
        {
            sendError(clientFd, -EPROTO, "bad alloc req");
            ::close(clientFd);
            return;
        }
        AllocReq req{};
        std::memcpy(&req, payload.data(), sizeof(req));

        auto result = handler_.handleAlloc(req, ctx);
        sendAllocRespWithFd(clientFd, result.resp, result.daxFd);
        if (result.daxFd >= 0)
        {
            ::close(result.daxFd);
        }
    }
    else if (type == MsgType::FREE_REQ)
    {
        if (payload.size() != sizeof(FreeReq))
        {
            sendError(clientFd, -EPROTO, "bad free req");
            ::close(clientFd);
            return;
        }
        FreeReq req{};
        std::memcpy(&req, payload.data(), sizeof(req));

        auto result = handler_.handleFree(req, ctx);
        if (result.resp.status == -EACCES)
        {
            sendError(clientFd, -EACCES, "invalid authToken");
            ::close(clientFd);
            return;
        }

        MsgHeader rh{};
        rh.magic = kMagic;
        rh.version = kVersion;
        rh.type = static_cast<uint16_t>(MsgType::FREE_RESP);
        rh.payloadLen = sizeof(result.resp);

        writeFull(clientFd, &rh, sizeof(rh));
        writeFull(clientFd, &result.resp, sizeof(result.resp));
    }
    else if (type == MsgType::STATS_REQ)
    {
        auto result = handler_.handleStats();

        MsgHeader rh{};
        rh.magic = kMagic;
        rh.version = kVersion;
        rh.type = static_cast<uint16_t>(MsgType::STATS_RESP);
        rh.payloadLen = result.payload.size();

        writeFull(clientFd, &rh, sizeof(rh));
        if (!result.payload.empty())
        {
            writeFull(clientFd, result.payload.data(), result.payload.size());
        }
    }
    else if (type == MsgType::GET_FD_REQ)
    {
        if (payload.size() != sizeof(GetFdReq))
        {
            sendError(clientFd, -EPROTO, "bad get_fd req");
            ::close(clientFd);
            return;
        }
        GetFdReq req{};
        std::memcpy(&req, payload.data(), sizeof(req));

        auto result = handler_.handleGetFd(req, ctx);
        if (result.resp.status == -EACCES)
        {
            sendError(clientFd, -EACCES, "invalid auth token");
            ::close(clientFd);
            return;
        }

        sendGetFdRespWithFd(clientFd, result.resp, result.daxFd);
        if (result.daxFd >= 0)
        {
            ::close(result.daxFd);
        }
    }
    else
    {
        sendError(clientFd, -ENOSYS, "unknown request");
    }

    ::close(clientFd);
}

}  // namespace maru
