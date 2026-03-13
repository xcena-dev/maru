// maru_test_client - Interactive test client for Maru Resource Manager
//
// Communicates with the resource manager over UDS using the binary IPC protocol.
// Supports stats, alloc, and a full mmap round-trip test.
//
// Note: The resource manager's reaper reclaims allocations when the owning process
// exits, so standalone free/get_fd commands are not useful.
//
// Usage:
//   maru_test_client [-s socket_path] <command> [args...]
//
// Commands:
//   stats                    Query pool statistics
//   alloc <size> [pool_id]   Allocate shared memory
//   mmap <size> [pool_id]    Full cycle: alloc -> mmap -> write -> read -> verify -> free

#include <getopt.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "ipc.h"

using namespace maru;

static const char *kDefaultSocketPath = "/tmp/maru-resourced/maru-resourced.sock";
static const char *g_socketPath = nullptr;
static constexpr uint32_t kAnyPoolId = 0xFFFFFFFFu;

// ----------------------------------------------------------------------------
// I/O helpers
// ----------------------------------------------------------------------------

static int writeFull(int fd, const void *buf, size_t len)
{
    const uint8_t *p = static_cast<const uint8_t *>(buf);
    size_t off = 0;
    while (off < len)
    {
        ssize_t n = ::write(fd, p + off, len - off);
        if (n < 0)
        {
            if (errno == EINTR)
                continue;
            return -errno;
        }
        off += static_cast<size_t>(n);
    }
    return 0;
}

static int readFull(int fd, void *buf, size_t len)
{
    uint8_t *p = static_cast<uint8_t *>(buf);
    size_t off = 0;
    while (off < len)
    {
        ssize_t n = ::read(fd, p + off, len - off);
        if (n == 0)
            return -EPIPE;
        if (n < 0)
        {
            if (errno == EINTR)
                continue;
            return -errno;
        }
        off += static_cast<size_t>(n);
    }
    return 0;
}

// Receive data + optional FD via SCM_RIGHTS.
// Uses a large control buffer to accommodate potential SCM_CREDENTIALS
// ancillary data that the kernel may auto-attach.
static int recvWithFd(int sockFd, void *buf, size_t len, int *fdOut)
{
    struct iovec iov
    {
    };
    iov.iov_base = buf;
    iov.iov_len = len;

    // Large enough for SCM_RIGHTS(int) + SCM_CREDENTIALS(ucred)
    char controlBuf[CMSG_SPACE(sizeof(int)) + CMSG_SPACE(sizeof(struct ucred))];
    std::memset(controlBuf, 0, sizeof(controlBuf));

    struct msghdr msg
    {
    };
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = controlBuf;
    msg.msg_controllen = sizeof(controlBuf);

    ssize_t n = ::recvmsg(sockFd, &msg, 0);
    if (n < 0)
        return -errno;
    if (static_cast<size_t>(n) != len)
        return -EIO;

    *fdOut = -1;
    for (struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg); cmsg;
         cmsg = CMSG_NXTHDR(&msg, cmsg))
    {
        if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS)
        {
            std::memcpy(fdOut, CMSG_DATA(cmsg), sizeof(int));
            break;
        }
    }
    return 0;
}

// ----------------------------------------------------------------------------
// Protocol helpers
// ----------------------------------------------------------------------------

static const char *resolveSocketPath()
{
    if (g_socketPath)
        return g_socketPath;
    return kDefaultSocketPath;
}

static int connectResourceManager()
{
    const char *path = resolveSocketPath();

    int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0)
    {
        fprintf(stderr, "error: socket() failed: %s\n", strerror(errno));
        return -1;
    }

    // Note: Do NOT set SO_PASSCRED on the client socket.
    // The daemon's accepted socket already has SO_PASSCRED (inherited from
    // its listening socket), so the kernel auto-attaches our credentials
    // on the daemon side. Setting it here would cause the kernel to inject
    // SCM_CREDENTIALS into our recvmsg calls, competing for control buffer
    // space with the SCM_RIGHTS FD we actually need.

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);

    if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0)
    {
        fprintf(stderr, "error: connect(%s) failed: %s\n", path, strerror(errno));
        ::close(fd);
        return -1;
    }
    return fd;
}

static int sendRequest(int fd, MsgType type, const void *payload,
                       uint32_t payloadLen)
{
    MsgHeader hdr{};
    hdr.magic = kMagic;
    hdr.version = kVersion;
    hdr.type = static_cast<uint16_t>(type);
    hdr.payloadLen = payloadLen;

    int rc = writeFull(fd, &hdr, sizeof(hdr));
    if (rc != 0)
    {
        fprintf(stderr, "error: write(header) failed: %s\n", strerror(-rc));
        return -1;
    }
    if (payloadLen > 0 && payload)
    {
        rc = writeFull(fd, payload, payloadLen);
        if (rc != 0)
        {
            fprintf(stderr, "error: write(payload) failed: %s\n", strerror(-rc));
            return -1;
        }
    }
    return 0;
}

static int recvHeader(int fd, MsgHeader *hdr)
{
    int rc = readFull(fd, hdr, sizeof(*hdr));
    if (rc != 0)
    {
        fprintf(stderr, "error: read(header) failed: %s\n", strerror(-rc));
        return -1;
    }
    if (hdr->magic != kMagic)
    {
        fprintf(stderr, "error: bad magic 0x%08X (expected 0x%08X)\n", hdr->magic, kMagic);
        return -1;
    }
    if (hdr->version != kVersion)
    {
        fprintf(stderr, "error: bad version %u (expected %u)\n", hdr->version, kVersion);
        return -1;
    }
    return 0;
}

// Returns true if the response is an error (prints message and returns true).
static bool handleErrorResp(int fd, const MsgHeader &hdr)
{
    if (static_cast<MsgType>(hdr.type) != MsgType::ERROR_RESP)
        return false;

    ErrorResp err{};
    if (hdr.payloadLen >= sizeof(err))
        readFull(fd, &err, sizeof(err));

    std::string msg;
    if (err.msgLen > 0 && hdr.payloadLen >= sizeof(err) + err.msgLen)
    {
        msg.resize(err.msgLen);
        readFull(fd, &msg[0], err.msgLen);
    }

    fprintf(stderr, "error: resource manager returned status=%d: %s\n", err.status, msg.empty() ? "(no message)" : msg.c_str());
    return true;
}

static std::string formatSize(uint64_t bytes)
{
    char buf[64];
    if (bytes >= (1ULL << 30))
        std::snprintf(buf, sizeof(buf), "%" PRIu64 " (%.2f GiB)", bytes, static_cast<double>(bytes) / (1ULL << 30));
    else if (bytes >= (1ULL << 20))
        std::snprintf(buf, sizeof(buf), "%" PRIu64 " (%.2f MiB)", bytes, static_cast<double>(bytes) / (1ULL << 20));
    else if (bytes >= (1ULL << 10))
        std::snprintf(buf, sizeof(buf), "%" PRIu64 " (%.2f KiB)", bytes, static_cast<double>(bytes) / (1ULL << 10));
    else
        std::snprintf(buf, sizeof(buf), "%" PRIu64 " B", bytes);
    return buf;
}

// Helper: send alloc request and receive response with FD
static int doAlloc(uint64_t size, uint32_t poolId, AllocResp *resp,
                   int *daxFd)
{
    int fd = connectResourceManager();
    if (fd < 0)
        return -1;

    AllocReq req{};
    req.size = size;
    req.poolId = poolId;
    req.reserved = 0;

    if (sendRequest(fd, MsgType::ALLOC_REQ, &req, sizeof(req)) != 0)
    {
        ::close(fd);
        return -1;
    }

    MsgHeader hdr{};
    if (recvHeader(fd, &hdr) != 0)
    {
        ::close(fd);
        return -1;
    }
    if (handleErrorResp(fd, hdr))
    {
        ::close(fd);
        return -1;
    }
    if (static_cast<MsgType>(hdr.type) != MsgType::ALLOC_RESP)
    {
        fprintf(stderr, "error: unexpected response type %u\n", hdr.type);
        ::close(fd);
        return -1;
    }

    *daxFd = -1;
    int rc = recvWithFd(fd, resp, sizeof(*resp), daxFd);
    ::close(fd);

    if (rc != 0)
    {
        fprintf(stderr, "error: recvWithFd failed: %s\n", strerror(-rc));
        return -1;
    }
    if (resp->status != 0)
    {
        fprintf(stderr, "error: alloc failed with status %d (%s)\n", resp->status, strerror(-resp->status));
        if (*daxFd >= 0)
        {
            ::close(*daxFd);
            *daxFd = -1;
        }
        return -1;
    }
    if (*daxFd < 0)
    {
        fprintf(stderr, "error: alloc succeeded but no FD received\n");
        return -1;
    }
    return 0;
}

// Helper: send free request for a handle
static int doFree(const Handle &h)
{

    int fd = connectResourceManager();
    if (fd < 0)
        return -1;

    FreeReq req{};
    req.handle = h;

    if (sendRequest(fd, MsgType::FREE_REQ, &req, sizeof(req)) != 0)
    {
        ::close(fd);
        return -1;
    }

    MsgHeader hdr{};
    if (recvHeader(fd, &hdr) != 0)
    {
        ::close(fd);
        return -1;
    }
    if (handleErrorResp(fd, hdr))
    {
        ::close(fd);
        return -1;
    }

    FreeResp resp{};
    int rc = readFull(fd, &resp, sizeof(resp));
    ::close(fd);

    if (rc != 0)
    {
        fprintf(stderr, "error: read(FreeResp) failed: %s\n", strerror(-rc));
        return -1;
    }
    if (resp.status != 0)
    {
        fprintf(stderr, "error: free failed with status %d (%s)\n", resp.status, strerror(-resp.status));
        return -1;
    }
    return 0;
}

// ----------------------------------------------------------------------------
// Commands
// ----------------------------------------------------------------------------

static int cmdStats()
{
    int fd = connectResourceManager();
    if (fd < 0)
        return 1;

    if (sendRequest(fd, MsgType::STATS_REQ, nullptr, 0) != 0)
    {
        ::close(fd);
        return 1;
    }

    MsgHeader hdr{};
    if (recvHeader(fd, &hdr) != 0)
    {
        ::close(fd);
        return 1;
    }
    if (handleErrorResp(fd, hdr))
    {
        ::close(fd);
        return 1;
    }
    if (static_cast<MsgType>(hdr.type) != MsgType::STATS_RESP)
    {
        fprintf(stderr, "error: unexpected response type %u\n", hdr.type);
        ::close(fd);
        return 1;
    }

    std::vector<uint8_t> buf(hdr.payloadLen);
    if (hdr.payloadLen > 0)
    {
        int rc = readFull(fd, buf.data(), hdr.payloadLen);
        if (rc != 0)
        {
            fprintf(stderr, "error: read(payload) failed: %s\n", strerror(-rc));
            ::close(fd);
            return 1;
        }
    }
    ::close(fd);

    if (buf.size() < sizeof(StatsRespHeader))
    {
        fprintf(stderr, "error: stats response too short (%zu bytes)\n", buf.size());
        return 1;
    }

    StatsRespHeader sh{};
    std::memcpy(&sh, buf.data(), sizeof(sh));

    fprintf(stdout, "Pools: %u\n\n", sh.numPools);

    if (sh.numPools == 0)
    {
        fprintf(stdout, "(no pools available)\n");
        return 0;
    }

    fprintf(stdout, "  %-6s  %-8s  %18s  %18s  %12s  %5s\n", "Pool", "Type", "Total", "Free", "Align", "Used%");
    fprintf(stdout,
            "  ------  --------  ------------------  ------------------  "
            "------------  -----\n");

    size_t off = sizeof(sh);
    for (uint32_t i = 0; i < sh.numPools; ++i)
    {
        if (off + sizeof(PoolInfo) > buf.size())
        {
            fprintf(stderr, "  (truncated at pool %u)\n", i);
            break;
        }
        PoolInfo pi{};
        std::memcpy(&pi, buf.data() + off, sizeof(pi));
        off += sizeof(pi);

        const char *typeStr =
            (pi.type == DaxType::DEV_DAX) ? "DEV_DAX" : "FS_DAX";
        double usedPct =
            (pi.totalSize > 0)
                ? 100.0 *
                      (1.0 - static_cast<double>(pi.freeSize) / pi.totalSize)
                : 0.0;

        fprintf(stdout, "  %-6u  %-8s  %18s  %18s  %12" PRIu64 "  %5.1f\n", pi.poolId, typeStr, formatSize(pi.totalSize).c_str(), formatSize(pi.freeSize).c_str(), pi.alignBytes, usedPct);
    }
    return 0;
}

static int cmdAlloc(int argc, char *argv[])
{
    if (argc < 1)
    {
        fprintf(stderr, "Usage: alloc <size_bytes> [pool_id]\n");
        return 1;
    }

    uint64_t size = std::strtoull(argv[0], nullptr, 0);
    uint32_t poolId = kAnyPoolId;
    if (argc >= 2)
        poolId = static_cast<uint32_t>(std::strtoul(argv[1], nullptr, 0));

    fprintf(stdout, "Allocating %" PRIu64 " bytes (pool=%u) ...\n", size, poolId);

    AllocResp resp{};
    int daxFd = -1;
    if (doAlloc(size, poolId, &resp, &daxFd) != 0)
        return 1;

    fprintf(stdout, "\nAllocation successful:\n");
    fprintf(stdout, "  region_id  = %" PRIu64 "\n", resp.handle.regionId);
    fprintf(stdout, "  offset     = %" PRIu64 "\n", resp.handle.offset);
    fprintf(stdout, "  length     = %" PRIu64 " (%s)\n", resp.handle.length, formatSize(resp.handle.length).c_str());
    fprintf(stdout, "  auth_token = %" PRIu64 "\n", resp.handle.authToken);
    fprintf(stdout, "  requested  = %" PRIu64 "\n", resp.requestedSize);
    fprintf(stdout, "  dax_fd     = %d\n", daxFd);
    fprintf(stdout,
            "\n(allocation will be reclaimed by reaper on process exit)\n");

    if (daxFd >= 0)
        ::close(daxFd);
    return 0;
}

// Full cycle: alloc -> mmap -> write pattern -> read verify -> munmap -> free
static int cmdMmap(int argc, char *argv[])
{
    if (argc < 1)
    {
        fprintf(stderr, "Usage: mmap <size_bytes> [pool_id]\n");
        return 1;
    }

    uint64_t size = std::strtoull(argv[0], nullptr, 0);
    uint32_t poolId = kAnyPoolId;
    if (argc >= 2)
        poolId = static_cast<uint32_t>(std::strtoul(argv[1], nullptr, 0));

    fprintf(stdout,
            "=== mmap round-trip test ===\n"
            "  requested size : %s\n"
            "  pool_id        : %u\n\n",
            formatSize(size).c_str(),
            poolId);

    // Step 1: Allocate
    fprintf(stdout, "[1/5] Allocating ...\n");

    AllocResp aresp{};
    int daxFd = -1;
    if (doAlloc(size, poolId, &aresp, &daxFd) != 0)
        return 1;

    Handle h = aresp.handle;
    fprintf(stdout,
            "  region_id=%" PRIu64 ", offset=%" PRIu64
            ", length=%s\n"
            "  auth_token=%" PRIu64 ", dax_fd=%d\n\n",
            h.regionId,
            h.offset,
            formatSize(h.length).c_str(),
            h.authToken,
            daxFd);

    // Step 2: mmap
    fprintf(stdout,
            "[2/5] Mapping memory (fd=%d, offset=%" PRIu64
            ", length=%" PRIu64 ") ...\n",
            daxFd,
            h.offset,
            h.length);

    void *ptr = ::mmap(nullptr, h.length, PROT_READ | PROT_WRITE, MAP_SHARED, daxFd, static_cast<off_t>(h.offset));
    if (ptr == MAP_FAILED)
    {
        fprintf(stderr, "  mmap failed: %s\n", strerror(errno));
        ::close(daxFd);
        return 1;
    }
    fprintf(stdout, "  mapped at %p\n\n", ptr);

    // Step 3: Write test pattern (use requested size, not aligned length)
    uint64_t testSize = aresp.requestedSize;
    fprintf(stdout, "[3/5] Writing test pattern (%s) ...\n", formatSize(testSize).c_str());

    uint8_t *mem = static_cast<uint8_t *>(ptr);
    for (uint64_t i = 0; i < testSize; ++i)
        mem[i] = static_cast<uint8_t>((i * 7 + 0xA5) & 0xFF);
    fprintf(stdout, "  wrote %s\n\n", formatSize(testSize).c_str());

    // Step 4: Read back and verify
    fprintf(stdout, "[4/5] Verifying data ...\n");

    uint64_t errors = 0;
    for (uint64_t i = 0; i < testSize; ++i)
    {
        uint8_t expected = static_cast<uint8_t>((i * 7 + 0xA5) & 0xFF);
        if (mem[i] != expected)
        {
            if (errors < 10)
            {
                fprintf(stderr,
                        "  MISMATCH at offset %" PRIu64
                        ": expected 0x%02X, got 0x%02X\n",
                        i,
                        expected,
                        mem[i]);
            }
            ++errors;
        }
    }

    if (errors == 0)
        fprintf(stdout, "  verified %s OK\n\n", formatSize(h.length).c_str());
    else
        fprintf(stderr, "  %" PRIu64 " byte(s) mismatched!\n\n", errors);

    // Step 5: Cleanup (munmap + close FD + free)
    fprintf(stdout, "[5/5] Cleaning up ...\n");

    ::munmap(ptr, h.length);
    fprintf(stdout, "  munmap OK\n");

    ::close(daxFd);
    fprintf(stdout, "  close(dax_fd) OK\n");

    if (doFree(h) != 0)
        return 1;
    fprintf(stdout, "  free OK\n\n");

    if (errors == 0)
        fprintf(stdout, "=== mmap test PASSED ===\n");
    else
        fprintf(stdout, "=== mmap test FAILED (%" PRIu64 " errors) ===\n", errors);

    return errors == 0 ? 0 : 1;
}

// ----------------------------------------------------------------------------
// Main
// ----------------------------------------------------------------------------

static void printUsage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s [-s socket_path] <command> [args...]\n"
            "\n"
            "Commands:\n"
            "  stats                    Query pool statistics\n"
            "  alloc  <size> [pool_id]  Allocate shared memory\n"
            "  mmap   <size> [pool_id]  Full mmap round-trip test\n"
            "\n"
            "Options:\n"
            "  -s <path>  Resource manager socket path\n"
            "             (default: /tmp/maru-resourced/maru-resourced.sock)\n"
            "\n"
            "Size accepts decimal or hex (0x prefix).\n"
            "pool_id 0xFFFFFFFF = any pool (default).\n",
            prog);
}

int main(int argc, char *argv[])
{
    int opt;
    while ((opt = getopt(argc, argv, "s:h")) != -1)
    {
        switch (opt)
        {
            case 's':
                g_socketPath = optarg;
                break;
            case 'h':
                printUsage(argv[0]);
                return 0;
            default:
                printUsage(argv[0]);
                return 1;
        }
    }

    if (optind >= argc)
    {
        printUsage(argv[0]);
        return 1;
    }

    const char *cmd = argv[optind];
    int cmdArgc = argc - optind - 1;
    char **cmdArgv = argv + optind + 1;

    if (std::strcmp(cmd, "stats") == 0)
        return cmdStats();
    else if (std::strcmp(cmd, "alloc") == 0)
        return cmdAlloc(cmdArgc, cmdArgv);
    else if (std::strcmp(cmd, "mmap") == 0)
        return cmdMmap(cmdArgc, cmdArgv);
    else
    {
        fprintf(stderr, "Unknown command: %s\n\n", cmd);
        printUsage(argv[0]);
        return 1;
    }
}
