// maru_test_client - Interactive test client for Maru Resource Manager
//
// Communicates with the resource manager over TCP using the binary IPC protocol.
// Supports stats, alloc, and a full mmap round-trip test.
//
// Usage:
//   maru_test_client [-H host] [-p port] <command> [args...]
//
// Commands:
//   stats                      Query pool statistics
//   alloc <size> [dax_path]   Allocate shared memory
//   mmap <size> [dax_path]    Full cycle: alloc -> mmap -> write -> read -> verify -> free

#include <arpa/inet.h>
#include <fcntl.h>
#include <getopt.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <atomic>
#include <string>
#include <vector>

#include "ipc.h"

using namespace maru;

static const char *g_host = "127.0.0.1";
static uint16_t g_port = 9850;
// Empty pool path means "any pool" — replaces the old kAnyPoolId sentinel.

// Client identity and request sequencing for the v2 protocol.
static std::string g_clientId;
static std::atomic<uint64_t> g_requestSeq{1};

static std::string makeClientId()
{
    char hostname[256];
    if (::gethostname(hostname, sizeof(hostname)) != 0)
        std::snprintf(hostname, sizeof(hostname), "unknown");
    return std::string(hostname) + ":" + std::to_string(::getpid());
}

/// Build a v2 request payload: [fixed struct][uint16 idLen][client_id][uint64 requestId]
static std::vector<uint8_t> buildPayload(const void *fixedStruct, size_t fixedSize,
                                          const std::string &clientId, uint64_t requestId)
{
    uint16_t idLen = static_cast<uint16_t>(clientId.size());
    size_t totalSize = fixedSize + sizeof(idLen) + idLen + sizeof(requestId);
    std::vector<uint8_t> buf(totalSize);

    size_t off = 0;
    std::memcpy(buf.data() + off, fixedStruct, fixedSize);
    off += fixedSize;
    std::memcpy(buf.data() + off, &idLen, sizeof(idLen));
    off += sizeof(idLen);
    std::memcpy(buf.data() + off, clientId.data(), idLen);
    off += idLen;
    std::memcpy(buf.data() + off, &requestId, sizeof(requestId));
    return buf;
}

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

// ----------------------------------------------------------------------------
// Protocol helpers
// ----------------------------------------------------------------------------

static int connectResourceManager()
{
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0)
    {
        fprintf(stderr, "error: socket() failed: %s\n", strerror(errno));
        return -1;
    }

    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(g_port);
    if (::inet_pton(AF_INET, g_host, &addr.sin_addr) != 1)
    {
        fprintf(stderr, "error: invalid host address: %s\n", g_host);
        ::close(fd);
        return -1;
    }

    if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0)
    {
        fprintf(stderr, "error: connect(%s:%u) failed: %s\n", g_host, g_port, strerror(errno));
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

    fprintf(stderr, "error: resource manager returned status=%d: %s\n",
            err.status, msg.empty() ? "(no message)" : msg.c_str());
    return true;
}

static std::string formatSize(uint64_t bytes)
{
    char buf[64];
    if (bytes >= (1ULL << 30))
        std::snprintf(buf, sizeof(buf), "%" PRIu64 " (%.2f GiB)", bytes,
                      static_cast<double>(bytes) / (1ULL << 30));
    else if (bytes >= (1ULL << 20))
        std::snprintf(buf, sizeof(buf), "%" PRIu64 " (%.2f MiB)", bytes,
                      static_cast<double>(bytes) / (1ULL << 20));
    else if (bytes >= (1ULL << 10))
        std::snprintf(buf, sizeof(buf), "%" PRIu64 " (%.2f KiB)", bytes,
                      static_cast<double>(bytes) / (1ULL << 10));
    else
        std::snprintf(buf, sizeof(buf), "%" PRIu64 " B", bytes);
    return buf;
}

/// Parse device_path from AllocResp payload (after fixed 48-byte struct).
static std::string parseDevicePath(const uint8_t *data, size_t dataLen)
{
    if (dataLen <= sizeof(AllocResp) + sizeof(uint32_t))
        return "";
    uint32_t pathLen = 0;
    std::memcpy(&pathLen, data + sizeof(AllocResp), sizeof(pathLen));
    size_t pathOff = sizeof(AllocResp) + sizeof(pathLen);
    if (pathLen == 0 || pathOff + pathLen > dataLen)
        return "";
    return std::string(reinterpret_cast<const char *>(data + pathOff), pathLen);
}

/// Build AllocReq payload: fixed struct + pool path bytes + client_id + request_id
static std::vector<uint8_t> buildAllocPayload(const AllocReq &req,
                                               const std::string &daxPath,
                                               const std::string &clientId,
                                               uint64_t requestId)
{
    uint16_t idLen = static_cast<uint16_t>(clientId.size());
    size_t totalSize = sizeof(req) + daxPath.size() + sizeof(idLen) + idLen + sizeof(requestId);
    std::vector<uint8_t> buf(totalSize);

    size_t off = 0;
    std::memcpy(buf.data() + off, &req, sizeof(req));
    off += sizeof(req);
    if (!daxPath.empty())
    {
        std::memcpy(buf.data() + off, daxPath.data(), daxPath.size());
        off += daxPath.size();
    }
    std::memcpy(buf.data() + off, &idLen, sizeof(idLen));
    off += sizeof(idLen);
    std::memcpy(buf.data() + off, clientId.data(), idLen);
    off += idLen;
    std::memcpy(buf.data() + off, &requestId, sizeof(requestId));
    return buf;
}

// Helper: send alloc request and receive response with device path
static int doAlloc(uint64_t size, const std::string &daxPath, AllocResp *resp,
                   std::string *devicePath)
{
    int fd = connectResourceManager();
    if (fd < 0)
        return -1;

    AllocReq req{};
    req.size = size;
    req.daxPathLen = static_cast<uint32_t>(daxPath.size());
    req.reserved = 0;

    auto payload = buildAllocPayload(req, daxPath, g_clientId, g_requestSeq++);
    if (sendRequest(fd, MsgType::ALLOC_REQ, payload.data(),
                    static_cast<uint32_t>(payload.size())) != 0)
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

    std::vector<uint8_t> respBuf(hdr.payloadLen);
    int rc = readFull(fd, respBuf.data(), hdr.payloadLen);
    ::close(fd);

    if (rc != 0)
    {
        fprintf(stderr, "error: read(AllocResp) failed: %s\n", strerror(-rc));
        return -1;
    }
    if (respBuf.size() < sizeof(AllocResp))
    {
        fprintf(stderr, "error: AllocResp too short\n");
        return -1;
    }

    std::memcpy(resp, respBuf.data(), sizeof(*resp));
    *devicePath = parseDevicePath(respBuf.data(), respBuf.size());

    if (resp->status != 0)
    {
        fprintf(stderr, "error: alloc failed with status %d (%s)\n",
                resp->status, strerror(-resp->status));
        return -1;
    }
    if (devicePath->empty())
    {
        fprintf(stderr, "error: alloc succeeded but no device path received\n");
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

    auto payload = buildPayload(&req, sizeof(req), g_clientId, g_requestSeq++);
    if (sendRequest(fd, MsgType::FREE_REQ, payload.data(),
                    static_cast<uint32_t>(payload.size())) != 0)
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
        fprintf(stderr, "error: free failed with status %d (%s)\n",
                resp.status, strerror(-resp.status));
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

    fprintf(stdout, "  %-6s  %-8s  %18s  %18s  %12s  %5s\n",
            "Pool", "Type", "Total", "Free", "Align", "Used%");
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
                ? 100.0 * (1.0 - static_cast<double>(pi.freeSize) / pi.totalSize)
                : 0.0;

        fprintf(stdout, "  %-6u  %-8s  %18s  %18s  %12" PRIu64 "  %5.1f\n",
                pi.poolId, typeStr,
                formatSize(pi.totalSize).c_str(),
                formatSize(pi.freeSize).c_str(),
                pi.alignBytes, usedPct);
    }
    return 0;
}

static int cmdAlloc(int argc, char *argv[])
{
    if (argc < 1)
    {
        fprintf(stderr, "Usage: alloc <size_bytes> [dax_path]\n");
        return 1;
    }

    uint64_t size = std::strtoull(argv[0], nullptr, 0);
    std::string daxPath;  // empty = any pool
    if (argc >= 2)
        daxPath = argv[1];

    fprintf(stdout, "Allocating %" PRIu64 " bytes (pool=%s) ...\n",
            size, daxPath.empty() ? "(any)" : daxPath.c_str());

    AllocResp resp{};
    std::string devicePath;
    if (doAlloc(size, daxPath, &resp, &devicePath) != 0)
        return 1;

    fprintf(stdout, "\nAllocation successful:\n");
    fprintf(stdout, "  region_id   = %" PRIu64 "\n", resp.handle.regionId);
    fprintf(stdout, "  offset      = %" PRIu64 "\n", resp.handle.offset);
    fprintf(stdout, "  length      = %" PRIu64 " (%s)\n", resp.handle.length,
            formatSize(resp.handle.length).c_str());
    fprintf(stdout, "  auth_token  = %" PRIu64 "\n", resp.handle.authToken);
    fprintf(stdout, "  requested   = %" PRIu64 "\n", resp.requestedSize);
    fprintf(stdout, "  device_path = %s\n", devicePath.c_str());
    fprintf(stdout,
            "\n(allocation will be reclaimed by reaper on process exit)\n");

    return 0;
}

// Full cycle: alloc -> open(path) -> mmap -> write -> read verify -> munmap -> free
static int cmdMmap(int argc, char *argv[])
{
    if (argc < 1)
    {
        fprintf(stderr, "Usage: mmap <size_bytes> [dax_path]\n");
        return 1;
    }

    uint64_t size = std::strtoull(argv[0], nullptr, 0);
    std::string daxPath;  // empty = any pool
    if (argc >= 2)
        daxPath = argv[1];

    fprintf(stdout,
            "=== mmap round-trip test ===\n"
            "  requested size : %s\n"
            "  dax_path      : %s\n\n",
            formatSize(size).c_str(),
            daxPath.empty() ? "(any)" : daxPath.c_str());

    // Step 1: Allocate
    fprintf(stdout, "[1/5] Allocating ...\n");

    AllocResp aresp{};
    std::string devicePath;
    if (doAlloc(size, daxPath, &aresp, &devicePath) != 0)
        return 1;

    Handle h = aresp.handle;
    fprintf(stdout,
            "  region_id=%" PRIu64 ", offset=%" PRIu64
            ", length=%s\n"
            "  auth_token=%" PRIu64 ", path=%s\n\n",
            h.regionId, h.offset, formatSize(h.length).c_str(),
            h.authToken, devicePath.c_str());

    // Step 2: open device path + mmap
    fprintf(stdout,
            "[2/5] Opening %s and mapping memory (offset=%" PRIu64
            ", length=%" PRIu64 ") ...\n",
            devicePath.c_str(), h.offset, h.length);

    int daxFd = ::open(devicePath.c_str(), O_RDWR);
    if (daxFd < 0)
    {
        fprintf(stderr, "  open(%s) failed: %s\n", devicePath.c_str(), strerror(errno));
        return 1;
    }

    void *ptr = ::mmap(nullptr, h.length, PROT_READ | PROT_WRITE, MAP_SHARED,
                       daxFd, static_cast<off_t>(h.offset));
    ::close(daxFd);  // mmap keeps its own reference

    if (ptr == MAP_FAILED)
    {
        fprintf(stderr, "  mmap failed: %s\n", strerror(errno));
        return 1;
    }
    fprintf(stdout, "  mapped at %p\n\n", ptr);

    // Step 3: Write test pattern
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
                fprintf(stderr,
                        "  MISMATCH at offset %" PRIu64
                        ": expected 0x%02X, got 0x%02X\n",
                        i, expected, mem[i]);
            ++errors;
        }
    }

    if (errors == 0)
        fprintf(stdout, "  verified %s OK\n\n", formatSize(testSize).c_str());
    else
        fprintf(stderr, "  %" PRIu64 " byte(s) mismatched!\n\n", errors);

    // Step 5: Cleanup
    fprintf(stdout, "[5/5] Cleaning up ...\n");

    ::munmap(ptr, h.length);
    fprintf(stdout, "  munmap OK\n");

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
            "Usage: %s [-H host] [-p port] <command> [args...]\n"
            "\n"
            "Commands:\n"
            "  stats                    Query pool statistics\n"
            "  alloc  <size> [dax_path]  Allocate shared memory\n"
            "  mmap   <size> [dax_path]  Full mmap round-trip test\n"
            "\n"
            "Options:\n"
            "  -H <host>  Resource manager host (default: 127.0.0.1)\n"
            "  -p <port>  Resource manager port (default: 9850)\n"
            "\n"
            "Size accepts decimal or hex (0x prefix).\n"
            "dax_path empty = any pool (default).\n",
            prog);
}

int main(int argc, char *argv[])
{
    g_clientId = makeClientId();

    int opt;
    while ((opt = getopt(argc, argv, "H:p:h")) != -1)
    {
        switch (opt)
        {
            case 'H':
                g_host = optarg;
                break;
            case 'p':
                g_port = static_cast<uint16_t>(std::atoi(optarg));
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
