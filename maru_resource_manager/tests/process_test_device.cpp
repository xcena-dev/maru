// Linked only into maru_rm_process_test_daemon. Run production main/TCP code
// with private file-backed DAX devices and a kernel-selected loopback port.
// This executable is never installed and refuses to start without its fixture.
#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <unistd.h>

#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "checkpoint_fault.h"

namespace {
std::string fixtureRoot;

std::string redirected(const char *path) {
    const std::string p(path);
    if (p.rfind("/dev/", 0) == 0 || p.rfind("/sys/", 0) == 0) {
        if (fixtureRoot.empty()) std::abort();
        return fixtureRoot + p;
    }
    return p;
}
} // namespace

extern "C" int __real_main(int, char **);
extern "C" int __real_stat(const char *, struct stat *);
extern "C" int __real_open(const char *, int, ...);
extern "C" DIR *__real_opendir(const char *);
extern "C" ssize_t __real_readlink(const char *, char *, size_t);
extern "C" int __real_bind(int, const struct sockaddr *, socklen_t);

extern "C" int __wrap_main(int, char **) {
    const char *root = std::getenv("MARU_TEST_DAX_ROOT");
    if (!root || !std::filesystem::path(root).is_absolute() ||
        !std::filesystem::is_regular_file(std::string(root) + "/.rm-test-fixture")) {
        std::fprintf(stderr, "Refusing to start: private DAX test fixture required\n");
        return 2;
    }
    fixtureRoot = std::filesystem::canonical(root).string();
    if (const char *fault = std::getenv("MARU_TEST_CHECKPOINT_FAILURE")) {
        std::istringstream fields(fault);
        std::string file, operation, extra;
        int error = 0;
        size_t bytes = 0;
        if (!(fields >> file >> operation >> error >> bytes) || (fields >> extra) ||
            (file != "pool_0.meta.tmp" && file != "pool_1.meta.tmp" &&
             file != "global.meta.tmp") ||
            (operation != "write" && operation != "fsync") ||
            (error != ENOSPC && error != EIO)) {
            std::fprintf(stderr, "Invalid checkpoint fault specification\n");
            return 2;
        }
        checkpoint_fault::arm(fixtureRoot + "/state/" + file,
            operation == "write" ? checkpoint_fault::Operation::Write
                                 : checkpoint_fault::Operation::Fsync,
            error, bytes);
    }
    // Do not accept caller-supplied state directories or listening addresses.
    std::vector<std::string> args{
        "maru_rm_process_test_daemon", "--state-dir", fixtureRoot + "/state",
        "--host", "127.0.0.1", "--port", "1", "--num-workers", "2",
        "--grace-period", "300"};
    std::vector<char *> argv;
    for (auto &arg : args) argv.push_back(arg.data());
    argv.push_back(nullptr);
    return __real_main(static_cast<int>(args.size()), argv.data());
}

extern "C" int __wrap_stat(const char *path, struct stat *st) {
    const int rc = __real_stat(redirected(path).c_str(), st);
    if (rc == 0 && std::string(path).rfind("/dev/dax", 0) == 0) {
        st->st_mode = (st->st_mode & ~S_IFMT) | S_IFCHR;
        st->st_rdev = makedev(240, 0);
    }
    return rc;
}

extern "C" int __wrap_open(const char *path, int flags, ...) {
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, unsigned int);
        va_end(args);
    }
    return __real_open(redirected(path).c_str(), flags, mode);
}

extern "C" DIR *__wrap_opendir(const char *path) {
    return __real_opendir(redirected(path).c_str());
}

extern "C" ssize_t __wrap_readlink(const char *path, char *buf, size_t size) {
    return __real_readlink(redirected(path).c_str(), buf, size);
}

extern "C" int __wrap_bind(int fd, const struct sockaddr *address, socklen_t size) {
    if (fixtureRoot.empty() || size != sizeof(sockaddr_in) ||
        address->sa_family != AF_INET) {
        errno = EINVAL;
        return -1;
    }
    auto addr = *reinterpret_cast<const sockaddr_in *>(address);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0; // No race-prone probe/close/rebind or production RM port.
    const int rc = __real_bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    if (rc != 0) return rc;
    socklen_t boundSize = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &boundSize) != 0)
        return -1;
    std::ofstream portFile(fixtureRoot + "/port");
    portFile << ntohs(addr.sin_port) << '\n';
    portFile.close();
    if (!portFile) { errno = EIO; return -1; }
    return 0;
}
