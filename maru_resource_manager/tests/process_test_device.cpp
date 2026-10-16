// Linked only into maru_rm_process_test_daemon. Run production main/TCP code
// with private file-backed DAX devices (fake_dax_fs.cpp) and a kernel-selected
// loopback port. This executable is never installed and refuses to start
// without its fixture.
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "checkpoint_fault.h"
#include "fake_dax_fs.h"

extern "C" int __real_main(int, char **);
extern "C" int __real_bind(int, const struct sockaddr *, socklen_t);

extern "C" int __wrap_main(int, char **) {
    const char *root = std::getenv("MARU_TEST_DAX_ROOT");
    if (!root || !std::filesystem::path(root).is_absolute() ||
        !std::filesystem::is_regular_file(std::string(root) + "/.rm-test-fixture")) {
        std::fprintf(stderr, "Refusing to start: private DAX test fixture required\n");
        return 2;
    }
    fakeRoot = std::filesystem::canonical(root).string();
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
        checkpoint_fault::arm(fakeRoot + "/state/" + file,
            operation == "write" ? checkpoint_fault::Operation::Write
                                 : checkpoint_fault::Operation::Fsync,
            error, bytes);
    }
    // Do not accept caller-supplied state directories or listening addresses.
    std::vector<std::string> args{
        "maru_rm_process_test_daemon", "--state-dir", fakeRoot + "/state",
        "--host", "127.0.0.1", "--port", "1", "--num-workers", "2",
        "--grace-period", "300"};
    std::vector<char *> argv;
    for (auto &arg : args) argv.push_back(arg.data());
    argv.push_back(nullptr);
    return __real_main(static_cast<int>(args.size()), argv.data());
}

extern "C" int __wrap_bind(int fd, const struct sockaddr *address, socklen_t size) {
    // The admin socket lives in the fixture's state directory.
    if (!fakeRoot.empty() && address->sa_family == AF_UNIX &&
        std::string(reinterpret_cast<const sockaddr_un *>(address)->sun_path)
                .rfind(fakeRoot + "/", 0) == 0)
        return __real_bind(fd, address, size);
    if (fakeRoot.empty() || size != sizeof(sockaddr_in) ||
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
    std::ofstream portFile(fakeRoot + "/port");
    portFile << ntohs(addr.sin_port) << '\n';
    portFile.close();
    if (!portFile) { errno = EIO; return -1; }
    return 0;
}
