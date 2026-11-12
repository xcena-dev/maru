#include "device_wipe.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstring>

#if defined(__x86_64__) || defined(__i386__)
#include <emmintrin.h>
#endif

namespace maru {

namespace {

// Non-temporal stores send the zeros to the device instead of leaving them
// in this host's cache, where neither the device nor other hosts would see
// them (DEV_DAX mappings are write-back cached and msync does not flush).
void streamZero(void *p, size_t len) {
#if defined(__x86_64__) || defined(__i386__)
    const __m128i zero = _mm_setzero_si128();
    auto *line = static_cast<__m128i *>(p);
    for (size_t i = 0; i < len / sizeof(__m128i); ++i) _mm_stream_si128(line + i, zero);
    _mm_sfence();
#else
    std::memset(p, 0, len);
#endif
}

} // namespace

int wipeDevice(const std::string &devPath, uint64_t size, uint64_t chunk,
               const std::atomic<bool> *cancel) {
    if (size == 0 || chunk == 0 || chunk % 4096 != 0 || size % 4096 != 0) return -EINVAL;
    const int fd = ::open(devPath.c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) return -errno;
    int rc = 0;
    for (uint64_t offset = 0; offset < size; offset += chunk) {
        if (cancel && cancel->load()) {
            rc = -ECANCELED;
            break;
        }
        const size_t len = static_cast<size_t>(std::min(chunk, size - offset));
        void *p = ::mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                         static_cast<off_t>(offset));
        if (p == MAP_FAILED) {
            rc = -errno;
            break;
        }
        streamZero(p, len);
        ::msync(p, len, MS_SYNC); // Regular-file backing (tests); DEV_DAX ignores it.
        ::munmap(p, len);
    }
    ::close(fd);
    return rc;
}

// Close every descriptor from 3 up. The child must not keep the RM's state
// directory lock, listening sockets or WAL open once the RM is gone.
static void closeInheritedFds() {
#ifdef SYS_close_range
    if (::syscall(SYS_close_range, 3U, ~0U, 0U) == 0) return;
#endif
    rlimit limit{};
    const int max = ::getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur != RLIM_INFINITY
                        ? static_cast<int>(limit.rlim_cur)
                        : 65536;
    for (int fd = 3; fd < max; ++fd) ::close(fd);
}

int wipeDeviceIsolated(const std::string &devPath, uint64_t size, uint64_t chunk,
                       const std::function<bool()> &cancelled) {
    const pid_t parent = ::getpid();
    const pid_t pid = ::fork();
    if (pid < 0) return -errno;
    if (pid == 0) {
        // Child of a multithreaded process: only system calls from here on.
        // Die with the RM (the forking thread lives until the wipe ends), and
        // do not outlive a parent that died before the request took effect.
        ::prctl(PR_SET_PDEATHSIG, SIGKILL);
        if (::getppid() != parent) ::_exit(ECANCELED);
        closeInheritedFds();
        const int rc = wipeDevice(devPath, size, chunk, nullptr);
        ::_exit(rc == 0 ? 0 : std::min(-rc, 255));
    }
    int status = 0;
    bool killed = false;
    while (true) {
        const pid_t done = ::waitpid(pid, &status, WNOHANG);
        if (done == pid) break;
        if (done < 0 && errno != EINTR) {
            const int rc = -errno;
            ::kill(pid, SIGKILL);
            return rc;
        }
        if (!killed && cancelled && cancelled()) {
            ::kill(pid, SIGKILL);
            killed = true;
        }
        ::poll(nullptr, 0, 100);
    }
    if (killed) return -ECANCELED;
    if (WIFEXITED(status)) return -WEXITSTATUS(status);
    return -EIO; // Killed by a signal: SIGBUS once the device went away.
}

} // namespace maru
