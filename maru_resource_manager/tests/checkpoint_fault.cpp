#include "checkpoint_fault.h"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <limits.h>

namespace checkpoint_fault {
namespace {
std::string target;
Operation operation = Operation::Write;
int failureError = EIO;
size_t remainingBytes = 0;
std::string truncateTarget;
int truncateError = EIO;
unsigned failureCount = 0;
unsigned shortWriteCount = 0;

bool isPath(int fd, const std::string &path) {
    char link[64];
    std::snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    char resolved[PATH_MAX];
    const ssize_t length = ::readlink(link, resolved, sizeof(resolved));
    return length > 0 && std::string(resolved, static_cast<size_t>(length)) == path;
}

bool matches(int fd, Operation requested) {
    return !target.empty() && operation == requested && isPath(fd, target);
}

int fail(const char *what, const std::string &path, int error) {
    ++failureCount;
    std::fprintf(stderr, "Injected %s failure: %s (errno=%d)\n", what, path.c_str(), error);
    errno = error;
    return -1;
}
} // namespace

void arm(const std::string &path, Operation op, int error, size_t bytesBeforeFailure) {
    target = path;
    operation = op;
    failureError = error;
    remainingBytes = bytesBeforeFailure;
    failureCount = shortWriteCount = 0;
}

void armTruncate(const std::string &path, int error) {
    truncateTarget = path;
    truncateError = error;
}

void clear() {
    target.clear();
    truncateTarget.clear();
}
unsigned failures() { return failureCount; }
unsigned shortWrites() { return shortWriteCount; }
} // namespace checkpoint_fault

extern "C" ssize_t __real_write(int, const void *, size_t);
extern "C" int __real_fsync(int);
extern "C" int __real_fdatasync(int);
extern "C" int __real_ftruncate(int, off_t);

extern "C" ssize_t __wrap_write(int fd, const void *buf, size_t count) {
    using namespace checkpoint_fault;
    if (!matches(fd, Operation::Write)) return __real_write(fd, buf, count);
    if (remainingBytes == 0) return fail("write", target, failureError);
    const size_t allowed = std::min(count, remainingBytes);
    const ssize_t written = __real_write(fd, buf, allowed);
    if (written > 0) {
        remainingBytes -= static_cast<size_t>(written);
        if (static_cast<size_t>(written) < count) ++shortWriteCount;
    }
    return written;
}

extern "C" int __wrap_fsync(int fd) {
    using namespace checkpoint_fault;
    if (matches(fd, Operation::Fsync)) return fail("fsync", target, failureError);
    return __real_fsync(fd);
}

extern "C" int __wrap_fdatasync(int fd) {
    using namespace checkpoint_fault;
    if (matches(fd, Operation::Fdatasync)) return fail("fdatasync", target, failureError);
    return __real_fdatasync(fd);
}

extern "C" int __wrap_ftruncate(int fd, off_t length) {
    using namespace checkpoint_fault;
    if (!truncateTarget.empty() && isPath(fd, truncateTarget))
        return fail("ftruncate", truncateTarget, truncateError);
    return __real_ftruncate(fd, length);
}
