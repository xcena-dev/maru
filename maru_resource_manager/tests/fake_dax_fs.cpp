// See fake_dax_fs.h.
#include "fake_dax_fs.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <unistd.h>

#include <cstdarg>
#include <cstdlib>

std::string fakeRoot;
int (*fakeOpenHook)(const char *path) = nullptr;

namespace {
std::string redirected(const char *path) {
    const std::string p(path);
    if (p.rfind("/dev/", 0) == 0 || p.rfind("/sys/", 0) == 0) {
        if (fakeRoot.empty()) std::abort();
        return fakeRoot + p;
    }
    return p;
}
} // namespace

extern "C" int __real_stat(const char *, struct stat *);
extern "C" int __real_open(const char *, int, ...);
extern "C" DIR *__real_opendir(const char *);
extern "C" ssize_t __real_readlink(const char *, char *, size_t);

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
    if (fakeOpenHook && fakeOpenHook(path) != 0) return -1;
    return __real_open(redirected(path).c_str(), flags, mode);
}

extern "C" DIR *__wrap_opendir(const char *path) {
    return __real_opendir(redirected(path).c_str());
}

extern "C" ssize_t __wrap_readlink(const char *path, char *buf, size_t size) {
    return __real_readlink(redirected(path).c_str(), buf, size);
}
