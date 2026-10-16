#include "device_exclusion.h"

#include <fcntl.h>
#include <unistd.h>

#include <cctype>
#include <cerrno>
#include <cstring>
#include <sstream>

#include "log.h"
#include "util.h"

namespace maru {

static constexpr const char *kExcludedDevicesFile = "excluded_devices";
static constexpr size_t kMaxExcludedDevicesBytes = 64 * 1024;

// Digits without a leading zero, as the kernel names dax devices.
static bool canonicalNumber(const std::string &s) {
    if (s.empty() || (s.size() > 1 && s[0] == '0')) return false;
    for (char c : s)
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    return true;
}

bool normalizeDaxName(const std::string &in, std::string &out) {
    const std::string name = in.rfind("/dev/", 0) == 0 ? in.substr(5) : in;
    if (name.rfind("dax", 0) != 0) return false;
    const size_t dot = name.find('.');
    if (dot == std::string::npos || !canonicalNumber(name.substr(3, dot - 3)) ||
        !canonicalNumber(name.substr(dot + 1)))
        return false;
    out = name;
    return true;
}

int loadExcludedDevices(const std::string &stateDir, std::set<std::string> &out) {
    const std::string path = stateDir + "/" + kExcludedDevicesFile;
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno != ENOENT) return -errno;
        out.clear();
        return 0;
    }
    std::string text;
    char buf[4096];
    ssize_t n;
    while ((n = ::read(fd, buf, sizeof(buf))) > 0 && text.size() <= kMaxExcludedDevicesBytes)
        text.append(buf, static_cast<size_t>(n));
    const int err = n < 0 ? -errno : 0;
    ::close(fd);
    if (err != 0) return err;
    if (text.size() > kMaxExcludedDevicesBytes) return -EFBIG;

    std::set<std::string> names;
    std::istringstream lines(text);
    std::string line;
    while (std::getline(lines, line)) {
        const size_t begin = line.find_first_not_of(" \t\r");
        if (begin == std::string::npos || line[begin] == '#') continue;
        line = line.substr(begin, line.find_last_not_of(" \t\r") - begin + 1);
        std::string name;
        if (!normalizeDaxName(line, name)) {
            logf(LogLevel::Error, "%s: invalid device entry '%s'", path.c_str(),
                 line.c_str());
            return -EINVAL;
        }
        names.insert(name);
    }
    out = std::move(names);
    return 0;
}

int syncExcludedDevices(const std::string &stateDir) { return syncDirectory(stateDir); }

int saveExcludedDevices(const std::string &stateDir,
                        const std::set<std::string> &names, int &syncRc) {
    syncRc = 0;
    const std::string path = stateDir + "/" + kExcludedDevicesFile;
    const std::string tmp = path + ".tmp";
    std::string text =
        "# Devices maru-resource-manager must not use. Managed by maru-rm-ctl.\n";
    for (const auto &name : names) text += name + "\n";

    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return -errno;
    int rc = writeFull(fd, text.data(), text.size());
    if (rc == 0 && ::fsync(fd) != 0) rc = -errno;
    ::close(fd);
    if (rc == 0 && ::rename(tmp.c_str(), path.c_str()) != 0) rc = -errno;
    if (rc != 0) {
        ::unlink(tmp.c_str());
        return rc;
    }
    // The rename is the commit point: callers must treat the new list as in
    // effect. Syncing the directory only makes it survive a power loss, which
    // callers must confirm before a device is handed to another owner.
    syncRc = syncExcludedDevices(stateDir);
    if (syncRc != 0)
        logf(LogLevel::Warn, "%s saved but its directory was not synced: %s", path.c_str(),
             std::strerror(-syncRc));
    return 0;
}

} // namespace maru
