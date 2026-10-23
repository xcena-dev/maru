#include "region_holds.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <vector>

#include "log.h"
#include "util.h"

namespace maru {

static constexpr const char *kHoldsFile = "holds";
static constexpr size_t kMaxHoldNameLen = 128;

bool validHoldName(const std::string &name) {
    if (name.empty() || name.size() > kMaxHoldNameLen) return false;
    for (char c : name)
        if (c < 0x21 || c > 0x7E) return false;
    return true;
}

static bool parseRegionId(const std::string &text, uint64_t &out) {
    if (text.empty() || text.size() > 20) return false;
    for (char c : text)
        if (c < '0' || c > '9') return false;
    errno = 0;
    char *end = nullptr;
    const unsigned long long value = std::strtoull(text.c_str(), &end, 10);
    if (errno != 0 || *end != '\0' || value == 0) return false;
    out = value;
    return true;
}

int loadRegionHolds(const std::string &stateDir, RegionHolds &holds,
                    std::set<uint64_t> &draining) {
    const std::string path = stateDir + "/" + kHoldsFile;
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno != ENOENT) return -errno;
        holds.clear();
        draining.clear();
        return 0;
    }
    std::string text;
    char buf[4096];
    ssize_t n;
    while ((n = ::read(fd, buf, sizeof(buf))) > 0) text.append(buf, static_cast<size_t>(n));
    const int err = n < 0 ? -errno : 0;
    ::close(fd);
    if (err != 0) return err;

    RegionHolds loadedHolds;
    std::set<uint64_t> loadedDraining;
    std::istringstream lines(text);
    std::string line;
    for (size_t number = 1; std::getline(lines, line); ++number) {
        std::istringstream words(line);
        std::vector<std::string> fields;
        for (std::string word; words >> word;) fields.push_back(word);
        uint64_t region = 0;
        bool valid = false;
        // One record per region and holder; this file is only ever written whole.
        if (fields.size() == 4 && fields[0] == "hold" && parseRegionId(fields[1], region) &&
            validHoldName(fields[2]) && validHoldName(fields[3]))
            valid = loadedHolds[region].emplace(fields[2], fields[3]).second;
        else if (fields.size() == 2 && fields[0] == "draining" && parseRegionId(fields[1], region))
            valid = loadedDraining.insert(region).second;
        if (!valid) {
            logf(LogLevel::Error,
                 "%s:%zu: malformed record '%s'; fix or restore the file (removing a hold "
                 "lets a remote agent's late write reach the region's next owner)",
                 path.c_str(), number, line.c_str());
            return -EINVAL;
        }
    }
    holds = std::move(loadedHolds);
    draining = std::move(loadedDraining);
    return 0;
}

int saveRegionHolds(const std::string &stateDir, const RegionHolds &holds,
                    const std::set<uint64_t> &draining, int &syncRc) {
    syncRc = 0;
    const std::string path = stateDir + "/" + kHoldsFile;
    const std::string tmp = path + ".tmp";
    std::string text;
    for (const auto &[region, holders] : holds)
        for (const auto &[holder, instance] : holders)
            text += "hold " + std::to_string(region) + " " + holder + " " + instance + "\n";
    for (uint64_t region : draining) text += "draining " + std::to_string(region) + "\n";

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
    // The rename is the commit point: callers must treat the new records as
    // in effect, and judge from syncRc whether they may build on them yet.
    syncRc = syncDirectory(stateDir);
    return 0;
}

} // namespace maru
