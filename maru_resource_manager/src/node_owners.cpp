#include "node_owners.h"

#include <fcntl.h>
#include <openssl/rand.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <sstream>
#include <vector>

#include "log.h"
#include "pool_manager.h"
#include "region_holds.h"
#include "util.h"

namespace maru {

static constexpr const char *kNodesFile = "nodes";
static constexpr const char *kRmIdFile = "rm_id";
static constexpr const char *kNodeOwnerPrefix = "@node:";
static constexpr size_t kNodeOwnerPrefixLen = 6;
static constexpr size_t kRmIdLen = 32;

bool validNodeId(const std::string &id) {
    return validHoldName(id) && id.size() < kMaxClientIdLen - kNodeOwnerPrefixLen;
}

std::string nodeOwnerId(const std::string &nodeId) { return kNodeOwnerPrefix + nodeId; }

std::string nodeIdOfOwner(const char *clientId) {
    if (std::strncmp(clientId, kNodeOwnerPrefix, kNodeOwnerPrefixLen) != 0) return "";
    return clientId + kNodeOwnerPrefixLen;
}

static int readSmallFile(const std::string &path, std::string &text) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;
    char buf[4096];
    ssize_t n;
    while ((n = ::read(fd, buf, sizeof(buf))) > 0) text.append(buf, static_cast<size_t>(n));
    const int err = n < 0 ? -errno : 0;
    ::close(fd);
    return err;
}

// Temporary file, fsync, rename. The rename is the commit point; syncRc says
// whether the directory sync made it durable.
static int replaceFile(const std::string &stateDir, const char *name, const std::string &text,
                       int &syncRc) {
    syncRc = 0;
    const std::string path = stateDir + "/" + name;
    const std::string tmp = path + ".tmp";
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
    syncRc = syncDirectory(stateDir);
    return 0;
}

int loadNodeOwners(const std::string &stateDir, NodeOwners &nodes, NodeOwners &releasing,
                   EndedRuns &ended) {
    const std::string path = stateDir + "/" + kNodesFile;
    std::string text;
    const int rc = readSmallFile(path, text);
    if (rc == -ENOENT) {
        nodes.clear();
        releasing.clear();
        ended.clear();
        return 0;
    }
    if (rc != 0) return rc;

    NodeOwners loaded, loadedReleasing;
    EndedRuns loadedEnded;
    auto endedOf = [&](const std::string &id) -> const std::vector<std::string> * {
        auto it = loadedEnded.find(id);
        return it == loadedEnded.end() ? nullptr : &it->second;
    };
    auto listed = [](const std::vector<std::string> *runs, const std::string &epoch) {
        return runs && std::find(runs->begin(), runs->end(), epoch) != runs->end();
    };
    std::istringstream lines(text);
    std::string line;
    for (size_t number = 1; std::getline(lines, line); ++number) {
        std::istringstream words(line);
        std::vector<std::string> fields;
        for (std::string word; words >> word;) fields.push_back(word);
        bool valid = false;
        if (fields.size() == 4 && fields[0] == "node") {
            valid = validNodeId(fields[1]) && validHoldName(fields[2]) &&
                    validHoldName(fields[3]) && loadedEnded.empty() && loadedReleasing.empty() &&
                    loaded.emplace(fields[1], NodeOwner{fields[2], fields[3]}).second;
        } else if (fields.size() == 3 && fields[0] == "ended") {
            // An ended run is not the current one, and is listed once.
            auto current = loaded.find(fields[1]);
            valid = validNodeId(fields[1]) && validHoldName(fields[2]) &&
                    loadedReleasing.empty() &&
                    (current == loaded.end() || current->second.epoch != fields[2]) &&
                    !listed(endedOf(fields[1]), fields[2]);
            if (valid) loadedEnded[fields[1]].push_back(fields[2]);
        } else if (fields.size() == 4 && fields[0] == "releasing") {
            // The node's last run, ended, and the node has no current run.
            const auto *runs = endedOf(fields[1]);
            valid = validNodeId(fields[1]) && validHoldName(fields[2]) &&
                    validHoldName(fields[3]) && !loaded.count(fields[1]) && runs &&
                    runs->back() == fields[2] &&
                    loadedReleasing.emplace(fields[1], NodeOwner{fields[2], fields[3]}).second;
        }
        if (!valid) {
            logf(LogLevel::Error,
                 "%s:%zu: malformed record '%s'; fix or restore the file (a lost record lets "
                 "an earlier run of the node, or another host, use its regions)",
                 path.c_str(), number, line.c_str());
            return -EINVAL;
        }
    }
    for (auto &[id, runs] : loadedEnded)
        if (runs.size() > kMaxEndedRuns)
            runs.erase(runs.begin(), runs.end() - static_cast<std::ptrdiff_t>(kMaxEndedRuns));
    nodes = std::move(loaded);
    releasing = std::move(loadedReleasing);
    ended = std::move(loadedEnded);
    return 0;
}

int saveNodeOwners(const std::string &stateDir, const NodeOwners &nodes,
                   const NodeOwners &releasing, const EndedRuns &ended, int &syncRc) {
    std::string text;
    for (const auto &[id, node] : nodes)
        text += "node " + id + " " + node.epoch + " " + node.host + "\n";
    for (const auto &[id, runs] : ended)
        for (const auto &epoch : runs) text += "ended " + id + " " + epoch + "\n";
    for (const auto &[id, node] : releasing)
        text += "releasing " + id + " " + node.epoch + " " + node.host + "\n";
    return replaceFile(stateDir, kNodesFile, text, syncRc);
}

int loadRmId(const std::string &stateDir, std::string &id) {
    const std::string path = stateDir + "/" + kRmIdFile;
    std::string text;
    const int rc = readSmallFile(path, text);
    id.clear();
    if (rc == -ENOENT) return 0;
    if (rc != 0) return rc;
    if (!text.empty() && text.back() == '\n') text.pop_back();
    bool valid = text.size() == kRmIdLen;
    for (char c : text) valid &= (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    if (!valid) {
        logf(LogLevel::Error,
             "%s is malformed; restore it (nodes name their CXL pools after it)", path.c_str());
        return -EINVAL;
    }
    id = text;
    return 0;
}

int createRmId(const std::string &stateDir, std::string &id) {
    unsigned char bytes[kRmIdLen / 2];
    if (RAND_bytes(bytes, sizeof(bytes)) != 1) return -EIO;
    static const char digits[] = "0123456789abcdef";
    std::string text;
    for (unsigned char b : bytes) {
        text += digits[b >> 4];
        text += digits[b & 0xf];
    }
    int syncRc = 0;
    const int rc = replaceFile(stateDir, kRmIdFile, text + "\n", syncRc);
    if (rc != 0) return rc;
    // A power loss must not bring up a different ID next time.
    if (syncRc != 0) return syncRc;
    id = text;
    return 0;
}

} // namespace maru
