#include "node_owners.h"

#include <fcntl.h>
#include <openssl/rand.h>
#include <unistd.h>

#include <cerrno>
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

int loadNodeOwners(const std::string &stateDir, NodeOwners &nodes) {
    const std::string path = stateDir + "/" + kNodesFile;
    std::string text;
    const int rc = readSmallFile(path, text);
    if (rc == -ENOENT) {
        nodes.clear();
        return 0;
    }
    if (rc != 0) return rc;

    NodeOwners loaded;
    std::istringstream lines(text);
    std::string line;
    for (size_t number = 1; std::getline(lines, line); ++number) {
        std::istringstream words(line);
        std::vector<std::string> fields;
        for (std::string word; words >> word;) fields.push_back(word);
        const bool valid = fields.size() == 4 && fields[0] == "node" && validNodeId(fields[1]) &&
                           validHoldName(fields[2]) && validHoldName(fields[3]) &&
                           loaded.emplace(fields[1], NodeOwner{fields[2], fields[3]}).second;
        if (!valid) {
            logf(LogLevel::Error,
                 "%s:%zu: malformed record '%s'; fix or restore the file (a lost record lets "
                 "an earlier run of the node, or another host, use its regions)",
                 path.c_str(), number, line.c_str());
            return -EINVAL;
        }
    }
    nodes = std::move(loaded);
    return 0;
}

int saveNodeOwners(const std::string &stateDir, const NodeOwners &nodes, int &syncRc) {
    std::string text;
    for (const auto &[id, node] : nodes)
        text += "node " + id + " " + node.epoch + " " + node.host + "\n";
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
