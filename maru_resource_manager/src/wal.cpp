#include "wal.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <functional>

#include "log.h"
#include "metadata.h"
#include "util.h"
#include "pool_validation.h"

namespace maru {

static constexpr const char *kLegacyWalName = "maru_resourced.wal";
static constexpr const char *kCurrentWalName = "maru-resource-manager.wal";

static constexpr uint32_t kWalMagic = 0x57414C21; // 'WAL!'
static constexpr uint32_t kWalVersion = 7;
// Same record layout, but v6 writers acknowledged records whose append had
// failed, so a torn v6 record may have been acknowledged. Replay only.
static constexpr uint32_t kLegacyWalVersion = 6;

struct WalHeader {
  uint32_t magic;
  uint32_t version;
  uint32_t type;
  uint32_t reserved;
  uint32_t payloadLen;
  uint64_t ts;
};

struct WalFreeV3 {
  uint64_t regionId;
};

WalStore::WalStore(const std::string &stateDir) : stateDir_(stateDir) {
  // Migrate legacy WAL filename if present
  std::string legacyPath = stateDir_ + "/" + kLegacyWalName;
  std::string currentPath = stateDir_ + "/" + kCurrentWalName;
  struct stat st{};
  bool legacyExists = (::stat(legacyPath.c_str(), &st) == 0);
  bool currentExists = (::stat(currentPath.c_str(), &st) == 0);

  if (legacyExists && !currentExists) {
    if (::rename(legacyPath.c_str(), currentPath.c_str()) == 0) {
      logf(LogLevel::Info, "Migrated legacy WAL: %s -> %s",
           legacyPath.c_str(), currentPath.c_str());
    } else {
      logf(LogLevel::Warn, "Failed to migrate legacy WAL %s: %s",
           legacyPath.c_str(), std::strerror(errno));
    }
  } else if (legacyExists && currentExists) {
    logf(LogLevel::Warn,
         "Both legacy WAL (%s) and current WAL (%s) exist. "
         "Legacy WAL will be ignored — review and remove manually.",
         legacyPath.c_str(), currentPath.c_str());
  }
}

WalStore::~WalStore() {
  if (walFd_ >= 0) {
    ::close(walFd_);
    walFd_ = -1;
  }
}

std::string WalStore::walPath() const {
  return stateDir_ + "/" + kCurrentWalName;
}

int WalStore::ensureOpen() {
  if (walFd_ >= 0) {
    return 0;
  }
  std::string path = walPath();
  ::mkdir(path.substr(0, path.find_last_of('/')).c_str(), 0755);
  walFd_ = ::open(path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
  if (walFd_ < 0) {
    return -errno;
  }
  // Before the first record is acknowledged, make sure the file itself is
  // found after a power loss (it is new after every checkpoint).
  if (!entrySynced_) {
    const int rc = syncDirectory(stateDir_);
    if (rc != 0) {
      ::close(walFd_);
      walFd_ = -1;
      return rc;
    }
    entrySynced_ = true;
  }
  return 0;
}

int WalStore::appendRecord(WalRecordType type, const void *payload, uint32_t len,
                           bool keepUnsynced, bool *written) {
  if (written) *written = false;
  if (appendError_ != 0) {
    return appendError_;
  }
  int rc = ensureOpen();
  if (rc != 0) {
    return rc;
  }
  struct stat st{};
  if (::fstat(walFd_, &st) != 0) {
    return -errno;
  }

  WalHeader hdr{};
  hdr.magic = kWalMagic;
  hdr.version = kWalVersion;
  hdr.type = static_cast<uint32_t>(type);
  hdr.reserved = 0;
  hdr.payloadLen = len;
  hdr.ts = nowSec();

  // One buffer avoids a separate header write. Regular-file writes are not
  // guaranteed crash-atomic; recovery discards an incomplete final record.
  std::vector<uint8_t> buf(sizeof(hdr) + len);
  std::memcpy(buf.data(), &hdr, sizeof(hdr));
  if (len > 0) {
    std::memcpy(buf.data() + sizeof(hdr), payload, len);
  }

  rc = writeFull(walFd_, buf.data(), buf.size());
  if (rc != 0) {
    // Only part of the record: the caller keeps its old state, so remove the
    // bytes. If that fails they stay a torn tail, which recovery discards as
    // long as nothing is appended after it.
    if (::ftruncate(walFd_, st.st_size) != 0) {
      appendError_ = rc;
      logf(LogLevel::Error,
           "cannot remove failed WAL record: %s; appends disabled until checkpoint",
           std::strerror(errno));
    }
    return rc;
  }
  if (::fdatasync(walFd_) != 0) {
    // The whole record is in the file but may or may not survive a crash, and
    // after a failed sync nothing in this file is known to be on disk. Replace
    // the log with a checkpoint before appending anything else.
    rc = -errno;
    if (written) *written = true;
    appendError_ = rc;
    logf(LogLevel::Error, "WAL sync failed: %s; appends disabled until checkpoint",
         std::strerror(-rc));
    // A caller that rejects the operation drops the record (best effort: it
    // may already be durable, which callers must tolerate).
    if (!keepUnsynced && ::ftruncate(walFd_, st.st_size) != 0)
      logf(LogLevel::Warn,
           "cannot remove rejected WAL record: %s; a restart before the next checkpoint "
           "may replay it as a leaked allocation",
           std::strerror(errno));
    return rc;
  }
  return 0;
}

int WalStore::appendAlloc(const Allocation &alloc) {
  return appendRecord(WalRecordType::ALLOC, &alloc, sizeof(alloc), false, nullptr);
}

int WalStore::appendFree(uint64_t regionId, bool *written) {
  WalFreeV3 fr{};
  fr.regionId = regionId;
  return appendRecord(WalRecordType::FREE, &fr, sizeof(fr), true, written);
}

static PoolState *findPool(std::vector<PoolState> &pools, uint32_t poolId) {
  for (auto &p : pools) {
    if (p.poolId == poolId) {
      return &p;
    }
  }
  return nullptr;
}

static void removeExtent(PoolState &pool, uint64_t off, uint64_t len) {
  std::vector<Extent> newList;
  for (const auto &ex : pool.freeList) {
    uint64_t exEnd = ex.offset + ex.length;
    uint64_t alEnd = off + len;
    if (alEnd <= ex.offset || off >= exEnd) {
      newList.push_back(ex);
      continue;
    }
    if (off > ex.offset) {
      newList.push_back(Extent{ex.offset, off - ex.offset});
    }
    if (alEnd < exEnd) {
      newList.push_back(Extent{alEnd, exEnd - alEnd});
    }
  }
  pool.freeList.swap(newList);
}

static void addExtent(PoolState &pool, uint64_t off, uint64_t len) {
  pool.freeList.push_back(Extent{off, len});
}

// True if the bytes can start a header this version appends. ts and the
// padding before it are not compared.
static bool headerPrefix(const uint8_t *bytes, size_t len) {
  const size_t known =
      std::min(len, offsetof(WalHeader, payloadLen) + sizeof(WalHeader::payloadLen));
  for (auto type : {WalRecordType::ALLOC, WalRecordType::FREE}) {
    WalHeader expected;
    std::memset(&expected, 0, sizeof(expected));
    expected.magic = kWalMagic;
    expected.version = kWalVersion;
    expected.type = static_cast<uint32_t>(type);
    expected.payloadLen = type == WalRecordType::ALLOC ? sizeof(Allocation) : sizeof(WalFreeV3);
    if (std::memcmp(bytes, &expected, known) == 0) return true;
  }
  return false;
}

// Incomplete final record: [offset, offset + length) ends the file.
struct TornTail {
  uint64_t offset = 0;
  uint64_t length = 0;
};

// Since v7, callers acknowledge a record only after fdatasync, and a failed
// append is removed (or blocks later appends), so an incomplete final v7
// record was cut short by a crash and never acknowledged: it ends the log.
// Any other bytes that do not parse, including a torn v6 record, are rejected.
static int visitRecords(const std::string &path,
                        const std::function<int(uint32_t, const std::vector<uint8_t> &)> &visit,
                        TornTail *torn = nullptr) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return -errno;
  struct stat st{};
  if (::fstat(fd, &st) != 0) {
    int rc = -errno;
    ::close(fd);
    return rc;
  }
  uint64_t remaining = st.st_size;
  TornTail tail;
  uint32_t lastVersion = 0;
  int rc = 0;
  while (remaining != 0) {
    const uint64_t offset = static_cast<uint64_t>(st.st_size) - remaining;
    if (remaining < sizeof(WalHeader)) {
      uint8_t partial[sizeof(WalHeader)]{};
      if ((rc = readFull(fd, partial, remaining)) != 0) break;
      // Magic alone cannot tell v7 from v6; then the previous record decides.
      const bool versionKnown = remaining > offsetof(WalHeader, version);
      if (!headerPrefix(partial, remaining) ||
          (!versionKnown && lastVersion != kWalVersion)) {
        rc = -EPROTO;
        break;
      }
      tail = {offset, remaining};
      break;
    }
    WalHeader hdr{};
    if ((rc = readFull(fd, &hdr, sizeof(hdr))) != 0) break;
    remaining -= sizeof(hdr);
    const bool alloc = hdr.type == static_cast<uint32_t>(WalRecordType::ALLOC);
    const bool free = hdr.type == static_cast<uint32_t>(WalRecordType::FREE);
    if (hdr.magic != kWalMagic ||
        (hdr.version != kWalVersion && hdr.version != kLegacyWalVersion) ||
        (!alloc && !free) ||
        hdr.payloadLen != (alloc ? sizeof(Allocation) : sizeof(WalFreeV3))) {
      rc = -EPROTO;
      break;
    }
    if (hdr.payloadLen > remaining) {
      if (hdr.version != kWalVersion) { rc = -EPROTO; break; }
      tail = {offset, sizeof(hdr) + remaining};
      break;
    }
    lastVersion = hdr.version;
    std::vector<uint8_t> payload(hdr.payloadLen);
    if ((rc = readFull(fd, payload.data(), payload.size())) != 0) break;
    remaining -= payload.size();
    if (alloc) {
      Allocation a{};
      std::memcpy(&a, payload.data(), sizeof(a));
      if (!validAllocationRecord(a)) { rc = -EUCLEAN; break; }
    }
    if ((rc = visit(hdr.type, payload)) != 0) break;
  }
  ::close(fd);
  if (rc == 0 && torn) *torn = tail;
  return rc;
}

int WalStore::discardTornTail() {
  TornTail tail;
  int rc = visitRecords(walPath(), [](uint32_t, const auto &) { return 0; }, &tail);
  if (rc == -ENOENT) return 0;
  if (rc != 0 || tail.length == 0) return rc;
  const int fd = ::open(walPath().c_str(), O_WRONLY);
  if (fd < 0) return -errno;
  rc = ::ftruncate(fd, static_cast<off_t>(tail.offset)) == 0 && ::fsync(fd) == 0
           ? 0 : -errno;
  ::close(fd);
  if (rc == 0) {
    logf(LogLevel::Warn,
         "discarded incomplete final WAL record (%llu bytes at offset %llu)",
         static_cast<unsigned long long>(tail.length),
         static_cast<unsigned long long>(tail.offset));
  }
  return rc;
}

int WalStore::referencedPools(std::set<uint32_t> &pools) const {
  std::set<uint32_t> staged;
  const int rc = visitRecords(walPath(), [&](uint32_t type, const auto &payload) {
    if (type == static_cast<uint32_t>(WalRecordType::ALLOC)) {
      Allocation a{};
      std::memcpy(&a, payload.data(), sizeof(a));
      staged.insert(a.poolId);
    }
    return 0;
  });
  if (rc == 0 || rc == -ENOENT) pools = std::move(staged);
  return rc;
}

int WalStore::liveAllocations(const std::set<uint32_t> &pools,
                              std::map<uint64_t, Allocation> &live) const {
  std::map<uint64_t, Allocation> staged;
  for (const auto &[id, a] : live)
    if (pools.count(a.poolId)) staged.emplace(id, a);
  const int rc = visitRecords(walPath(), [&](uint32_t type, const auto &payload) {
    if (type == static_cast<uint32_t>(WalRecordType::ALLOC)) {
      Allocation a{};
      std::memcpy(&a, payload.data(), sizeof(a));
      if (pools.count(a.poolId)) staged[a.handle.regionId] = a;
    } else {
      WalFreeV3 fr{};
      std::memcpy(&fr, payload.data(), sizeof(fr));
      staged.erase(fr.regionId); // Region IDs are never reused across pools.
    }
    return 0;
  });
  if (rc != 0 && rc != -ENOENT) return rc;
  live = std::move(staged);
  return 0;
}

int WalStore::replay(std::vector<PoolState> &pools,
                     std::map<uint64_t, Allocation> &allocations,
                     uint64_t &nextRegionId) {
  for (const auto &p : pools) {
    if (validateFreeList(p) != 0) return -EUCLEAN;
    for (const auto &[id, a] : allocations) {
      if (a.poolId == p.poolId && (id != a.handle.regionId || !validAllocation(p, a)))
        return -EUCLEAN;
    }
  }
  auto stagedPools = pools;
  auto stagedAllocations = allocations;
  uint64_t stagedNext = nextRegionId;
  const int rc = visitRecords(walPath(), [&](uint32_t type, const auto &payload) {
    if (type == static_cast<uint32_t>(WalRecordType::ALLOC)) {
      Allocation al{};
      std::memcpy(&al, payload.data(), sizeof(al));
      // Reserve IDs even for devices that have not appeared yet. Otherwise a
      // live pool can reuse one before a later rescan replays this record.
      if (al.handle.regionId >= stagedNext) {
        stagedNext = al.handle.regionId + 1;
      }
      PoolState *pool = findPool(stagedPools, al.poolId);
      if (!pool) {
        return 0;
      }
      if (!validAllocation(*pool, al)) return -EUCLEAN;
      auto existing = stagedAllocations.find(al.handle.regionId);
      if (existing != stagedAllocations.end() && existing->second.poolId != al.poolId) {
        // Older writers may already have reused an ID across pools. Reject
        // the replay so the caller rolls back instead of losing a live handle.
        return -EEXIST;
      }
      removeExtent(*pool, al.realOffset, al.allocLength);
      stagedAllocations[al.handle.regionId] = al;
    } else {
      WalFreeV3 fr{};
      std::memcpy(&fr, payload.data(), sizeof(fr));
      auto globalIt = stagedAllocations.find(fr.regionId);
      if (globalIt == stagedAllocations.end()) {
        return 0;
      }
      const Allocation &al = globalIt->second;
      PoolState *pool = findPool(stagedPools, al.poolId);
      if (!pool) {
        return 0;
      }
      if (!validAllocation(*pool, al)) return -EUCLEAN;
      addExtent(*pool, al.realOffset, al.allocLength);
      stagedAllocations.erase(globalIt);
    }
    return 0;
  });
  if (rc != 0) return rc;
  pools = std::move(stagedPools);
  allocations = std::move(stagedAllocations);
  nextRegionId = stagedNext;
  return 0;
}

int WalStore::checkpoint(const std::vector<PoolState> &pools,
                         MetadataStore &metadata,
                         const std::map<uint64_t, Allocation> &allocations,
                         uint64_t nextRegionId) {
  for (const auto &pool : pools) {
    if (validatePoolState(pool, allocations) != 0) return -EUCLEAN;
  }
  for (const auto &[id, a] : allocations) {
    bool found = false;
    for (const auto &p : pools) if (p.poolId == a.poolId) found = true;
    if (!found) return -EAGAIN;
    if (id >= nextRegionId) return -EUCLEAN;
  }
  for (const auto &pool : pools) {
    int rc = metadata.save(pool);
    if (rc != 0) {
      return rc;
    }
  }
  int rc = metadata.saveGlobal(allocations, nextRegionId);
  if (rc != 0) {
    return rc;
  }
  // The renames must be on disk before the WAL they replace is removed: a
  // power loss could otherwise keep the unlink and lose the checkpoint.
  rc = syncDirectory(stateDir_);
  if (rc != 0) {
    return rc;
  }

  // Close the WAL fd before unlinking so the next append reopens a fresh file
  if (walFd_ >= 0) {
    ::close(walFd_);
    walFd_ = -1;
  }
  if (::unlink(walPath().c_str()) != 0 && errno != ENOENT) return -errno;
  entrySynced_ = false;
  appendError_ = 0;
  return 0;
}

} // namespace maru
