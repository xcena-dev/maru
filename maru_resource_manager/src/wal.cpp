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

static uint32_t payloadSize(uint32_t type) {
  switch (static_cast<WalRecordType>(type)) {
  case WalRecordType::ALLOC: return sizeof(Allocation);
  case WalRecordType::FREE: return sizeof(WalFreeV3);
  }
  return 0;
}

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

static void encodeRecord(std::vector<uint8_t> &buf, WalRecordType type,
                         const void *payload, uint32_t len) {
  WalHeader hdr{};
  hdr.magic = kWalMagic;
  hdr.version = kWalVersion;
  hdr.type = static_cast<uint32_t>(type);
  hdr.reserved = 0;
  hdr.payloadLen = len;
  hdr.ts = nowSec();
  const size_t at = buf.size();
  buf.resize(at + sizeof(hdr) + len);
  std::memcpy(buf.data() + at, &hdr, sizeof(hdr));
  if (len > 0) std::memcpy(buf.data() + at + sizeof(hdr), payload, len);
}

int WalStore::appendBuffer(const std::vector<uint8_t> &buf, bool keepUnsynced,
                           bool *written) {
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

  // One buffer avoids a separate header write. Regular-file writes are not
  // guaranteed crash-atomic; recovery discards an incomplete final record.
  rc = writeFull(walFd_, buf.data(), buf.size());
  if (rc != 0) {
    // Only part of the records: the caller keeps its old state, so remove
    // the bytes. If that fails they stay a torn tail, which recovery
    // discards as long as nothing is appended after it.
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
  std::vector<uint8_t> buf;
  encodeRecord(buf, WalRecordType::ALLOC, &alloc, sizeof(alloc));
  return appendBuffer(buf, false, nullptr);
}

int WalStore::appendFree(uint64_t regionId, bool *written) {
  return appendFrees({regionId}, written);
}

int WalStore::appendFrees(const std::vector<uint64_t> &regionIds, bool *written) {
  if (written) *written = false;
  if (regionIds.empty()) return 0;
  std::vector<uint8_t> buf;
  for (uint64_t id : regionIds) {
    WalFreeV3 fr{};
    fr.regionId = id;
    encodeRecord(buf, WalRecordType::FREE, &fr, sizeof(fr));
  }
  return appendBuffer(buf, true, written);
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
    expected.payloadLen = payloadSize(expected.type);
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
    if (hdr.magic != kWalMagic ||
        (hdr.version != kWalVersion && hdr.version != kLegacyWalVersion) ||
        payloadSize(hdr.type) == 0 || hdr.payloadLen != payloadSize(hdr.type)) {
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

// Fields that identify an allocation record; the token is recomputed at load.
static bool sameAllocation(const Allocation &a, const Allocation &b) {
  return a.handle.regionId == b.handle.regionId && a.handle.offset == b.handle.offset &&
         a.handle.length == b.handle.length && a.nonce == b.nonce &&
         std::strncmp(a.clientId, b.clientId, sizeof(a.clientId)) == 0 &&
         a.requestedSize == b.requestedSize && a.allocLength == b.allocLength &&
         a.poolId == b.poolId && a.realOffset == b.realOffset;
}

int WalStore::buildLedger(const std::map<uint64_t, Allocation> &global,
                          uint64_t globalNextRegionId, Ledger &out) const {
  Ledger ledger;
  ledger.live = global;
  ledger.nextRegionId = globalNextRegionId;
  const int rc = visitRecords(walPath(), [&](uint32_t type, const auto &payload) {
    if (type == static_cast<uint32_t>(WalRecordType::ALLOC)) {
      Allocation a{};
      std::memcpy(&a, payload.data(), sizeof(a));
      const uint64_t id = a.handle.regionId;
      // Reserve every ID the log ever used, freed or not, in any pool.
      if (id >= ledger.nextRegionId) ledger.nextRegionId = id + 1;
      auto live = ledger.live.find(id);
      if (live == ledger.live.end()) {
        ledger.live.emplace(id, a);
      } else if (!sameAllocation(live->second, a)) {
        // Writers before #91 could reuse an ID across pools; which record a
        // later FREE meant cannot be known.
        logf(LogLevel::Error,
             "region %llu is recorded for pool %u and for pool %u (an RM before #91 reused "
             "region IDs); stop every client of this RM, then move %s aside and restart",
             static_cast<unsigned long long>(id), live->second.poolId, a.poolId,
             walPath().c_str());
        return -EEXIST;
      }
      // An identical record is one global.meta already holds: a log whose
      // removal after a checkpoint was lost is replayed with the same result,
      // unless an RM before #91 reused a region ID in it.
    } else {
      WalFreeV3 fr{};
      std::memcpy(&fr, payload.data(), sizeof(fr));
      ledger.live.erase(fr.regionId);
    }
    return 0;
  });
  if (rc != 0 && rc != -ENOENT) return rc;
  out = std::move(ledger);
  return 0;
}

bool WalStore::startsAtFirstRegion() const {
  bool first = false;
  visitRecords(walPath(), [&](uint32_t type, const auto &payload) {
    if (type == static_cast<uint32_t>(WalRecordType::ALLOC)) {
      Allocation a{};
      std::memcpy(&a, payload.data(), sizeof(a));
      first = a.handle.regionId == 1;
    }
    return 1; // Only the first record matters.
  });
  return first;
}

int WalStore::checkpoint(MetadataStore &metadata,
                         const std::map<uint64_t, Allocation> &allocations,
                         uint64_t nextRegionId) {
  int rc = metadata.saveGlobal(allocations, nextRegionId);
  if (rc != 0) return rc;
  // The rename must be on disk before the WAL it replaces is removed: a
  // power loss could otherwise keep the unlink and lose the checkpoint. A
  // lost unlink replays the log onto the checkpoint that covers it, with the
  // same result (see buildLedger).
  rc = syncDirectory(stateDir_);
  if (rc != 0) return rc;
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
