#include "metadata.h"

#include <algorithm>
#include <ctime>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <limits>

#include "util.h"
#include "pool_validation.h"

namespace maru {

static constexpr uint32_t kMetaMagic = 0x4D455441; // 'META'
static constexpr uint32_t kMetaVersion = 6;

static constexpr uint32_t kGlobalMetaMagic = 0x474C4F42; // 'GLOB'
static constexpr uint32_t kGlobalMetaVersion = 2;

static constexpr uint32_t kRecordVersion = 7;

// pool_N.meta version 7: identity and layout only. The free list is derived
// from the device range and the allocation ledger.
struct RecordV7 {
  uint32_t magic;
  uint32_t version;
  uint32_t poolId;
  uint32_t crc32; // CRC32 of the whole record with this field zero
  uint32_t type;
  uint32_t reserved;
  uint8_t uuid[16];
  uint64_t dataBegin;
  uint64_t dataEnd;
  uint64_t alignBytes;
};
static_assert(sizeof(RecordV7) == 64, "RecordV7 must be 64 bytes");

static uint32_t recordCrc(RecordV7 rec) {
  rec.crc32 = 0;
  return crc32(&rec, sizeof(rec));
}

// Write path atomically: tmp, fsync, rename, directory fsync.
static int replaceFile(const std::string &dir, const std::string &path,
                       const void *data, size_t len) {
  ::mkdir(dir.c_str(), 0755);
  const std::string tmp = path + ".tmp";
  int fd = ::open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return -errno;
  int rc = writeFull(fd, data, len);
  if (rc == 0 && ::fsync(fd) != 0) rc = -errno;
  ::close(fd);
  if (rc == 0 && ::rename(tmp.c_str(), path.c_str()) != 0) rc = -errno;
  if (rc != 0) {
    ::unlink(tmp.c_str());
    return rc;
  }
  return syncDirectory(dir);
}

struct MetaHeader {
  uint32_t magic;
  uint32_t version;
  uint32_t poolId;
  uint32_t crc32; // CRC32 of payload (everything after header)
  uint64_t totalSize;
  uint64_t freeCount;
};

// Check file size before allocating from on-disk counts. Reject truncation,
// trailing bytes and multiplication overflow without changing caller state.
static int readPayload(int fd, size_t headerSize, uint64_t count,
                       size_t recordSize, uint32_t expectedCrc,
                       std::vector<uint8_t> &payload) {
  struct stat st{};
  if (::fstat(fd, &st) != 0) return -errno;
  if (st.st_size < 0 || static_cast<uint64_t>(st.st_size) < headerSize)
    return -EPROTO;
  const uint64_t bytes = static_cast<uint64_t>(st.st_size) - headerSize;
  if (bytes % recordSize != 0 || count != bytes / recordSize ||
      bytes > std::numeric_limits<size_t>::max()) return -EPROTO;
  payload.resize(static_cast<size_t>(bytes));
  if (!payload.empty()) {
    const int rc = readFull(fd, payload.data(), payload.size());
    if (rc != 0) return rc;
  }
  const uint32_t actual = payload.empty() ? 0 : crc32(payload.data(), payload.size());
  return actual == expectedCrc ? 0 : -EILSEQ;
}

MetadataStore::MetadataStore(const std::string &stateDir)
    : stateDir_(stateDir) {}

std::string MetadataStore::metaPath(uint32_t poolId) const {
  return stateDir_ + "/pool_" + std::to_string(poolId) + ".meta";
}

std::string MetadataStore::globalMetaPath() const {
  return stateDir_ + "/global.meta";
}

int MetadataStore::load(uint32_t poolId, PoolState &pool) {
  std::string path = metaPath(poolId);
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return -errno;
  }

  MetaHeader hdr{};
  int rc = readFull(fd, &hdr, sizeof(hdr));
  if (rc != 0) {
    ::close(fd);
    return rc;
  }
  if (hdr.magic != kMetaMagic || hdr.version != kMetaVersion ||
      hdr.poolId != poolId) {
    ::close(fd);
    return -EPROTO;
  }

  std::vector<uint8_t> payload;
  rc = readPayload(fd, sizeof(hdr), hdr.freeCount, sizeof(Extent), hdr.crc32, payload);
  ::close(fd);
  if (rc != 0) return rc;
  PoolState loaded = pool;
  loaded.poolId = hdr.poolId;
  loaded.totalSize = hdr.totalSize;
  loaded.freeList.clear();
  loaded.freeSize = 0;

  for (uint64_t i = 0; i < hdr.freeCount; ++i) {
    Extent ex{};
    std::memcpy(&ex, payload.data() + i * sizeof(Extent), sizeof(Extent));
    if (ex.length == 0 || ex.offset > UINT64_MAX - ex.length ||
        loaded.freeSize > UINT64_MAX - ex.length) return -EUCLEAN;
    loaded.freeList.push_back(ex);
    loaded.freeSize += ex.length;
  }

  pool = std::move(loaded);
  return 0;
}

int MetadataStore::save(const PoolState &pool) {
  ::mkdir(stateDir_.c_str(), 0755);

  std::string path = metaPath(pool.poolId);
  std::string tmp = path + ".tmp";

  int fd = ::open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) {
    return -errno;
  }

  // Build payload (extents) to compute CRC32
  std::vector<uint8_t> payload(pool.freeList.size() * sizeof(Extent));
  for (size_t i = 0; i < pool.freeList.size(); ++i) {
    std::memcpy(payload.data() + i * sizeof(Extent), &pool.freeList[i],
                sizeof(Extent));
  }

  MetaHeader hdr{};
  hdr.magic = kMetaMagic;
  hdr.version = kMetaVersion;
  hdr.poolId = pool.poolId;
  hdr.crc32 = payload.empty() ? 0 : crc32(payload.data(), payload.size());
  hdr.totalSize = pool.totalSize;
  hdr.freeCount = pool.freeList.size();

  int rc = writeFull(fd, &hdr, sizeof(hdr));
  if (rc != 0) {
    ::close(fd);
    return rc;
  }

  if (!payload.empty()) {
    rc = writeFull(fd, payload.data(), payload.size());
    if (rc != 0) {
      ::close(fd);
      return rc;
    }
  }

  if (::fsync(fd) != 0) {
    rc = -errno;
    ::close(fd);
    return rc;
  }
  ::close(fd);

  if (::rename(tmp.c_str(), path.c_str()) != 0) {
    return -errno;
  }

  return 0;
}

void MetadataStore::loadRecord(uint32_t poolId, PoolRecord &out) {
  out = PoolRecord{};
  const std::string path = metaPath(poolId);
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    out.error = -errno;
    out.kind = errno == ENOENT ? PoolRecord::Kind::Absent : PoolRecord::Kind::IoError;
    return;
  }
  uint32_t head[2]{};
  int rc = readFull(fd, head, sizeof(head));
  struct stat st{};
  if (rc == 0 && ::fstat(fd, &st) != 0) rc = -errno;
  if (rc != 0) {
    ::close(fd);
    out.error = rc;
    out.kind = rc == -EPIPE ? PoolRecord::Kind::Corrupt : PoolRecord::Kind::IoError;
    return;
  }
  if (head[0] != kMetaMagic || head[1] < kMetaVersion) {
    ::close(fd);
    out.error = -EPROTO;
    out.kind = PoolRecord::Kind::Corrupt;
    return;
  }
  if (head[1] > kRecordVersion) {
    ::close(fd);
    out.kind = PoolRecord::Kind::Newer;
    return;
  }
  if (head[1] == kMetaVersion) {
    // Its free list is not used: free lists come from the ledger.
    ::close(fd);
    out.kind = PoolRecord::Kind::V6;
    return;
  }
  RecordV7 rec{};
  if (st.st_size != static_cast<off_t>(sizeof(rec))) {
    ::close(fd);
    out.error = -EPROTO;
    out.kind = PoolRecord::Kind::Corrupt;
    return;
  }
  rc = ::lseek(fd, 0, SEEK_SET) == 0 ? readFull(fd, &rec, sizeof(rec)) : -errno;
  ::close(fd);
  if (rc != 0) {
    out.error = rc;
    out.kind = rc == -EPIPE ? PoolRecord::Kind::Corrupt : PoolRecord::Kind::IoError;
    return;
  }
  if (rec.crc32 != recordCrc(rec) || rec.poolId != poolId ||
      (rec.type != static_cast<uint32_t>(DaxType::DEV_DAX) &&
       rec.type != static_cast<uint32_t>(DaxType::FS_DAX))) {
    out.error = -EILSEQ;
    out.kind = PoolRecord::Kind::Corrupt;
    return;
  }
  out.kind = PoolRecord::Kind::V7;
  out.type = static_cast<DaxType>(rec.type);
  std::memcpy(out.uuid.data(), rec.uuid, sizeof(rec.uuid));
  out.dataBegin = rec.dataBegin;
  out.dataEnd = rec.dataEnd;
  out.alignBytes = rec.alignBytes;
}

int MetadataStore::saveRecord(uint32_t poolId, DaxType type,
                              const std::array<uint8_t, 16> &uuid,
                              uint64_t dataBegin, uint64_t dataEnd,
                              uint64_t alignBytes) {
  RecordV7 rec{};
  rec.magic = kMetaMagic;
  rec.version = kRecordVersion;
  rec.poolId = poolId;
  rec.type = static_cast<uint32_t>(type);
  std::memcpy(rec.uuid, uuid.data(), sizeof(rec.uuid));
  rec.dataBegin = dataBegin;
  rec.dataEnd = dataEnd;
  rec.alignBytes = alignBytes;
  rec.crc32 = recordCrc(rec);
  return replaceFile(stateDir_, metaPath(poolId), &rec, sizeof(rec));
}

int MetadataStore::archiveRecord(uint32_t poolId, const std::string &tag,
                                 std::string &archived) {
  archived.clear();
  const std::string path = metaPath(poolId);
  int in = ::open(path.c_str(), O_RDONLY);
  if (in < 0) return errno == ENOENT ? 0 : -errno;
  std::vector<uint8_t> data;
  uint8_t buf[4096];
  int rc = 0;
  for (;;) {
    const ssize_t n = ::read(in, buf, sizeof(buf));
    if (n < 0 && errno == EINTR) continue;
    if (n < 0) rc = -errno;
    if (n <= 0) break;
    data.insert(data.end(), buf, buf + n);
  }
  ::close(in);
  if (rc != 0) return rc;
  const std::string base =
      path + "." + tag + "-" + std::to_string(static_cast<long long>(std::time(nullptr)));
  std::string name = base;
  int out = -1;
  for (int n = 1; n < 1000; ++n) {
    out = ::open(name.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (out >= 0 || errno != EEXIST) break;
    name = base + "." + std::to_string(n);
  }
  if (out < 0) return -errno;
  rc = writeFull(out, data.data(), data.size());
  if (rc == 0 && ::fsync(out) != 0) rc = -errno;
  ::close(out);
  if (rc != 0) {
    ::unlink(name.c_str());
    return rc;
  }
  archived = name;
  return syncDirectory(stateDir_);
}

std::vector<uint32_t> MetadataStore::listRecordIds() const {
  std::vector<uint32_t> ids;
  DIR *dir = ::opendir(stateDir_.c_str());
  if (!dir) return ids;
  while (const struct dirent *ent = ::readdir(dir)) {
    const std::string name = ent->d_name;
    static const std::string prefix = "pool_", suffix = ".meta";
    if (name.size() <= prefix.size() + suffix.size() || name.compare(0, prefix.size(), prefix) != 0 ||
        name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0)
      continue;
    const std::string digits =
        name.substr(prefix.size(), name.size() - prefix.size() - suffix.size());
    if (digits.size() > 10 || (digits.size() > 1 && digits[0] == '0') ||
        digits.find_first_not_of("0123456789") != std::string::npos)
      continue;
    const unsigned long long id = std::stoull(digits);
    if (id <= UINT32_MAX) ids.push_back(static_cast<uint32_t>(id));
  }
  ::closedir(dir);
  std::sort(ids.begin(), ids.end());
  return ids;
}

struct GlobalMetaHeader {
  uint32_t magic;
  uint32_t version;
  uint32_t crc32; // CRC32 of payload (everything after header)
  uint32_t reserved;
  uint64_t nextRegionId;
  uint64_t allocCount;
};

int MetadataStore::loadGlobal(std::map<uint64_t, Allocation> &allocations,
                              uint64_t &nextRegionId) {
  std::string path = globalMetaPath();
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return -errno;
  }

  GlobalMetaHeader hdr{};
  int rc = readFull(fd, &hdr, sizeof(hdr));
  if (rc != 0) {
    ::close(fd);
    return rc;
  }
  if (hdr.magic != kGlobalMetaMagic || hdr.version != kGlobalMetaVersion) {
    ::close(fd);
    return -EPROTO;
  }

  std::vector<uint8_t> payload;
  rc = readPayload(fd, sizeof(hdr), hdr.allocCount, sizeof(Allocation), hdr.crc32, payload);
  ::close(fd);
  if (rc != 0) return rc;
  if (hdr.nextRegionId == 0) return -EUCLEAN;
  std::map<uint64_t, Allocation> loaded;

  for (uint64_t i = 0; i < hdr.allocCount; ++i) {
    Allocation al{};
    std::memcpy(&al, payload.data() + i * sizeof(Allocation),
                sizeof(Allocation));
    if (!validAllocationRecord(al) || al.handle.regionId >= hdr.nextRegionId ||
        !loaded.emplace(al.handle.regionId, al).second) return -EUCLEAN;
  }

  allocations = std::move(loaded);
  nextRegionId = hdr.nextRegionId;
  return 0;
}

int MetadataStore::saveGlobal(const std::map<uint64_t, Allocation> &allocations,
                              uint64_t nextRegionId) {
  ::mkdir(stateDir_.c_str(), 0755);
  const std::string tmp = globalMetaPath() + ".tmp";
  int fd = ::open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) {
    return -errno;
  }

  // Build payload to compute CRC32
  std::vector<uint8_t> payload(allocations.size() * sizeof(Allocation));
  size_t off = 0;
  for (const auto &kv : allocations) {
    std::memcpy(payload.data() + off, &kv.second, sizeof(Allocation));
    off += sizeof(Allocation);
  }

  GlobalMetaHeader hdr{};
  hdr.magic = kGlobalMetaMagic;
  hdr.version = kGlobalMetaVersion;
  hdr.crc32 = payload.empty() ? 0 : crc32(payload.data(), payload.size());
  hdr.reserved = 0;
  hdr.nextRegionId = nextRegionId;
  hdr.allocCount = allocations.size();

  int rc = writeFull(fd, &hdr, sizeof(hdr));
  if (rc == 0 && !payload.empty()) rc = writeFull(fd, payload.data(), payload.size());
  if (rc == 0 && ::fsync(fd) != 0) rc = -errno;
  ::close(fd);
  if (rc == 0 && ::rename(tmp.c_str(), globalMetaPath().c_str()) != 0) rc = -errno;
  if (rc != 0) ::unlink(tmp.c_str());
  return rc;
}

} // namespace maru
