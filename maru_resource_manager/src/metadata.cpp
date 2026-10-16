#include "metadata.h"

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

  std::string path = globalMetaPath();
  std::string tmp = path + ".tmp";

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

} // namespace maru
