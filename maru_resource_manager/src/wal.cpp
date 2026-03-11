#include "wal.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "metadata.h"
#include "util.h"

namespace maru {

static constexpr uint32_t kWalMagic = 0x57414C21; // 'WAL!'
static constexpr uint32_t kWalVersion = 6;

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

WalStore::WalStore(const std::string &stateDir) : stateDir_(stateDir) {}

WalStore::~WalStore() {
  if (walFd_ >= 0) {
    ::close(walFd_);
    walFd_ = -1;
  }
}

std::string WalStore::walPath() const {
  return stateDir_ + "/maru-resource-manager.wal";
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
  return 0;
}

int WalStore::appendRecord(WalRecordType type, const void *payload,
                           uint32_t len) {
  int rc = ensureOpen();
  if (rc != 0) {
    return rc;
  }

  WalHeader hdr{};
  hdr.magic = kWalMagic;
  hdr.version = kWalVersion;
  hdr.type = static_cast<uint32_t>(type);
  hdr.reserved = 0;
  hdr.payloadLen = len;
  hdr.ts = nowSec();

  // Assemble header + payload into a single buffer so that the write is
  // atomic for small records (< PIPE_BUF).  This prevents a partial WAL
  // entry if the process crashes between two separate write() calls.
  std::vector<uint8_t> buf(sizeof(hdr) + len);
  std::memcpy(buf.data(), &hdr, sizeof(hdr));
  if (len > 0) {
    std::memcpy(buf.data() + sizeof(hdr), payload, len);
  }

  rc = writeFull(walFd_, buf.data(), buf.size());
  if (rc != 0) {
    return rc;
  }
  ::fdatasync(walFd_);
  return 0;
}

int WalStore::appendAlloc(const Allocation &alloc) {
  return appendRecord(WalRecordType::ALLOC, &alloc, sizeof(alloc));
}

int WalStore::appendFree(uint64_t regionId) {
  WalFreeV3 fr{};
  fr.regionId = regionId;
  return appendRecord(WalRecordType::FREE, &fr, sizeof(fr));
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

int WalStore::replay(std::vector<PoolState> &pools,
                     std::map<uint64_t, Allocation> &allocations,
                     uint64_t &nextRegionId) {
  std::string path = walPath();
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return -errno;
  }

  while (true) {
    WalHeader hdr{};
    int rc = readFull(fd, &hdr, sizeof(hdr));
    if (rc != 0) {
      if (rc == -EPIPE) {
        break;
      }
      ::close(fd);
      return rc;
    }
    if (hdr.magic != kWalMagic || hdr.version != kWalVersion) {
      ::close(fd);
      return -EPROTO;
    }
    std::vector<uint8_t> payload(hdr.payloadLen);
    if (hdr.payloadLen > 0) {
      rc = readFull(fd, payload.data(), hdr.payloadLen);
      if (rc != 0) {
        ::close(fd);
        return rc;
      }
    }

    if (hdr.type == static_cast<uint32_t>(WalRecordType::ALLOC)) {
      if (payload.size() != sizeof(Allocation)) {
        continue;
      }
      Allocation al{};
      std::memcpy(&al, payload.data(), sizeof(al));
      PoolState *pool = findPool(pools, al.poolId);
      if (!pool) {
        continue;
      }
      removeExtent(*pool, al.realOffset, al.allocLength);
      allocations[al.handle.regionId] = al;
      if (al.handle.regionId >= nextRegionId) {
        nextRegionId = al.handle.regionId + 1;
      }
    } else if (hdr.type == static_cast<uint32_t>(WalRecordType::FREE)) {
      if (payload.size() != sizeof(WalFreeV3)) {
        continue;
      }
      WalFreeV3 fr{};
      std::memcpy(&fr, payload.data(), sizeof(fr));
      auto globalIt = allocations.find(fr.regionId);
      if (globalIt == allocations.end()) {
        continue;
      }
      const Allocation &al = globalIt->second;
      PoolState *pool = findPool(pools, al.poolId);
      if (!pool) {
        continue;
      }
      addExtent(*pool, al.realOffset, al.allocLength);
      allocations.erase(globalIt);
    }
  }

  ::close(fd);
  return 0;
}

int WalStore::checkpoint(const std::vector<PoolState> &pools,
                         MetadataStore &metadata,
                         const std::map<uint64_t, Allocation> &allocations,
                         uint64_t nextRegionId) {
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

  // Close the WAL fd before unlinking so the next append reopens a fresh file
  if (walFd_ >= 0) {
    ::close(walFd_);
    walFd_ = -1;
  }
  ::unlink(walPath().c_str());
  return 0;
}

} // namespace maru
