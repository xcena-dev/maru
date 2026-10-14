#pragma once

#include <string>
#include <set>

#include "pool_manager.h"

namespace maru {

enum class WalRecordType : uint32_t { ALLOC = 1, FREE = 2 };

class WalStore {
public:
  explicit WalStore(const std::string &stateDir);
  ~WalStore();

  // Non-copyable
  WalStore(const WalStore &) = delete;
  WalStore &operator=(const WalStore &) = delete;

  // A failed ALLOC is removed (best effort) and must be rejected: if it still
  // survives a crash, recovery restores an allocation its client never got,
  // which is only a leak.
  int appendAlloc(const Allocation &alloc);
  // A FREE whose record reached the file but whose sync failed is kept and
  // sets `written`: the caller must complete the free, because recovery may
  // apply it. Losing it instead only brings the allocation back (a leak).
  int appendFree(uint64_t regionId, bool *written = nullptr);
  // An append's outcome on disk is unknown; a checkpoint must replace the log.
  bool appendsDisabled() const { return appendError_ != 0; }
  // Read/validate the log without opening any device or changing recovery state.
  int referencedPools(std::set<uint32_t> &pools) const;

  int replay(std::vector<PoolState> &pools,
             std::map<uint64_t, Allocation> &allocations,
             uint64_t &nextRegionId);
  // Truncate an incomplete final record so appends do not follow it. Call
  // only after recovery has validated every complete record.
  int discardTornTail();

  int checkpoint(const std::vector<PoolState> &pools,
                 class MetadataStore &metadata,
                 const std::map<uint64_t, Allocation> &allocations,
                 uint64_t nextRegionId);

private:
  std::string stateDir_;
  int walFd_{-1};
  // Set when an append's outcome on disk is unknown (sync failed, or a
  // partial record could not be removed); cleared by a checkpoint.
  int appendError_{0};

  std::string walPath() const;
  int ensureOpen();
  int appendRecord(WalRecordType type, const void *payload, uint32_t len,
                   bool keepUnsynced, bool *written);
};

} // namespace maru
