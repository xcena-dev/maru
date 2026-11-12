#pragma once

#include <string>
#include <set>
#include <utility>
#include <vector>

#include "pool_manager.h"

namespace maru {

enum class WalRecordType : uint32_t { ALLOC = 1, FREE = 2 };

/// Every live allocation, independent of device layout.
struct Ledger {
  std::map<uint64_t, Allocation> live;
  uint64_t nextRegionId{1};
};

class MetadataStore;

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
  // FREE records for every ID in one write and one sync, with appendFree's
  // outcome rules.
  int appendFrees(const std::vector<uint64_t> &regionIds, bool *written = nullptr);
  // An append's outcome on disk is unknown; a checkpoint must replace the log.
  bool appendsDisabled() const { return appendError_ != 0; }

  /// Apply the log to the global checkpoint's allocations (see Ledger).
  /// Reads only the log; never opens a device. -EEXIST if two different
  /// records claim one region ID (only RMs before #91 reused IDs).
  int buildLedger(const std::map<uint64_t, Allocation> &global,
                  uint64_t globalNextRegionId, Ledger &out) const;
  /// True if the log's first record allocates region 1: the log then reaches
  /// back to an empty state, so no lost global checkpoint held allocations.
  bool startsAtFirstRegion() const;

  // Truncate an incomplete final record so appends do not follow it. Call
  // only after recovery has validated every complete record.
  int discardTornTail();

  /// Write the global checkpoint, then remove the log.
  int checkpoint(MetadataStore &metadata,
                 const std::map<uint64_t, Allocation> &allocations,
                 uint64_t nextRegionId);

private:
  std::string stateDir_;
  int walFd_{-1};
  // Whether the WAL's directory entry is known to survive a power loss. A
  // record's fdatasync does not cover the entry of a newly created file.
  bool entrySynced_{false};
  // Set when an append's outcome on disk is unknown (sync failed, or a
  // partial record could not be removed); cleared by a checkpoint.
  int appendError_{0};

  std::string walPath() const;
  int ensureOpen();
  int appendBuffer(const std::vector<uint8_t> &buf, bool keepUnsynced, bool *written);
};

} // namespace maru
