#pragma once

#include <array>
#include <string>
#include <vector>

#include "pool_manager.h"

namespace maru {

/// What pool_N.meta holds. Version 7 records the pool's identity and the
/// layout it was served with. Version 6 (written by earlier RMs) holds a free
/// list only; recovery treats it like a missing record, except that it shows
/// the pool had history.
struct PoolRecord {
  enum class Kind { Absent, V6, V7, Corrupt, Newer, IoError };
  Kind kind{Kind::Absent};
  int error{0};  // -errno for IoError, the format error for Corrupt
  // Version 7
  DaxType type{DaxType::DEV_DAX};
  std::array<uint8_t, 16> uuid{};
  uint64_t dataBegin{0};
  uint64_t dataEnd{0};
  uint64_t alignBytes{0};
};

class MetadataStore {
public:
  explicit MetadataStore(const std::string &stateDir);

  /// Version 6 pool checkpoint, written by earlier RMs. Kept for tests that
  /// build earlier states; recovery does not read its free list.
  int load(uint32_t poolId, PoolState &pool);
  int save(const PoolState &pool);

  /// Read and classify pool_N.meta. Never fails: the outcome is in out.kind.
  void loadRecord(uint32_t poolId, PoolRecord &out);
  /// Write a version 7 record (tmp, fsync, rename, directory fsync).
  int saveRecord(uint32_t poolId, DaxType type, const std::array<uint8_t, 16> &uuid,
                 uint64_t dataBegin, uint64_t dataEnd, uint64_t alignBytes);
  /// Copy pool_N.meta to pool_N.meta.<tag>-<unixtime>[.n] without replacing
  /// an earlier copy. archived is empty if there was no record.
  int archiveRecord(uint32_t poolId, const std::string &tag, std::string &archived);
  /// Pool IDs of every pool_N.meta (exact names; no .tmp or archive copies).
  /// -errno if the state directory cannot be read: an unread record must
  /// never look like a missing one.
  int listRecordIds(std::vector<uint32_t> &ids) const;

  // Global state management
  int loadGlobal(std::map<uint64_t, Allocation> &allocations,
                 uint64_t &nextRegionId);
  int saveGlobal(const std::map<uint64_t, Allocation> &allocations,
                 uint64_t nextRegionId);

private:
  std::string stateDir_;
  std::string metaPath(uint32_t poolId) const;
  std::string globalMetaPath() const;
};

} // namespace maru
