#pragma once

#include "pool_manager.h"

namespace maru {

bool validPoolLayout(const PoolState &pool);
bool validExtent(const PoolState &pool, uint64_t offset, uint64_t length);
bool validAllocationRecord(const Allocation &allocation);
bool validAllocation(const PoolState &pool, const Allocation &allocation);
int validateFreeList(const PoolState &pool);
int validatePoolState(const PoolState &pool,
                      const std::map<uint64_t, Allocation> &allocations);

} // namespace maru
