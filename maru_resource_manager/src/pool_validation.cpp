#include "pool_validation.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>

namespace maru {

bool validPoolLayout(const PoolState &p) {
    return p.alignBytes != 0 && p.dataEnd > p.dataBegin &&
           p.totalSize == p.dataEnd - p.dataBegin &&
           ((p.type == DaxType::DEV_DAX && p.dataBegin == p.alignBytes) ||
            (p.type == DaxType::FS_DAX && p.dataBegin == 0));
}

bool validExtent(const PoolState &p, uint64_t offset, uint64_t length) {
    return validPoolLayout(p) && length != 0 && offset >= p.dataBegin &&
           offset < p.dataEnd && length <= p.dataEnd - offset;
}

bool validAllocationRecord(const Allocation &a) {
    return a.handle.regionId != 0 &&
           a.handle.regionId != std::numeric_limits<uint64_t>::max() &&
           a.allocLength != 0 && a.handle.length == a.allocLength &&
           a.requestedSize != 0 && a.requestedSize <= a.allocLength &&
           a.realOffset <= std::numeric_limits<uint64_t>::max() - a.allocLength &&
           a.clientId[0] != '\0' &&
           std::memchr(a.clientId, '\0', sizeof(a.clientId)) != nullptr;
}

bool validAllocation(const PoolState &p, const Allocation &a) {
    return validAllocationRecord(a) && a.poolId == p.poolId &&
           validExtent(p, a.realOffset, a.allocLength) &&
           a.realOffset % p.alignBytes == 0 && a.allocLength % p.alignBytes == 0 &&
           a.handle.offset == (p.type == DaxType::DEV_DAX ? a.realOffset : 0);
}

static bool disjoint(std::vector<Extent> extents) {
    std::sort(extents.begin(), extents.end(),
              [](const Extent &a, const Extent &b) { return a.offset < b.offset; });
    for (size_t i = 1; i < extents.size(); ++i) {
        // Each range was checked for overflow before calling this helper.
        if (extents[i - 1].offset + extents[i - 1].length > extents[i].offset)
            return false;
    }
    return true;
}

int validateFreeList(const PoolState &p) {
    if (!validPoolLayout(p)) return -EUCLEAN;
    uint64_t sum = 0;
    for (const auto &e : p.freeList) {
        if (!validExtent(p, e.offset, e.length) || e.length > p.totalSize - sum)
            return -EUCLEAN;
        sum += e.length;
    }
    return sum == p.freeSize && disjoint(p.freeList) ? 0 : -EUCLEAN;
}

int validatePoolState(const PoolState &p,
                      const std::map<uint64_t, Allocation> &allocations) {
    if (validateFreeList(p) != 0) return -EUCLEAN;
    auto extents = p.freeList;
    uint64_t accounted = p.freeSize;
    for (const auto &[id, a] : allocations) {
        if (a.poolId != p.poolId) continue;
        if (id != a.handle.regionId || !validAllocation(p, a) ||
            a.allocLength > p.totalSize - accounted)
            return -EUCLEAN;
        accounted += a.allocLength;
        extents.push_back({a.realOffset, a.allocLength});
    }
    return accounted == p.totalSize && disjoint(std::move(extents)) ? 0 : -EUCLEAN;
}

} // namespace maru
