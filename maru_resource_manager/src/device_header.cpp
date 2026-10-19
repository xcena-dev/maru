// Copyright 2026 XCENA Inc.
#include "device_header.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <openssl/rand.h>
#include <sys/mman.h>
#include <unistd.h>

#if defined(__x86_64__) || defined(__i386__)
#include <emmintrin.h>
#endif

namespace maru {

namespace {

constexpr uintptr_t kCacheLineSize = 64;

// Write back and invalidate the CPU cache lines covering [p, p+n).
//
// DEV_DAX mappings are write-back cached, and msync() does not touch CPU
// caches there (device-dax has no page cache and no fsync op). CXL 2.x has
// no cross-host cache coherence, so a header written here would otherwise
// sit in this host's cache, invisible to other hosts sharing the device,
// and a header read here would leave a copy that goes stale when another
// host rewrites it. clflush covers both: it writes back dirty lines
// (writer side) and invalidates clean copies (reader side); mfence orders
// it against the surrounding loads and stores. clflush is preferred over
// clflushopt/clwb: available on every x86-64 without CPUID dispatch, and
// the header is a single cache line so flush throughput is irrelevant.
void flushCacheRange(const void *p, size_t n) {
#if defined(__x86_64__) || defined(__i386__)
    const uintptr_t addr = reinterpret_cast<uintptr_t>(p);
    const uintptr_t end = addr + n;
    for (uintptr_t line = addr & ~(kCacheLineSize - 1); line < end;
         line += kCacheLineSize)
        _mm_clflush(reinterpret_cast<const void *>(line));
    _mm_mfence();
#else
    (void)p;
    (void)n;
#endif
}

} // namespace

int readDeviceHeader(const std::string &devPath, DeviceHeader &out,
                     uint64_t mapSize) {
    int fd = ::open(devPath.c_str(), O_RDONLY);
    if (fd < 0)
        return -errno;

    void *ptr = ::mmap(nullptr, mapSize, PROT_READ, MAP_SHARED, fd, 0);
    int err = errno;
    ::close(fd);
    if (ptr == MAP_FAILED)
        return -err;

    // Drop any locally cached copy first so the read comes from the device,
    // not from a line another host may have made stale.
    flushCacheRange(ptr, sizeof(out));
    std::memcpy(&out, ptr, sizeof(out));
    ::munmap(ptr, mapSize);

    if (std::memcmp(out.magic, kDeviceHeaderMagic, sizeof(out.magic)) != 0)
        return -ENODATA;

    return 0;
}

int writeDeviceHeader(const std::string &devPath, const DeviceHeader &hdr,
                      uint64_t mapSize) {
    int fd = ::open(devPath.c_str(), O_RDWR);
    if (fd < 0)
        return -errno;

    void *ptr =
        ::mmap(nullptr, mapSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    int err = errno;
    ::close(fd);
    if (ptr == MAP_FAILED)
        return -err;

    std::memcpy(ptr, &hdr, sizeof(hdr));
    // Push the store to the device; on DEV_DAX msync alone leaves it in the
    // CPU cache where other hosts cannot see it.
    flushCacheRange(ptr, sizeof(hdr));
    // Best effort, for regular-file backing (tests); DEV_DAX rejects msync.
    ::msync(ptr, sizeof(hdr), MS_SYNC);
    ::munmap(ptr, mapSize);
    return 0;
}

uint64_t deviceTagAreaEnd(uint64_t mapSize) {
    return std::min(mapSize, kDeviceTagAreaLimit);
}

int readDeviceIdentity(const std::string &devPath, uint64_t mapSize,
                       DeviceIdentity &out) {
    int fd = ::open(devPath.c_str(), O_RDONLY);
    if (fd < 0)
        return -errno;

    void *ptr = ::mmap(nullptr, mapSize, PROT_READ, MAP_SHARED, fd, 0);
    int err = errno;
    ::close(fd);
    if (ptr == MAP_FAILED)
        return -err;

    const uint64_t end = deviceTagAreaEnd(mapSize);
    // Drop locally cached lines first: another host may have rewritten them.
    flushCacheRange(ptr, end);
    const auto *base = static_cast<const uint8_t *>(ptr);
    std::memcpy(&out.header, base, sizeof(out.header));
    out.headerRc = std::memcmp(out.header.magic, kDeviceHeaderMagic,
                               sizeof(out.header.magic)) == 0
                       ? 0
                       : -ENODATA;

    bool any = false, all = true, first = true;
    for (uint64_t off = kDeviceTagStride; off + sizeof(DeviceTag) <= end;
         off += kDeviceTagStride) {
        DeviceTag tag{};
        std::memcpy(&tag, base + off, sizeof(tag));
        const bool present =
            std::memcmp(tag.magic, kDeviceTagMagic, sizeof(tag.magic)) == 0;
        any = any || present;
        if (!present || tag.offset != off) {
            all = false;
            continue;
        }
        if (first) {
            std::memcpy(out.tagUuid, tag.uuid, sizeof(out.tagUuid));
            first = false;
        } else if (std::memcmp(out.tagUuid, tag.uuid, sizeof(out.tagUuid)) != 0) {
            all = false;
        }
    }
    ::munmap(ptr, mapSize);
    out.tags = !any ? TagState::Absent
                    : (all && !first ? TagState::Consistent : TagState::Inconsistent);
    return 0;
}

int writeDeviceIdentity(const std::string &devPath, uint64_t mapSize,
                        const DeviceHeader *hdr, const uint8_t uuid[16]) {
    int fd = ::open(devPath.c_str(), O_RDWR);
    if (fd < 0)
        return -errno;

    void *ptr =
        ::mmap(nullptr, mapSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    int err = errno;
    ::close(fd);
    if (ptr == MAP_FAILED)
        return -err;

    auto *base = static_cast<uint8_t *>(ptr);
    const uint64_t end = deviceTagAreaEnd(mapSize);
    for (uint64_t off = kDeviceTagStride; off + sizeof(DeviceTag) <= end;
         off += kDeviceTagStride) {
        DeviceTag tag{};
        std::memcpy(tag.magic, kDeviceTagMagic, sizeof(tag.magic));
        std::memcpy(tag.uuid, uuid, sizeof(tag.uuid));
        tag.offset = off;
        std::memcpy(base + off, &tag, sizeof(tag));
    }
    if (hdr) std::memcpy(base, hdr, sizeof(*hdr));
    // See writeDeviceHeader: make the stores visible to other hosts.
    flushCacheRange(base, end);
    ::msync(base, end, MS_SYNC);
    ::munmap(ptr, mapSize);
    return 0;
}

void initDeviceHeader(DeviceHeader &hdr) {
    std::memset(&hdr, 0, sizeof(hdr));
    std::memcpy(hdr.magic, kDeviceHeaderMagic, sizeof(hdr.magic));
    hdr.version = kDeviceHeaderVersion;

    RAND_bytes(hdr.uuid, sizeof(hdr.uuid));
    // RFC 4122 UUID v4: set version and variant bits
    hdr.uuid[6] = (hdr.uuid[6] & 0x0F) | 0x40; // version 4
    hdr.uuid[8] = (hdr.uuid[8] & 0x3F) | 0x80; // variant 1
}

std::string uuidToString(const uint8_t uuid[16]) {
    char buf[37];
    std::snprintf(buf, sizeof(buf),
                  "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-"
                  "%02x%02x%02x%02x%02x%02x",
                  uuid[0], uuid[1], uuid[2], uuid[3], uuid[4], uuid[5],
                  uuid[6], uuid[7], uuid[8], uuid[9], uuid[10], uuid[11],
                  uuid[12], uuid[13], uuid[14], uuid[15]);
    return std::string(buf);
}

} // namespace maru
