// Copyright 2026 XCENA Inc.
#include "device_header.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <openssl/rand.h>
#include <sys/mman.h>
#include <unistd.h>

namespace maru {

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
    ::msync(ptr, sizeof(hdr), MS_SYNC);
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
