// Copyright 2026 XCENA Inc.
// Unit tests for device_header.h/cpp

#include <gtest/gtest.h>

#include <cstring>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

#include "device_header.h"

using namespace maru;

// ── uuidToString ────────────────────────────────────────────────────────────

TEST(UuidToString, KnownUuid) {
    uint8_t uuid[16] = {0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
                         0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00};
    EXPECT_EQ(uuidToString(uuid), "550e8400-e29b-41d4-a716-446655440000");
}

TEST(UuidToString, AllZeros) {
    uint8_t uuid[16] = {};
    EXPECT_EQ(uuidToString(uuid), "00000000-0000-0000-0000-000000000000");
}

TEST(UuidToString, AllFF) {
    uint8_t uuid[16];
    std::memset(uuid, 0xFF, 16);
    EXPECT_EQ(uuidToString(uuid), "ffffffff-ffff-ffff-ffff-ffffffffffff");
}

// ── initDeviceHeader ────────────────────────────────────────────────────────

TEST(InitDeviceHeader, MagicAndVersion) {
    DeviceHeader hdr{};
    initDeviceHeader(hdr);

    EXPECT_EQ(std::memcmp(hdr.magic, kDeviceHeaderMagic, 8), 0);
    EXPECT_EQ(hdr.version, kDeviceHeaderVersion);
    EXPECT_EQ(hdr.reserved, 0u);
}

TEST(InitDeviceHeader, UuidV4VersionBits) {
    DeviceHeader hdr{};
    initDeviceHeader(hdr);

    // RFC 4122: byte[6] high nibble must be 0x4 (version 4)
    EXPECT_EQ(hdr.uuid[6] & 0xF0, 0x40);
}

TEST(InitDeviceHeader, UuidV4VariantBits) {
    DeviceHeader hdr{};
    initDeviceHeader(hdr);

    // RFC 4122: byte[8] top 2 bits must be 10 (variant 1)
    EXPECT_EQ(hdr.uuid[8] & 0xC0, 0x80);
}

TEST(InitDeviceHeader, GeneratesUniqueUuids) {
    DeviceHeader hdr1{}, hdr2{};
    initDeviceHeader(hdr1);
    initDeviceHeader(hdr2);

    EXPECT_NE(std::memcmp(hdr1.uuid, hdr2.uuid, 16), 0);
}

TEST(InitDeviceHeader, StructSize) {
    EXPECT_EQ(sizeof(DeviceHeader), kDeviceHeaderSize);
    EXPECT_EQ(kDeviceHeaderSize, 32u);
}

// ── readDeviceHeader / writeDeviceHeader (via regular file) ─────────────────

class DeviceHeaderFileTest : public ::testing::Test {
protected:
    std::string tmpFile_;
    static constexpr uint64_t kMapSize = 4096; // page-aligned for mmap

    void SetUp() override {
        char tmpl[] = "/tmp/maru_test_dev_XXXXXX";
        int fd = ::mkstemp(tmpl);
        ASSERT_GE(fd, 0);
        tmpFile_ = tmpl;

        // Extend file to kMapSize so mmap succeeds
        ASSERT_EQ(::ftruncate(fd, kMapSize), 0);
        ::close(fd);
    }

    void TearDown() override {
        if (!tmpFile_.empty())
            ::unlink(tmpFile_.c_str());
    }
};

TEST_F(DeviceHeaderFileTest, WriteAndReadBack) {
    DeviceHeader hdr{};
    initDeviceHeader(hdr);

    ASSERT_EQ(writeDeviceHeader(tmpFile_, hdr, kMapSize), 0);

    DeviceHeader readBack{};
    ASSERT_EQ(readDeviceHeader(tmpFile_, readBack, kMapSize), 0);

    EXPECT_EQ(std::memcmp(readBack.magic, hdr.magic, 8), 0);
    EXPECT_EQ(readBack.version, hdr.version);
    EXPECT_EQ(std::memcmp(readBack.uuid, hdr.uuid, 16), 0);
}

TEST_F(DeviceHeaderFileTest, ReadUninitializedReturnsEnodata) {
    // File is all zeros — magic doesn't match
    DeviceHeader hdr{};
    int ret = readDeviceHeader(tmpFile_, hdr, kMapSize);
    EXPECT_EQ(ret, -ENODATA);
}

TEST_F(DeviceHeaderFileTest, ReadNonexistentFileReturnsError) {
    DeviceHeader hdr{};
    int ret = readDeviceHeader("/tmp/nonexistent_maru_dev_99999", hdr, kMapSize);
    EXPECT_LT(ret, 0);
}

TEST_F(DeviceHeaderFileTest, WriteNonexistentFileReturnsError) {
    DeviceHeader hdr{};
    initDeviceHeader(hdr);
    int ret = writeDeviceHeader("/tmp/nonexistent_maru_dev_99999", hdr, kMapSize);
    EXPECT_LT(ret, 0);
}

TEST_F(DeviceHeaderFileTest, OverwriteWithNewUuid) {
    DeviceHeader hdr1{}, hdr2{};
    initDeviceHeader(hdr1);
    initDeviceHeader(hdr2);

    ASSERT_EQ(writeDeviceHeader(tmpFile_, hdr1, kMapSize), 0);
    ASSERT_EQ(writeDeviceHeader(tmpFile_, hdr2, kMapSize), 0);

    DeviceHeader readBack{};
    ASSERT_EQ(readDeviceHeader(tmpFile_, readBack, kMapSize), 0);

    EXPECT_EQ(std::memcmp(readBack.uuid, hdr2.uuid, 16), 0);
}

TEST_F(DeviceHeaderFileTest, UuidStringRoundtrip) {
    DeviceHeader hdr{};
    initDeviceHeader(hdr);

    ASSERT_EQ(writeDeviceHeader(tmpFile_, hdr, kMapSize), 0);

    DeviceHeader readBack{};
    ASSERT_EQ(readDeviceHeader(tmpFile_, readBack, kMapSize), 0);

    EXPECT_EQ(uuidToString(readBack.uuid), uuidToString(hdr.uuid));
}
