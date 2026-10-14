// Copyright 2026 XCENA Inc.
#include <gtest/gtest.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <unistd.h>

#include "wal.h"

using namespace maru;

namespace {
PoolState pool(uint32_t id) {
    PoolState p{};
    p.poolId = id;
    p.totalSize = 16384;
    p.dataBegin = 0;
    p.dataEnd = p.totalSize;
    p.alignBytes = 4096;
    p.type = DaxType::FS_DAX;
    p.freeSize = p.totalSize;
    p.freeList.push_back({0, p.totalSize});
    return p;
}

Allocation allocation(uint32_t poolId, uint64_t regionId, uint64_t nonce) {
    Allocation a{};
    a.poolId = poolId;
    a.handle.regionId = regionId;
    a.handle.length = 4096;
    a.handle.authToken = nonce + 100;
    a.allocLength = 4096;
    a.requestedSize = 4096;
    a.nonce = nonce;
    std::strcpy(a.clientId, "test-client");
    return a;
}

class WalReplayTest : public ::testing::Test {
protected:
    std::string dir;
    std::unique_ptr<WalStore> wal;
    void SetUp() override {
        char path[] = "/tmp/maru-wal-replay-XXXXXX";
        ASSERT_NE(::mkdtemp(path), nullptr);
        dir = path;
        wal = std::make_unique<WalStore>(dir);
    }
    void TearDown() override {
        wal.reset();
        std::filesystem::remove_all(dir);
    }
};

TEST_F(WalReplayTest, MissingPoolReservesIdsBeforeLateJoin) {
    const auto old = allocation(0, 42, 11);
    ASSERT_EQ(wal->appendAlloc(old), 0);
    std::vector<PoolState> initial{pool(1)};
    std::map<uint64_t, Allocation> live;
    uint64_t next = 1;
    ASSERT_EQ(wal->replay(initial, live, next), 0);
    ASSERT_EQ(next, 43u);
    EXPECT_TRUE(live.empty());

    // A new allocation must not reuse the missing pool's region ID.
    auto current = allocation(1, next++, 22);
    live.emplace(current.handle.regionId, current);
    ASSERT_EQ(wal->appendAlloc(current), 0);

    // rescanDevicesLocked replays into only the newly staged pools, with the
    // same global allocation map used by the already-serving pools.
    std::vector<PoolState> late{pool(0)};
    ASSERT_EQ(wal->replay(late, live, next), 0);
    ASSERT_EQ(live.size(), 2u);
    EXPECT_EQ(live.at(42).nonce, old.nonce);
    EXPECT_EQ(live.at(43).nonce, current.nonce);
    EXPECT_EQ(live.at(43).handle.authToken, current.handle.authToken);
    EXPECT_EQ(late[0].freeList.front().offset, 4096u);
    EXPECT_EQ(initial[0].freeList.front().offset, 0u);
    EXPECT_EQ(next, 44u);
}

TEST_F(WalReplayTest, MissingAndFreedRecordsStillAdvanceHighWaterMark) {
    ASSERT_EQ(wal->appendAlloc(allocation(0, 90, 1)), 0);
    ASSERT_EQ(wal->appendFree(90), 0);
    std::vector<PoolState> pools;
    std::map<uint64_t, Allocation> allocations;
    uint64_t next = 1;
    ASSERT_EQ(wal->replay(pools, allocations, next), 0);
    EXPECT_EQ(next, 91u);
    next = 200;
    ASSERT_EQ(wal->replay(pools, allocations, next), 0);
    EXPECT_EQ(next, 200u);
}

TEST_F(WalReplayTest, LegacyCrossPoolCollisionCannotOverwriteLiveHandle) {
    ASSERT_EQ(wal->appendAlloc(allocation(0, 42, 11)), 0);
    ASSERT_EQ(wal->appendFree(42), 0);
    auto current = allocation(1, 42, 22);
    std::map<uint64_t, Allocation> live{{42, current}};
    std::vector<PoolState> late{pool(0)};
    uint64_t next = 43;
    EXPECT_EQ(wal->replay(late, live, next), -EEXIST);
    ASSERT_EQ(live.size(), 1u);
    EXPECT_EQ(live.at(42).poolId, 1u);
    EXPECT_EQ(live.at(42).nonce, current.nonce);
    EXPECT_EQ(live.at(42).handle.authToken, current.handle.authToken);
    ASSERT_EQ(late[0].freeList.size(), 1u);
    EXPECT_EQ(late[0].freeList[0].offset, 0u);
    EXPECT_EQ(late[0].freeList[0].length, late[0].totalSize);
}

TEST_F(WalReplayTest, SamePoolReplayAndFreeStillWork) {
    auto a = allocation(0, 42, 11);
    ASSERT_EQ(wal->appendAlloc(a), 0);
    std::map<uint64_t, Allocation> live{{42, a}};
    std::vector<PoolState> pools{pool(0)};
    uint64_t next = 43;
    ASSERT_EQ(wal->replay(pools, live, next), 0);
    EXPECT_EQ(live.at(42).nonce, a.nonce);
    ASSERT_EQ(wal->appendFree(42), 0);
    pools = {pool(0)};
    ASSERT_EQ(wal->replay(pools, live, next), 0);
    EXPECT_TRUE(live.empty());
    uint64_t freeBytes = 0;
    for (const auto &extent : pools[0].freeList) freeBytes += extent.length;
    EXPECT_EQ(freeBytes, pools[0].totalSize);
}
} // namespace
