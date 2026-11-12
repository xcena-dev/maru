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

TEST_F(WalReplayTest, LedgerKeepsEveryPoolAndReservesIds) {
    // Allocations of pools whose device has not appeared stay in the ledger,
    // and every ID the log used, freed or not, is reserved.
    ASSERT_EQ(wal->appendAlloc(allocation(0, 42, 11)), 0);
    ASSERT_EQ(wal->appendAlloc(allocation(1, 90, 12)), 0);
    ASSERT_EQ(wal->appendFree(90), 0);
    Ledger ledger;
    ASSERT_EQ(wal->buildLedger({}, 1, ledger), 0);
    ASSERT_EQ(ledger.live.size(), 1u);
    EXPECT_EQ(ledger.live.at(42).nonce, 11u);
    EXPECT_EQ(ledger.nextRegionId, 91u);
    ASSERT_EQ(wal->buildLedger({}, 200, ledger), 0);
    EXPECT_EQ(ledger.nextRegionId, 200u);
}

TEST_F(WalReplayTest, CoveredLogReplaysWithTheSameResult) {
    // A checkpoint whose WAL removal was lost: the log is applied again onto
    // the global checkpoint that already holds its records.
    const auto a = allocation(0, 42, 11);
    ASSERT_EQ(wal->appendAlloc(a), 0);
    ASSERT_EQ(wal->appendAlloc(allocation(0, 43, 12)), 0);
    ASSERT_EQ(wal->appendFree(43), 0);
    Ledger ledger;
    ASSERT_EQ(wal->buildLedger({{42, a}}, 44, ledger), 0);
    ASSERT_EQ(ledger.live.size(), 1u);
    EXPECT_EQ(ledger.live.at(42).nonce, a.nonce);
    EXPECT_EQ(ledger.nextRegionId, 44u);
}

TEST_F(WalReplayTest, LegacyCrossPoolCollisionIsRejected) {
    // Freed in between, an ID used again by another pool is a reuse.
    ASSERT_EQ(wal->appendAlloc(allocation(0, 42, 11)), 0);
    ASSERT_EQ(wal->appendFree(42), 0);
    ASSERT_EQ(wal->appendAlloc(allocation(1, 42, 22)), 0);
    Ledger ledger;
    ASSERT_EQ(wal->buildLedger({}, 1, ledger), 0);
    EXPECT_EQ(ledger.live.at(42).poolId, 1u);
    // Two pools claiming it at once cannot be resolved.
    EXPECT_EQ(wal->buildLedger({{42, allocation(2, 42, 33)}}, 43, ledger), -EEXIST);
    EXPECT_EQ(ledger.live.at(42).poolId, 1u);
}
} // namespace
