// Copyright 2026 XCENA Inc.
// Unit tests for PoolManager UUID and device UUID lookup.

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <unistd.h>

#include "pool_manager.h"

using namespace maru;

// ── Test fixture ────────────────────────────────────────────────────────────

class PoolManagerUuidTest : public ::testing::Test {
protected:
    std::string tmpDir_;
    std::unique_ptr<PoolManager> pm_;

    void SetUp() override {
        char tmpl[] = "/tmp/maru_pm_test_XXXXXX";
        ASSERT_NE(::mkdtemp(tmpl), nullptr);
        tmpDir_ = tmpl;
        pm_ = std::make_unique<PoolManager>(tmpDir_);
    }

    void TearDown() override {
        pm_.reset();
        auto removePath = [](const std::string &path) {
            std::string cmd = "rm -rf " + path;
            int ret __attribute__((unused)) = ::system(cmd.c_str());
        };
        removePath(tmpDir_);
    }
};

// ── getDeviceUuidForRegion ──────────────────────────────────────────────────

TEST_F(PoolManagerUuidTest, GetUuidForNonexistentRegionReturnsEmpty) {
    std::string uuid = pm_->getDeviceUuidForRegion(9999);
    EXPECT_EQ(uuid, "");
}

// ── getStats ────────────────────────────────────────────────────────────────

TEST_F(PoolManagerUuidTest, GetStatsEmptyPools) {
    std::vector<PoolState> stats;
    pm_->getStats(stats);
    EXPECT_TRUE(stats.empty());
}
