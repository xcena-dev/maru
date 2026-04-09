// Copyright 2026 XCENA Inc.
// Unit tests for PoolManager UUID, node mapping, and path resolution.

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
        // Clean up temp dir
        auto removePath = [](const std::string &path) {
            std::string cmd = "rm -rf " + path;
            int ret __attribute__((unused)) = ::system(cmd.c_str());
        };
        removePath(tmpDir_);
    }

    std::string getHostname() {
        char buf[256];
        if (::gethostname(buf, sizeof(buf)) == 0) {
            buf[sizeof(buf) - 1] = '\0';
            return std::string(buf);
        }
        return "";
    }
};

// ── registerNode ────────────────────────────────────────────────────────────

TEST_F(PoolManagerUuidTest, RegisterNodeSingleDevice) {
    std::vector<PoolManager::DeviceMapping> mappings = {
        {"uuid-A", "/dev/dax0.0"},
    };
    int matched = pm_->registerNode("node-0", mappings);
    // No pools loaded, so matched = 0
    EXPECT_EQ(matched, 0);
}

TEST_F(PoolManagerUuidTest, RegisterNodeMultipleDevices) {
    std::vector<PoolManager::DeviceMapping> mappings = {
        {"uuid-A", "/dev/dax0.0"},
        {"uuid-B", "/dev/dax1.0"},
    };
    int matched = pm_->registerNode("node-0", mappings);
    EXPECT_EQ(matched, 0);
}

TEST_F(PoolManagerUuidTest, RegisterNodeDuplicateSkipped) {
    std::vector<PoolManager::DeviceMapping> mappings = {
        {"uuid-A", "/dev/dax0.0"},
    };
    int first = pm_->registerNode("node-0", mappings);
    int second = pm_->registerNode("node-0", mappings);
    EXPECT_EQ(first, 0);
    EXPECT_EQ(second, 0); // silently skipped
}

TEST_F(PoolManagerUuidTest, RegisterMultipleNodes) {
    std::vector<PoolManager::DeviceMapping> m0 = {
        {"uuid-A", "/dev/dax0.0"},
    };
    std::vector<PoolManager::DeviceMapping> m1 = {
        {"uuid-A", "/dev/dax1.0"},
    };
    pm_->registerNode("node-0", m0);
    pm_->registerNode("node-1", m1);

    // Both registrations succeed without error
    // Verified by resolvePathForClient below
}

// ── resolvePathForClient ────────────────────────────────────────────────────

TEST_F(PoolManagerUuidTest, ResolveEmptyUuidReturnsEmpty) {
    std::string path = pm_->resolvePathForClient("", "node-0:1234");
    EXPECT_EQ(path, "");
}

TEST_F(PoolManagerUuidTest, ResolveRemoteClientFromMapping) {
    std::vector<PoolManager::DeviceMapping> mappings = {
        {"uuid-A", "/dev/dax0.0"},
    };
    pm_->registerNode("node-0", mappings);

    std::string path = pm_->resolvePathForClient("uuid-A", "node-0:1234");
    EXPECT_EQ(path, "/dev/dax0.0");
}

TEST_F(PoolManagerUuidTest, ResolveRemoteClientDifferentPaths) {
    pm_->registerNode("node-0", {{"uuid-A", "/dev/dax1.0"}});
    pm_->registerNode("node-1", {{"uuid-A", "/dev/dax0.0"}});

    EXPECT_EQ(pm_->resolvePathForClient("uuid-A", "node-0:100"), "/dev/dax1.0");
    EXPECT_EQ(pm_->resolvePathForClient("uuid-A", "node-1:200"), "/dev/dax0.0");
}

TEST_F(PoolManagerUuidTest, ResolveUnregisteredNodeReturnsEmpty) {
    pm_->registerNode("node-0", {{"uuid-A", "/dev/dax0.0"}});

    std::string path = pm_->resolvePathForClient("uuid-A", "node-99:1234");
    EXPECT_EQ(path, "");
}

TEST_F(PoolManagerUuidTest, ResolveUnknownUuidReturnsEmpty) {
    pm_->registerNode("node-0", {{"uuid-A", "/dev/dax0.0"}});

    std::string path = pm_->resolvePathForClient("uuid-UNKNOWN", "node-0:1234");
    EXPECT_EQ(path, "");
}

TEST_F(PoolManagerUuidTest, ResolveClientIdWithoutPort) {
    pm_->registerNode("node-0", {{"uuid-A", "/dev/dax0.0"}});

    // clientId without ":pid" should use whole string as hostname
    std::string path = pm_->resolvePathForClient("uuid-A", "node-0");
    EXPECT_EQ(path, "/dev/dax0.0");
}

TEST_F(PoolManagerUuidTest, ResolveLocalClientNoPool) {
    // Local hostname but no pool loaded — should return empty
    std::string hostname = getHostname();
    if (hostname.empty()) GTEST_SKIP() << "Cannot determine hostname";

    std::string path = pm_->resolvePathForClient("uuid-A", hostname + ":1234");
    EXPECT_EQ(path, "");
}

// ── getStats includes deviceUuid ────────────────────────────────────────────

TEST_F(PoolManagerUuidTest, GetStatsEmptyPools) {
    std::vector<PoolState> stats;
    pm_->getStats(stats);
    EXPECT_TRUE(stats.empty());
}
