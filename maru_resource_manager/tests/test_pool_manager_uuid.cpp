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

// ── registerNodes ───────────────────────────────────────────────────────────

TEST_F(PoolManagerUuidTest, RegisterNodeSingleDevice) {
    PoolManager::NodeList nodes = {
        {"node-0", {{"uuid-A", "/dev/dax0.0"}}},
    };
    int matched = pm_->registerNodes(nodes);
    // No pools loaded, so matched = 0
    EXPECT_EQ(matched, 0);
}

TEST_F(PoolManagerUuidTest, RegisterNodeMultipleDevices) {
    PoolManager::NodeList nodes = {
        {"node-0", {{"uuid-A", "/dev/dax0.0"}, {"uuid-B", "/dev/dax1.0"}}},
    };
    int matched = pm_->registerNodes(nodes);
    EXPECT_EQ(matched, 0);
}

TEST_F(PoolManagerUuidTest, RegisterNodesDuplicateUpdates) {
    pm_->registerNodes({{"node-0", {{"uuid-A", "/dev/dax0.0"}}}});

    // Re-register same node with updated path
    pm_->registerNodes({{"node-0", {{"uuid-A", "/dev/dax1.0"}}}});
    EXPECT_EQ(pm_->resolvePathForClient("uuid-A", "node-0:1234"), "/dev/dax1.0");
}

TEST_F(PoolManagerUuidTest, RegisterNodesDeviceRemoved) {
    // node-0 has uuid-A
    pm_->registerNodes({{"node-0", {{"uuid-A", "/dev/dax0.0"}}}});
    EXPECT_EQ(pm_->resolvePathForClient("uuid-A", "node-0:1234"), "/dev/dax0.0");

    // node-0 re-registers without uuid-A (device removed), uuid-B added
    pm_->registerNodes({{"node-0", {{"uuid-B", "/dev/dax1.0"}}}});
    EXPECT_EQ(pm_->resolvePathForClient("uuid-A", "node-0:1234"), "");  // stale cleared
    EXPECT_EQ(pm_->resolvePathForClient("uuid-B", "node-0:1234"), "/dev/dax1.0");
}

TEST_F(PoolManagerUuidTest, RegisterMultipleNodes) {
    PoolManager::NodeList nodes = {
        {"node-0", {{"uuid-A", "/dev/dax0.0"}}},
        {"node-1", {{"uuid-A", "/dev/dax1.0"}}},
    };
    pm_->registerNodes(nodes);

    // Both registrations succeed — verified by resolvePathForClient below
}

TEST_F(PoolManagerUuidTest, MultipleMetaServersPreserveOtherNodes) {
    // MetaServer 1 registers node-0 and node-1
    pm_->registerNodes({
        {"node-0", {{"uuid-A", "/dev/dax0.0"}}},
        {"node-1", {{"uuid-A", "/dev/dax1.0"}}},
    });

    // MetaServer 2 registers node-2 — node-0 and node-1 must survive
    pm_->registerNodes({
        {"node-2", {{"uuid-A", "/dev/dax2.0"}}},
    });

    EXPECT_EQ(pm_->resolvePathForClient("uuid-A", "node-0:100"), "/dev/dax0.0");
    EXPECT_EQ(pm_->resolvePathForClient("uuid-A", "node-1:200"), "/dev/dax1.0");
    EXPECT_EQ(pm_->resolvePathForClient("uuid-A", "node-2:300"), "/dev/dax2.0");
}

// ── resolvePathForClient ────────────────────────────────────────────────────

TEST_F(PoolManagerUuidTest, ResolveEmptyUuidReturnsEmpty) {
    std::string path = pm_->resolvePathForClient("", "node-0:1234");
    EXPECT_EQ(path, "");
}

TEST_F(PoolManagerUuidTest, ResolveRemoteClientFromMapping) {
    pm_->registerNodes({{"node-0", {{"uuid-A", "/dev/dax0.0"}}}});

    std::string path = pm_->resolvePathForClient("uuid-A", "node-0:1234");
    EXPECT_EQ(path, "/dev/dax0.0");
}

TEST_F(PoolManagerUuidTest, ResolveRemoteClientDifferentPaths) {
    pm_->registerNodes({
        {"node-0", {{"uuid-A", "/dev/dax1.0"}}},
        {"node-1", {{"uuid-A", "/dev/dax0.0"}}},
    });

    EXPECT_EQ(pm_->resolvePathForClient("uuid-A", "node-0:100"), "/dev/dax1.0");
    EXPECT_EQ(pm_->resolvePathForClient("uuid-A", "node-1:200"), "/dev/dax0.0");
}

TEST_F(PoolManagerUuidTest, ResolveUnregisteredNodeReturnsEmpty) {
    pm_->registerNodes({{"node-0", {{"uuid-A", "/dev/dax0.0"}}}});

    std::string path = pm_->resolvePathForClient("uuid-A", "node-99:1234");
    EXPECT_EQ(path, "");
}

TEST_F(PoolManagerUuidTest, ResolveUnknownUuidReturnsEmpty) {
    pm_->registerNodes({{"node-0", {{"uuid-A", "/dev/dax0.0"}}}});

    std::string path = pm_->resolvePathForClient("uuid-UNKNOWN", "node-0:1234");
    EXPECT_EQ(path, "");
}

TEST_F(PoolManagerUuidTest, ResolveClientIdWithoutPort) {
    pm_->registerNodes({{"node-0", {{"uuid-A", "/dev/dax0.0"}}}});

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
