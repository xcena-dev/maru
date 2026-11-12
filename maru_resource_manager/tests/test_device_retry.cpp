// Copyright 2026 XCENA Inc.
#include <gtest/gtest.h>

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <unistd.h>

#include "pool_manager.h"
#include "pool_manager_test_peer.h"

using namespace maru;

class DeviceRetryTest : public ::testing::Test {
protected:
    std::string dir;
    void SetUp() override {
        char path[] = "/tmp/maru-device-retry-XXXXXX";
        ASSERT_NE(::mkdtemp(path), nullptr);
        dir = path;
        setLogLevel(LogLevel::Info);
    }
    void TearDown() override {
        setLogLevel(LogLevel::Error);
        std::filesystem::remove_all(dir);
    }
};

TEST_F(DeviceRetryTest, OnlyDeviceDaxDriverIsEligible) {
    auto device = dir + "/dax0.0";
    std::filesystem::create_directory(device);
    EXPECT_FALSE(isDeviceDaxBound(device));
    std::filesystem::create_symlink("../../../bus/dax/drivers/kmem", device + "/driver");
    EXPECT_FALSE(isDeviceDaxBound(device));
    std::filesystem::remove(device + "/driver");
    std::filesystem::create_symlink("../../../bus/dax/drivers/device_dax", device + "/driver");
    EXPECT_TRUE(isDeviceDaxBound(device));
    std::filesystem::remove(device + "/driver");
    EXPECT_FALSE(isDeviceDaxBound(device));
}

TEST_F(DeviceRetryTest, FailuresLogOnlyOnChangeAndRetryAfterRecovery) {
    PoolManager pm(dir);
    auto device = dir + "/late-device";
    testing::internal::CaptureStderr();
    EXPECT_EQ(PoolManagerTestPeer::build(pm, device, DaxType::DEV_DAX), -ENOENT);
    auto first = testing::internal::GetCapturedStderr();
    EXPECT_NE(first.find("device size"), std::string::npos);
    testing::internal::CaptureStderr();
    EXPECT_EQ(PoolManagerTestPeer::build(pm, device, DaxType::DEV_DAX), -ENOENT);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());

    // A directory has a readable size but cannot be mapped as a DAX header.
    std::filesystem::create_directory(device);
    testing::internal::CaptureStderr();
    EXPECT_LT(PoolManagerTestPeer::build(pm, device, DaxType::DEV_DAX), 0);
    EXPECT_NE(testing::internal::GetCapturedStderr().find("read device header"), std::string::npos);
    testing::internal::CaptureStderr();
    EXPECT_LT(PoolManagerTestPeer::build(pm, device, DaxType::DEV_DAX), 0);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());

    // The same directory is a valid FS_DAX pool; success clears suppression.
    testing::internal::CaptureStderr();
    EXPECT_EQ(PoolManagerTestPeer::build(pm, device, DaxType::FS_DAX), 0);
    EXPECT_NE(testing::internal::GetCapturedStderr().find("device recovered"), std::string::npos);
    std::filesystem::remove(device);
    testing::internal::CaptureStderr();
    EXPECT_EQ(PoolManagerTestPeer::build(pm, device, DaxType::DEV_DAX), -ENOENT);
    EXPECT_NE(testing::internal::GetCapturedStderr().find("device size"), std::string::npos);
}

TEST_F(DeviceRetryTest, FailureStateIsIndependentPerPathAndClearedOnDisappearance) {
    PoolManager pm(dir);
    testing::internal::CaptureStderr();
    EXPECT_EQ(PoolManagerTestPeer::build(pm, dir + "/a", DaxType::DEV_DAX), -ENOENT);
    EXPECT_EQ(PoolManagerTestPeer::build(pm, dir + "/b", DaxType::DEV_DAX), -ENOENT);
    auto output = testing::internal::GetCapturedStderr();
    EXPECT_NE(output.find(dir + "/a"), std::string::npos);
    EXPECT_NE(output.find(dir + "/b"), std::string::npos);
    PoolManagerTestPeer::scan(pm);  // Neither temporary path appears in sysfs.
    testing::internal::CaptureStderr();
    EXPECT_EQ(PoolManagerTestPeer::build(pm, dir + "/a", DaxType::DEV_DAX), -ENOENT);
    EXPECT_NE(testing::internal::GetCapturedStderr().find(dir + "/a"), std::string::npos);
}
