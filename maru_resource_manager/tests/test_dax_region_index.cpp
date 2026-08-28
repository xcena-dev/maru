// Copyright 2026 XCENA Inc.
// Unit tests for dax_region index derivation from a dev_dax device name.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "pool_manager.h"

using namespace maru;

namespace {

uint32_t indexOf(const std::string &devName) {
    uint32_t index = 0xFFFFFFFFu;
    EXPECT_TRUE(parseRegionIndexFromDaxName(devName, index)) << devName;
    return index;
}

bool rejects(const std::string &devName) {
    uint32_t index = 0;
    return !parseRegionIndexFromDaxName(devName, index);
}

}  // namespace

TEST(DaxRegionIndexTest, ParsesRegionIndexFromDeviceName) {
    EXPECT_EQ(indexOf("dax0.0"), 0u);
    EXPECT_EQ(indexOf("dax0.1"), 0u);
    EXPECT_EQ(indexOf("dax3.7"), 3u);
    EXPECT_EQ(indexOf("dax12.0"), 12u);
}

TEST(DaxRegionIndexTest, RejectsMalformedNames) {
    EXPECT_TRUE(rejects("dax"));
    EXPECT_TRUE(rejects("dax0"));         // no dev_dax id
    EXPECT_TRUE(rejects("dax0."));        // trailing dot
    EXPECT_TRUE(rejects("dax.0"));        // no region id
    EXPECT_TRUE(rejects("daxfoo"));
    EXPECT_TRUE(rejects("dax_region"));   // attribute group dir, not a device
    EXPECT_TRUE(rejects("pmem0"));
    EXPECT_TRUE(rejects(""));
}

// Regression: an hmem-backed device resolves to
// "../../../devices/platform/hmem.0/dax0.0", which carries no "region<N>"
// component, so deriving the index from the sysfs link target cannot work and
// the device used to be skipped, leaving the daemon with no pools.
TEST(DaxRegionIndexTest, ResolvesHmemDeviceWhoseLinkTargetHasNoRegion) {
    const std::string hmemTarget = "../../../devices/platform/hmem.0/dax0.0";
    ASSERT_EQ(hmemTarget.find("region"), std::string::npos);

    EXPECT_EQ(indexOf("dax0.0"), 0u);
}
