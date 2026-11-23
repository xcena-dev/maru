// Holds of processes that map a region (MAP_HOLD): a region whose owner is
// gone or freed it stays out of use while a process still maps it, and the
// reaper drops the hold only of a process it can see is gone. Fake devices only.
#include "recovery_fixture.h"

#include <chrono>

#include "admin_server.h"

namespace {
constexpr auto npos = std::string::npos;
using std::chrono::hours;
const std::string kBoot = "11111111-1111-4111-8111-111111111111";
constexpr uint32_t kNoSuchPid = 2147483647;  // Above any pid_max

class MapHoldTest : public HeaderRecoveryTest {
protected:
    std::string bootFile;
    void SetUp() override {
        HeaderRecoveryTest::SetUp();
        bootFile = fakeRoot + "/boot_id";
        std::ofstream(bootFile) << kBoot << "\n";
        addDevice(0);
    }
    std::unique_ptr<PoolManager> start(int gracePeriodSec = 30) {
        auto pm = std::make_unique<PoolManager>(state, gracePeriodSec);
        PoolManagerTestPeer::setBootIdPath(*pm, bootFile);
        EXPECT_EQ(pm->loadPools(), 0);
        return pm;
    }
    static std::string thisHost() {
        char host[256]{};
        EXPECT_EQ(::gethostname(host, sizeof(host) - 1), 0);
        return host;
    }
    static uint64_t ownPidNs() {
        struct stat st{};
        EXPECT_EQ(::stat("/proc/self/ns/pid", &st), 0);
        return st.st_ino;
    }
    // A process on this host, in this boot and pid namespace: this one, or
    // one that does not exist.
    static HoldRecord process(bool alive = true) {
        HoldRecord p;
        p.host = thisHost();
        p.bootId = kBoot;
        p.pid = alive ? ::getpid() : kNoSuchPid;
        p.pidStart = alive ? getPidStartTime(::getpid()) : 12345;
        p.pidNs = ownPidNs();
        return p;
    }
    int hold(PoolManager &pm, const Handle &h, const std::string &holder, const HoldRecord &who) {
        std::string path;
        const int rc = pm.mapHold(h, holder, who, path);
        EXPECT_EQ(path, rc == 0 ? dev(0) : "");
        return rc;
    }
    std::string access(PoolManager &pm, const Handle &h) {
        std::string path;
        const int rc = pm.verifyAndGetPath(h, path);
        return rc == 0 ? path : "error " + std::to_string(rc);
    }
    uint64_t reap(PoolManager &pm) {
        uint64_t reaped = 0;
        pm.reapExpired(reaped);
        return reaped;
    }
    std::vector<std::string> holders(PoolManager &pm, const Handle &h) {
        return PoolManagerTestPeer::holders(pm, h.regionId);
    }
    using Names = std::vector<std::string>;
};
} // namespace

// maru#115: the owner (a maru-server) is gone and its region is reclaimed
// while a Handler still maps it. The range must not go to the next owner.
TEST_F(MapHoldTest, ReclaimedRegionStaysOutOfUseWhileAProcessMapsIt) {
    auto pm = start(0);
    const std::string server1 = exitedLocalClient(), server2 = "other-host:7";
    Handle r{};
    ASSERT_EQ(allocate(*pm, r, 0, server1), 0);
    ASSERT_EQ(hold(*pm, r, "handler-a", process()), 0);

    EXPECT_EQ(reap(*pm), 1u);  // maru-server 1 is gone: R is reclaimed.
    EXPECT_EQ(access(*pm, r), "error " + std::to_string(-ENOENT));
    EXPECT_EQ(pm->allocationCount(), 1u);
    Handle next{};
    ASSERT_EQ(allocate(*pm, next, 0, server2), 0);
    EXPECT_NE(next.offset, r.offset);  // Handler A still maps R's range.
    reap(*pm);
    EXPECT_EQ(holders(*pm, r), Names{"handler-a"});  // Alive: kept.

    // A restart keeps R out of use too.
    pm.reset();
    pm = start(0);
    EXPECT_EQ(holders(*pm, r), Names{"handler-a"});
    Handle third{};
    ASSERT_EQ(allocate(*pm, third, 0, server2), 0);
    EXPECT_NE(third.offset, r.offset);

    // Handler A unmaps: the range is free again.
    ASSERT_EQ(pm->unhold(r, "handler-a", "handler-a"), 0);
    EXPECT_EQ(pm->allocationCount(), 2u);
    Handle reused{};
    ASSERT_EQ(allocate(*pm, reused, 0, server2), 0);
    EXPECT_EQ(reused.offset, r.offset);
    EXPECT_EQ(bytes(state + "/holds"), "");
}

TEST_F(MapHoldTest, FreedRegionIsReleasedOnceItsMappersAreGone) {
    auto pm = start();
    PoolManagerTestPeer::setHoldGrace(*pm, hours(1));
    Handle r{};
    ASSERT_EQ(allocate(*pm, r), 0);
    HoldRecord otherHost = process(false), otherNs = process(false);
    otherHost.host = "other-host";
    otherNs.pidNs += 1;
    ASSERT_EQ(hold(*pm, r, "dead", process(false)), 0);
    ASSERT_EQ(hold(*pm, r, "alive", process()), 0);
    ASSERT_EQ(hold(*pm, r, "other-host", otherHost), 0);
    ASSERT_EQ(hold(*pm, r, "other-ns", otherNs), 0);
    const auto listing = AdminServer::handleLine(*pm, "allocations");
    EXPECT_NE(listing.find("alive[epoch=-,host=" + thisHost() + ",boot=" + kBoot + ",pid=" +
                           std::to_string(::getpid()) + ","),
              npos)
        << listing;
    ASSERT_EQ(pm->verifyAndFree(r, "offline-test-owner"), 0);  // The owner frees it.
    EXPECT_EQ(pm->allocationCount(), 1u);

    // A gone process's hold goes after the grace; one the RM cannot see stays.
    reap(*pm);
    EXPECT_EQ(holders(*pm, r).size(), 4u);
    PoolManagerTestPeer::ageGoneHolders(*pm, hours(1));
    reap(*pm);
    EXPECT_EQ(holders(*pm, r), (Names{"alive", "other-host", "other-ns"}));
    ASSERT_EQ(pm->unhold(r, "alive", "alive"), 0);
    ASSERT_EQ(pm->unhold(r, "other-ns", "other-ns"), 0);
    EXPECT_EQ(pm->allocationCount(), 1u);
    // The operator vouches for the process on the other host.
    EXPECT_NE(AdminServer::handleLine(*pm, "drop-holds other-host --confirm").find("released=1"),
              npos);
    EXPECT_EQ(pm->allocationCount(), 0u);
    EXPECT_EQ(bytes(state + "/holds"), "");
}

TEST_F(MapHoldTest, ChecksIdempotenceAndTheHoldsFile) {
    auto pm = start();
    Handle a{};
    ASSERT_EQ(allocate(*pm, a), 0);
    const HoldRecord p = process();
    ASSERT_EQ(hold(*pm, a, "h1", p), 0);
    const std::string line = "hold " + std::to_string(a.regionId) + " h1 h1 - " + p.host + " " +
                             kBoot + " " + std::to_string(p.pid) + " " +
                             std::to_string(p.pidStart) + " " + std::to_string(p.pidNs) + "\n";
    EXPECT_EQ(bytes(state + "/holds"), line);
    EXPECT_EQ(hold(*pm, a, "h1", p), 0);  // Again: no change.
    EXPECT_EQ(bytes(state + "/holds"), line);

    Handle bad = a;
    bad.authToken ^= 1;
    EXPECT_EQ(hold(*pm, bad, "h2", p), -EACCES);
    Handle absent = a;
    absent.regionId = 999;
    EXPECT_EQ(hold(*pm, absent, "h2", p), -ENOENT);
    HoldRecord noHost = p, badBoot = p, dashBoot = p, withEpoch = p;
    noHost.host.clear();
    badBoot.bootId = "a b";
    dashBoot.bootId = "-";
    withEpoch.nodeEpoch = "e1";
    EXPECT_EQ(hold(*pm, a, "h 2", p), -EINVAL);
    for (const HoldRecord &who : {noHost, badBoot, dashBoot, withEpoch})
        EXPECT_EQ(hold(*pm, a, "h2", who), -EINVAL);
    EXPECT_EQ(holders(*pm, a), Names{"h1"});
    // No boot ID: kept, written as "-", and never reaped.
    HoldRecord unknown = process(false);
    unknown.bootId.clear();
    ASSERT_EQ(hold(*pm, a, "h3", unknown), 0);
    EXPECT_NE(bytes(state + "/holds").find(" h3 h3 - " + p.host + " - "), npos);

    // Read back as written; a freed region takes no new hold.
    const std::string file = bytes(state + "/holds");
    pm.reset();
    pm = start();
    PoolManagerTestPeer::setHoldGrace(*pm, hours(0));
    reap(*pm);
    EXPECT_EQ(holders(*pm, a), (Names{"h1", "h3"}));
    EXPECT_EQ(bytes(state + "/holds"), file);
    ASSERT_EQ(pm->verifyAndFree(a, "offline-test-owner"), 0);
    EXPECT_EQ(hold(*pm, a, "h4", p), -ENOENT);
    ASSERT_EQ(pm->unhold(a, "h1", "h1"), 0);
    ASSERT_EQ(pm->unhold(a, "h3", "h3"), 0);
    EXPECT_EQ(pm->allocationCount(), 0u);
}
