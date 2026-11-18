// Worker holds of node-owned regions (NODE_HOLD): proof that a worker maps a
// region, fenced by NODE_CLAIM, listed in the claim, and dropped by the
// reaper only for a worker the RM can see is gone. Fake devices only.
#include "recovery_fixture.h"

#include <signal.h>

#include <atomic>
#include <chrono>
#include <set>
#include <thread>

#include "admin_server.h"
#include "checkpoint_fault.h"

namespace {
constexpr auto npos = std::string::npos;
using std::chrono::hours;
using std::chrono::milliseconds;
const std::string kBoot = "11111111-1111-4111-8111-111111111111";
constexpr uint32_t kNoSuchPid = 2147483647;  // Above any pid_max

class NodeHoldTest : public HeaderRecoveryTest {
protected:
    std::string bootFile;
    void SetUp() override {
        HeaderRecoveryTest::SetUp();
        bootFile = fakeRoot + "/boot_id";
        std::ofstream(bootFile) << kBoot << "\n";
        addDevice(0);
    }
    std::unique_ptr<PoolManager> start() {
        auto pm = std::make_unique<PoolManager>(state);
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
    // A worker on this host, in this boot and pid namespace.
    static HoldRecord worker(const std::string &epoch, uint32_t pid = ::getpid(),
                             uint64_t start = getPidStartTime(::getpid())) {
        HoldRecord w;
        w.nodeEpoch = epoch;
        w.host = thisHost();
        w.bootId = kBoot;
        w.pid = pid;
        w.pidStart = start;
        w.pidNs = ownPidNs();
        return w;
    }
    static HoldRecord deadWorker(const std::string &epoch) {
        return worker(epoch, kNoSuchPid, 12345);
    }
    std::vector<PoolManager::NodeRegion> claim(PoolManager &pm, const std::string &epoch,
                                               const std::string &node = "n1") {
        std::vector<PoolManager::NodeRegion> regions;
        EXPECT_EQ(pm.nodeClaim(node, epoch, thisHost(), regions), 0);
        return regions;
    }
    Handle nodeAlloc(PoolManager &pm, const std::string &epoch, const std::string &node = "n1") {
        PoolManager::NodeRegion r{};
        EXPECT_EQ(pm.nodeAlloc(node, epoch, H, dev(0), r), 0);
        return r.handle;
    }
    int hold(PoolManager &pm, const Handle &h, const std::string &holder, const HoldRecord &who,
             const std::string &node = "n1") {
        std::string path;
        const int rc = pm.nodeHold(h, node, holder, who, path);
        if (rc == 0) {
            EXPECT_EQ(path, dev(0));
        }
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

TEST_F(NodeHoldTest, ChecksAndIdempotence) {
    auto pm = start();
    claim(*pm, "e1");
    const Handle a = nodeAlloc(*pm, "e1");
    const HoldRecord w = worker("e1");
    ASSERT_EQ(hold(*pm, a, "w1", w), 0);
    const std::string line = "hold " + std::to_string(a.regionId) + " w1 w1 e1 " + w.host + " " +
                             kBoot + " " + std::to_string(w.pid) + " " +
                             std::to_string(w.pidStart) + " " + std::to_string(w.pidNs) + "\n";
    EXPECT_EQ(bytes(state + "/holds"), line);
    EXPECT_EQ(hold(*pm, a, "w1", w), 0);  // Again: no change.
    EXPECT_EQ(bytes(state + "/holds"), line);

    Handle bad = a;
    bad.authToken ^= 1;
    EXPECT_EQ(hold(*pm, bad, "w2", w), -EACCES);
    claim(*pm, "e1", "n2");
    HoldRecord other = w;
    EXPECT_EQ(hold(*pm, a, "w2", other, "n2"), -EPERM);
    other.nodeEpoch = "e0";
    EXPECT_EQ(hold(*pm, a, "w2", other), -ESTALE);
    Handle absent = a;
    absent.regionId = 999;
    EXPECT_EQ(hold(*pm, absent, "w2", w), -ENOENT);
    for (const auto &[holder, host, boot] : std::vector<std::tuple<std::string, std::string,
                                                                   std::string>>{
             {"w 2", w.host, kBoot}, {"w2", "", kBoot}, {"w2", w.host, "-"},
             {"w2", w.host, "a b"}}) {
        HoldRecord who = w;
        who.host = host;
        who.bootId = boot;
        EXPECT_EQ(hold(*pm, a, holder, who), -EINVAL) << holder << "/" << host << "/" << boot;
    }
    // No boot ID: kept, written as "-".
    HoldRecord unknown = w;
    unknown.bootId.clear();
    ASSERT_EQ(hold(*pm, a, "w3", unknown), 0);
    EXPECT_NE(bytes(state + "/holds").find(" w3 w3 e1 " + w.host + " - "), npos);

    // A region freed while held drains; no new hold on it.
    ASSERT_EQ(pm->nodeFree(a, "n1", "e1"), 0);
    EXPECT_EQ(hold(*pm, a, "w4", w), -ENOENT);
    EXPECT_EQ(pm->unhold(a, "w1", "w1"), 0);
    EXPECT_EQ(pm->unhold(a, "w1", "w1"), 0);
    EXPECT_EQ(pm->allocationCount(), 1u);
    EXPECT_EQ(pm->unhold(a, "w3", "w3"), 0);
    EXPECT_EQ(pm->allocationCount(), 0u);
    EXPECT_EQ(bytes(state + "/holds"), "");

    // A later run's hold of the same worker replaces its record.
    const Handle b = nodeAlloc(*pm, "e1");
    ASSERT_EQ(hold(*pm, b, "w1", w), 0);
    claim(*pm, "e2");
    ASSERT_EQ(hold(*pm, b, "w1", worker("e2")), 0);
    EXPECT_NE(bytes(state + "/holds").find(" w1 w1 e2 "), npos);
}

TEST_F(NodeHoldTest, ClaimListsHoldersAndFencesLaterOldEpochHolds) {
    auto pm = start();
    claim(*pm, "e1");
    const Handle a = nodeAlloc(*pm, "e1");
    // Workers of run e1 keep holding while run e2 claims the node.
    std::atomic<int> made{0};
    std::vector<std::set<std::string>> held(4);
    std::vector<std::thread> workers;
    for (size_t t = 0; t < held.size(); ++t)
        workers.emplace_back([&, t] {
            for (int i = 0;; ++i) {
                const std::string name = "t" + std::to_string(t) + "-" + std::to_string(i);
                const int rc = hold(*pm, a, name, worker("e1"));
                if (rc == -ESTALE) return;
                EXPECT_EQ(rc, 0);
                held[t].insert(name);
                ++made;
            }
        });
    while (made < 16) std::this_thread::yield();
    const auto regions = claim(*pm, "e2");
    for (auto &w : workers) w.join();
    std::set<std::string> before;
    for (const auto &names : held) before.insert(names.begin(), names.end());
    ASSERT_EQ(regions.size(), 1u);
    std::set<std::string> listed;
    for (const auto &[holder, h] : regions[0].holders) {
        listed.insert(holder);
        EXPECT_EQ(h.nodeEpoch, "e1");
        EXPECT_EQ(h.host, thisHost());
        EXPECT_EQ(h.bootId, kBoot);
        EXPECT_EQ(h.pid, static_cast<uint32_t>(::getpid()));
        EXPECT_EQ(h.pidStart, getPidStartTime(::getpid()));
        EXPECT_EQ(h.pidNs, ownPidNs());
    }
    // Every hold made before the claim is listed; none got in after it.
    EXPECT_GE(before.size(), 16u);
    EXPECT_EQ(listed, before);
    const auto now = holders(*pm, a);
    EXPECT_EQ(std::set<std::string>(now.begin(), now.end()), before);
    EXPECT_EQ(hold(*pm, a, "late", worker("e1")), -ESTALE);

    // The earlier run's holds still protect the region.
    ASSERT_EQ(pm->nodeFree(a, "n1", "e2"), 0);
    EXPECT_EQ(access(*pm, a), "error " + std::to_string(-ENOENT));
    const Handle b = nodeAlloc(*pm, "e2");
    EXPECT_NE(b.offset, a.offset);
    for (const auto &name : before) ASSERT_EQ(pm->unhold(a, name, name), 0);
    EXPECT_EQ(pm->allocationCount(), 1u);
}

TEST_F(NodeHoldTest, LeaseExpiryDrainsUntilTheLastWorkerUnholds) {
    auto pm = start();
    claim(*pm, "e1");
    const Handle a = nodeAlloc(*pm, "e1");
    ASSERT_EQ(hold(*pm, a, "w1", worker("e1")), 0);
    ASSERT_EQ(hold(*pm, a, "w2", worker("e1")), 0);
    ASSERT_EQ(pm->unhold(a, "w1", "w1"), 0);
    PoolManagerTestPeer::ageNode(*pm, "n1", hours(1));
    reap(*pm);
    EXPECT_EQ(PoolManagerTestPeer::nodeCount(*pm), 0u);
    EXPECT_EQ(pm->allocationCount(), 1u);
    EXPECT_EQ(access(*pm, a), "error " + std::to_string(-ENOENT));
    claim(*pm, "e2");
    const Handle b = nodeAlloc(*pm, "e2");
    EXPECT_NE(b.offset, a.offset);
    reap(*pm);  // w2 is alive: kept.
    EXPECT_EQ(holders(*pm, a), Names{"w2"});
    ASSERT_EQ(pm->unhold(a, "w2", "w2"), 0);
    EXPECT_EQ(pm->allocationCount(), 1u);
    EXPECT_EQ(nodeAlloc(*pm, "e2").offset, a.offset);
}

TEST_F(NodeHoldTest, ReaperDropsOnlyWorkersSeenGoneHere) {
    auto pm = start();
    PoolManagerTestPeer::setHoldGrace(*pm, hours(1));
    claim(*pm, "e1");
    const Handle a = nodeAlloc(*pm, "e1");
    const uint64_t pidStart = getPidStartTime(::getpid());
    HoldRecord otherNs = deadWorker("e1"), otherHost = deadWorker("e1"),
               rebooted = deadWorker("e1"), noBoot = deadWorker("e1");
    otherNs.pidNs += 1;
    otherHost.host = "other-host";
    rebooted.bootId = "22222222-2222-4222-8222-222222222222";
    noBoot.bootId.clear();
    ASSERT_EQ(hold(*pm, a, "dead", deadWorker("e1")), 0);
    ASSERT_EQ(hold(*pm, a, "reused", worker("e1", ::getpid(), pidStart - 1)), 0);
    ASSERT_EQ(hold(*pm, a, "alive", worker("e1")), 0);
    ASSERT_EQ(hold(*pm, a, "other-ns", otherNs), 0);
    ASSERT_EQ(hold(*pm, a, "other-host", otherHost), 0);
    ASSERT_EQ(hold(*pm, a, "rebooted", rebooted), 0);
    ASSERT_EQ(hold(*pm, a, "no-boot", noBoot), 0);
    std::string path;
    ASSERT_EQ(pm->hold(a, "agent", "i1", path), 0);
    const auto listing = AdminServer::handleLine(*pm, "allocations");
    EXPECT_NE(listing.find("alive[epoch=e1,host=" + thisHost() + ",boot=" + kBoot +
                           ",pid=" + std::to_string(::getpid()) + ",start=" +
                           std::to_string(pidStart) + ",ns=" + std::to_string(ownPidNs()) + "]"),
              npos)
        << listing;
    EXPECT_NE(listing.find(" held_by=agent,alive["), npos) << listing;

    // An earlier boot goes at once; a gone process after the grace.
    reap(*pm);
    EXPECT_EQ(holders(*pm, a), (Names{"agent", "alive", "dead", "no-boot", "other-host",
                                      "other-ns", "reused"}));
    PoolManagerTestPeer::ageGoneHolders(*pm, hours(1));
    reap(*pm);
    const Names kept{"agent", "alive", "no-boot", "other-host", "other-ns"};
    EXPECT_EQ(holders(*pm, a), kept);
    PoolManagerTestPeer::ageGoneHolders(*pm, hours(1));
    reap(*pm);
    EXPECT_EQ(holders(*pm, a), kept);

    // A restart reads the identities back and judges them again, grace and all.
    ASSERT_EQ(hold(*pm, a, "dead-2", deadWorker("e1")), 0);
    ASSERT_EQ(pm->nodeFree(a, "n1", "e1"), 0);
    for (const auto &name : Names{"alive", "no-boot", "other-host", "other-ns"})
        ASSERT_EQ(pm->unhold(a, name, name), 0);
    ASSERT_EQ(pm->unhold(a, "agent", "i1"), 0);
    pm.reset();
    pm = start();
    PoolManagerTestPeer::setHoldGrace(*pm, hours(1));
    EXPECT_EQ(holders(*pm, a), Names{"dead-2"});
    reap(*pm);
    EXPECT_EQ(pm->allocationCount(), 1u);
    PoolManagerTestPeer::ageGoneHolders(*pm, hours(1));
    reap(*pm);
    // Its last hold gone, the draining region is freed.
    EXPECT_EQ(pm->allocationCount(), 0u);
    EXPECT_EQ(bytes(state + "/holds"), "");
}

TEST_F(NodeHoldTest, UnholdByTheNodeAndTheReaperIsSafe) {
    auto pm = start();
    PoolManagerTestPeer::setHoldGrace(*pm, milliseconds(0));
    claim(*pm, "e1");
    const Handle a = nodeAlloc(*pm, "e1");
    const Handle b = nodeAlloc(*pm, "e1");
    ASSERT_EQ(hold(*pm, a, "w1", deadWorker("e1")), 0);
    ASSERT_EQ(hold(*pm, b, "w1", deadWorker("e1")), 0);
    // The node first, then the reaper.
    ASSERT_EQ(pm->unhold(a, "w1", "w1"), 0);
    reap(*pm);
    EXPECT_EQ(pm->unhold(a, "w1", "w1"), 0);
    // The reaper first (grace 0), then the node.
    EXPECT_TRUE(holders(*pm, b).empty());
    EXPECT_EQ(pm->unhold(b, "w1", "w1"), 0);
    EXPECT_EQ(access(*pm, a), dev(0));
    EXPECT_EQ(access(*pm, b), dev(0));
    EXPECT_EQ(bytes(state + "/holds"), "");
}

TEST_F(NodeHoldTest, HoldsFileKeepsOldLinesAndRefusesBadOnes) {
    Handle a{};
    {
        auto pm = start();
        claim(*pm, "e1");
        a = nodeAlloc(*pm, "e1");
        ASSERT_EQ(pm->checkpoint(), 0);
    }
    const std::string r = std::to_string(a.regionId);
    // A line of an earlier RM: no identity, never reaped.
    std::ofstream(state + "/holds") << "hold " << r << " agent i1\n";
    {
        auto pm = start();
        PoolManagerTestPeer::setHoldGrace(*pm, milliseconds(0));
        reap(*pm);
        EXPECT_EQ(holders(*pm, a), Names{"agent"});
    }
    for (const std::string &text : std::vector<std::string>{
             "hold " + r + " w1 w1 e1 host boot 1 2\n",
             "hold " + r + " w1 w1 e1 host boot x 2 3\n",
             "hold " + r + " w1 w1 e1 host boot 4294967296 2 3\n",
             "hold " + r + " w1 w1 e1 host boot 1 2 3 4\n"}) {
        SCOPED_TRACE(text);
        std::ofstream(state + "/holds") << text;
        expectRecoveryRefused(-EINVAL);
    }
}

TEST_F(NodeHoldTest, NodeHoldsListsTheHoldsNowAndChangesNothing) {
    auto pm = start();
    claim(*pm, "e1");
    const Handle a = nodeAlloc(*pm, "e1");
    const Handle b = nodeAlloc(*pm, "e1");
    const Handle gone = nodeAlloc(*pm, "e1");
    ASSERT_EQ(hold(*pm, a, "w1@e1", worker("e1")), 0);
    std::string path;
    ASSERT_EQ(pm->hold(b, "agent-1", "i1", path), 0);
    ASSERT_EQ(pm->hold(gone, "agent-1", "i1", path), 0);
    ASSERT_EQ(pm->nodeFree(gone, "n1", "e1"), 0);  // Draining: not listed.
    claim(*pm, "e2");
    ASSERT_EQ(hold(*pm, a, "w1@e2", worker("e2")), 0);
    const std::string holdsFile = bytes(state + "/holds");
    const std::string nodesFile = bytes(state + "/nodes");

    std::vector<PoolManager::NodeRegion> regions;
    ASSERT_EQ(pm->nodeHolds("n1", "e2", regions), 0);
    ASSERT_EQ(regions.size(), 2u);
    EXPECT_EQ(regions[0].handle.regionId, a.regionId);
    EXPECT_EQ(regions[0].devPath, dev(0));
    std::map<std::string, std::string> epochs;
    for (const auto &[holder, h] : regions[0].holders) epochs[holder] = h.nodeEpoch;
    EXPECT_EQ(epochs, (std::map<std::string, std::string>{{"w1@e1", "e1"}, {"w1@e2", "e2"}}));
    ASSERT_EQ(regions[1].holders.size(), 1u);
    EXPECT_EQ(regions[1].holders[0].first, "agent-1");
    EXPECT_EQ(regions[1].holders[0].second.nodeEpoch, "");
    EXPECT_EQ(bytes(state + "/holds"), holdsFile);
    EXPECT_EQ(bytes(state + "/nodes"), nodesFile);

    // The earlier run's hold goes: listed no more.
    ASSERT_EQ(pm->unhold(a, "w1@e1", "w1@e1"), 0);
    ASSERT_EQ(pm->nodeHolds("n1", "e2", regions), 0);
    EXPECT_EQ(regions[0].holders.size(), 1u);

    // Only the current run asks; names are checked.
    EXPECT_EQ(pm->nodeHolds("n1", "e1", regions), -ESTALE);
    EXPECT_TRUE(regions.empty());
    EXPECT_EQ(pm->nodeHolds("n9", "e2", regions), -ESTALE);
    EXPECT_EQ(pm->nodeHolds("n 1", "e2", regions), -EINVAL);
    EXPECT_EQ(pm->nodeHolds("n1", "", regions), -EINVAL);

    // It does not renew the lease, and a fenced run may not ask.
    PoolManagerTestPeer::ageNode(*pm, "n1", milliseconds(899000));
    ASSERT_EQ(pm->nodeHolds("n1", "e2", regions), 0);
    PoolManagerTestPeer::ageNode(*pm, "n1", milliseconds(1000));
    reap(*pm);
    EXPECT_EQ(PoolManagerTestPeer::nodeCount(*pm), 0u);
    EXPECT_EQ(pm->nodeHolds("n1", "e2", regions), -ESTALE);
}
