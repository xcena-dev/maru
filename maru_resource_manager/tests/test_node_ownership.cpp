// Node ownership on the fake devices of the recovery fixture: a node's
// regions survive its process and the grace period, are fenced by its current
// run (epoch), and are freed by that run, its lease, maru-rm-ctl or a reboot
// of this host. Nothing here opens a real DAX device.
#include "recovery_fixture.h"

#include <chrono>
#include <functional>

#include "admin_server.h"
#include "checkpoint_fault.h"

namespace {
constexpr auto npos = std::string::npos;
using std::chrono::hours;
using std::chrono::milliseconds;
using Regions = std::vector<PoolManager::NodeRegion>;

class NodeOwnershipTest : public HeaderRecoveryTest {
protected:
    std::string nodesFile() { return bytes(state + "/nodes"); }
    std::string thisHost() {
        char host[256]{};
        EXPECT_EQ(::gethostname(host, sizeof(host) - 1), 0);
        return host;
    }
    std::string access(PoolManager &pm, const Handle &h) {
        std::string path;
        const int rc = pm.verifyAndGetPath(h, path);
        return rc == 0 ? path : "error " + std::to_string(rc);
    }
    std::string admin(PoolManager &pm, const std::string &line) {
        return AdminServer::handleLine(pm, line);
    }
    Regions claim(PoolManager &pm, const std::string &node, const std::string &epoch,
                  const std::string &host = "host-a", int expected = 0) {
        Regions regions;
        EXPECT_EQ(pm.nodeClaim(node, epoch, host, regions), expected);
        return regions;
    }
    Handle nodeAlloc(PoolManager &pm, const std::string &node, const std::string &epoch,
                     int expected = 0) {
        PoolManager::NodeRegion r{};
        EXPECT_EQ(pm.nodeAlloc(node, epoch, H, dev(0), r), expected);
        if (expected == 0) {
            EXPECT_EQ(r.devPath, dev(0));
            EXPECT_EQ(r.requestedSize, H);
            EXPECT_FALSE(r.deviceUuid.empty());
        }
        return r.handle;
    }
    static bool same(const Handle &a, const Handle &b) {
        return a.regionId == b.regionId && a.offset == b.offset && a.length == b.length &&
               a.authToken == b.authToken;
    }
    uint64_t reap(PoolManager &pm) {
        uint64_t reaped = 0;
        pm.reapExpired(reaped);
        return reaped;
    }
};
} // namespace

TEST_F(NodeOwnershipTest, ClaimAllocFreeRenewRelease) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    EXPECT_EQ(pm.rmId().size(), 32u);
    EXPECT_TRUE(claim(pm, "node-a", "e1").empty());
    EXPECT_EQ(nodesFile(), "node node-a e1 host-a\n");
    const Handle a = nodeAlloc(pm, "node-a", "e1");
    const Handle b = nodeAlloc(pm, "node-a", "e1");
    EXPECT_EQ(access(pm, a), dev(0));

    // The same run again gets the same regions, tokens unchanged.
    Regions regions = claim(pm, "node-a", "e1");
    ASSERT_EQ(regions.size(), 2u);
    EXPECT_TRUE(same(regions[0].handle, a));
    EXPECT_TRUE(same(regions[1].handle, b));
    EXPECT_EQ(regions[0].devPath, dev(0));
    EXPECT_EQ(regions[0].requestedSize, H);
    EXPECT_EQ(pm.nodeRenew("node-a", "e1"), 0);

    EXPECT_EQ(pm.nodeFree(a, "node-a", "e1"), 0);
    EXPECT_EQ(pm.nodeFree(a, "node-a", "e1"), -ENOENT);
    Handle bad = b;
    bad.authToken ^= 1;
    EXPECT_EQ(pm.nodeFree(bad, "node-a", "e1"), -EACCES);
    ASSERT_EQ(claim(pm, "node-a", "e1").size(), 1u);

    uint32_t released = 0;
    EXPECT_EQ(pm.nodeRelease("node-a", "e1", released), 0);
    EXPECT_EQ(released, 1u);
    EXPECT_EQ(pm.allocationCount(), 0u);
    EXPECT_EQ(PoolManagerTestPeer::nodeCount(pm), 0u);
    EXPECT_EQ(nodesFile(), "");
    // Forgotten: the run is over, and any host may claim the node.
    EXPECT_EQ(pm.nodeRenew("node-a", "e1"), -EKEYEXPIRED);
    EXPECT_EQ(pm.nodeRelease("node-a", "e1", released), -ESTALE);
    EXPECT_TRUE(claim(pm, "node-a", "e2", "host-b").empty());
    EXPECT_EQ(pm.checkpoint(), 0);
}

TEST_F(NodeOwnershipTest, EarlierRunIsFenced) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    claim(pm, "node-a", "e1");
    const Handle a = nodeAlloc(pm, "node-a", "e1");
    Regions regions = claim(pm, "node-a", "e2");
    ASSERT_EQ(regions.size(), 1u);
    EXPECT_TRUE(same(regions[0].handle, a));
    EXPECT_EQ(nodesFile(), "node node-a e2 host-a\n");

    uint32_t released = 7;
    nodeAlloc(pm, "node-a", "e1", -ESTALE);
    EXPECT_EQ(pm.nodeFree(a, "node-a", "e1"), -ESTALE);
    EXPECT_EQ(pm.nodeRenew("node-a", "e1"), -ESTALE);
    EXPECT_EQ(pm.nodeRelease("node-a", "e1", released), -ESTALE);
    EXPECT_EQ(released, 0u);
    EXPECT_EQ(access(pm, a), dev(0));
    // A node without a record has no current run; no other run claimed it.
    nodeAlloc(pm, "node-z", "e1", -ESTALE);
    EXPECT_EQ(pm.nodeFree(a, "node-z", "e1"), -ESTALE);
    EXPECT_EQ(pm.nodeRenew("node-z", "e1"), -EKEYEXPIRED);
    // Another node's region, or a process's, is not this node's to free.
    claim(pm, "node-b", "e1");
    EXPECT_EQ(pm.nodeFree(a, "node-b", "e1"), -EPERM);
    Handle mine{};
    ASSERT_EQ(allocate(pm, mine), 0);
    EXPECT_EQ(pm.nodeFree(mine, "node-a", "e2"), -EPERM);
    EXPECT_EQ(pm.nodeFree(a, "node-a", "e2"), 0);

    for (const std::string &name : std::vector<std::string>{
             "", "node a", std::string(122, 'n'), std::string("n\0", 2), "n\x7f"}) {
        Regions none;
        EXPECT_EQ(pm.nodeClaim(name, "e1", "host-a", none), -EINVAL);
        EXPECT_EQ(pm.nodeClaim("node-a", name, "host-a", none), name.size() == 122 ? 0 : -EINVAL);
        EXPECT_EQ(pm.nodeRenew(name, "e1"), -EINVAL);
    }
    Regions none;
    EXPECT_EQ(pm.nodeClaim(std::string(121, 'n'), "e1", "host-a", none), 0);
    EXPECT_EQ(pm.nodeClaim("node-c", "e1", "", none), -EINVAL);
}

TEST_F(NodeOwnershipTest, ClaimFromAnotherHostIsRefused) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    claim(pm, "node-a", "e1", "host-a");
    const Handle a = nodeAlloc(pm, "node-a", "e1");
    claim(pm, "node-a", "e1", "host-b", -EEXIST);
    claim(pm, "node-a", "e2", "host-b", -EEXIST);
    EXPECT_EQ(pm.nodeRenew("node-a", "e1"), 0);
    EXPECT_EQ(nodesFile(), "node node-a e1 host-a\n");
    std::string out;
    EXPECT_EQ(pm.adminReleaseNode("node-a", true, out), 0) << out;
    EXPECT_EQ(access(pm, a), "error " + std::to_string(-ENOENT));
    EXPECT_TRUE(claim(pm, "node-a", "e3", "host-b").empty());
    EXPECT_EQ(nodesFile(), "node node-a e3 host-b\n");
}

TEST_F(NodeOwnershipTest, LeaseExpiryFreesAndForgets) {
    addDevice(0);
    PoolManager pm(state, 30, 900);
    ASSERT_EQ(pm.loadPools(), 0);
    EXPECT_EQ(pm.nodeLeaseMs(), 900000u);
    claim(pm, "node-a", "e1");
    claim(pm, "node-b", "e1");
    nodeAlloc(pm, "node-a", "e1");
    nodeAlloc(pm, "node-a", "e1");
    const Handle b = nodeAlloc(pm, "node-b", "e1");
    // CLAIM, ALLOC and RENEW each start the lease again; FREE does not.
    for (const auto &renew : std::vector<std::function<void()>>{
             [&] { EXPECT_EQ(pm.nodeRenew("node-b", "e1"), 0); },
             [&] { nodeAlloc(pm, "node-b", "e1"); },
             [&] { claim(pm, "node-b", "e1"); }}) {
        PoolManagerTestPeer::ageNode(pm, "node-b", milliseconds(899000));
        renew();
        PoolManagerTestPeer::ageNode(pm, "node-b", milliseconds(899000));
        EXPECT_EQ(reap(pm), 0u);
        EXPECT_EQ(PoolManagerTestPeer::nodeCount(pm), 2u);
    }

    PoolManagerTestPeer::ageNode(pm, "node-a", milliseconds(900000));
    reap(pm);
    EXPECT_EQ(pm.allocationCount(), 2u);
    EXPECT_EQ(PoolManagerTestPeer::nodeCount(pm), 1u);
    EXPECT_EQ(nodesFile(), "node node-b e1 host-a\n");
    EXPECT_EQ(pm.nodeRenew("node-a", "e1"), -EKEYEXPIRED);
    nodeAlloc(pm, "node-a", "e1", -ESTALE);
    EXPECT_TRUE(claim(pm, "node-a", "e2", "host-b").empty());
    EXPECT_EQ(access(pm, b), dev(0));

    // A release that cannot be logged fences the run at once and is retried.
    nodeAlloc(pm, "node-a", "e2");
    PoolManagerTestPeer::ageNode(pm, "node-a", milliseconds(900000));
    checkpoint_fault::arm(state + "/maru-resource-manager.wal",
                          checkpoint_fault::Operation::Write, ENOSPC);
    reap(pm);
    checkpoint_fault::clear();
    EXPECT_EQ(pm.allocationCount(), 3u);
    EXPECT_EQ(pm.nodeRenew("node-a", "e2"), -EKEYEXPIRED);
    nodeAlloc(pm, "node-a", "e2", -ESTALE);
    reap(pm);
    EXPECT_EQ(pm.allocationCount(), 2u);
    EXPECT_EQ(PoolManagerTestPeer::nodeCount(pm), 1u);
}

TEST_F(NodeOwnershipTest, HeldRegionDrainsOnLeaseExpiry) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    claim(pm, "node-a", "e1");
    const Handle a = nodeAlloc(pm, "node-a", "e1");
    std::string path;
    ASSERT_EQ(pm.hold(a, "agent-1", "i1", path), 0);
    PoolManagerTestPeer::ageNode(pm, "node-a", hours(1));
    reap(pm);
    EXPECT_EQ(PoolManagerTestPeer::nodeCount(pm), 0u);
    EXPECT_EQ(pm.allocationCount(), 1u);
    EXPECT_EQ(access(pm, a), "error " + std::to_string(-ENOENT));
    EXPECT_NE(bytes(state + "/holds").find("draining " + std::to_string(a.regionId)), npos);
    // A later run of the node does not get it back.
    EXPECT_TRUE(claim(pm, "node-a", "e2").empty());
    Handle next = nodeAlloc(pm, "node-a", "e2");
    EXPECT_NE(next.offset, a.offset);
    ASSERT_EQ(pm.unhold(a, "agent-1", "i1"), 0);
    EXPECT_EQ(pm.allocationCount(), 1u);
    // The same for NODE_RELEASE.
    ASSERT_EQ(pm.hold(next, "agent-1", "i1", path), 0);
    uint32_t released = 0;
    ASSERT_EQ(pm.nodeRelease("node-a", "e2", released), 0);
    EXPECT_EQ(released, 1u);
    EXPECT_EQ(pm.allocationCount(), 1u);
    ASSERT_EQ(pm.unhold(next, "agent-1", "i1"), 0);
    EXPECT_EQ(pm.allocationCount(), 0u);
}

TEST_F(NodeOwnershipTest, PidReaperAndGraceNeverTouchNodeOwners) {
    addDevice(0);
    const std::string bootFile = fakeRoot + "/boot_id";
    std::ofstream(bootFile) << "11111111-1111-4111-8111-111111111111\n";
    // The run's process exits, its connection drops, the grace period is 0.
    // Restarts in the same boot do not judge the node by any PID either.
    const std::string gone = exitedLocalClient();
    Handle a{};
    for (int run = 0; run < 3; ++run) {
        SCOPED_TRACE(run);
        PoolManager pm(state, 0);
        PoolManagerTestPeer::setBootIdPath(pm, bootFile);
        ASSERT_EQ(pm.loadPools(), 0);
        if (run == 0) {
            claim(pm, "1", "e1", thisHost());
            a = nodeAlloc(pm, "1", "e1");
        }
        pm.clientDisconnected(gone);
        pm.clientDisconnected("@node:1");
        pm.clientDisconnected("host-a:1");
        EXPECT_EQ(reap(pm), 0u);
        EXPECT_EQ(access(pm, a), dev(0));
        // Process requests never act as a node.
        Handle h{};
        std::string devPath, uuid;
        uint64_t requested = 0;
        EXPECT_EQ(pm.alloc(H, "@node:1", h, devPath, uuid, dev(0), requested), -EINVAL);
        EXPECT_EQ(pm.verifyAndFree(a, "@node:1"), -EPERM);
        EXPECT_EQ(pm.allocationCount(), 1u);
    }
}

TEST_F(NodeOwnershipTest, RestartKeepsOwnersAndRestartsLeases) {
    addDevice(0);
    // Restored from the WAL, or from a checkpoint.
    for (bool checkpointed : {false, true}) {
        SCOPED_TRACE(checkpointed ? "checkpointed" : "WAL only");
        resetState();
        Handle a{}, b{};
        std::string rmId;
        {
            PoolManager pm(state);
            ASSERT_EQ(pm.loadPools(), 0);
            rmId = pm.rmId();
            claim(pm, "node-a", "e1");
            a = nodeAlloc(pm, "node-a", "e1");
            claim(pm, "node-b", "e1", "host-b");
            b = nodeAlloc(pm, "node-b", "e1");
            claim(pm, "node-a", "e2");
            if (checkpointed) {
                ASSERT_EQ(pm.checkpoint(), 0);
            }
        }
        EXPECT_EQ(nodesFile(), "node node-a e2 host-a\nnode node-b e1 host-b\n");
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_EQ(pm.rmId(), rmId);
        EXPECT_EQ(pm.allocationCount(), 2u);
        // A lease that ran out before the restart starts afresh.
        PoolManagerTestPeer::ageNode(pm, "node-a", milliseconds(899000));
        EXPECT_EQ(reap(pm), 0u);
        EXPECT_EQ(PoolManagerTestPeer::nodeCount(pm), 2u);
        nodeAlloc(pm, "node-a", "e1", -ESTALE);
        claim(pm, "node-b", "e2", "host-a", -EEXIST);
        Regions regions = claim(pm, "node-a", "e3");
        ASSERT_EQ(regions.size(), 1u);
        EXPECT_TRUE(same(regions[0].handle, a));
        EXPECT_EQ(access(pm, b), dev(0));
        EXPECT_EQ(pm.nodeFree(b, "node-b", "e1"), 0);
        std::vector<PoolState> stats;
        pm.getStats(stats);
        ASSERT_EQ(stats.size(), 1u);
        EXPECT_EQ(stats[0].freeSize, stats[0].totalSize - H);
        EXPECT_EQ(pm.checkpoint(), 0);
    }
}

TEST_F(NodeOwnershipTest, NodeTableFileRules) {
    addDevice(0);
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        claim(pm, "node-a", "e1");
        nodeAlloc(pm, "node-a", "e1");
        ASSERT_EQ(pm.checkpoint(), 0);
    }
    // A malformed table stops the start before anything is decided.
    for (const char *text : {"node node-a e1\n", "node node-a e1 host-a x\n", "bogus\n", "\n",
                             "node node-a e1 host-a\nnode node-a e2 host-a\n"}) {
        SCOPED_TRACE(text);
        std::ofstream(state + "/nodes") << text;
        expectRecoveryRefused(-EINVAL);
    }
    // Lost: the regions keep their owner, which any host may claim (or its
    // lease frees them).
    fs::remove(state + "/nodes");
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_EQ(PoolManagerTestPeer::nodeCount(pm), 1u);
        EXPECT_NE(admin(pm, "allocations").find("owner=node node=node-a epoch=-\n"), npos);
        nodeAlloc(pm, "node-a", "e1", -ESTALE);
        EXPECT_EQ(claim(pm, "node-a", "e2", "host-b").size(), 1u);
        EXPECT_EQ(nodesFile(), "node node-a e2 host-b\n");
    }
    fs::remove(state + "/nodes");
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        PoolManagerTestPeer::ageNode(pm, "node-a", hours(1));
        reap(pm);
        EXPECT_EQ(pm.allocationCount(), 0u);
        EXPECT_EQ(PoolManagerTestPeer::nodeCount(pm), 0u);
    }

    // A claim is refused until the table is written, and reported until it
    // is durable; the same run's retry confirms it.
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    checkpoint_fault::arm(state + "/nodes.tmp", checkpoint_fault::Operation::Write, ENOSPC);
    claim(pm, "node-a", "e1", "host-a", -ENOSPC);
    checkpoint_fault::clear();
    EXPECT_EQ(PoolManagerTestPeer::nodeCount(pm), 0u);
    EXPECT_EQ(nodesFile(), "");
    checkpoint_fault::arm(state, checkpoint_fault::Operation::Fsync, EIO);
    claim(pm, "node-a", "e1", "host-a", -EIO);
    EXPECT_EQ(nodesFile(), "node node-a e1 host-a\n");
    claim(pm, "node-a", "e1", "host-a", -EIO);
    checkpoint_fault::clear();
    claim(pm, "node-a", "e1", "host-a");
    // Forgetting is in effect once written, durable or not.
    checkpoint_fault::arm(state + "/nodes.tmp", checkpoint_fault::Operation::Fsync, EIO);
    uint32_t released = 0;
    EXPECT_EQ(pm.nodeRelease("node-a", "e1", released), -EIO);
    EXPECT_EQ(pm.nodeRenew("node-a", "e1"), 0);
    checkpoint_fault::arm(state, checkpoint_fault::Operation::Fsync, EIO);
    EXPECT_EQ(pm.nodeRelease("node-a", "e1", released), 0);
    checkpoint_fault::clear();
    EXPECT_EQ(nodesFile(), "");
}

TEST_F(NodeOwnershipTest, RebootFreesNodesOfThisHost) {
    addDevice(0);
    const std::string bootFile = fakeRoot + "/boot_id";
    auto boot = [&](const std::string &id) { std::ofstream(bootFile) << id << "\n"; };
    auto run = [&](const std::function<void(PoolManager &)> &body) {
        PoolManager rm(state);
        PoolManagerTestPeer::setBootIdPath(rm, bootFile);
        ASSERT_EQ(rm.loadPools(), 0);
        body(rm);
    };
    const std::string host = thisHost();
    Handle local{}, held{}, remote{};
    boot("11111111-1111-4111-8111-111111111111");
    run([&](PoolManager &rm) {
        claim(rm, "local", "e1", host);
        local = nodeAlloc(rm, "local", "e1");
        held = nodeAlloc(rm, "local", "e1");
        std::string path;
        ASSERT_EQ(rm.hold(held, "agent-1", "i1", path), 0);
        claim(rm, "remote", "e1", "host-b");
        remote = nodeAlloc(rm, "remote", "e1");
    });
    run([&](PoolManager &rm) { EXPECT_EQ(rm.allocationCount(), 3u); });
    boot("22222222-2222-4222-8222-222222222222");
    run([&](PoolManager &rm) {
        EXPECT_EQ(rm.allocationCount(), 2u);  // remote, and held (draining)
        EXPECT_EQ(access(rm, local), "error " + std::to_string(-ENOENT));
        EXPECT_EQ(access(rm, held), "error " + std::to_string(-ENOENT));
        EXPECT_EQ(access(rm, remote), dev(0));
        // The record stays: the node is still bound to this host.
        EXPECT_EQ(nodesFile(), "node local e1 " + host + "\nnode remote e1 host-b\n");
        claim(rm, "local", "e2", "host-b", -EEXIST);
        EXPECT_TRUE(claim(rm, "local", "e2", host).empty());
        ASSERT_EQ(rm.unhold(held, "agent-1", "i1"), 0);
        EXPECT_EQ(rm.allocationCount(), 1u);
    });
}

TEST_F(NodeOwnershipTest, RmIdPersists) {
    addDevice(0);
    std::string id;
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        id = pm.rmId();
    }
    ASSERT_EQ(id.size(), 32u);
    EXPECT_EQ(id.find_first_not_of("0123456789abcdef"), npos);
    EXPECT_EQ(bytes(state + "/rm_id"), id + "\n");
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_EQ(pm.rmId(), id);
        EXPECT_NE(admin(pm, "status").find("\nrm_id=" + id + " nodes=0\n"), npos);
    }
    for (const std::string &text : {std::string("xyz\n"), id.substr(1) + "\n",
                                    id + "0\n", std::string(32, 'A') + "\n"}) {
        SCOPED_TRACE(text);
        std::ofstream(state + "/rm_id") << text;
        expectRecoveryRefused(-EINVAL);
    }
    // A new state directory is a new RM.
    resetState();
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    EXPECT_NE(pm.rmId(), id);
}

TEST_F(NodeOwnershipTest, AdminShowsAndReleasesNodes) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    claim(pm, "node-a", "e1");
    const Handle a = nodeAlloc(pm, "node-a", "e1");
    const Handle b = nodeAlloc(pm, "node-a", "e1");
    std::string path;
    ASSERT_EQ(pm.hold(b, "agent-1", "i1", path), 0);
    EXPECT_NE(admin(pm, "status").find(" nodes=1\n"), npos);
    auto listing = admin(pm, "allocations");
    EXPECT_NE(listing.find("client=@node:node-a owner=node node=node-a epoch=e1\n"), npos)
        << listing;
    EXPECT_NE(listing.find("node=node-a epoch=e1 held_by=agent-1\n"), npos) << listing;

    auto reply = admin(pm, "release-node node-a");
    EXPECT_EQ(reply.rfind("ERR 1 ", 0), 0u) << reply;
    EXPECT_NE(reply.find("node=node-a epoch=e1 host=host-a\n"), npos) << reply;
    EXPECT_NE(reply.find("region=" + std::to_string(a.regionId) + " "), npos) << reply;
    EXPECT_EQ(pm.allocationCount(), 2u);
    reply = admin(pm, "release-node node-a --confirm");
    EXPECT_EQ(reply.rfind("OK\n", 0), 0u) << reply;
    EXPECT_NE(reply.find("released node=node-a regions=2\n"), npos) << reply;
    EXPECT_EQ(pm.allocationCount(), 1u);  // b drains until unheld
    EXPECT_NE(admin(pm, "allocations").find("node=node-a epoch=- held_by=agent-1 draining\n"),
              npos);
    EXPECT_EQ(admin(pm, "release-node node-a").rfind("ERR 2 ", 0), 0u);
    EXPECT_EQ(pm.nodeRenew("node-a", "e1"), -EKEYEXPIRED);
    for (const std::string &bad : std::vector<std::string>{
             "release-node", "release-node node-a --force", "release-node a b --confirm"})
        EXPECT_EQ(admin(pm, bad).rfind("ERR 22 ", 0), 0u) << bad;

    // release of one region of a node needs no running-owner check.
    claim(pm, "node-a", "e2");
    const Handle c = nodeAlloc(pm, "node-a", "e2");
    reply = admin(pm, "release " + std::to_string(c.regionId) + " --confirm");
    EXPECT_EQ(reply.rfind("OK\n", 0), 0u) << reply;
    EXPECT_TRUE(claim(pm, "node-a", "e2").empty());
}

TEST_F(NodeOwnershipTest, MissingRmIdWithNodeStateRefusesStart) {
    addDevice(0);
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        claim(pm, "node-a", "e1");
        nodeAlloc(pm, "node-a", "e1");
    }
    const std::string rmId = bytes(state + "/rm_id");
    const std::string nodes = nodesFile();
    // A node record, or a node's region without one.
    fs::remove(state + "/rm_id");
    expectRecoveryRefused(-ENOKEY);
    fs::remove(state + "/nodes");
    expectRecoveryRefused(-ENOKEY);
    std::ofstream(state + "/rm_id") << rmId;
    std::ofstream(state + "/nodes") << nodes;
    // Only a draining region of a forgotten node.
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        const Handle b = nodeAlloc(pm, "node-a", "e1");
        std::string path;
        ASSERT_EQ(pm.hold(b, "agent-1", "i1", path), 0);
        uint32_t released = 0;
        ASSERT_EQ(pm.nodeRelease("node-a", "e1", released), 0);
        EXPECT_EQ(pm.allocationCount(), 1u);
    }
    EXPECT_EQ(nodesFile(), "");
    fs::remove(state + "/rm_id");
    expectRecoveryRefused(-ENOKEY);
    std::ofstream(state + "/rm_id") << rmId;
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_EQ(pm.rmId() + "\n", rmId);
        ASSERT_EQ(pm.allocationCount(), 1u);
        ASSERT_EQ(admin(pm, "drop-holds agent-1 --confirm").rfind("OK\n", 0), 0u);
        EXPECT_EQ(pm.allocationCount(), 0u);
    }
    // Without node state a new ID is harmless.
    fs::remove(state + "/rm_id");
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    EXPECT_NE(pm.rmId() + "\n", rmId);
}

TEST_F(NodeOwnershipTest, LoadedNodeTableMustBeDurableBeforeAClaimSucceeds) {
    addDevice(0);
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        claim(pm, "node-a", "e1");
    }
    // An earlier RM may have left the table unsynced; this start cannot sync it.
    checkpoint_fault::arm(state, checkpoint_fault::Operation::Fsync, EIO);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    claim(pm, "node-a", "e1", "host-a", -EIO);
    EXPECT_EQ(pm.nodeRenew("node-a", "e1"), 0);
    checkpoint_fault::clear();
    claim(pm, "node-a", "e1");
    // Durable now: a further retry does not sync again.
    checkpoint_fault::arm(state, checkpoint_fault::Operation::Fsync, EIO);
    claim(pm, "node-a", "e1");
    EXPECT_EQ(checkpoint_fault::failures(), 0u);
    checkpoint_fault::clear();
}

TEST_F(NodeOwnershipTest, FailedClaimLeavesAFencedRunFenced) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    claim(pm, "node-a", "e1");
    nodeAlloc(pm, "node-a", "e1");
    // Run e2's claim is in effect but not durable.
    checkpoint_fault::arm(state, checkpoint_fault::Operation::Fsync, EIO);
    claim(pm, "node-a", "e2", "host-a", -EIO);
    // Its lease runs out, and the release cannot be logged: fenced.
    PoolManagerTestPeer::ageNode(pm, "node-a", hours(1));
    checkpoint_fault::arm(state + "/maru-resource-manager.wal",
                          checkpoint_fault::Operation::Write, ENOSPC);
    reap(pm);
    EXPECT_EQ(pm.nodeRenew("node-a", "e2"), -EKEYEXPIRED);
    EXPECT_EQ(pm.nodeRenew("node-a", "e1"), -ESTALE);  // e2 claimed after it
    // The same run's retry fails again: still fenced, lease not renewed.
    checkpoint_fault::arm(state, checkpoint_fault::Operation::Fsync, EIO);
    claim(pm, "node-a", "e2", "host-a", -EIO);
    EXPECT_EQ(pm.nodeRenew("node-a", "e2"), -EKEYEXPIRED);
    checkpoint_fault::clear();
    reap(pm);
    EXPECT_EQ(PoolManagerTestPeer::nodeCount(pm), 0u);
    EXPECT_EQ(pm.allocationCount(), 0u);
}
