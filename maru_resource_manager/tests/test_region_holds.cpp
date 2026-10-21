// Region holds of remote agents on the fake devices of the recovery fixture:
// a held region that is freed drains (stays out of use) until its last hold
// is dropped. Nothing here opens a real DAX device.
#include "recovery_fixture.h"

#include <functional>

#include "admin_server.h"
#include "checkpoint_fault.h"

namespace {
constexpr auto npos = std::string::npos;

class RegionHoldTest : public HeaderRecoveryTest {
protected:
    const std::string owner = "offline-test-owner";
    std::string holdsFile() { return bytes(state + "/holds"); }
    std::string access(PoolManager &pm, const Handle &h) {
        std::string path;
        const int rc = pm.verifyAndGetPath(h, path);
        return rc == 0 ? path : "error " + std::to_string(rc);
    }
    std::string admin(PoolManager &pm, const std::string &line) {
        return AdminServer::handleLine(pm, line);
    }
    static std::string region(const Handle &h) { return std::to_string(h.regionId); }
};

// Sees every open() while set (fakeOpenHook).
std::function<void(const std::string &)> onOpen;
int openHook(const char *path) {
    if (onOpen) onOpen(path);
    return 0;
}
} // namespace

TEST_F(RegionHoldTest, FreedHeldRegionDrainsUntilUnheld) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle h{};
    ASSERT_EQ(allocate(pm, h), 0);
    std::string path;
    ASSERT_EQ(pm.hold(h, "node-a", "epoch-1", path), 0);
    EXPECT_EQ(path, dev(0));
    EXPECT_EQ(pm.hold(h, "node-a", "epoch-1", path), 0); // Again: no change.
    EXPECT_EQ(holdsFile(), "hold " + region(h) + " node-a epoch-1\n");

    ASSERT_EQ(pm.verifyAndFree(h, owner), 0);
    EXPECT_EQ(access(pm, h), "error " + std::to_string(-ENOENT));
    EXPECT_EQ(pm.hold(h, "node-b", "epoch-1", path), -ENOENT);
    EXPECT_EQ(pm.verifyAndFree(h, owner), 0); // Already freed.
    EXPECT_EQ(pm.allocationCount(), 1u);
    Handle other{};
    ASSERT_EQ(allocate(pm, other), 0);
    EXPECT_EQ(other.offset, h.offset + H); // The range stays out of use.
    // Only the holding instance's unhold counts.
    EXPECT_EQ(pm.unhold(h, "node-a", "epoch-0"), 0);
    EXPECT_EQ(pm.allocationCount(), 2u);
    ASSERT_EQ(pm.unhold(h, "node-a", "epoch-1"), 0);
    EXPECT_EQ(pm.allocationCount(), 1u);
    EXPECT_EQ(pm.unhold(h, "node-a", "epoch-1"), 0);
    EXPECT_EQ(holdsFile(), "");
    Handle reused{};
    ASSERT_EQ(allocate(pm, reused), 0);
    EXPECT_EQ(reused.offset, h.offset);

    // A newer instance of the holder replaces the older one.
    ASSERT_EQ(pm.hold(reused, "node-a", "epoch-1", path), 0);
    ASSERT_EQ(pm.hold(reused, "node-a", "epoch-2", path), 0);
    EXPECT_EQ(holdsFile(), "hold " + region(reused) + " node-a epoch-2\n");
    EXPECT_EQ(pm.unhold(reused, "node-a", "epoch-1"), 0);
    EXPECT_NE(holdsFile(), "");
    ASSERT_EQ(pm.unhold(reused, "node-a", "epoch-2"), 0);
    EXPECT_EQ(pm.verifyAndFree(reused, owner), 0);
    EXPECT_EQ(pm.allocationCount(), 1u);

    // A token that does not match, a freed region, bad names.
    Handle bad = other;
    bad.authToken ^= 1;
    EXPECT_EQ(pm.hold(bad, "node-a", "epoch-1", path), -EACCES);
    ASSERT_EQ(pm.hold(other, "node-a", "epoch-1", path), 0);
    EXPECT_EQ(pm.unhold(bad, "node-a", "epoch-1"), -EACCES);
    ASSERT_EQ(pm.unhold(other, "node-a", "epoch-1"), 0);
    EXPECT_EQ(pm.unhold(h, "node-a", "epoch-1"), 0); // No longer allocated.
    EXPECT_EQ(pm.hold(h, "node-a", "epoch-1", path), -ENOENT);
    for (const std::string &name : std::vector<std::string>{
             "", "node a", std::string(129, 'n'), std::string("n\0", 2), "n\x7f"}) {
        EXPECT_EQ(pm.hold(other, name, "epoch-1", path), -EINVAL);
        EXPECT_EQ(pm.hold(other, "node-a", name, path), -EINVAL);
        EXPECT_EQ(pm.unhold(other, name, "epoch-1"), -EINVAL);
        uint32_t dropped = 7;
        EXPECT_EQ(pm.resetHolds(name, "epoch-1", dropped), -EINVAL);
        EXPECT_EQ(dropped, 0u);
    }
    EXPECT_EQ(pm.hold(other, std::string(128, 'n'), std::string(128, '~'), path), 0);
    EXPECT_EQ(pm.checkpoint(), 0);
}

TEST_F(RegionHoldTest, ReaperDrainsHeldRegionsOfGoneOwners) {
    addDevice(0);
    PoolManager pm(state, 0);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle remote{}, exited{};
    ASSERT_EQ(allocate(pm, remote), 0);
    ASSERT_EQ(allocate(pm, exited, 0, exitedLocalClient()), 0);
    std::string path;
    ASSERT_EQ(pm.hold(remote, "node-a", "epoch-1", path), 0);
    ASSERT_EQ(pm.hold(exited, "node-a", "epoch-1", path), 0);
    pm.clientDisconnected(owner);
    uint64_t reaped = 0;
    pm.reapExpired(reaped);
    EXPECT_EQ(reaped, 2u);
    EXPECT_EQ(pm.allocationCount(), 2u);
    EXPECT_EQ(access(pm, remote), "error " + std::to_string(-ENOENT));
    pm.reapExpired(reaped); // Never collected twice.
    EXPECT_EQ(reaped, 0u);
    EXPECT_EQ(pm.allocationCount(), 2u);
    ASSERT_EQ(pm.unhold(remote, "node-a", "epoch-1"), 0);
    ASSERT_EQ(pm.unhold(exited, "node-a", "epoch-1"), 0);
    EXPECT_EQ(pm.allocationCount(), 0u);

    // A draining region no longer counts for its owner: the owner's PID,
    // reused by a new client, is not judged by the old start time.
    const std::string me = localClient(::getpid());
    Handle mine{};
    ASSERT_EQ(allocate(pm, mine, 0, me), 0);
    ASSERT_EQ(pm.hold(mine, "node-a", "epoch-1", path), 0);
    PoolManagerTestPeer::setPidStartTime(pm, ::getpid(), getPidStartTime(::getpid()) - 1);
    ASSERT_EQ(pm.verifyAndFree(mine, me), 0);
    Handle next{};
    ASSERT_EQ(allocate(pm, next, 0, me), 0);
    pm.reapExpired(reaped);
    EXPECT_EQ(reaped, 0u);
    EXPECT_EQ(access(pm, next), dev(0));

    // A release that fails when the last hold goes is retried by the reaper.
    checkpoint_fault::arm(state + "/maru-resource-manager.wal",
                          checkpoint_fault::Operation::Write, ENOSPC);
    EXPECT_EQ(pm.unhold(mine, "node-a", "epoch-1"), 0);
    checkpoint_fault::clear();
    EXPECT_EQ(pm.allocationCount(), 2u);
    pm.reapExpired(reaped);
    EXPECT_EQ(reaped, 0u);
    EXPECT_EQ(pm.allocationCount(), 1u);
    EXPECT_EQ(holdsFile(), "");
}

TEST_F(RegionHoldTest, ResetDropsHoldsOfEarlierInstances) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle a{}, b{}, c{};
    ASSERT_EQ(allocate(pm, a), 0);
    ASSERT_EQ(allocate(pm, b), 0);
    ASSERT_EQ(allocate(pm, c), 0);
    std::string path;
    ASSERT_EQ(pm.hold(a, "node-a", "epoch-1", path), 0);
    ASSERT_EQ(pm.hold(b, "node-a", "epoch-2", path), 0);
    ASSERT_EQ(pm.hold(c, "node-b", "epoch-1", path), 0);
    ASSERT_EQ(pm.verifyAndFree(a, owner), 0);
    ASSERT_EQ(pm.verifyAndFree(c, owner), 0);
    uint32_t dropped = 0;
    ASSERT_EQ(pm.resetHolds("node-a", "epoch-2", dropped), 0);
    EXPECT_EQ(dropped, 1u);
    EXPECT_EQ(pm.allocationCount(), 2u); // a was released; b and c stay.
    EXPECT_EQ(holdsFile(), "hold " + region(b) + " node-a epoch-2\nhold " + region(c) +
                               " node-b epoch-1\ndraining " + region(c) + "\n");
    ASSERT_EQ(pm.verifyAndFree(b, owner), 0); // The current instance's hold drains it.
    EXPECT_EQ(pm.allocationCount(), 2u);
    ASSERT_EQ(pm.resetHolds("node-a", "epoch-2", dropped), 0);
    EXPECT_EQ(dropped, 0u);
    ASSERT_EQ(pm.resetHolds("node-a", "epoch-3", dropped), 0);
    EXPECT_EQ(dropped, 1u);
    EXPECT_EQ(pm.allocationCount(), 1u);
    Handle reused{};
    ASSERT_EQ(allocate(pm, reused), 0);
    EXPECT_EQ(reused.offset, a.offset);
}

TEST_F(RegionHoldTest, HoldsSurviveARestart) {
    addDevice(0);
    Handle a{}, b{}, c{};
    // Restored from the WAL, or from a checkpoint.
    for (bool checkpointed : {false, true}) {
        SCOPED_TRACE(checkpointed ? "checkpointed" : "WAL only");
        resetState();
        {
            PoolManager pm(state);
            ASSERT_EQ(pm.loadPools(), 0);
            ASSERT_EQ(allocate(pm, a), 0);
            ASSERT_EQ(allocate(pm, b), 0);
            ASSERT_EQ(allocate(pm, c), 0);
            std::string path;
            ASSERT_EQ(pm.hold(a, "node-a", "epoch-1", path), 0);
            ASSERT_EQ(pm.hold(b, "node-a", "epoch-1", path), 0);
            ASSERT_EQ(pm.hold(c, "node-b", "epoch-1", path), 0);
            ASSERT_EQ(pm.verifyAndFree(b, owner), 0);
            ASSERT_EQ(pm.verifyAndFree(c, owner), 0);
            if (checkpointed) {
                ASSERT_EQ(pm.checkpoint(), 0);
            }
        }
        // node-b's hold was lost while the RM was down; records of regions that
        // are not allocated are stale.
        std::ofstream(state + "/holds") << "hold " << region(a) << " node-a epoch-1\n"
                                        << "hold " << region(b) << " node-a epoch-1\n"
                                        << "hold 999 node-z epoch-9\ndraining " << region(b)
                                        << "\ndraining " << region(c) << "\ndraining 998\n";
        {
            PoolManager pm(state);
            ASSERT_EQ(pm.loadPools(), 0);
            EXPECT_EQ(pm.allocationCount(), 2u); // c was released.
            EXPECT_EQ(access(pm, a), dev(0));
            EXPECT_EQ(access(pm, b), "error " + std::to_string(-ENOENT));
            EXPECT_EQ(holdsFile(), "hold " + region(a) + " node-a epoch-1\nhold " + region(b) +
                                       " node-a epoch-1\ndraining " + region(b) + "\n");
            std::vector<PoolState> stats;
            pm.getStats(stats);
            ASSERT_EQ(stats.size(), 1u);
            EXPECT_EQ(stats[0].freeSize, stats[0].totalSize - 2 * H);
            ASSERT_EQ(pm.unhold(b, "node-a", "epoch-1"), 0);
            EXPECT_EQ(pm.allocationCount(), 1u);
        }
    }
    // A malformed file stops the start before anything is decided.
    for (const char *text : {"hold 1 node-a\n", "draining x\n", "hold 0 node-a e\n",
                             "hold 1 node-a e\nhold 1 node-a e\n", "draining 5\ndraining 5\n",
                             "bogus\n", "\n", "hold 1 node-a e extra\n"}) {
        SCOPED_TRACE(text);
        std::ofstream(state + "/holds") << text;
        expectRecoveryRefused(-EINVAL);
    }
}

TEST_F(RegionHoldTest, FailedSaveChangesNothing) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle h{};
    ASSERT_EQ(allocate(pm, h), 0);
    std::string path;
    checkpoint_fault::arm(state + "/holds.tmp", checkpoint_fault::Operation::Write, ENOSPC);
    EXPECT_EQ(pm.hold(h, "node-a", "epoch-1", path), -ENOSPC);
    checkpoint_fault::clear();
    ASSERT_EQ(pm.hold(h, "node-a", "epoch-1", path), 0);
    // The free is refused, so its owner retries; the region stays usable.
    checkpoint_fault::arm(state + "/holds.tmp", checkpoint_fault::Operation::Fsync, EIO);
    EXPECT_EQ(pm.verifyAndFree(h, owner), -EIO);
    EXPECT_EQ(pm.unhold(h, "node-a", "epoch-1"), -EIO);
    uint32_t dropped = 0;
    EXPECT_EQ(pm.resetHolds("node-a", "epoch-2", dropped), -EIO);
    EXPECT_EQ(dropped, 0u);
    checkpoint_fault::clear();
    EXPECT_EQ(access(pm, h), dev(0));
    ASSERT_EQ(pm.verifyAndFree(h, owner), 0);
    EXPECT_EQ(pm.allocationCount(), 1u);
    ASSERT_EQ(pm.unhold(h, "node-a", "epoch-1"), 0);
    EXPECT_EQ(pm.allocationCount(), 0u);
}

TEST_F(RegionHoldTest, UnsyncedSaveKeepsTheNewState) {
    // The file is renamed into place but the directory sync fails: the file
    // has the new state, so memory keeps it. Adding protection reports the
    // error; removing it succeeds (a power loss only brings more holds back).
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle h{}, other{};
    ASSERT_EQ(allocate(pm, h), 0);
    ASSERT_EQ(allocate(pm, other), 0);
    std::string path;
    ASSERT_EQ(pm.hold(other, "node-a", "epoch-1", path), 0);
    checkpoint_fault::arm(state, checkpoint_fault::Operation::Fsync, EIO);
    EXPECT_EQ(pm.hold(h, "node-a", "epoch-1", path), -EIO);
    EXPECT_EQ(pm.verifyAndFree(h, owner), -EIO);
    EXPECT_EQ(holdsFile(), "hold " + region(h) + " node-a epoch-1\nhold " + region(other) +
                               " node-a epoch-1\ndraining " + region(h) + "\n");
    EXPECT_EQ(access(pm, h), "error " + std::to_string(-ENOENT));
    EXPECT_EQ(pm.verifyAndFree(h, owner), 0); // The owner's retry.
    EXPECT_EQ(pm.unhold(h, "node-a", "epoch-1"), 0);
    EXPECT_EQ(pm.allocationCount(), 1u);
    uint32_t dropped = 0;
    EXPECT_EQ(pm.resetHolds("node-a", "epoch-2", dropped), 0);
    EXPECT_EQ(dropped, 1u);
    std::string out;
    EXPECT_EQ(pm.hold(other, "node-b", "epoch-1", path), -EIO);
    EXPECT_EQ(pm.adminDropHolds("node-b", true, out), 0);
    EXPECT_NE(out.find("WARNING: syncing"), npos) << out;
    checkpoint_fault::clear();
    EXPECT_GE(checkpoint_fault::failures(), 6u);
    EXPECT_EQ(holdsFile(), "");
    EXPECT_EQ(access(pm, other), dev(0));
}

TEST_F(RegionHoldTest, ResetNeverReleasesARegionHeldMeanwhile) {
    // Both devices lost their headers and every owner exited, so both pools
    // would start over. A hold that lands on dax1's allocation while dax0 is
    // reset (the lock is released for its device I/O) keeps dax1 as it is.
    addDevice(0, false);
    addDevice(1, false);
    std::map<uint64_t, Allocation> ledger;
    for (uint32_t id : {0u, 1u}) {
        auto a = allocation(id);
        a.handle.regionId = 42 + id;
        std::snprintf(a.clientId, sizeof(a.clientId), "%s", exitedLocalClient().c_str());
        a.handle.authToken = computeAuthToken(a.handle, a.nonce, a.clientId);
        ledger[a.handle.regionId] = a;
        ASSERT_EQ(MetadataStore(state).save(pool(id)), 0);
    }
    ASSERT_EQ(MetadataStore(state).saveGlobal(ledger, 44), 0);
    // Opens of dax0 by its first probe; the next one is executePlans' reread.
    int probeOpens = 0;
    onOpen = [&](const std::string &path) { probeOpens += path == dev(0); };
    fakeOpenHook = openHook;
    probeDeviceIo(0, dev(0), DaxType::DEV_DAX, H);
    PoolManager pm(state);
    int opens = 0;
    onOpen = [&](const std::string &path) {
        if (path == dev(0) && opens++ == probeOpens)
            PoolManagerTestPeer::addHold(pm, 43, "node-a", "epoch-1");
    };
    ASSERT_EQ(pm.loadPools(), 0);
    onOpen = nullptr;
    EXPECT_GT(opens, probeOpens);
    EXPECT_EQ(pm.allocationCount(), 1u);
    std::vector<PoolState> stats;
    pm.getStats(stats);
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_EQ(stats[0].devPath, dev(0));
    EXPECT_EQ(PoolManagerTestPeer::unserved(pm, dev(1)).rfind("skipped: an allocation was held", 0),
              0u);
    // Judged again with the hold: refused, never started over.
    ASSERT_EQ(pm.rescanDevices(), 0);
    EXPECT_EQ(PoolManagerTestPeer::unserved(pm, dev(1)).rfind("refused", 0), 0u);
    EXPECT_EQ(pm.allocationCount(), 1u);
}

TEST_F(RegionHoldTest, RecoveryKeepsHeldAllocationsOfGoneOwners) {
    // A local owner from an earlier boot: its held allocation drains, the
    // other is released.
    addDevice(0);
    const std::string bootFile = fakeRoot + "/boot_id";
    auto boot = [&](const std::string &id) { std::ofstream(bootFile) << id << "\n"; };
    auto run = [&](const std::function<void(PoolManager &)> &body) {
        PoolManager rm(state);
        PoolManagerTestPeer::setBootIdPath(rm, bootFile);
        ASSERT_EQ(rm.loadPools(), 0);
        body(rm);
    };
    const std::string me = localClient(::getpid());
    Handle held{}, plain{};
    boot("11111111-1111-4111-8111-111111111111");
    run([&](PoolManager &rm) {
        ASSERT_EQ(allocate(rm, held, 0, me), 0);
        ASSERT_EQ(allocate(rm, plain, 0, me), 0);
        std::string path;
        ASSERT_EQ(rm.hold(held, "node-a", "epoch-1", path), 0);
    });
    boot("22222222-2222-4222-8222-222222222222");
    run([&](PoolManager &rm) {
        EXPECT_EQ(rm.allocationCount(), 1u);
        EXPECT_EQ(access(rm, held), "error " + std::to_string(-ENOENT));
        EXPECT_NE(holdsFile().find("draining " + region(held)), npos);
        ASSERT_EQ(rm.unhold(held, "node-a", "epoch-1"), 0);
        EXPECT_EQ(rm.allocationCount(), 0u);
    });

    // A device that lost its header starts over only when every allocation
    // on it is of an exited owner; a held one may still be written.
    resetState();
    resetDevice(0, false);
    auto exited = allocation();
    std::snprintf(exited.clientId, sizeof(exited.clientId), "%s", exitedLocalClient().c_str());
    exited.handle.authToken = computeAuthToken(exited.handle, exited.nonce, exited.clientId);
    auto p = pool();
    p.freeSize = B - 2 * H;
    p.freeList = {{2 * H, B - 2 * H}};
    save(p, {{42, exited}});
    std::ofstream(state + "/holds") << "hold 42 node-a epoch-1\n";
    EXPECT_NE(expectDeviceRefused(0).find("header lost"), npos);
    std::ofstream(state + "/holds") << "";
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_TRUE(pm.hasPools());
        EXPECT_EQ(pm.allocationCount(), 0u);
    }
}

TEST_F(RegionHoldTest, AdminListsAndDropsHolds) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle a{}, b{};
    ASSERT_EQ(allocate(pm, a), 0);
    ASSERT_EQ(allocate(pm, b), 0);
    std::string path;
    ASSERT_EQ(pm.hold(a, "node-a", "epoch-1", path), 0);
    ASSERT_EQ(pm.hold(b, "node-a", "epoch-1", path), 0);
    ASSERT_EQ(pm.hold(b, "node-b", "epoch-1", path), 0);
    EXPECT_NE(admin(pm, "status\n").find("\nholds=3 draining=0\n"), npos);
    const auto listing = admin(pm, "allocations dax0.0");
    EXPECT_NE(listing.find("region=" + region(a) + " "), npos) << listing;
    EXPECT_NE(listing.find("owner=remote held_by=node-a\n"), npos) << listing;
    EXPECT_NE(listing.find("owner=remote held_by=node-a,node-b\n"), npos) << listing;

    // release of a held region: free for its owner, draining for the rest.
    auto reply = admin(pm, "release " + region(a) + " --confirm");
    EXPECT_EQ(reply.rfind("OK\n", 0), 0u) << reply;
    EXPECT_NE(reply.find("held_by=node-a draining\n"), npos) << reply;
    EXPECT_NE(admin(pm, "status").find("\nholds=3 draining=1\n"), npos);
    EXPECT_NE(admin(pm, "allocations").find("held_by=node-a draining\n"), npos);

    reply = admin(pm, "drop-holds node-a");
    EXPECT_EQ(reply.rfind("ERR 1 ", 0), 0u) << reply;
    EXPECT_NE(reply.find("region=" + region(a) + " holder=node-a instance=epoch-1 draining\n"),
              npos);
    EXPECT_NE(reply.find("region=" + region(b) + " holder=node-a instance=epoch-1\n"), npos);
    reply = admin(pm, "drop-holds node-a --confirm");
    EXPECT_EQ(reply.rfind("OK\n", 0), 0u) << reply;
    EXPECT_NE(reply.find("dropped holder=node-a holds=2 released=1\n"), npos) << reply;
    EXPECT_EQ(pm.allocationCount(), 1u);
    EXPECT_NE(admin(pm, "status").find("\nholds=1 draining=0\n"), npos);
    EXPECT_EQ(admin(pm, "drop-holds node-a").rfind("OK\nholder node-a holds no region\n", 0),
              0u);
    for (const std::string &bad : std::vector<std::string>{
             "drop-holds", "drop-holds node-a --force", "drop-holds " + std::string(129, 'n'),
             "drop-holds node-a --confirm extra"})
        EXPECT_EQ(admin(pm, bad).rfind("ERR 22 ", 0), 0u) << bad;

    // discard: the operator confirmed nothing maps the pool, agents included.
    ASSERT_EQ(pm.verifyAndFree(b, owner), 0);
    reply = admin(pm, "discard dax0.0 --confirm");
    EXPECT_EQ(reply.rfind("OK\n", 0), 0u) << reply;
    EXPECT_EQ(pm.allocationCount(), 0u);
    EXPECT_NE(admin(pm, "status").find("\nholds=0 draining=0\n"), npos);
    EXPECT_EQ(holdsFile(), "");
}
