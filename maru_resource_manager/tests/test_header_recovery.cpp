// Exercise production discovery/recovery using a private fake /dev and sysfs.
// This target links fake_dax_fs.cpp; it never opens real DAX devices.
#include "recovery_fixture.h"
#include "server_config.h"
#include <thread>
#include <mutex>
#include <functional>
#include <condition_variable>

std::vector<std::string> openedPaths;
static std::mutex openedMutex;

// One open() of a chosen path (the nth) either stalls until released, as an
// unresponsive device does, or runs an action instead (UnresponsiveDevice*).
static std::mutex hangMutex;
static std::condition_variable hangCv;
static std::string hangPath;
static int hangOpen = -1;
static int hangOpensSeen = 0;
static bool hangReleased = true;
static std::function<void()> hangAction;

static void armHang(const std::string &path, int nthOpen, std::function<void()> action = {}) {
    std::lock_guard<std::mutex> lock(hangMutex);
    hangPath = path;
    hangOpen = nthOpen;
    hangOpensSeen = 0;
    hangReleased = false;
    hangAction = std::move(action);
}

static void releaseHang() {
    {
        std::lock_guard<std::mutex> lock(hangMutex);
        hangReleased = true;
        hangPath.clear();
    }
    hangCv.notify_all();
}

int recordOpen(const char *path) {
    {
        std::lock_guard<std::mutex> lock(openedMutex);
        openedPaths.emplace_back(path);
    }
    // Holds a caller in open(), e.g. a wipe child (test_device_exclusion.cpp).
    if (std::string(path) == "/dev/dax-hang") {
        ::sleep(30);
        errno = EIO;
        return -1;
    }
    std::unique_lock<std::mutex> lock(hangMutex);
    if (!hangPath.empty() && path == hangPath && hangOpensSeen++ == hangOpen) {
        if (hangAction) {
            auto action = hangAction;
            lock.unlock();
            action();
            return 0;
        }
        hangCv.wait_for(lock, std::chrono::seconds(30), [] { return hangReleased; });
    }
    return 0;
}

TEST_F(HeaderRecoveryTest, FreshPoolInitializesHeaderAfterValidation) {
    addDevice(0, false);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    DeviceHeader header{};
    EXPECT_EQ(readDeviceHeader(dev(0), header, H), 0);
    Handle h{};
    ASSERT_EQ(allocate(pm, h), 0);
    EXPECT_EQ(h.offset, H);
    EXPECT_EQ(pm.verifyAndFree(h, "offline-test-owner"), 0);
    EXPECT_EQ(pm.checkpoint(), 0);
    EXPECT_FALSE(fs::exists(state + "/maru-resource-manager.wal"));
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    ASSERT_EQ(allocate(restarted, h), 0);
    EXPECT_EQ(h.offset, H);
    // Kernels without the per-device align attribute: the region's, else
    // 2 MiB, never a skipped device.
    fs::remove(fakeRoot + "/sys/bus/dax/devices/dax0.0/align");
    PoolManager older(state);
    ASSERT_EQ(older.loadPools(), 0);
    EXPECT_TRUE(older.hasPools());
}

TEST_F(HeaderRecoveryTest, LegacyRecordIsReplaced) {
    addDevice(0, false);
    // A version 6 record of an earlier RM, whatever its free list and total
    // say (here the header range that RMs before #40 kept).
    for (bool header : {true, false}) {
        SCOPED_TRACE(header ? "header" : "no header");
        resetState();
        resetDevice(0, header);
        const auto before = head(0, 32);
        auto p = pool();
        p.totalSize = p.freeSize = B;
        p.freeList = {{0, B}};
        save(p);
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        std::vector<PoolState> stats;
        pm.getStats(stats);
        ASSERT_EQ(stats.size(), 1u);
        EXPECT_EQ(stats[0].freeSize, B - H);
        EXPECT_EQ(record().kind, PoolRecord::Kind::V7);
        DeviceHeader hdr{};
        EXPECT_EQ(readDeviceHeader(dev(0), hdr, H), 0);
        // With its header the device keeps its UUID; without one it started
        // over and the old record was kept aside.
        EXPECT_EQ(head(0, 32) == before, header);
        EXPECT_EQ(archives().size(), header ? 0u : 1u);
        Handle h{};
        ASSERT_EQ(allocate(pm, h), 0);
        EXPECT_EQ(h.offset, H);
    }
}

TEST_F(HeaderRecoveryTest, LegacyPoolWithLiveAllocationIsServed) {
    addDevice(0);
    // The ledger carries over; the version 6 record's free list and total
    // are not read.
    auto p = pool();
    p.totalSize = 4 * (B - H);
    p.freeSize = B - 2 * H;
    p.freeList = {{2 * H, B - 2 * H}};
    save(p, {{42, allocation()}});
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        std::vector<PoolState> stats;
        pm.getStats(stats);
        ASSERT_EQ(stats.size(), 1u);
        EXPECT_EQ(stats[0].totalSize, B - H);
        EXPECT_EQ(stats[0].freeSize, B - 2 * H);
        // Recorded as version 7 with the device's layout.
        const auto saved = record();
        ASSERT_EQ(saved.kind, PoolRecord::Kind::V7);
        EXPECT_EQ(saved.dataBegin, H);
        EXPECT_EQ(saved.dataEnd, B);
        EXPECT_EQ(pm.checkpoint(), 0);
    }
    PoolManager restarted(state); // The version 7 path.
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.allocationCount(), 1u);
}

TEST_F(HeaderRecoveryTest, ResizedDeviceIsResetUnlessItsAllocationsMayBeInUse) {
    addDevice(0, false);
    addDevice(1);
    // A device re-provisioned to another size lost its header; its
    // checkpoint is consistent for the old size and holds no allocation.
    for (uint64_t size : {B / 2, 2 * B}) {
        SCOPED_TRACE(size);
        resetState();
        resetDevice(0, false);
        auto p = pool();
        p.totalSize = p.freeSize = size - H;
        p.freeList = {{H, size - H}};
        save(p);
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        std::vector<PoolState> stats;
        pm.getStats(stats);
        ASSERT_EQ(stats.size(), 2u);
        EXPECT_EQ(record().kind, PoolRecord::Kind::V7);
        EXPECT_EQ(record().dataEnd, B);
        DeviceHeader hdr{};
        EXPECT_EQ(readDeviceHeader(dev(0), hdr, H), 0);
        Handle h{};
        ASSERT_EQ(allocate(pm, h), 0);
        EXPECT_EQ(h.offset, H);
        EXPECT_GE(h.regionId, 43u); // IDs of the history are not reused.
    }

    // With its header the pool starts over under the same UUID; so does a
    // device whose size is unchanged but whose identity tags no longer
    // match (an interleave member changed).
    resetState();
    resetDevice(0, true);
    { PoolManager pm(state); ASSERT_EQ(pm.loadPools(), 0); } // Records version 7.
    const auto header = head(0, 32);
    resetDevice(0, false, 2 * B);
    std::ofstream(fakeRoot + dev(0), std::ios::in | std::ios::out | std::ios::binary)
        << header; // The header survived, the rest of the mapping changed.
    { PoolManager pm(state); ASSERT_EQ(pm.loadPools(), 0); }
    EXPECT_EQ(head(0, 32), header);
    EXPECT_EQ(record().dataEnd, 2 * B);
    {
        std::fstream f(fakeRoot + dev(0), std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(3 * kDeviceTagStride);
        f << std::string(32, 'x');
    }
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_EQ(PoolManagerTestPeer::unserved(pm, dev(0)), "");
    }
    EXPECT_EQ(head(0, 32), header);
    EXPECT_EQ(archives().size(), 2u); // Both started over.
    EXPECT_EQ(head(0, 3 * kDeviceTagStride + 7).substr(3 * kDeviceTagStride), "MARUTAG");

    // A recorded allocation whose owner may still run keeps the device
    // refused, and the other device is served.
    {
        std::fstream f(fakeRoot + dev(0), std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(3 * kDeviceTagStride);
        f << std::string(32, 'x');
    }
    auto a = allocation();
    ASSERT_EQ(MetadataStore(state).saveGlobal({{42, a}}, 43), 0);
    EXPECT_NE(expectDeviceRefused(0).find("identity tags"), std::string::npos);
}

TEST_F(HeaderRecoveryTest, DamagedRecordOrIdentityMismatchIsResetOrRefused) {
    addDevice(0);
    { PoolManager pm(state); ASSERT_EQ(pm.loadPools(), 0); } // Records version 7.
    auto original = bytes(state + "/pool_0.meta");
    // A damaged CRC or UUID: nothing recorded on the pool, so it starts over
    // and keeps the damaged file aside.
    for (size_t field : {size_t(12), size_t(24)}) {
        auto corrupt = original;
        corrupt[field] ^= 0x7f;
        for (const auto &old : archives()) fs::remove(old);
        std::ofstream(state + "/pool_0.meta", std::ios::binary) << corrupt;
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_EQ(record().kind, PoolRecord::Kind::V7);
        ASSERT_EQ(archives().size(), 1u);
        EXPECT_EQ(bytes(archives().front()), corrupt);
    }
    // With an allocation whose owner may run, nothing is replaced.
    auto corrupt = original;
    corrupt[12] ^= 0x7f;
    std::ofstream(state + "/pool_0.meta", std::ios::binary) << corrupt;
    ASSERT_EQ(MetadataStore(state).saveGlobal({{42, allocation()}}, 43), 0);
    EXPECT_NE(expectDeviceRefused(0).find("damaged"), std::string::npos);
    // A record from a newer RM is never replaced either.
    std::string newer = original;
    newer[4] = 8;
    std::ofstream(state + "/pool_0.meta", std::ios::binary) << newer;
    EXPECT_NE(expectDeviceRefused(0).find("newer"), std::string::npos);

    // A device identity that does not match the records is refused, and
    // neither device is written.
    {
        resetState();
        resetDevice(0, true);
        addDevice(1);
        { PoolManager pm(state); ASSERT_EQ(pm.loadPools(), 0); } // Both recorded.
        // The header and identity tags of each device.
        const auto area0 = head(0, H), area1 = head(1, H);
        auto put = [&](uint32_t id, const std::string &area) {
            std::fstream f(fakeRoot + dev(id), std::ios::in | std::ios::out | std::ios::binary);
            f.write(area.data(), static_cast<std::streamsize>(area.size()));
        };
        auto expectBothRefused = [&](const char *what) {
            SCOPED_TRACE(what);
            const auto other = head(1, H);
            EXPECT_FALSE(expectDeviceRefused(0).empty());
            EXPECT_EQ(head(1, H), other);
            PoolManager pm(state);
            ASSERT_EQ(pm.loadPools(), 0);
            EXPECT_FALSE(pm.hasPools());
        };
        put(0, area1); // The dax numbers swapped.
        put(1, area0);
        expectBothRefused("swapped");
        put(0, area0); // Two devices with one UUID.
        put(1, area0);
        expectBothRefused("duplicate UUID");
        put(0, area1); // dax1.0's identity is on dax0.0 and dax1.0 has none.
        put(1, std::string(H, '\0'));
        expectBothRefused("header lost, its UUID elsewhere");
        put(0, area0); // A Maru header of a version this RM does not know.
        put(1, area1);
        auto newer = area0;
        newer[8] = 2;
        put(0, newer);
        EXPECT_NE(expectDeviceRefused(0).find("version"), std::string::npos);
    }
}

TEST_F(HeaderRecoveryTest, BadGlobalMetadataDoesNotInitializeFreshHeader) {
    addDevice(0, false);
    // Shorter than its header, or a header of another format.
    std::ofstream(state + "/global.meta") << "broken";
    {
        SCOPED_TRACE("truncated");
        expectRecoveryRefused(-EPIPE);
    }
    std::ofstream(state + "/global.meta") << std::string(32, 'x');
    {
        SCOPED_TRACE("wrong magic");
        expectRecoveryRefused(-EPROTO);
    }
}

TEST_F(HeaderRecoveryTest, MissingGlobalCheckpointIsNotAnEmptyLedger) {
    addDevice(0);
    ASSERT_EQ(MetadataStore(state).save(pool()), 0);
    expectRecoveryRefused(-EUCLEAN);
    // Unless the WAL reaches back to region 1: the first checkpoint was cut
    // short, and the WAL holds every allocation there ever was.
    auto first = allocation();
    first.handle.regionId = 1;
    ASSERT_EQ(WalStore(state).appendAlloc(first), 0);
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_EQ(pm.allocationCount(), 1u);
        EXPECT_TRUE(fs::exists(state + "/global.meta"));
    }

    // global.meta is created empty and the WAL kept, so a restart before the
    // first checkpoint replays the same history: region 42, freed and then
    // used by another pool (as RMs before #91 could), stays pool 0's.
    resetState();
    auto reused = allocation(1);
    auto reuse = allocation(0);
    reuse.nonce = 99;
    {
        WalStore wal(state);
        ASSERT_EQ(wal.appendAlloc(reused), 0);
        ASSERT_EQ(wal.appendFree(42), 0);
        ASSERT_EQ(wal.appendAlloc(reuse), 0);
    }
    for (int start = 0; start < 2; ++start) {
        SCOPED_TRACE(start);
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_TRUE(pm.hasPools());
        EXPECT_EQ(pm.allocationCount(), 1u);
    }
    // Two pools claiming one ID at once cannot be resolved.
    auto rival = allocation(2);
    rival.nonce = 7;
    ASSERT_EQ(WalStore(state).appendAlloc(rival), 0);
    expectRecoveryRefused(-EEXIST);
}

TEST_F(HeaderRecoveryTest, MissingPoolRecordKeepsAllocationsThatFit) {
    addDevice(0);
    const auto a = allocation();
    ASSERT_EQ(MetadataStore(state).saveGlobal({{42, a}}, 43), 0);
    {
        PoolManager pm(state); // The header is adopted, the allocation fits.
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_EQ(pm.allocationCount(), 1u);
        EXPECT_EQ(pm.verifyAndFree(a.handle, a.clientId), 0);
    }
    resetState();
    resetDevice(0, false); // Without the header the allocation's data is gone.
    ASSERT_EQ(MetadataStore(state).saveGlobal({{42, a}}, 43), 0);
    EXPECT_NE(expectDeviceRefused(0).find("header lost"), std::string::npos);
}

TEST_F(HeaderRecoveryTest, MissingHeaderWithoutLiveAllocationGetsFreshHeader) {
    addDevice(0, false);
    // A power cycle took the header; the memory came back with any content.
    std::fstream(fakeRoot + dev(0), std::ios::in | std::ios::out | std::ios::binary) << "garbage";
    // The last run freed every region: a stale checkpoint total, a checkpoint
    // entry freed in the WAL, WAL pairs.
    auto p = pool();
    p.totalSize = 4 * (B - H);
    p.freeSize = B - 2 * H;
    p.freeList = {{2 * H, B - 2 * H}};
    save(p, {{42, allocation()}});
    auto later = allocation();
    later.handle.regionId = 43;
    {
        WalStore wal(state);
        ASSERT_EQ(wal.appendFree(42), 0);
        ASSERT_EQ(wal.appendAlloc(later), 0);
        ASSERT_EQ(wal.appendFree(43), 0);
    }
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    DeviceHeader header{};
    EXPECT_EQ(readDeviceHeader(dev(0), header, H), 0);
    EXPECT_EQ(pm.allocationCount(), 0u);
    Handle h{};
    ASSERT_EQ(allocate(pm, h), 0);
    EXPECT_EQ(h.offset, H);
    EXPECT_EQ(h.regionId, 44u); // IDs from the history are not reused.
    EXPECT_EQ(pm.verifyAndFree(h, "offline-test-owner"), 0);
    EXPECT_EQ(pm.checkpoint(), 0);
}

TEST_F(HeaderRecoveryTest, MissingHeaderWithLiveAllocationIsRefused) {
    addDevice(0, false);
    // Live in the checkpoint, or only in the WAL.
    for (bool walOnly : {false, true}) {
        resetState();
        if (walOnly) {
            save(pool());
            ASSERT_EQ(WalStore(state).appendAlloc(allocation()), 0);
        } else {
            auto p = pool();
            p.freeSize = B - 2 * H;
            p.freeList = {{2 * H, B - 2 * H}};
            save(p, {{42, allocation()}});
        }
        SCOPED_TRACE(walOnly ? "live in the WAL" : "live in the checkpoint");
        EXPECT_NE(expectDeviceRefused(0).find("header lost"), std::string::npos);
    }
    // The owner is a local process that exited: its allocation is released
    // and the pool starts over.
    resetState();
    auto exited = allocation();
    std::snprintf(exited.clientId, sizeof(exited.clientId), "%s", exitedLocalClient().c_str());
    exited.handle.authToken = computeAuthToken(exited.handle, exited.nonce, exited.clientId);
    auto p = pool();
    p.freeSize = B - 2 * H;
    p.freeList = {{2 * H, B - 2 * H}};
    save(p, {{42, exited}});
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_EQ(pm.allocationCount(), 0u);
        EXPECT_TRUE(pm.hasPools());
    }
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.allocationCount(), 0u);
    // Only in the WAL, for a device that appears at a rescan: it is refused
    // and the served pool and checkpoints carry on.
    resetState();
    resetDevice(0, true);
    ASSERT_EQ(WalStore(state).appendAlloc(allocation(1)), 0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    addDevice(1, false);
    EXPECT_EQ(pm.rescanDevices(), 0);
    EXPECT_EQ(head(1, B), std::string(B, '\0'));
    EXPECT_EQ(PoolManagerTestPeer::unserved(pm, dev(1)).rfind("refused", 0), 0u);
    EXPECT_EQ(pm.checkpoint(), 0);
    EXPECT_EQ(pm.allocationCount(), 1u);
}

TEST_F(HeaderRecoveryTest, HeaderAllocationInCheckpointIsRefusedWhileItsOwnerMayRun) {
    addDevice(0);
    auto a = allocation(0, 0);
    save(pool(), {{42, a}});
    EXPECT_NE(expectDeviceRefused(0).find("may still be in use"), std::string::npos);
    std::snprintf(a.clientId, sizeof(a.clientId), "%s", exitedLocalClient().c_str());
    save(pool(), {{42, a}});
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    EXPECT_TRUE(pm.hasPools());
    EXPECT_EQ(pm.allocationCount(), 0u);
}

TEST_F(HeaderRecoveryTest, FreedHeaderAllocationInWalStartsThePoolOver) {
    addDevice(0);
    save(pool());
    WalStore wal(state);
    ASSERT_EQ(wal.appendAlloc(allocation(0, 0)), 0);
    ASSERT_EQ(wal.appendFree(42), 0);
    const auto header = head(0, 32);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0); // Nothing is live.
    EXPECT_TRUE(pm.hasPools());
    EXPECT_EQ(head(0, 32), header);
    EXPECT_EQ(record().kind, PoolRecord::Kind::V7);
    EXPECT_EQ(pm.checkpoint(), 0);
}

TEST_F(HeaderRecoveryTest, WalOnlyCrashRecoveryRetainsLiveAllocationAndToken) {
    addDevice(0);
    const auto a = allocation();
    ASSERT_EQ(WalStore(state).appendAlloc(a), 0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    EXPECT_EQ(pm.allocationCount(), 1u);
    EXPECT_TRUE(fs::exists(state + "/global.meta")); // Created before any pool record.
    Handle next{};
    ASSERT_EQ(allocate(pm, next), 0);
    EXPECT_EQ(next.regionId, 43u);
    EXPECT_EQ(next.offset, 2 * H);
    EXPECT_EQ(pm.verifyAndFree(a.handle, a.clientId), 0);
    ASSERT_EQ(pm.checkpoint(), 0);
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.allocationCount(), 1u);
    EXPECT_EQ(restarted.verifyAndFree(next, "offline-test-owner"), 0);

    // A restart keeps a local owner's allocation within one boot; one
    // recorded in an earlier boot (boot_id= line of rm.conf) is released.
    {
        resetState();
        const std::string bootFile = fakeRoot + "/boot_id";
        auto boot = [&](const std::string &id) { std::ofstream(bootFile) << id << "\n"; };
        const std::string b1 = "11111111-1111-4111-8111-111111111111";
        const std::string b2 = "22222222-2222-4222-8222-222222222222";
        // A local owner whose PID runs: this process.
        const std::string me = localClient(::getpid());
        auto run = [&](const std::function<void(PoolManager &)> &body) {
            PoolManager rm(state);
            PoolManagerTestPeer::setBootIdPath(rm, bootFile);
            ASSERT_EQ(rm.loadPools(), 0);
            body(rm);
        };
        boot(b1);
        run([&](PoolManager &rm) {
            Handle h{};
            ASSERT_EQ(allocate(rm, h, 0, me), 0);
        });
        EXPECT_EQ(readBootMarker(state), b1);
        run([&](PoolManager &rm) { EXPECT_EQ(rm.allocationCount(), 1u); }); // Same boot.
        boot(b2); // After a reboot the PID belongs to some other process.
        run([&](PoolManager &rm) { EXPECT_EQ(rm.allocationCount(), 0u); });
        EXPECT_EQ(readBootMarker(state), b2);
        run([&](PoolManager &rm) {
            Handle h{};
            ASSERT_EQ(allocate(rm, h, 0, me), 0);
        });
        // An earlier RM started since: it rewrote rm.conf without the line.
        std::ofstream(state + "/rm.conf") << "host=0.0.0.0\nport=9850\nlog_level=info\n";
        boot(b1);
        run([&](PoolManager &rm) { EXPECT_EQ(rm.allocationCount(), 1u); });
        EXPECT_EQ(readBootMarker(state), b1);
        std::ofstream(state + "/rm.conf") << "boot_id=not-a-boot-id\n"; // Ignored.
        boot(b2);
        run([&](PoolManager &rm) { EXPECT_EQ(rm.allocationCount(), 1u); });
        // An unreadable boot ID removes the line, so no later start guesses.
        fs::remove(bootFile);
        run([&](PoolManager &rm) { EXPECT_EQ(rm.allocationCount(), 1u); });
        EXPECT_EQ(readBootMarker(state), "");
    }
}

TEST_F(HeaderRecoveryTest, CheckpointPlusWalFreeDoesNotResurrectAllocation) {
    addDevice(0);
    const auto a = allocation();
    auto p = pool();
    p.freeList = {{2 * H, B - 2 * H}};
    p.freeSize = B - 2 * H;
    save(p, {{42, a}});
    ASSERT_EQ(WalStore(state).appendFree(42), 0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    EXPECT_EQ(pm.allocationCount(), 0u);
    Handle h{};
    ASSERT_EQ(allocate(pm, h), 0);
    EXPECT_EQ(h.offset, H);
    EXPECT_GE(h.regionId, 43u);
}

TEST_F(HeaderRecoveryTest, MissingDeviceKeepsItsAllocationsUntilLateDiscovery) {
    addDevice(0);
    const auto a = allocation(1);
    // With or without a version 6 record of its pool: the ledger keeps the
    // allocation, and the checkpoint writes it into global.meta.
    for (bool legacy : {false, true}) {
        SCOPED_TRACE(legacy ? "version 6 record" : "no record");
        resetState();
        fs::remove_all(fakeRoot + "/sys/bus/dax/devices/dax1.0");
        if (legacy) {
            auto p = pool(1);
            p.freeSize = B - 2 * H;
            p.freeList = {{2 * H, B - 2 * H}};
            save(p);
        }
        ASSERT_EQ(WalStore(state).appendAlloc(a), 0);
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_EQ(pm.checkpoint(), 0);
        EXPECT_FALSE(fs::exists(state + "/maru-resource-manager.wal"));
        addDevice(1);
        ASSERT_EQ(pm.rescanDevices(), 0);
        EXPECT_EQ(PoolManagerTestPeer::unserved(pm, dev(1)), "");
        EXPECT_EQ(pm.allocationCount(), 1u);
        EXPECT_EQ(pm.verifyAndFree(a.handle, a.clientId), 0);
        EXPECT_EQ(pm.checkpoint(), 0);
    }
}

TEST_F(HeaderRecoveryTest, RescanWithoutNewDevicesDoesNotReadWal) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle h{};
    ASSERT_EQ(allocate(pm, h), 0);
    // Any replay would reject these bytes; a rescan with no new pool must not
    // re-read the log under the manager lock.
    std::ofstream(state + "/maru-resource-manager.wal", std::ios::app) << "junk";
    EXPECT_EQ(pm.rescanDevices(), 0);
    EXPECT_EQ(pm.allocationCount(), 1u);
    EXPECT_EQ(PoolManagerTestPeer::checkpointBlock(pm), "");
}

TEST_F(HeaderRecoveryTest, TornFinalWalRecordIsDiscardedBeforeAppend) {
    addDevice(0);
    const std::string walPath = state + "/maru-resource-manager.wal";
    auto second = allocation(0, 2 * H);
    second.handle.regionId = 43;
    // A crash during the second append can leave part of its header (even
    // just magic bytes), or the whole header and part of its payload.
    for (size_t kept : {size_t{3}, size_t{10}, size_t{40}}) {
        fs::remove(walPath);
        ASSERT_EQ(WalStore(state).appendAlloc(allocation()), 0);
        const auto complete = bytes(walPath);
        ASSERT_EQ(WalStore(state).appendAlloc(second), 0);
        fs::resize_file(walPath, complete.size() + kept);
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_EQ(pm.allocationCount(), 1u);
        EXPECT_EQ(bytes(walPath), complete);
        Handle h{};
        ASSERT_EQ(allocate(pm, h), 0);
        EXPECT_EQ(h.regionId, 43u);
        PoolManager restarted(state);
        ASSERT_EQ(restarted.loadPools(), 0);
        EXPECT_EQ(restarted.allocationCount(), 2u);
    }
    // Not a prefix of the final record: zero fill, a header prefix with an
    // unknown type, or a torn record followed by another. WAL left untouched,
    // and no fresh device gets a header.
    addDevice(1, false);
    fs::remove(walPath);
    ASSERT_EQ(WalStore(state).appendAlloc(allocation()), 0);
    const auto one = bytes(walPath);
    for (const auto &tail : {std::string(16, '\0'), one.substr(0, 8) + std::string("\7\0\0\0", 4),
                             one.substr(0, 10) + one}) {
        std::ofstream(walPath, std::ios::binary | std::ios::trunc) << one + tail;
        const auto corrupt = bytes(walPath);
        PoolManager pm(state);
        EXPECT_EQ(pm.loadPools(), -EPROTO);
        EXPECT_EQ(bytes(walPath), corrupt);
        DeviceHeader header{};
        EXPECT_EQ(readDeviceHeader(dev(1), header, H), -ENODATA);
    }
    // dax1.0 starts over and logs the release of an exited owner's
    // allocation, after the torn tail is cut, so a restart reads it.
    fs::remove(walPath);
    auto exited = allocation(1);
    exited.handle.regionId = 50;
    std::snprintf(exited.clientId, sizeof(exited.clientId), "%s", exitedLocalClient().c_str());
    ASSERT_EQ(WalStore(state).appendAlloc(exited), 0);
    const auto complete = bytes(walPath);
    ASSERT_EQ(WalStore(state).appendAlloc(second), 0);
    fs::resize_file(walPath, complete.size() + 10);
    std::string out;
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        ASSERT_EQ(pm.adminAllocations("dax1.0", out), 0);
        EXPECT_EQ(out, "");
        EXPECT_EQ(PoolManagerTestPeer::unserved(pm, dev(1)), "");
    }
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    ASSERT_EQ(restarted.adminAllocations("dax1.0", out), 0);
    EXPECT_EQ(out, "");
}

TEST_F(HeaderRecoveryTest, LegacyWalReplaysButItsTornTailIsRejected) {
    addDevice(0);
    const std::string walPath = state + "/maru-resource-manager.wal";
    const auto record = [&](const Allocation &a) {
        fs::remove(walPath);
        EXPECT_EQ(WalStore(state).appendAlloc(a), 0);
        auto r = bytes(walPath);
        r[4] = 6; // WalHeader::version, as written before v7.
        return r;
    };
    auto second = allocation(0, 2 * H);
    second.handle.regionId = 43;
    const auto v6 = record(allocation());
    const auto v6Second = record(second);
    // A v6 writer acknowledged even failed appends, so its torn tail may be
    // an allocation a client still holds.
    for (const auto &tail : {v6Second.substr(0, 40), v6Second.substr(0, 10),
                             v6Second.substr(0, 3)}) {
        std::ofstream(walPath, std::ios::binary | std::ios::trunc) << v6 + tail;
        PoolManager pm(state);
        EXPECT_EQ(pm.loadPools(), -EPROTO);
        EXPECT_EQ(bytes(walPath), v6 + tail);
    }
    // Complete v6 records still replay, and v7 records are appended after them.
    std::ofstream(walPath, std::ios::binary | std::ios::trunc) << v6;
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        EXPECT_EQ(pm.allocationCount(), 1u);
        Handle h{};
        ASSERT_EQ(allocate(pm, h), 0);
    }
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.allocationCount(), 2u);
}

TEST_F(HeaderRecoveryTest, PartialWalAppendIsNotAcknowledged) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle kept{};
    ASSERT_EQ(allocate(pm, kept), 0);
    const std::string walPath = state + "/maru-resource-manager.wal";
    const auto walBefore = bytes(walPath);
    std::vector<PoolState> before;
    pm.getStats(before);
    // A short write followed by ENOSPC: neither operation takes effect.
    checkpoint_fault::arm(walPath, checkpoint_fault::Operation::Write, ENOSPC, 10);
    Handle h{};
    EXPECT_EQ(allocate(pm, h), -ENOSPC);
    EXPECT_EQ(checkpoint_fault::failures(), 1u);
    checkpoint_fault::arm(walPath, checkpoint_fault::Operation::Write, ENOSPC, 10);
    EXPECT_EQ(pm.verifyAndFree(kept, "offline-test-owner"), -ENOSPC);
    EXPECT_EQ(checkpoint_fault::failures(), 1u);
    checkpoint_fault::clear();
    EXPECT_EQ(bytes(walPath), walBefore);
    EXPECT_EQ(pm.allocationCount(), 1u);
    std::vector<PoolState> after;
    pm.getStats(after);
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(after[0].freeSize, before[0].freeSize);
    ASSERT_EQ(after[0].freeList.size(), before[0].freeList.size());
    EXPECT_EQ(after[0].freeList[0].offset, before[0].freeList[0].offset);
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.allocationCount(), 1u);
    EXPECT_EQ(restarted.verifyAndFree(kept, "offline-test-owner"), 0);
}

// A whole record is in the WAL but fdatasync fails (and so does the rollback
// truncate), then the RM restarts before any checkpoint: recovery may apply
// the record, so the reply must match what recovery would do or be harmless.
TEST_F(HeaderRecoveryTest, UnsyncedCompleteRecordsStaySafeAcrossRestart) {
    using checkpoint_fault::Operation;
    addDevice(0);
    const std::string walPath = state + "/maru-resource-manager.wal";
    const std::string exitedOwner = localClient(2147483647);
    auto failSync = [&] {
        checkpoint_fault::arm(walPath, Operation::Fdatasync, EIO);
        checkpoint_fault::armTruncate(walPath, EIO);
    };

    // FREE: completed despite the failed sync, since recovery may apply it.
    // The client is told it succeeded and drops its mapping.
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        Handle h{};
        ASSERT_EQ(allocate(pm, h), 0);
        failSync();
        EXPECT_EQ(pm.verifyAndFree(h, "offline-test-owner"), 0);
        checkpoint_fault::clear();
        EXPECT_EQ(pm.allocationCount(), 0u);
        PoolManager restarted(state);
        ASSERT_EQ(restarted.loadPools(), 0);
        EXPECT_EQ(restarted.allocationCount(), 0u);
        EXPECT_EQ(restarted.verifyAndFree(h, "offline-test-owner"), -ENOENT);
    }

    // ALLOC: rejected. If the record still survives, recovery restores an
    // allocation its client never got: a leak the reaper reclaims with that
    // client, never a range handed to two clients.
    fs::remove(walPath);
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        failSync();
        Handle h{};
        EXPECT_EQ(allocate(pm, h, 0, exitedOwner), -EIO);
        checkpoint_fault::clear();
        EXPECT_EQ(pm.allocationCount(), 0u);
        PoolManager restarted(state);
        ASSERT_EQ(restarted.loadPools(), 0);
        EXPECT_EQ(restarted.allocationCount(), 1u);
        uint64_t reaped = 0;
        restarted.reapExpired(reaped);
        EXPECT_EQ(reaped, 1u);
        EXPECT_EQ(restarted.allocationCount(), 0u);
    }

    // After any failed sync the next operation first replaces the log, so a
    // later restart cannot replay the uncertain record.
    fs::remove(walPath);
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        checkpoint_fault::arm(walPath, Operation::Fdatasync, EIO);
        Handle rejected{};
        EXPECT_EQ(allocate(pm, rejected), -EIO);
        checkpoint_fault::clear();
        Handle h{};
        ASSERT_EQ(allocate(pm, h), 0); // Checkpoints first, then logs this ALLOC.
        PoolManager restarted(state);
        ASSERT_EQ(restarted.loadPools(), 0);
        EXPECT_EQ(restarted.allocationCount(), 1u);
        EXPECT_EQ(restarted.verifyAndFree(h, "offline-test-owner"), 0);
    }
}

TEST_F(HeaderRecoveryTest, CheckpointKeepsTheWalUntilItsRenamesAreDurable) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle h{};
    ASSERT_EQ(allocate(pm, h), 0);
    const std::string wal = state + "/maru-resource-manager.wal";
    ASSERT_TRUE(fs::exists(wal));
    checkpoint_fault::arm(state, checkpoint_fault::Operation::Fsync, EIO);
    EXPECT_EQ(pm.checkpoint(), -EIO);
    checkpoint_fault::clear();
    EXPECT_EQ(checkpoint_fault::failures(), 1u);
    EXPECT_TRUE(fs::exists(wal)); // It still covers the allocation.
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.verifyAndFree(h, "offline-test-owner"), 0);
}

TEST_F(HeaderRecoveryTest, NewWalEntryIsSyncedBeforeItsFirstRecordIsAcknowledged) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    ASSERT_EQ(pm.checkpoint(), 0); // The next append creates the WAL.
    checkpoint_fault::arm(state, checkpoint_fault::Operation::Fsync, EIO);
    Handle h{};
    EXPECT_EQ(allocate(pm, h), -EIO);
    EXPECT_EQ(pm.allocationCount(), 0u);
    checkpoint_fault::clear();
    EXPECT_EQ(checkpoint_fault::failures(), 1u);
    ASSERT_EQ(allocate(pm, h), 0); // The retry syncs the entry first.
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.verifyAndFree(h, "offline-test-owner"), 0);
}

TEST_F(HeaderRecoveryTest, UnremovableFailedAppendBlocksAppendsUntilCheckpoint) {
    addDevice(0);
    // A refused pool with a live allocation does not stop the checkpoint
    // that replaces the log.
    addDevice(1, false);
    ASSERT_EQ(WalStore(state).appendAlloc(allocation(1)), 0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle kept{};
    ASSERT_EQ(allocate(pm, kept), 0);
    const std::string walPath = state + "/maru-resource-manager.wal";
    const auto walBefore = bytes(walPath);
    checkpoint_fault::arm(walPath, checkpoint_fault::Operation::Write, ENOSPC, 10);
    checkpoint_fault::armTruncate(walPath, EIO);
    Handle h{};
    EXPECT_EQ(allocate(pm, h), -ENOSPC);
    EXPECT_EQ(checkpoint_fault::failures(), 2u); // The write, then its rollback.
    checkpoint_fault::clear();
    ASSERT_EQ(bytes(walPath).size(), walBefore.size() + 10);

    // A crash now leaves a torn record that nobody was told about.
    const std::string crashed = fakeRoot + "/crashed";
    fs::copy(state, crashed, fs::copy_options::recursive);
    {
        PoolManager restarted(crashed);
        ASSERT_EQ(restarted.loadPools(), 0);
        EXPECT_EQ(restarted.allocationCount(), 2u);
        EXPECT_EQ(bytes(crashed + "/maru-resource-manager.wal"), walBefore);
    }

    // Otherwise the next request checkpoints first, replacing the log.
    ASSERT_EQ(allocate(pm, h), 0);
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.allocationCount(), 3u);
}

TEST_F(HeaderRecoveryTest, ReaperRetriesFreeAfterWalFailure) {
    addDevice(0);
    PoolManager pm(state, 0);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle h{};
    ASSERT_EQ(allocate(pm, h), 0);
    pm.clientDisconnected("offline-test-owner");
    checkpoint_fault::arm(state + "/maru-resource-manager.wal",
                          checkpoint_fault::Operation::Write, ENOSPC);
    uint64_t reaped = 0;
    pm.reapExpired(reaped);
    checkpoint_fault::clear();
    EXPECT_EQ(reaped, 0u);
    EXPECT_EQ(pm.allocationCount(), 1u);
    pm.reapExpired(reaped);
    EXPECT_EQ(reaped, 1u);
    EXPECT_EQ(pm.allocationCount(), 0u);
    // A pool that is not served: an exited local owner's allocation leaves
    // the ledger, a remote owner past its grace period does not (it may
    // still run behind a network break).
    auto exited = allocation(9);
    exited.handle.regionId = 90;
    std::snprintf(exited.clientId, sizeof(exited.clientId), "%s", exitedLocalClient().c_str());
    auto remote = allocation(9);
    remote.handle.regionId = 91;
    PoolManagerTestPeer::addAllocation(pm, exited);
    PoolManagerTestPeer::addAllocation(pm, remote);
    pm.clientDisconnected("offline-test-owner");
    pm.reapExpired(reaped);
    EXPECT_EQ(reaped, 1u);
    EXPECT_EQ(pm.allocationCount(), 1u);
}

TEST_F(HeaderRecoveryTest, RefusedRescanPreservesLivePool) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle h{};
    ASSERT_EQ(allocate(pm, h), 0);
    addDevice(1); // Holds the UUID dax0.0 has: refused, never written.
    const auto header0 = head(0, 32);
    std::ofstream(fakeRoot + dev(1), std::ios::in | std::ios::out | std::ios::binary)
        << header0;
    EXPECT_EQ(pm.rescanDevices(), 0);
    EXPECT_EQ(PoolManagerTestPeer::unserved(pm, dev(1)).rfind("refused", 0), 0u);
    EXPECT_EQ(head(1, 32), header0);
    EXPECT_EQ(pm.allocationCount(), 1u);
    EXPECT_EQ(pm.checkpoint(), 0);
    EXPECT_EQ(pm.verifyAndFree(h, "offline-test-owner"), 0);
}

TEST_F(HeaderRecoveryTest, RuntimeAllocationAndReaperCannotExposeHeader) {
    PoolManager pm(state);
    auto p = pool();
    p.freeList = {{0, B}};
    p.freeSize = B;
    PoolManagerTestPeer::inject(pm, p, {});
    Handle h{};
    EXPECT_EQ(allocate(pm, h), -EUCLEAN);

    PoolManager other(state);
    const auto invalid = allocation(0, 0);
    PoolManagerTestPeer::inject(other, pool(), invalid);
    EXPECT_EQ(other.verifyAndFree(invalid.handle, invalid.clientId), -EUCLEAN);
    // A local dead PID makes the reaper attempt the same deallocation path.
    PoolManager reaper(state);
    auto dead = invalid;
    std::snprintf(dead.clientId, sizeof(dead.clientId), "%s", localClient(2147483647).c_str());
    PoolManagerTestPeer::inject(reaper, pool(), dead);
    uint64_t reaped = 99;
    reaper.reapExpired(reaped);
    EXPECT_EQ(reaped, 0u);
    EXPECT_EQ(reaper.allocationCount(), 1u);
}

TEST_F(HeaderRecoveryTest, FsDaxFileCreationFailureKeepsFreeList) {
    const auto p = fsDaxPool();
    PoolManager pm(state);
    PoolManagerTestPeer::inject(pm, p, {});
    // Left by a crash between file creation and the WAL append.
    std::ofstream(p.devPath + "/maru_1.dat") << "stale";
    Handle h{};
    std::string path, uuid;
    uint64_t size;
    EXPECT_EQ(pm.alloc(H, "offline-test-owner", h, path, uuid, p.devPath, size), -ENOMEM);
    std::vector<PoolState> stats;
    pm.getStats(stats);
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_EQ(validateFreeList(stats[0]), 0);
    EXPECT_EQ(stats[0].freeSize, B);
    ASSERT_EQ(pm.alloc(H, "offline-test-owner", h, path, uuid, p.devPath, size), 0);
    EXPECT_EQ(h.regionId, 2u);
    EXPECT_EQ(h.offset, 0u); // FS_DAX keeps file offset zero; it has no header.
    EXPECT_TRUE(fs::exists(path));
    EXPECT_EQ(pm.verifyAndFree(h, "offline-test-owner"), 0);
    EXPECT_FALSE(fs::exists(path));
}

TEST_F(HeaderRecoveryTest, OrphanFsDaxFilesAreRemoved) {
    const auto p = fsDaxPool(5);
    PoolManager pm(state);
    PoolManagerTestPeer::inject(pm, p, allocation(5, 0)); // Owns maru_42.dat.
    // FS_DAX pool IDs follow sysfs numbering and can change across boots, so
    // a file whose region another pool owns is kept as well.
    auto elsewhere = allocation();
    elsewhere.handle.regionId = 43;
    PoolManagerTestPeer::inject(pm, pool(), elsewhere);
    // maru_50.dat: an ID this state has not issued yet.
    const std::vector<std::string> kept{"maru_42.dat", "maru_43.dat", "maru_50.dat",
                                        "maru_007.dat", "maru_7.dat.bak", "maru_x.dat",
                                        "notes.txt"};
    for (const auto &name : kept) std::ofstream(p.devPath + "/" + name) << "x";
    std::ofstream(p.devPath + "/maru_7.dat") << "x";
    // Fresh (or wrong) state issued no IDs, so it owns nothing to remove.
    PoolManagerTestPeer::removeOrphanFsDaxFiles(pm, 1);
    EXPECT_TRUE(fs::exists(p.devPath + "/maru_7.dat"));
    // A pool that is not served still owns its files through the ledger.
    auto absent = allocation(9);
    absent.handle.regionId = 7;
    PoolManagerTestPeer::addAllocation(pm, absent);
    PoolManagerTestPeer::removeOrphanFsDaxFiles(pm, 50);
    EXPECT_TRUE(fs::exists(p.devPath + "/maru_7.dat"));
    PoolManagerTestPeer::removeAllocation(pm, 7);
    PoolManagerTestPeer::removeOrphanFsDaxFiles(pm, 50);
    EXPECT_FALSE(fs::exists(p.devPath + "/maru_7.dat"));
    for (const auto &name : kept) EXPECT_TRUE(fs::exists(p.devPath + "/" + name)) << name;
}

TEST_F(HeaderRecoveryTest, EachDeviceIsJudgedOnItsOwn) {
    addDevice(0, false);
    addDevice(1, false);
    // dax1.0 lost its header while an allocation on it may be in use: it is
    // refused untouched, and dax0.0, a new device, is served.
    save(pool(1), {{42, allocation(1)}});
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    std::vector<PoolState> stats;
    pm.getStats(stats);
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_EQ(stats[0].devPath, dev(0));
    EXPECT_EQ(PoolManagerTestPeer::unserved(pm, dev(1)).rfind("refused", 0), 0u);
    EXPECT_EQ(head(1, B), std::string(B, '\0'));
}

TEST_F(HeaderRecoveryTest, LateHeaderAllocationFailsWithoutLosingExistingAllocations) {
    addDevice(0);
    const auto bad = allocation(1, 0);
    ASSERT_EQ(WalStore(state).appendAlloc(bad), 0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle h{};
    ASSERT_EQ(allocate(pm, h), 0);
    EXPECT_EQ(h.regionId, 43u);
    addDevice(1); // Its recorded allocation covers the header: refused.
    EXPECT_EQ(pm.rescanDevices(), 0);
    EXPECT_EQ(PoolManagerTestPeer::unserved(pm, dev(1)).rfind("refused", 0), 0u);
    EXPECT_EQ(pm.allocationCount(), 2u);
    EXPECT_EQ(pm.checkpoint(), 0);
    EXPECT_EQ(pm.verifyAndFree(h, "offline-test-owner"), 0);
}

TEST_F(HeaderRecoveryTest, NormalReaperReturnsOnlyDataRange) {
    auto a = allocation();
    std::snprintf(a.clientId, sizeof(a.clientId), "%s", localClient(2147483647).c_str());
    auto p = pool();
    p.freeList = {{2 * H, B - 2 * H}};
    p.freeSize = B - 2 * H;
    PoolManager pm(state);
    PoolManagerTestPeer::inject(pm, p, a);
    uint64_t reaped = 0;
    pm.reapExpired(reaped);
    EXPECT_EQ(reaped, 1u);
    EXPECT_EQ(pm.allocationCount(), 0u);
    std::vector<PoolState> stats;
    pm.getStats(stats);
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_EQ(stats[0].freeSize, B - H);
    ASSERT_EQ(stats[0].freeList.size(), 1u);
    EXPECT_EQ(stats[0].freeList[0].offset, H);
    EXPECT_EQ(stats[0].freeList[0].length, B - H);
}

TEST_F(HeaderRecoveryTest, UnresponsiveDeviceDoesNotBlockOtherPools) {
    addDevice(0);
    PoolManager pm(state);
    PoolManagerTestPeer::setDeviceIoTimeout(pm, 200);
    ASSERT_EQ(pm.loadPools(), 0);
    auto waitIdle = [&] {
        for (int i = 0; i < 500 && PoolManagerTestPeer::slotHeld(pm, dev(1)); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        ASSERT_FALSE(PoolManagerTestPeer::slotHeld(pm, dev(1)));
    };
    // The device stops answering at its first read, at the read just before
    // the write, or at the write itself.
    for (int nth : {0, 1, 2}) {
        SCOPED_TRACE(nth);
        fs::remove_all(fakeRoot + "/sys/bus/dax/devices/dax1.0");
        fs::remove_all(fakeRoot + "/sys/class/dax/dax1.0");
        addDevice(1, false);
        resetDevice(1, false); // A new device each round.
        armHang(dev(1), nth);
        const auto started = std::chrono::steady_clock::now();
        EXPECT_EQ(pm.rescanDevices(), 0);
        EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(5));
        EXPECT_EQ(PoolManagerTestPeer::unserved(pm, dev(1)).rfind("busy", 0), 0u);
        Handle h{};
        ASSERT_EQ(allocate(pm, h), 0); // Other pools carry on meanwhile.
        ASSERT_EQ(pm.verifyAndFree(h, "offline-test-owner"), 0);
        std::string out;
        EXPECT_EQ(pm.adminInclude("dax1.0", PoolManager::IncludeMode::Judge, out), -EBUSY);
        if (nth == 2) { // The write can still land: not handed over meanwhile.
            EXPECT_EQ(pm.adminExclude("dax1.0", out), -EBUSY);
        }
        releaseHang();
        waitIdle();
        ASSERT_EQ(pm.rescanDevices(), 0); // Judged again once it answers.
        EXPECT_EQ(PoolManagerTestPeer::unserved(pm, dev(1)), "");
        out.clear();
        ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
        out.clear();
        fs::remove(state + "/excluded_devices");
        PoolManagerTestPeer::reloadExcluded(pm);
    }
    // Changed by someone else between the read and the write: not written.
    fs::remove_all(fakeRoot + "/sys/bus/dax/devices/dax1.0");
    fs::remove_all(fakeRoot + "/sys/class/dax/dax1.0");
    addDevice(1, false);
    resetDevice(1, false);
    DeviceHeader theirs{};
    initDeviceHeader(theirs);
    armHang(dev(1), 1, [&] { writeDeviceHeader(dev(1), theirs, H); });
    EXPECT_EQ(pm.rescanDevices(), 0);
    releaseHang();
    EXPECT_EQ(head(1, 32), std::string(reinterpret_cast<const char *>(&theirs), 32));
    EXPECT_NE(PoolManagerTestPeer::unserved(pm, dev(1)).find("changed"), std::string::npos);
    // A device that hangs at startup is skipped; the others are served, and
    // the device can be excluded while only its timed-out read holds it.
    armHang(dev(1), 0);
    PoolManager restarted(state);
    PoolManagerTestPeer::setDeviceIoTimeout(restarted, 200);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_TRUE(restarted.hasPools());
    EXPECT_EQ(PoolManagerTestPeer::unserved(restarted, dev(1)).rfind("busy", 0), 0u);
    std::string out;
    EXPECT_EQ(restarted.adminExclude("dax1.0", out), 0) << out;
    releaseHang();
    auto waitRestartedIdle = [&] {
        for (int i = 0; i < 500 && PoolManagerTestPeer::slotHeld(restarted, dev(1)); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        ASSERT_FALSE(PoolManagerTestPeer::slotHeld(restarted, dev(1)));
    };
    waitRestartedIdle();
    // An include whose device write times out leaves the device excluded,
    // but not ready to hand over until the write returns.
    armHang(dev(1), 2);
    out.clear();
    EXPECT_NE(restarted.adminInclude("dax1.0", PoolManager::IncludeMode::Reinit, out), 0);
    out.clear();
    EXPECT_EQ(restarted.adminExclude("dax1.0", out), -EBUSY) << out;
    out.clear();
    ASSERT_EQ(restarted.adminStatus(out), 0);
    EXPECT_NE(out.find("state=excluded write_pending=yes"), std::string::npos) << out;
    releaseHang();
    waitRestartedIdle();
    out.clear();
    EXPECT_EQ(restarted.adminExclude("dax1.0", out), 0) << out;
}

namespace {
struct CheckpointFaultCase {
    std::string file;
    std::string phase;
    int error;
};

void PrintTo(const CheckpointFaultCase &fault, std::ostream *os) {
    *os << fault.file << '/' << fault.phase << '/'
        << (fault.error == ENOSPC ? "ENOSPC" : "EIO");
}

std::vector<CheckpointFaultCase> checkpointFaultCases() {
    // A checkpoint writes only global.meta and returns any errno as is, so
    // one case per phase is enough.
    std::vector<CheckpointFaultCase> cases;
    for (const auto *phase : {"HeaderWrite", "PartialPayloadWrite", "Fsync"})
        cases.push_back({"global.meta", phase, std::string(phase) == "Fsync" ? EIO : ENOSPC});
    return cases;
}
} // namespace

class CheckpointFailureTest : public HeaderRecoveryTest,
                              public ::testing::WithParamInterface<CheckpointFaultCase> {
    void TearDown() override {
        checkpoint_fault::clear();
        HeaderRecoveryTest::TearDown();
    }
};

TEST_P(CheckpointFailureTest, RetainsWalAndAllowsRetry) {
    addDevice(0);
    addDevice(1);
    const std::string walPath = state + "/maru-resource-manager.wal";
    const std::string globalPath = state + "/global.meta";
    Handle old{}, a{}, b{}, c{}, later{};
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        const auto record0 = bytes(state + "/pool_0.meta");
        const auto record1 = bytes(state + "/pool_1.meta");
        ASSERT_EQ(allocate(pm, old), 0);
        ASSERT_EQ(pm.checkpoint(), 0);
        const auto oldGlobal = bytes(globalPath);
        ASSERT_EQ(pm.verifyAndFree(old, "offline-test-owner"), 0);
        ASSERT_EQ(allocate(pm, a), 0);
        ASSERT_EQ(allocate(pm, b), 0);
        ASSERT_EQ(allocate(pm, c, 1), 0);
        const auto walBefore = bytes(walPath);
        ASSERT_FALSE(walBefore.empty());

        const auto &fault = GetParam();
        const bool partial = fault.phase == "PartialPayloadWrite";
        checkpoint_fault::arm(globalPath + ".tmp",
            fault.phase == "Fsync" ? checkpoint_fault::Operation::Fsync
                                   : checkpoint_fault::Operation::Write,
            fault.error, partial ? 40 : 0); // 32-byte header + 8 payload bytes.
        const int rc = pm.checkpoint();
        checkpoint_fault::clear();
        ASSERT_EQ(checkpoint_fault::failures(), 1u); // Prove the failpoint was reached.
        EXPECT_EQ(checkpoint_fault::shortWrites(), partial ? 1u : 0u);
        EXPECT_EQ(rc, -fault.error);
        EXPECT_EQ(bytes(globalPath), oldGlobal);
        EXPECT_EQ(bytes(walPath), walBefore); // Not just present: byte-for-byte intact.
        EXPECT_FALSE(fs::exists(globalPath + ".tmp"));
        // Checkpoints never write pool records.
        EXPECT_EQ(bytes(state + "/pool_0.meta"), record0);
        EXPECT_EQ(bytes(state + "/pool_1.meta"), record1);
        EXPECT_EQ(pm.allocationCount(), 3u);

        // A failed checkpoint must also leave the appendable WAL connected to
        // its pathname. Continue allocating, then retry with the fault removed.
        ASSERT_EQ(allocate(pm, later, 1), 0);
        const auto extendedWal = bytes(walPath);
        EXPECT_GT(extendedWal.size(), walBefore.size());
        EXPECT_EQ(extendedWal.substr(0, walBefore.size()), walBefore);
        ASSERT_EQ(pm.checkpoint(), 0);
        EXPECT_FALSE(fs::exists(walPath));
    }
    {
        PoolManager restarted(state);
        ASSERT_EQ(restarted.loadPools(), 0);
        EXPECT_EQ(restarted.allocationCount(), 4u);
        EXPECT_EQ(restarted.verifyAndFree(old, "offline-test-owner"), -ENOENT);
        ASSERT_EQ(restarted.verifyAndFree(a, "offline-test-owner"), 0);
    }

    // Stopped after global.meta was replaced, before the WAL was removed: the
    // log is replayed onto the checkpoint that already holds it, with the
    // same result.
    Handle d{};
    std::string covered;
    {
        PoolManager restarted(state);
        ASSERT_EQ(restarted.loadPools(), 0);
        ASSERT_EQ(allocate(restarted, d), 0);
        covered = bytes(walPath); // a's FREE and d's ALLOC
        ASSERT_EQ(restarted.checkpoint(), 0);
    }
    std::ofstream(walPath, std::ios::binary) << covered;
    {
        PoolManager restarted(state);
        ASSERT_EQ(restarted.loadPools(), 0);
        EXPECT_EQ(restarted.allocationCount(), 4u);
        EXPECT_EQ(restarted.verifyAndFree(a, "offline-test-owner"), -ENOENT);
        for (const auto &handle : {b, c, later, d})
            EXPECT_EQ(restarted.verifyAndFree(handle, "offline-test-owner"), 0);
        EXPECT_EQ(restarted.checkpoint(), 0);
    }
}

INSTANTIATE_TEST_SUITE_P(CheckpointIo, CheckpointFailureTest,
    ::testing::ValuesIn(checkpointFaultCases()),
    [](const ::testing::TestParamInfo<CheckpointFaultCase> &info) {
        auto file = info.param.file;
        std::replace(file.begin(), file.end(), '.', '_');
        return file + "_" + info.param.phase;
    });
