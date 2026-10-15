// Exercise production discovery/recovery using a private fake /dev and sysfs.
// This target links fake_dax_fs.cpp; it never opens real DAX devices.
#include "recovery_fixture.h"

std::vector<std::string> openedPaths;

int recordOpen(const char *path) {
    openedPaths.emplace_back(path);
    // Holds a caller in open(), e.g. a wipe child (test_device_exclusion.cpp).
    if (std::string(path) == "/dev/dax-hang") {
        ::sleep(30);
        errno = EIO;
        return -1;
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
}

TEST_F(HeaderRecoveryTest, LegacyAndRewrittenTotalAreRejectedWithoutDeviceWrites) {
    addDevice(0, false);
    for (bool rewritten : {false, true}) {
        auto p = pool();
        p.totalSize = rewritten ? B - H : B;
        p.freeSize = B;
        p.freeList = {{0, B}};
        save(p);
        const auto before = bytes(state + "/pool_0.meta");
        PoolManager pm(state);
        EXPECT_EQ(pm.loadPools(), -EUCLEAN);
        EXPECT_FALSE(pm.hasPools());
        EXPECT_EQ(pm.checkpoint(), -EAGAIN);
        EXPECT_EQ(bytes(state + "/pool_0.meta"), before);
        DeviceHeader header{};
        EXPECT_EQ(readDeviceHeader(dev(0), header, H), -ENODATA);
    }
}

TEST_F(HeaderRecoveryTest, StaleCheckpointTotalUsesDeviceRange) {
    addDevice(0);
    // A pre-validation RM saved the device size it saw at load but kept the
    // old free list; the extents and the allocation cover this device exactly.
    for (uint64_t total : {4 * (B - H), (B - H) / 2}) {
        auto p = pool();
        p.totalSize = total;
        p.freeSize = B - 2 * H;
        p.freeList = {{2 * H, B - 2 * H}};
        save(p, {{42, allocation()}});
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        std::vector<PoolState> stats;
        pm.getStats(stats);
        ASSERT_EQ(stats.size(), 1u);
        EXPECT_EQ(stats[0].totalSize, B - H);
        EXPECT_EQ(stats[0].freeSize, B - 2 * H);
        EXPECT_EQ(pm.checkpoint(), 0);
        PoolState saved = pool();
        ASSERT_EQ(MetadataStore(state).load(0, saved), 0);
        EXPECT_EQ(saved.totalSize, B - H);
    }
}

TEST_F(HeaderRecoveryTest, ResizedDeviceCheckpointIsStillRejected) {
    addDevice(0, false);
    // Consistent checkpoints of a device half and twice this size.
    for (uint64_t size : {B / 2, 2 * B}) {
        auto p = pool();
        p.totalSize = p.freeSize = size - H;
        p.freeList = {{H, size - H}};
        save(p);
        SCOPED_TRACE(size);
        expectRecoveryRefused(-EUCLEAN);
    }
}

TEST_F(HeaderRecoveryTest, CrcAndOversizedCountsFailWithoutReplacingMetadata) {
    addDevice(0);
    save(pool());
    auto original = bytes(state + "/pool_0.meta");
    for (size_t field : {size_t(12), size_t(24)}) {
        auto corrupt = original;
        corrupt[field] ^= 0x7f;
        std::ofstream(state + "/pool_0.meta", std::ios::binary) << corrupt;
        PoolManager pm(state);
        EXPECT_EQ(pm.loadPools(), -EUCLEAN);
        EXPECT_EQ(pm.checkpoint(), -EAGAIN);
        EXPECT_EQ(bytes(state + "/pool_0.meta"), corrupt);
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
}

TEST_F(HeaderRecoveryTest, MissingPoolCheckpointWithLiveAllocationIsRejected) {
    addDevice(0);
    const auto a = allocation();
    ASSERT_EQ(MetadataStore(state).saveGlobal({{42, a}}, 43), 0);
    expectRecoveryRefused(-EUCLEAN);
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

TEST_F(HeaderRecoveryTest, MissingHeaderWithLiveAllocationIsRejected) {
    addDevice(0, false);
    auto restart = [&] {
        fs::remove_all(state);
        fs::create_directories(state);
        ASSERT_EQ(initSecret(state, false), 0);
    };
    // Live in the checkpoint, or only in the WAL.
    for (bool walOnly : {false, true}) {
        restart();
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
        expectRecoveryRefused(-EUCLEAN);
    }
    // Only in the WAL, for a device that appears at a rescan: startup could
    // not apply the record.
    restart();
    ASSERT_EQ(WalStore(state).appendAlloc(allocation(1)), 0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0); // dax0.0 has no history now.
    addDevice(1, false);
    EXPECT_EQ(pm.rescanDevices(), -EUCLEAN);
    EXPECT_EQ(head(1, B), std::string(B, '\0'));
    EXPECT_EQ(pm.checkpoint(), -EAGAIN);
}

TEST_F(HeaderRecoveryTest, HeaderAllocationInCheckpointIsRejected) {
    addDevice(0);
    auto a = allocation(0, 0);
    save(pool(), {{42, a}});
    expectRecoveryRefused(-EUCLEAN);
}

TEST_F(HeaderRecoveryTest, HeaderAllocFreeInWalIsRejectedAndRetained) {
    addDevice(0);
    save(pool());
    WalStore wal(state);
    ASSERT_EQ(wal.appendAlloc(allocation(0, 0)), 0);
    ASSERT_EQ(wal.appendFree(42), 0);
    const auto before = bytes(state + "/maru-resource-manager.wal");
    PoolManager pm(state);
    EXPECT_EQ(pm.loadPools(), -EUCLEAN);
    EXPECT_EQ(pm.checkpoint(), -EAGAIN);
    EXPECT_EQ(bytes(state + "/maru-resource-manager.wal"), before);
}

TEST_F(HeaderRecoveryTest, WalOnlyCrashRecoveryRetainsLiveAllocationAndToken) {
    addDevice(0);
    const auto a = allocation();
    ASSERT_EQ(WalStore(state).appendAlloc(a), 0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    EXPECT_EQ(pm.allocationCount(), 1u);
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

TEST_F(HeaderRecoveryTest, MissingDeviceKeepsWalUntilLateDiscovery) {
    addDevice(0);
    const auto a = allocation(1);
    ASSERT_EQ(WalStore(state).appendAlloc(a), 0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    EXPECT_EQ(pm.checkpoint(), -EAGAIN);
    EXPECT_NE(PoolManagerTestPeer::checkpointBlock(pm).find("pool(s) 1 "), std::string::npos);
    EXPECT_TRUE(fs::exists(state + "/maru-resource-manager.wal"));
    ASSERT_EQ(pm.rescanDevices(), 0); // No new device: still paused.
    EXPECT_EQ(pm.checkpoint(), -EAGAIN);
    addDevice(1);
    ASSERT_EQ(pm.rescanDevices(), 0);
    EXPECT_EQ(PoolManagerTestPeer::checkpointBlock(pm), "");
    EXPECT_EQ(pm.allocationCount(), 1u);
    EXPECT_EQ(pm.verifyAndFree(a.handle, a.clientId), 0);
    EXPECT_EQ(pm.checkpoint(), 0);
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
        EXPECT_EQ(restarted.allocationCount(), 1u);
        EXPECT_EQ(bytes(crashed + "/maru-resource-manager.wal"), walBefore);
    }

    // Otherwise the next request checkpoints first, replacing the log.
    ASSERT_EQ(allocate(pm, h), 0);
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.allocationCount(), 2u);
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
}

TEST_F(HeaderRecoveryTest, FailedRescanPreservesLivePoolAndWal) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle h{};
    ASSERT_EQ(allocate(pm, h), 0);
    addDevice(1, false);
    auto p = pool(1);
    p.freeList = {{0, B}};
    p.freeSize = B;
    ASSERT_EQ(MetadataStore(state).save(p), 0);
    const auto walBefore = bytes(state + "/maru-resource-manager.wal");
    EXPECT_EQ(pm.rescanDevices(), -EUCLEAN);
    EXPECT_EQ(pm.allocationCount(), 1u);
    EXPECT_EQ(pm.checkpoint(), -EAGAIN);
    EXPECT_NE(PoolManagerTestPeer::checkpointBlock(pm).find(dev(1)), std::string::npos);
    EXPECT_EQ(bytes(state + "/maru-resource-manager.wal"), walBefore);
    DeviceHeader header{};
    EXPECT_EQ(readDeviceHeader(dev(1), header, H), -ENODATA);
    EXPECT_EQ(pm.verifyAndFree(h, "offline-test-owner"), 0);
}

TEST_F(HeaderRecoveryTest, BoundsOverflowOverlapAndLiveFreeIntersectionAreRejected) {
    addDevice(0);
    for (auto extents : std::vector<std::vector<Extent>>{
             {{H, B}}, {{UINT64_MAX - H + 1, H}}, {{H, H}, {H, H}}}) {
        auto p = pool();
        p.freeList = extents;
        ASSERT_EQ(MetadataStore(state).save(p), 0);
        ASSERT_EQ(MetadataStore(state).saveGlobal({}, 43), 0);
        SCOPED_TRACE(extents.front().offset);
        expectRecoveryRefused(-EUCLEAN);
    }
    save(pool(), {{42, allocation()}}); // Valid ranges, but free/live overlap.
    expectRecoveryRefused(-EUCLEAN);
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
    // A missing pool may own it through a WAL-only allocation.
    PoolManagerTestPeer::removeOrphanFsDaxFiles(pm, 50, "no device for pool(s) 9");
    EXPECT_TRUE(fs::exists(p.devPath + "/maru_7.dat"));
    PoolManagerTestPeer::removeOrphanFsDaxFiles(pm, 50);
    EXPECT_FALSE(fs::exists(p.devPath + "/maru_7.dat"));
    for (const auto &name : kept) EXPECT_TRUE(fs::exists(p.devPath + "/" + name)) << name;
}

TEST_F(HeaderRecoveryTest, InvalidSecondPoolCannotInitializeFirstPoolHeader) {
    addDevice(0, false);
    addDevice(1, false);
    auto bad = pool(1);
    bad.freeList = {{0, B}};
    bad.freeSize = B;
    save(bad);
    PoolManager pm(state);
    EXPECT_EQ(pm.loadPools(), -EUCLEAN);
    DeviceHeader h{};
    EXPECT_EQ(readDeviceHeader(dev(0), h, H), -ENODATA);
    EXPECT_EQ(readDeviceHeader(dev(1), h, H), -ENODATA);
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
    addDevice(1);
    EXPECT_EQ(pm.rescanDevices(), -EUCLEAN);
    EXPECT_EQ(pm.allocationCount(), 1u);
    EXPECT_EQ(pm.checkpoint(), -EAGAIN);
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
    // The checkpoint returns any errno as is, so one per phase is enough.
    std::vector<CheckpointFaultCase> cases;
    for (const auto *file : {"pool_0.meta", "pool_1.meta", "global.meta"})
        for (const auto *phase : {"HeaderWrite", "PartialPayloadWrite", "Fsync"})
            cases.push_back({file, phase, std::string(phase) == "Fsync" ? EIO : ENOSPC});
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
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle old{};
    ASSERT_EQ(allocate(pm, old), 0);
    ASSERT_EQ(pm.checkpoint(), 0);
    const auto oldPool0 = bytes(state + "/pool_0.meta");
    const auto oldPool1 = bytes(state + "/pool_1.meta");
    const auto oldGlobal = bytes(state + "/global.meta");

    // Both pool checkpoints and the global ledger now differ from the baseline.
    ASSERT_EQ(pm.verifyAndFree(old, "offline-test-owner"), 0);
    Handle a{}, b{}, c{};
    ASSERT_EQ(allocate(pm, a), 0);
    ASSERT_EQ(allocate(pm, b), 0);
    ASSERT_EQ(allocate(pm, c, 1), 0);
    const std::string walPath = state + "/maru-resource-manager.wal";
    const auto walBefore = bytes(walPath);
    ASSERT_FALSE(walBefore.empty());

    const auto &fault = GetParam();
    const auto path = state + "/" + fault.file;
    const auto publishedBefore = bytes(path);
    const bool partial = fault.phase == "PartialPayloadWrite";
    checkpoint_fault::arm(path + ".tmp",
        fault.phase == "Fsync" ? checkpoint_fault::Operation::Fsync
                               : checkpoint_fault::Operation::Write,
        fault.error, partial ? 40 : 0); // 32-byte header + 8 payload bytes.
    const int rc = pm.checkpoint();
    checkpoint_fault::clear();
    ASSERT_EQ(checkpoint_fault::failures(), 1u); // Prove the failpoint was reached.
    EXPECT_EQ(checkpoint_fault::shortWrites(), partial ? 1u : 0u);
    EXPECT_EQ(rc, -fault.error);
    EXPECT_EQ(bytes(path), publishedBefore); // Failed file was not renamed.
    EXPECT_EQ(bytes(walPath), walBefore); // Not just present: byte-for-byte intact.
    EXPECT_EQ(bytes(state + "/global.meta"), oldGlobal);
    if (fault.file != "pool_0.meta") {
        EXPECT_NE(bytes(state + "/pool_0.meta"), oldPool0);
    }
    if (fault.file == "global.meta") {
        EXPECT_NE(bytes(state + "/pool_1.meta"), oldPool1);
    }
    EXPECT_EQ(pm.allocationCount(), 3u);

    // A failed checkpoint must also leave the appendable WAL connected to its
    // pathname. Continue allocating, then retry with the fault removed.
    Handle later{};
    ASSERT_EQ(allocate(pm, later, 1), 0);
    const auto extendedWal = bytes(walPath);
    EXPECT_GT(extendedWal.size(), walBefore.size());
    EXPECT_EQ(extendedWal.substr(0, walBefore.size()), walBefore);
    ASSERT_EQ(pm.checkpoint(), 0);
    EXPECT_FALSE(fs::exists(walPath));
    EXPECT_FALSE(fs::exists(path + ".tmp"));

    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.allocationCount(), 4u);
    EXPECT_EQ(restarted.verifyAndFree(old, "offline-test-owner"), -ENOENT);
    for (const auto &handle : {a, b, c, later}) {
        EXPECT_EQ(restarted.verifyAndFree(handle, "offline-test-owner"), 0);
    }
    EXPECT_EQ(restarted.checkpoint(), 0);
}

INSTANTIATE_TEST_SUITE_P(CheckpointIo, CheckpointFailureTest,
    ::testing::ValuesIn(checkpointFaultCases()),
    [](const ::testing::TestParamInfo<CheckpointFaultCase> &info) {
        auto file = info.param.file;
        std::replace(file.begin(), file.end(), '.', '_');
        return file + "_" + info.param.phase;
    });
