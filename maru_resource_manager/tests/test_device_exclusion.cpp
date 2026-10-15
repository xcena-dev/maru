// Device exclusion (maru-rm-ctl exclude/include/release) on the fake devices
// of the recovery fixture. Nothing here opens a real DAX device.
#include "recovery_fixture.h"

#include <sys/file.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <sys/un.h>

#include <set>

#include "admin_server.h"
#include "checkpoint_fault.h"
#include "device_exclusion.h"
#include "device_wipe.h"

namespace {
constexpr auto npos = std::string::npos;

class DeviceExclusionTest : public HeaderRecoveryTest {
protected:
    void writeExcluded(const std::string &text) {
        std::ofstream(state + "/excluded_devices") << text;
    }
    std::set<std::string> excludedOnDisk() {
        std::set<std::string> names;
        EXPECT_EQ(loadExcludedDevices(state, names), 0);
        return names;
    }
    std::set<std::string> poolPaths(PoolManager &pm) {
        std::vector<PoolState> pools;
        pm.getStats(pools);
        std::set<std::string> paths;
        for (const auto &p : pools) paths.insert(p.devPath);
        return paths;
    }
    std::string uuidOf(PoolManager &pm, uint32_t id) {
        std::vector<PoolState> pools;
        pm.getStats(pools);
        for (const auto &p : pools)
            if (p.poolId == id) return p.deviceUuid;
        return "";
    }
    // What a ROOF format leaves at offset 0: its superblock magic "FGSB".
    void writeRoofSuperblock(uint32_t id) {
        const uint32_t magic = 0x46475342;
        std::string head(64, '\0');
        std::memcpy(head.data(), &magic, sizeof(magic));
        std::fstream f(fakeRoot + dev(id), std::ios::in | std::ios::out | std::ios::binary);
        f.write(head.data(), static_cast<std::streamsize>(head.size()));
    }
    std::string head(uint32_t id) { return bytes(fakeRoot + dev(id)).substr(0, 64); }
    int allocateAs(PoolManager &pm, const std::string &client, Handle &h, uint32_t id = 0) {
        std::string path, uuid;
        uint64_t requested;
        return pm.alloc(H, client, h, path, uuid, dev(id), requested);
    }
    std::string localClient(pid_t pid) {
        char host[256]{};
        EXPECT_EQ(::gethostname(host, sizeof(host)), 0);
        return std::string(host) + ":" + std::to_string(pid);
    }
};

std::string ask(const std::string &socketPath, const std::string &request) {
    const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, socketPath.c_str(), sizeof(addr.sun_path) - 1);
    if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return "connect failed";
    }
    ::send(fd, request.data(), request.size(), MSG_NOSIGNAL);
    ::shutdown(fd, SHUT_WR);
    std::string reply;
    char buf[1024];
    for (ssize_t n; (n = ::recv(fd, buf, sizeof(buf), 0)) > 0;) reply.append(buf, n);
    ::close(fd);
    return reply;
}
} // namespace

TEST(DeviceExclusionNames, NormalizesKernelNamesOnly) {
    std::string out;
    EXPECT_TRUE(normalizeDaxName("dax2.0", out));
    EXPECT_EQ(out, "dax2.0");
    EXPECT_TRUE(normalizeDaxName("/dev/dax12.3", out));
    EXPECT_EQ(out, "dax12.3");
    for (const char *bad : {"", "dax2", "dax.0", "dax02.0", "dax2.0x", "/dev/sda",
                            "/dev/../dax2.0", "pmem0", "dax2.0 "})
        EXPECT_FALSE(normalizeDaxName(bad, out)) << bad;
}

TEST_F(DeviceExclusionTest, ExcludedDeviceIsNeverRegisteredOrWritten) {
    addDevice(0);
    addDevice(1, false);
    writeExcluded("# handed to ROOF\n  /dev/dax1.0  \n\n");
    PoolManager pm(state);
    openedPaths.clear();
    ASSERT_EQ(pm.loadPools(), 0);
    EXPECT_EQ(poolPaths(pm), std::set<std::string>{dev(0)});
    ASSERT_EQ(pm.rescanDevices(), 0);
    EXPECT_EQ(poolPaths(pm), std::set<std::string>{dev(0)});
    Handle h{};
    EXPECT_EQ(allocate(pm, h, 1), -ENOENT);
    EXPECT_EQ(std::count(openedPaths.begin(), openedPaths.end(), dev(1)), 0);
    EXPECT_GT(std::count(openedPaths.begin(), openedPaths.end(), dev(0)), 0);
    DeviceHeader header{};
    EXPECT_EQ(readDeviceHeader(dev(1), header, H), -ENODATA); // Never initialized.
    EXPECT_EQ(pm.checkpoint(), 0);
}

TEST_F(DeviceExclusionTest, MalformedListRefusesStartBeforeTouchingDevices) {
    addDevice(0, false);
    writeExcluded("dax1.0\n/dev/sda\n");
    PoolManager pm(state);
    EXPECT_EQ(pm.loadPools(), -EINVAL);
    DeviceHeader header{};
    EXPECT_EQ(readDeviceHeader(dev(0), header, H), -ENODATA);
}

TEST_F(DeviceExclusionTest, RoofFormattedDeviceIsSkippedWithoutList) {
    addDevice(0);
    addDevice(1, false);
    writeRoofSuperblock(1);
    const auto before = head(1);
    // Old pool history must not turn the foreign device into a startup failure.
    ASSERT_EQ(MetadataStore(state).save(pool(1)), 0);
    ASSERT_EQ(MetadataStore(state).saveGlobal({}, 43), 0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    EXPECT_EQ(poolPaths(pm), std::set<std::string>{dev(0)});
    ASSERT_EQ(pm.rescanDevices(), 0);
    EXPECT_EQ(head(1), before);
    EXPECT_EQ(pm.checkpoint(), 0);
}

TEST_F(DeviceExclusionTest, ExcludeRefusesLiveAllocationsUntilFreed) {
    addDevice(0);
    addDevice(1);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle h{};
    ASSERT_EQ(allocate(pm, h, 1), 0);
    std::string out;
    EXPECT_EQ(pm.adminExclude("/dev/dax1.0", out), -EBUSY);
    EXPECT_NE(out.find("region=" + std::to_string(h.regionId)), npos) << out;
    EXPECT_FALSE(fs::exists(state + "/excluded_devices"));
    EXPECT_EQ(poolPaths(pm).size(), 2u);

    ASSERT_EQ(pm.verifyAndFree(h, "offline-test-owner"), 0);
    out.clear();
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    EXPECT_NE(out.find("checkpoint=written"), npos) << out;
    EXPECT_EQ(poolPaths(pm), std::set<std::string>{dev(0)});
    EXPECT_EQ(excludedOnDisk(), std::set<std::string>{"dax1.0"});
    EXPECT_EQ(allocate(pm, h, 1), -ENOENT);
    out.clear();
    EXPECT_EQ(pm.adminExclude("dax1.0", out), 0); // Idempotent.

    // Restart and rescans keep it out and never touch its header.
    const auto header = head(1);
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    ASSERT_EQ(restarted.rescanDevices(), 0);
    EXPECT_EQ(poolPaths(restarted), std::set<std::string>{dev(0)});
    EXPECT_EQ(head(1), header);
    out.clear();
    ASSERT_EQ(restarted.adminStatus(out), 0);
    EXPECT_NE(out.find("device=/dev/dax1.0 state=excluded"), npos) << out;
    EXPECT_NE(out.find("checkpoints=enabled"), npos) << out;
}

TEST_F(DeviceExclusionTest, ExcludeOfAbsentDeviceUsesCheckpointAndWal) {
    addDevice(0);
    auto p = pool(1);
    p.freeList = {{2 * H, B - 2 * H}};
    p.freeSize = B - 2 * H;
    save(p, {{42, allocation(1)}});
    ASSERT_EQ(WalStore(state).appendFree(42), 0); // Freed after the checkpoint.
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    EXPECT_EQ(pm.checkpoint(), -EAGAIN); // Pool 1 has no device.
    std::string out;
    EXPECT_EQ(pm.adminRelease(42, true, out), -ENODEV);
    out.clear();
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    EXPECT_EQ(pm.allocationCount(), 0u);
    EXPECT_EQ(PoolManagerTestPeer::checkpointBlock(pm), "");
    EXPECT_EQ(pm.checkpoint(), 0);
}

TEST_F(DeviceExclusionTest, ExcludeOfAbsentDeviceRefusesWalOnlyAllocation) {
    addDevice(0);
    ASSERT_EQ(WalStore(state).appendAlloc(allocation(1)), 0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    std::string out;
    EXPECT_EQ(pm.adminExclude("dax1.0", out), -EBUSY);
    EXPECT_NE(out.find("region=42"), npos) << out;
    EXPECT_FALSE(fs::exists(state + "/excluded_devices"));
}

TEST_F(DeviceExclusionTest, StartupRefusesExcludedPoolWithLiveAllocation) {
    addDevice(0);
    ASSERT_EQ(WalStore(state).appendAlloc(allocation(1)), 0);
    writeExcluded("dax1.0\n"); // Edited by hand without draining.
    PoolManager pm(state);
    EXPECT_EQ(pm.loadPools(), -EBUSY);
    // Allocation history alone is fine.
    ASSERT_EQ(WalStore(state).appendFree(42), 0);
    PoolManager drained(state);
    ASSERT_EQ(drained.loadPools(), 0);
    EXPECT_EQ(drained.checkpoint(), 0);
}

TEST_F(DeviceExclusionTest, IncludeRestoresPoolButRoofDeviceNeedsReinit) {
    addDevice(0);
    addDevice(1);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    const auto uuid = uuidOf(pm, 1);
    std::string out;
    EXPECT_EQ(pm.adminInclude("dax1.0", false, out), -EINVAL); // Not excluded.
    out.clear();
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    out.clear();
    ASSERT_EQ(pm.adminInclude("dax1.0", false, out), 0) << out;
    EXPECT_EQ(poolPaths(pm).size(), 2u);
    EXPECT_EQ(uuidOf(pm, 1), uuid);
    EXPECT_TRUE(excludedOnDisk().empty());
    ASSERT_EQ(pm.checkpoint(), 0); // pool_1.meta now exists.

    // Hand it to ROOF, then try to take it back.
    out.clear();
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    writeRoofSuperblock(1);
    const auto roof = head(1);
    out.clear();
    EXPECT_EQ(pm.adminInclude("dax1.0", false, out), -EBUSY);
    EXPECT_NE(out.find("ROOF"), npos) << out;
    EXPECT_NE(out.find("--reinit"), npos) << out;
    EXPECT_EQ(head(1), roof);
    EXPECT_EQ(excludedOnDisk(), std::set<std::string>{"dax1.0"});
    EXPECT_EQ(poolPaths(pm).size(), 1u);

    out.clear();
    ASSERT_EQ(pm.adminInclude("dax1.0", true, out), 0) << out;
    const auto archived = out.find("previous_metadata=");
    ASSERT_NE(archived, npos) << out;
    EXPECT_TRUE(fs::exists(out.substr(archived + 18, out.find('\n', archived) - archived - 18)));
    DeviceHeader header{};
    ASSERT_EQ(readDeviceHeader(dev(1), header, H), 0);
    EXPECT_NE(uuidToString(header.uuid), uuid);
    std::vector<PoolState> pools;
    pm.getStats(pools);
    for (const auto &p : pools) EXPECT_EQ(p.freeSize, p.totalSize) << p.devPath;
    EXPECT_EQ(pm.checkpoint(), 0);
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(poolPaths(restarted).size(), 2u);
}

TEST_F(DeviceExclusionTest, ReleaseNeedsExitedOwnerAndConfirmation) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle mine{}, remote{}, gone{};
    ASSERT_EQ(allocateAs(pm, localClient(::getpid()), mine), 0);
    ASSERT_EQ(allocate(pm, remote), 0); // "offline-test-owner" is not local.
    ASSERT_EQ(allocateAs(pm, localClient(2147483647), gone), 0);
    std::string out;
    EXPECT_EQ(pm.adminRelease(mine.regionId, true, out), -EBUSY);
    EXPECT_EQ(pm.adminRelease(remote.regionId, false, out), -EPERM);
    EXPECT_EQ(pm.adminRelease(gone.regionId, false, out), -EPERM);
    out.clear();
    ASSERT_EQ(pm.adminRelease(remote.regionId, true, out), 0) << out;
    ASSERT_EQ(pm.adminRelease(gone.regionId, true, out), 0) << out;
    EXPECT_EQ(pm.adminRelease(gone.regionId, true, out), -ENOENT);
    out.clear();
    ASSERT_EQ(pm.adminAllocations("dax0.0", out), 0);
    EXPECT_NE(out.find("owner=running"), npos) << out;
    PoolManager restarted(state); // Releases went through the WAL.
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.allocationCount(), 1u);
}

TEST_F(DeviceExclusionTest, AdminSocketIsPrivateAndParsesStrictly) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    const std::string path = state + "/admin.sock";
    AdminServer admin(pm, path);
    ASSERT_EQ(admin.start(), 0);
    struct stat st{};
    ASSERT_EQ(::lstat(path.c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 0777, 0600u);
    EXPECT_EQ(ask(path, "status\n").rfind("OK\npool=0 ", 0), 0u);
    for (const char *bad : {"exclude\n", "release 12abc --confirm\n", "include dax1.0 --force\n",
                            "bogus\n", "status"})
        EXPECT_EQ(ask(path, bad).rfind("ERR 22 ", 0), 0u) << bad;
    EXPECT_EQ(ask(path, "exclude /dev/dax0.0\n").rfind("OK\n", 0), 0u);
    EXPECT_EQ(excludedOnDisk(), std::set<std::string>{"dax0.0"});
    admin.stop();
    EXPECT_FALSE(fs::exists(path));
    // Anything but a stale socket in its place is never removed.
    std::ofstream(path) << "keep";
    AdminServer other(pm, path);
    EXPECT_EQ(other.start(), -EEXIST);
    EXPECT_TRUE(fs::exists(path));
}

TEST_F(DeviceExclusionTest, IncludeAfterFreesSinceTheLastCheckpointStartsEmpty) {
    addDevice(0);
    addDevice(1);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle x{};
    ASSERT_EQ(allocate(pm, x, 1), 0);
    ASSERT_EQ(pm.checkpoint(), 0);                  // Checkpoint lists x.
    ASSERT_EQ(pm.verifyAndFree(x, "offline-test-owner"), 0); // Freed in the WAL only.
    // A WAL record for an absent pool keeps checkpoints paused, so the
    // checkpoint still lists x when the device comes back.
    auto absent = allocation(2);
    absent.handle.regionId = 500;
    ASSERT_EQ(WalStore(state).appendAlloc(absent), 0);
    std::string out;
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    EXPECT_NE(out.find("checkpoint=deferred"), npos) << out;
    out.clear();
    ASSERT_EQ(pm.adminInclude("dax1.0", false, out), 0) << out;
    EXPECT_NE(out.find("previous_metadata="), npos) << out;
    Handle y{};
    ASSERT_EQ(allocate(pm, y, 1), 0);
    EXPECT_EQ(y.offset, x.offset);
    // Crash before any checkpoint: the files on disk must still recover.
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.verifyAndFree(y, "offline-test-owner"), 0);
}

TEST_F(DeviceExclusionTest, FailedListSaveKeepsTheDeviceExcluded) {
    addDevice(0);
    addDevice(1);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle h{};
    ASSERT_EQ(allocate(pm, h, 1), 0);
    ASSERT_EQ(pm.checkpoint(), 0); // pool_1.meta has a hole for h.
    ASSERT_EQ(pm.verifyAndFree(h, "offline-test-owner"), 0);
    std::string out;
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    writeRoofSuperblock(1);
    const auto meta = bytes(state + "/pool_1.meta");
    checkpoint_fault::arm(state + "/excluded_devices.tmp", checkpoint_fault::Operation::Write,
                          ENOSPC);
    out.clear();
    EXPECT_EQ(pm.adminInclude("dax1.0", true, out), -ENOSPC);
    checkpoint_fault::clear();
    EXPECT_EQ(checkpoint_fault::failures(), 1u);
    // --reinit asked to discard the device, so its header may already be new;
    // the old metadata then stays archived, and ownership is rolled back.
    EXPECT_NE(out.find("header was already written"), npos) << out;
    const auto kept = out.find("previous metadata kept at ");
    ASSERT_NE(kept, npos) << out;
    EXPECT_EQ(bytes(out.substr(kept + 26, out.find('\n', kept) - kept - 26)), meta);
    EXPECT_NE(bytes(state + "/pool_1.meta"), meta);
    EXPECT_EQ(excludedOnDisk(), std::set<std::string>{"dax1.0"});
    EXPECT_EQ(poolPaths(pm), std::set<std::string>{dev(0)});
    {
        PoolManager restarted(state);
        ASSERT_EQ(restarted.loadPools(), 0);
        EXPECT_EQ(poolPaths(restarted), std::set<std::string>{dev(0)});
    }
    out.clear();
    ASSERT_EQ(pm.adminInclude("dax1.0", true, out), 0) << out; // Retry works.
}

TEST_F(DeviceExclusionTest, UnsyncedListIsNotReportedAsReadyForHandover) {
    addDevice(0);
    addDevice(1);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    checkpoint_fault::arm(state, checkpoint_fault::Operation::Fsync, EIO);
    std::string out;
    EXPECT_EQ(pm.adminExclude("dax1.0", out), PoolManager::kAdminIncomplete);
    EXPECT_NE(out.find("excluded /dev/dax1.0"), npos) << out;
    EXPECT_NE(out.find("do not hand it over"), npos) << out;
    // Renamed into place: memory must agree with the file.
    EXPECT_EQ(excludedOnDisk(), std::set<std::string>{"dax1.0"});
    EXPECT_EQ(poolPaths(pm), std::set<std::string>{dev(0)});
    out.clear();
    ASSERT_EQ(pm.adminStatus(out), 0);
    EXPECT_NE(out.find("excluded_list=not-durable"), npos) << out;
    // Neither a retry nor a wipe may take the already-excluded shortcut. The
    // retry is neither a success nor a refusal on the admin socket.
    {
        const std::string sock = state + "/admin.sock";
        AdminServer admin(pm, sock);
        ASSERT_EQ(admin.start(), 0);
        const std::string reply = ask(sock, "exclude dax1.0\n");
        admin.stop();
        EXPECT_EQ(reply.rfind("INCOMPLETE\n/dev/dax1.0 is excluded, but", 0), 0u) << reply;
        EXPECT_NE(reply.find("not durable yet"), npos) << reply;
    }
    out.clear();
    EXPECT_EQ(pm.adminWipe("dax1.0", true, out), -EIO);
    EXPECT_NE(out.find("do not hand it over"), npos) << out;
    checkpoint_fault::clear();
    // The list, the exclusion's checkpoint, the retry and the wipe.
    EXPECT_EQ(checkpoint_fault::failures(), 4u);

    out.clear();
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out; // The retry syncs.
    out.clear();
    ASSERT_EQ(pm.adminStatus(out), 0);
    EXPECT_EQ(out.find("not-durable"), npos) << out;
    out.clear();
    EXPECT_EQ(pm.adminWipe("dax1.0", false, out), -EPERM); // Only --confirm is missing.
}

TEST_F(DeviceExclusionTest, RestartRechecksThatTheListIsOnDisk) {
    addDevice(0);
    addDevice(1);
    {
        PoolManager pm(state);
        ASSERT_EQ(pm.loadPools(), 0);
        std::string out;
        ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    }
    // Whether the previous RM synced the list died with it: check again.
    checkpoint_fault::arm(state, checkpoint_fault::Operation::Fsync, EIO);
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    std::string out;
    ASSERT_EQ(restarted.adminStatus(out), 0);
    EXPECT_NE(out.find("excluded_list=not-durable"), npos) << out;
    out.clear();
    EXPECT_EQ(restarted.adminWipe("dax1.0", true, out), -EIO);
    checkpoint_fault::clear();
    out.clear();
    ASSERT_EQ(restarted.adminExclude("dax1.0", out), 0) << out;
    out.clear();
    ASSERT_EQ(restarted.adminStatus(out), 0);
    EXPECT_EQ(out.find("not-durable"), npos) << out;
}

TEST_F(DeviceExclusionTest, UnsyncedIncludeStandsButIsReported) {
    addDevice(0);
    addDevice(1);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    std::string out;
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    checkpoint_fault::arm(state, checkpoint_fault::Operation::Fsync, EIO);
    out.clear();
    const int rc = pm.adminInclude("dax1.0", false, out);
    checkpoint_fault::clear();
    // Losing this list would only keep the device excluded.
    ASSERT_EQ(rc, 0) << out;
    EXPECT_NE(out.find("WARNING: the excluded device list is not durable yet"), npos) << out;
    EXPECT_EQ(poolPaths(pm), (std::set<std::string>{dev(0), dev(1)}));
    out.clear();
    ASSERT_EQ(pm.adminStatus(out), 0);
    EXPECT_NE(out.find("excluded_list=not-durable"), npos) << out;
}

TEST_F(DeviceExclusionTest, AllocationsOfAbsentDeviceIncludeTheWal) {
    addDevice(0);
    auto p = pool(1);
    p.freeList = {{2 * H, B - 2 * H}};
    p.freeSize = B - 2 * H;
    save(p, {{42, allocation(1)}});
    {
        WalStore wal(state);
        ASSERT_EQ(wal.appendFree(42), 0); // Freed after the checkpoint.
        auto later = allocation(1, 3 * H);
        later.handle.regionId = 43;
        ASSERT_EQ(wal.appendAlloc(later), 0); // Allocated after it.
    }
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    for (const std::string filter : {"dax1.0", ""}) {
        std::string out;
        ASSERT_EQ(pm.adminAllocations(filter, out), 0) << out;
        EXPECT_EQ(out.find("region=42 "), npos) << filter << ": " << out;
        EXPECT_NE(out.find("region=43 pool=1 device=(not present)"), npos)
            << filter << ": " << out;
    }
    std::string out;
    ASSERT_EQ(pm.adminAllocations("dax0.0", out), 0);
    EXPECT_EQ(out, "");

    // An unreadable WAL is an error, never an empty listing.
    std::ofstream(state + "/maru-resource-manager.wal", std::ios::binary | std::ios::trunc)
        << std::string(64, '\xff');
    for (const std::string filter : {"dax1.0", ""}) {
        out.clear();
        EXPECT_EQ(pm.adminAllocations(filter, out), -EPROTO) << filter;
        EXPECT_NE(out.find("cannot read WAL"), npos) << filter << ": " << out;
    }
}

TEST_F(DeviceExclusionTest, IncludeIgnoresUnrelatedRejectedDevice) {
    addDevice(0);
    addDevice(1);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    std::string out;
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    addDevice(2);
    auto bad = pool(2);
    bad.freeList = {{0, B}}; // Covers the header: rejected on every rescan.
    bad.freeSize = B;
    ASSERT_EQ(MetadataStore(state).save(bad), 0);
    EXPECT_EQ(pm.rescanDevices(), -EUCLEAN);
    out.clear();
    ASSERT_EQ(pm.adminInclude("dax1.0", false, out), 0) << out;
    EXPECT_EQ(poolPaths(pm), (std::set<std::string>{dev(0), dev(1)}));
}

TEST_F(DeviceExclusionTest, HandEditedListIsNeverOverwritten) {
    addDevice(0);
    addDevice(1);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    std::string out;
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    writeExcluded("dax1.0\ndax3.0\n");
    out.clear();
    EXPECT_EQ(pm.adminExclude("dax0.0", out), -ESTALE);
    EXPECT_EQ(pm.adminInclude("dax1.0", false, out), -ESTALE);
    EXPECT_EQ(excludedOnDisk(), (std::set<std::string>{"dax1.0", "dax3.0"}));
    EXPECT_EQ(poolPaths(pm), std::set<std::string>{dev(0)});
}

TEST_F(DeviceExclusionTest, LiveAdminSocketIsNeverTakenOver) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    const std::string path = state + "/admin.sock";
    AdminServer first(pm, path);
    ASSERT_EQ(first.start(), 0);
    AdminServer second(pm, path);
    EXPECT_EQ(second.start(), -EADDRINUSE);
    second.stop();
    EXPECT_EQ(ask(path, "status\n").rfind("OK\n", 0), 0u); // Still served.
    first.stop();
    // A stale socket (no server) is replaced.
    {
        const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
        ASSERT_EQ(::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)), 0);
        ::close(fd);
    }
    AdminServer third(pm, path);
    EXPECT_EQ(third.start(), 0);
}

TEST_F(DeviceExclusionTest, HeaderIsWrittenBeforeTheListReleasesTheDevice) {
    addDevice(0);
    addDevice(1, false); // Excluded before the RM ever gave it a header.
    writeExcluded("dax1.0\n");
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    checkpoint_fault::arm(state + "/excluded_devices.tmp", checkpoint_fault::Operation::Write,
                          ENOSPC);
    std::string out;
    EXPECT_EQ(pm.adminInclude("dax1.0", false, out), -ENOSPC);
    checkpoint_fault::clear();
    EXPECT_NE(out.find("header was already written"), npos) << out;
    DeviceHeader header{};
    EXPECT_EQ(readDeviceHeader(dev(1), header, H), 0);
    EXPECT_EQ(excludedOnDisk(), std::set<std::string>{"dax1.0"});
    {
        PoolManager restarted(state); // Still excluded, so a restart is clean.
        ASSERT_EQ(restarted.loadPools(), 0);
        EXPECT_EQ(poolPaths(restarted), std::set<std::string>{dev(0)});
    }
    out.clear();
    ASSERT_EQ(pm.adminInclude("dax1.0", false, out), 0) << out;
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(poolPaths(restarted), (std::set<std::string>{dev(0), dev(1)}));
}

TEST_F(DeviceExclusionTest, ArchivesAreNeverOverwritten) {
    addDevice(0);
    addDevice(1);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    std::string out;
    for (int round = 0; round < 3; ++round) {
        ASSERT_EQ(pm.checkpoint(), 0);
        ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
        ASSERT_EQ(pm.adminInclude("dax1.0", false, out), 0) << out;
    }
    size_t archives = 0;
    for (const auto &entry : fs::directory_iterator(state))
        archives += entry.path().filename().string().rfind("pool_1.meta.include-", 0) == 0;
    EXPECT_EQ(archives, 3u);
}

TEST_F(DeviceExclusionTest, WipeZeroesOnlyExcludedDevicesWhenConfirmed) {
    addDevice(0);
    addDevice(1);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    Handle h{};
    ASSERT_EQ(allocate(pm, h, 1), 0);
    {   // A client's data in the allocated range.
        std::fstream f(fakeRoot + dev(1), std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(static_cast<std::streamoff>(h.offset));
        f << std::string(4096, 'k');
    }
    std::string out;
    EXPECT_EQ(pm.adminWipe("dax1.0", true, out), -EINVAL); // Still in use by the RM.
    ASSERT_EQ(pm.verifyAndFree(h, "offline-test-owner"), 0);
    out.clear();
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    const auto before = bytes(fakeRoot + dev(1));
    out.clear();
    EXPECT_EQ(pm.adminWipe("/dev/dax1.0", false, out), -EPERM);
    EXPECT_EQ(bytes(fakeRoot + dev(1)), before);
    out.clear();
    ASSERT_EQ(pm.adminWipe("/dev/dax1.0", true, out), 0) << out;
    EXPECT_NE(out.find("wiped /dev/dax1.0 bytes=" + std::to_string(B)), npos) << out;
    EXPECT_EQ(bytes(fakeRoot + dev(1)), std::string(B, '\0')); // Header included.

    // Reset for reuse: a wiped device comes back with a fresh header.
    out.clear();
    ASSERT_EQ(pm.adminInclude("dax1.0", false, out), 0) << out;
    DeviceHeader header{};
    EXPECT_EQ(readDeviceHeader(dev(1), header, H), 0);
    ASSERT_EQ(allocate(pm, h, 1), 0);

    // Absent devices cannot be wiped.
    out.clear();
    ASSERT_EQ(pm.adminExclude("dax3.0", out), 0) << out;
    out.clear();
    EXPECT_NE(pm.adminWipe("dax3.0", true, out), 0);
}

TEST_F(DeviceExclusionTest, WipeStopsBetweenChunksWhenCancelled) {
    addDevice(1);
    const std::string pattern(B, 'k');
    std::ofstream(fakeRoot + dev(1), std::ios::binary | std::ios::trunc) << pattern;
    std::atomic<bool> cancel{true};
    EXPECT_EQ(wipeDevice(dev(1), B, H, &cancel), -ECANCELED);
    EXPECT_EQ(bytes(fakeRoot + dev(1)), pattern);
    cancel = false;
    ASSERT_EQ(wipeDevice(dev(1), B, H, &cancel), 0); // Eight 2 MiB chunks.
    EXPECT_EQ(bytes(fakeRoot + dev(1)), std::string(B, '\0'));
    EXPECT_EQ(wipeDevice(dev(1), B, 1000), -EINVAL); // Not page-aligned.
}

TEST_F(DeviceExclusionTest, IsolatedWipeSurvivesFaultsAndStopsOnCancel) {
    addDevice(1);
    // Mapping past the end of a file faults like an unbound device-dax mapping:
    // SIGBUS kills the child only.
    EXPECT_EQ(wipeDeviceIsolated(dev(1), 2 * B, H), -EIO);
    EXPECT_EQ(wipeDeviceIsolated(dev(1), B, H, [] { return true; }), -ECANCELED);
    EXPECT_EQ(wipeDeviceIsolated("/dev/dax9.0", B, H), -ENOENT);
    ASSERT_EQ(wipeDeviceIsolated(dev(1), B, H), 0);
    EXPECT_EQ(bytes(fakeRoot + dev(1)), std::string(B, '\0'));
}

TEST_F(DeviceExclusionTest, RequestOfADepartedClientIsNotRun) {
    addDevice(0);
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    const std::string path = state + "/admin.sock";
    AdminServer admin(pm, path);
    ASSERT_EQ(admin.start(), 0);
    auto connectTo = [&] {
        const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
        EXPECT_EQ(::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)), 0);
        return fd;
    };
    const int busy = connectTo(); // Holds the admin thread until it sends a line.
    ::usleep(100 * 1000);
    const int gone = connectTo(); // Queued behind it; gives up before its turn.
    const std::string request = "exclude dax0.0\n";
    ASSERT_EQ(::send(gone, request.data(), request.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(request.size()));
    ::close(gone);
    const std::string status = "status\n";
    ASSERT_EQ(::send(busy, status.data(), status.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(status.size()));
    char buf[256];
    EXPECT_GT(::recv(busy, buf, sizeof(buf), 0), 0);
    ::close(busy);
    // Both connections are served by now once a later request gets through.
    EXPECT_EQ(ask(path, "status\n").rfind("OK\n", 0), 0u);
    EXPECT_FALSE(fs::exists(state + "/excluded_devices"));
    EXPECT_EQ(poolPaths(pm), std::set<std::string>{dev(0)});
}

TEST_F(DeviceExclusionTest, IncludeBeforeAnyGlobalCheckpointStaysRestartable) {
    addDevice(0);
    addDevice(1);
    auto absent = allocation(2);
    absent.handle.regionId = 500;
    ASSERT_EQ(WalStore(state).appendAlloc(absent), 0); // Keeps checkpoints paused.
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    std::string out;
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    out.clear();
    ASSERT_EQ(pm.adminInclude("dax1.0", false, out), 0) << out;
    EXPECT_FALSE(fs::exists(state + "/global.meta"));
    EXPECT_FALSE(fs::exists(state + "/pool_1.meta")); // No pool metadata without it.
    Handle h{};
    ASSERT_EQ(allocate(pm, h, 1), 0);
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
    EXPECT_EQ(restarted.verifyAndFree(h, "offline-test-owner"), 0);
}

TEST_F(DeviceExclusionTest, ExcludeWaitsForAnUncertainWalRecordToBeReplaced) {
    addDevice(0);
    addDevice(1);
    auto absent = allocation(2);
    absent.handle.regionId = 500;
    ASSERT_EQ(WalStore(state).appendAlloc(absent), 0); // Keeps checkpoints paused.
    PoolManager pm(state);
    ASSERT_EQ(pm.loadPools(), 0);
    const std::string walPath = state + "/maru-resource-manager.wal";
    checkpoint_fault::arm(walPath, checkpoint_fault::Operation::Fdatasync, EIO);
    checkpoint_fault::armTruncate(walPath, EIO);
    Handle h{};
    EXPECT_EQ(allocate(pm, h, 1), -EIO); // Rejected, but its ALLOC stays in the WAL.
    checkpoint_fault::clear();
    std::string out;
    EXPECT_EQ(pm.adminExclude("dax1.0", out), -EAGAIN);
    EXPECT_NE(out.find("outcome is unknown"), npos) << out;
    EXPECT_FALSE(fs::exists(state + "/excluded_devices"));
    {
        PoolManager restarted(state); // Nothing was excluded: still restartable.
        EXPECT_EQ(restarted.loadPools(), 0);
    }
    // Once a checkpoint is possible it replaces the log, and exclude proceeds.
    addDevice(2);
    ASSERT_EQ(pm.rescanDevices(), 0);
    out.clear();
    ASSERT_EQ(pm.adminExclude("dax1.0", out), 0) << out;
    PoolManager restarted(state);
    ASSERT_EQ(restarted.loadPools(), 0);
}

TEST_F(DeviceExclusionTest, WipeChildDiesWithTheRmAndHoldsNoDescriptors) {
    // Orphans reparent to this process, so the test reaps the wipe child
    // itself: PID 1 of a CI container (tail -f /dev/null) never reaps them,
    // and an unreaped zombie still answers kill(pid, 0).
    ASSERT_EQ(::prctl(PR_SET_CHILD_SUBREAPER, 1), 0);
    const std::string lockPath = state + "/maru-resource-manager.lock";
    const pid_t rm = ::fork();
    if (rm == 0) {
        const int lockFd = ::open(lockPath.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        if (lockFd < 0 || ::flock(lockFd, LOCK_EX) != 0) ::_exit(1);
        wipeDeviceIsolated("/dev/dax-hang", B, H); // The child stalls in open().
        ::_exit(0);
    }
    pid_t wipe = 0;
    for (int i = 0; rm > 0 && i < 250 && wipe == 0; ++i) {
        std::ifstream children("/proc/" + std::to_string(rm) + "/task/" +
                               std::to_string(rm) + "/children");
        children >> wipe;
        if (wipe == 0) ::usleep(20 * 1000);
    }
    if (rm > 0) {
        ::kill(rm, SIGKILL); // The RM dies mid-wipe.
        ::waitpid(rm, nullptr, 0);
    }
    int status = 0;
    bool reaped = false;
    for (int i = 0; wipe > 0 && i < 250 && !reaped; ++i) {
        reaped = ::waitpid(wipe, &status, WNOHANG) == wipe;
        if (!reaped) ::usleep(20 * 1000);
    }
    if (wipe > 0 && !reaped) { // Never leave a runaway wipe behind.
        ::kill(wipe, SIGKILL);
        ::waitpid(wipe, nullptr, 0);
    }
    ::prctl(PR_SET_CHILD_SUBREAPER, 0);
    ASSERT_GT(rm, 0);
    ASSERT_GT(wipe, 0) << "wipe child never started";
    ASSERT_TRUE(reaped) << "wipe child outlived the RM";
    EXPECT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL) << status;
    const int fd = ::open(lockPath.c_str(), O_RDWR | O_CLOEXEC);
    ASSERT_GE(fd, 0);
    EXPECT_EQ(::flock(fd, LOCK_EX | LOCK_NB), 0); // A new RM can take the lock.
    ::close(fd);
}
