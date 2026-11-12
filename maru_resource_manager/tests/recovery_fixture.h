// Shared fixture for recovery tests: a private fake /dev and sysfs under
// fakeRoot. The linker wraps filesystem calls (fake_dax_fs.cpp), so these
// tests never open real DAX devices.
#pragma once

#include <gtest/gtest.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <ostream>
#include <string>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "device_header.h"
#include "checkpoint_fault.h"
#include "fake_dax_fs.h"
#include "metadata.h"
#include "pool_manager_test_peer.h"
#include "pool_validation.h"
#include "util.h"
#include "wal.h"

namespace fs = std::filesystem;
using namespace maru;

// Every path the code under test passed to open(), for access checks.
extern std::vector<std::string> openedPaths;
// The fakeOpenHook of these tests: records into openedPaths, and stalls
// /dev/dax-hang (test_header_recovery.cpp).
int recordOpen(const char *path);

class HeaderRecoveryTest : public ::testing::Test {
protected:
    static constexpr uint64_t H = 2ULL << 20;
    static constexpr uint64_t B = 16ULL << 20;
    std::string state;
    void SetUp() override {
        char path[] = "/tmp/maru-header-recovery-XXXXXX";
        ASSERT_NE(::mkdtemp(path), nullptr);
        fakeRoot = path;
        fakeOpenHook = recordOpen;
        state = fakeRoot + "/state";
        fs::create_directories(state);
        fs::create_directories(fakeRoot + "/dev");
        fs::create_directories(fakeRoot + "/sys/bus/dax/devices");
        fs::create_directories(fakeRoot + "/sys/class/block");
        ASSERT_EQ(initSecret(state, false), 0);
    }
    void TearDown() override {
        fs::remove_all(fakeRoot);
        fakeRoot.clear();
        fakeOpenHook = nullptr;
    }
    std::string dev(uint32_t id) { return "/dev/dax" + std::to_string(id) + ".0"; }
    void addDevice(uint32_t id, bool header = true) {
        const auto name = "dax" + std::to_string(id) + ".0";
        const auto sys = fakeRoot + "/sys/bus/dax/devices/" + name;
        fs::create_directories(sys);
        fs::create_directories(fakeRoot + "/sys/class/dax/" + name);
        fs::create_symlink("/sys/bus/dax/drivers/device_dax", sys + "/driver");
        std::ofstream(sys + "/align") << H;
        std::ofstream(fakeRoot + "/sys/class/dax/" + name + "/size") << B;
        int fd = ::open(dev(id).c_str(), O_CREAT | O_RDWR, 0600);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(::ftruncate(fd, B), 0);
        ::close(fd);
        if (header) {
            DeviceHeader hdr{};
            initDeviceHeader(hdr);
            ASSERT_EQ(writeDeviceHeader(dev(id), hdr, H), 0);
        }
    }
    PoolState pool(uint32_t id = 0) {
        PoolState p{};
        p.poolId = id;
        p.devPath = dev(id);
        p.type = DaxType::DEV_DAX;
        p.alignBytes = p.dataBegin = H;
        p.dataEnd = B;
        p.totalSize = p.freeSize = B - H;
        p.freeList = {{H, B - H}};
        return p;
    }
    Allocation allocation(uint32_t id = 0, uint64_t offset = H) {
        Allocation a{};
        a.poolId = id;
        a.handle.regionId = 42;
        a.handle.offset = a.realOffset = offset;
        a.handle.length = a.allocLength = a.requestedSize = H;
        a.nonce = 17;
        std::strcpy(a.clientId, "offline-test-owner");
        a.handle.authToken = computeAuthToken(a.handle, a.nonce, a.clientId);
        return a;
    }
    void save(PoolState p, const std::map<uint64_t, Allocation> &a = {}) {
        MetadataStore m(state);
        ASSERT_EQ(m.save(p), 0);
        ASSERT_EQ(m.saveGlobal(a, 43), 0);
    }
    // A whole file, read in one call (fast even for a device); "" if missing.
    std::string bytes(const std::string &path) {
        std::ifstream in(path, std::ios::binary | std::ios::ate);
        if (!in) return {};
        std::string data(static_cast<size_t>(in.tellg()), '\0');
        in.seekg(0).read(data.data(), static_cast<std::streamsize>(data.size()));
        return data;
    }
    // The first n bytes of a device.
    std::string head(uint32_t id, size_t n = 64) {
        std::string data(n, '\0');
        std::ifstream(fakeRoot + dev(id), std::ios::binary)
            .read(data.data(), static_cast<std::streamsize>(n));
        return data;
    }
    int allocate(PoolManager &pm, Handle &h, uint32_t id = 0,
                 const std::string &client = "offline-test-owner") {
        std::string path, uuid;
        uint64_t requested;
        return pm.alloc(H, client, h, path, uuid, dev(id), requested);
    }
    // The client ID of a process on this host; the host name is cut so the
    // pid still fits Allocation::clientId.
    std::string localClient(pid_t pid) {
        char host[256]{};
        EXPECT_EQ(::gethostname(host, sizeof(host)), 0);
        return std::string(host).substr(0, 80) + ":" + std::to_string(pid);
    }
    // An FS_DAX pool over the directory fakeRoot/files: no header, so its data
    // starts at offset zero.
    PoolState fsDaxPool(uint32_t id = 0) {
        auto p = pool(id);
        p.type = DaxType::FS_DAX;
        p.dataBegin = 0;
        p.totalSize = p.freeSize = B;
        p.freeList = {{0, B}};
        p.devPath = fakeRoot + "/files";
        fs::create_directory(p.devPath);
        return p;
    }
    // Rewrite a device as `size` zero bytes, optionally with a fresh header.
    void resetDevice(uint32_t id, bool header, uint64_t size = B) {
        const auto name = "dax" + std::to_string(id) + ".0";
        std::ofstream(fakeRoot + "/sys/class/dax/" + name + "/size") << size;
        fs::remove(fakeRoot + dev(id));
        int fd = ::open(dev(id).c_str(), O_CREAT | O_RDWR, 0600);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(::ftruncate(fd, static_cast<off_t>(size)), 0);
        ::close(fd);
        if (header) {
            DeviceHeader hdr{};
            initDeviceHeader(hdr);
            ASSERT_EQ(writeDeviceHeader(dev(id), hdr, H), 0);
        }
    }
    void resetState() {
        fs::remove_all(state);
        fs::create_directories(state);
        ASSERT_EQ(initSecret(state, false), 0);
    }
    PoolRecord record(uint32_t id = 0) {
        PoolRecord r;
        MetadataStore(state).loadRecord(id, r);
        return r;
    }
    std::vector<std::string> archives(uint32_t id = 0) {
        std::vector<std::string> found;
        const auto prefix = "pool_" + std::to_string(id) + ".meta.reset-";
        for (const auto &entry : fs::directory_iterator(state))
            if (entry.path().filename().string().rfind(prefix, 0) == 0)
                found.push_back(entry.path().string());
        return found;
    }
    // The client ID of a local process that does not exist.
    std::string exitedLocalClient() { return localClient(2147483647); }
    // Startup leaves the device unserved and refused, and changes neither
    // the device, its pool record nor the WAL. Returns the reason.
    std::string expectDeviceRefused(uint32_t id) {
        const auto device = bytes(fakeRoot + dev(id));
        const auto meta = bytes(state + "/pool_" + std::to_string(id) + ".meta");
        const auto wal = bytes(state + "/maru-resource-manager.wal");
        std::string why;
        {
            PoolManager pm(state);
            EXPECT_EQ(pm.loadPools(), 0);
            std::vector<PoolState> stats;
            pm.getStats(stats);
            for (const auto &p : stats) EXPECT_NE(p.devPath, dev(id));
            why = PoolManagerTestPeer::unserved(pm, dev(id));
            EXPECT_EQ(why.rfind("refused", 0), 0u) << why;
        }
        EXPECT_TRUE(bytes(fakeRoot + dev(id)) == device) << dev(id) << " changed";
        EXPECT_EQ(bytes(state + "/pool_" + std::to_string(id) + ".meta"), meta);
        EXPECT_EQ(bytes(state + "/maru-resource-manager.wal"), wal);
        return why;
    }
    // Recovery refuses with rc and changes nothing: no pool is served, no
    // checkpoint is written, and every state file and device keeps its bytes.
    void expectRecoveryRefused(int rc) {
        const auto before = snapshot();
        {
            PoolManager pm(state);
            EXPECT_EQ(pm.loadPools(), rc);
            EXPECT_FALSE(pm.hasPools());
            EXPECT_EQ(pm.checkpoint(), -EAGAIN);
        }
        const auto after = snapshot();
        for (const auto &[path, data] : before) {
            const auto it = after.find(path);
            EXPECT_TRUE(it != after.end()) << path << " removed";
            EXPECT_TRUE(it == after.end() || it->second == data) << path << " changed";
        }
        for (const auto &[path, data] : after)
            EXPECT_TRUE(before.count(path)) << path << " created";
    }
    // Every regular file in the state directory and the fake /dev.
    std::map<std::string, std::string> snapshot() {
        std::map<std::string, std::string> files;
        for (const auto *dir : {"/state", "/dev"})
            for (const auto &entry : fs::directory_iterator(fakeRoot + dir))
                if (entry.is_regular_file())
                    files[entry.path().string()] = bytes(entry.path().string());
        return files;
    }
};
