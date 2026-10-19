// Per-device recovery: what a device holds (DeviceProbe), what the RM will
// do with it (DevicePlan), and the per-device operation slots. Shared by
// pool_manager.cpp, pool_recovery.cpp and the test peer.
#pragma once

#include <array>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "device_header.h"
#include "metadata.h"
#include "pool_manager.h"

namespace maru {

using Uuid = std::array<uint8_t, 16>;

/// Read-only facts about one device. Gathered without the pool lock.
struct DeviceProbe
{
    uint32_t poolId{0};
    std::string devPath;
    DaxType type{DaxType::DEV_DAX};
    int rc{0};          // 0, or -errno: the device is skipped this time
    std::string stage;  // What failed when rc != 0
    uint64_t size{0};
    uint64_t align{0};
    uint64_t dataBegin{0};
    uint64_t dataEnd{0};
    // DEV_DAX only
    enum class Header { None, Ours, Roof, BadVersion };
    Header header{Header::None};
    Uuid headerUuid{};
    TagState tags{TagState::Absent};
    Uuid tagUuid{};

    /// True if the device's header and tags still read as they did.
    bool sameIdentity(const DeviceProbe &other) const;
};

/// Probe one device: size, alignment, and for DEV_DAX the header and identity
/// tags. defaultAlign applies when the device exposes no alignment.
DeviceProbe probeDeviceIo(uint32_t poolId, const std::string &devPath, DaxType type,
                          uint64_t defaultAlign);

/// What recovery does with one device.
struct DevicePlan
{
    DeviceProbe probe;
    enum class Action { Serve, Reset, Refuse, Skip };
    Action action{Action::Skip};
    std::string reason;
    Uuid uuid{};            // Identity the pool is served with (DEV_DAX)
    bool newUuid{false};    // Generate the identity when the plan runs
    bool timedOut{false};   // The device did not answer; its slot stays held
    bool writeHeader{false};
    bool writeTags{false};
    bool writeRecord{false};
    bool archive{false};    // Keep a copy of the old record (reset)
    std::vector<uint64_t> frees;  // Ledger entries of exited owners (reset)
    size_t live{0};
    size_t ownersAlive{0};
    std::string archived;   // Where a reset kept the previous record
};

/// "hostname:pid" names a process on this host.
bool isLocalClientId(const char *clientId);
/// Last path component.
std::string baseNameOf(const std::string &path);

/// One operation per device at a time. A slot whose holder timed out stays
/// held until that thread returns.
class DeviceSlots
{
public:
    bool tryAcquire(const std::string &path)
    {
        std::lock_guard<std::mutex> lock(mu_);
        return held_.emplace(path, false).second;
    }
    void release(const std::string &path)
    {
        std::lock_guard<std::mutex> lock(mu_);
        held_.erase(path);
    }
    bool isHeld(const std::string &path)
    {
        std::lock_guard<std::mutex> lock(mu_);
        return held_.count(path) != 0;
    }
    bool heldByTimedOutRead(const std::string &path)
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = held_.find(path);
        return it != held_.end() && it->second;
    }
    void markTimedOut(const std::string &path)
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = held_.find(path);
        if (it != held_.end()) it->second = true;
    }

private:
    std::mutex mu_;
    std::map<std::string, bool> held_;
};

/// Device I/O on a detached thread, started on construction. wait() must be
/// called once. If the deadline passes first, the slot for path stays held
/// until fn returns, then is released, and the result is dropped. A timed-out
/// read lets exclude go ahead; a timed-out write keeps the device busy, since
/// the write can still land. fn must not touch PoolManager state.
template <typename T>
class DeviceIo
{
public:
    DeviceIo(std::shared_ptr<DeviceSlots> slots, std::string path, bool writes,
             std::function<T()> fn)
        : state_(std::make_shared<State>()), slots_(std::move(slots)), path_(std::move(path)),
          writes_(writes)
    {
        std::thread([state = state_, slots = slots_, path = path_, fn]() {
            T value = fn();
            bool abandoned = false;
            {
                std::lock_guard<std::mutex> lock(state->mu);
                state->value = std::move(value);
                state->done = true;
                abandoned = state->abandoned;
            }
            state->cv.notify_all();
            if (abandoned) slots->release(path);
        }).detach();
    }

    bool wait(std::chrono::steady_clock::time_point deadline, T &out)
    {
        std::unique_lock<std::mutex> lock(state_->mu);
        if (!state_->cv.wait_until(lock, deadline, [&] { return state_->done; }))
        {
            state_->abandoned = true;
            if (!writes_) slots_->markTimedOut(path_);
            return false;
        }
        out = std::move(state_->value);
        return true;
    }

private:
    struct State
    {
        std::mutex mu;
        std::condition_variable cv;
        bool done{false};
        bool abandoned{false};
        T value{};
    };
    std::shared_ptr<State> state_;
    std::shared_ptr<DeviceSlots> slots_;
    std::string path_;
    bool writes_;
};

/// One DeviceIo, waited for up to timeoutMs.
template <typename T>
bool runDeviceIo(const std::shared_ptr<DeviceSlots> &slots, const std::string &path,
                 bool writes, int timeoutMs, std::function<T()> fn, T &out)
{
    DeviceIo<T> io(slots, path, writes, std::move(fn));
    return io.wait(std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs), out);
}

}  // namespace maru
