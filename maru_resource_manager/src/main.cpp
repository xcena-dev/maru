#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <thread>

#include "log.h"
#include "pool_manager.h"
#include "reaper.h"
#include "server_config.h"
#include "tcp_server.h"
#include "util.h"

static volatile std::sig_atomic_t gStop = 0;
static volatile std::sig_atomic_t gRescan = 0;

static void onSignal(int) { gStop = 1; }
static void onRescan(int) { gRescan = 1; }

int main(int argc, char **argv) {
    maru::ServerConfig cfg = maru::parseArgs(argc, argv);
    maru::setLogLevel(cfg.logLevel);

    // Ensure state directory exists
    maru::ensureDirExists(cfg.stateDir);

    // Persist config for reference
    maru::writeConfigFile(cfg);

    // Startup banner
    maru::logf(maru::LogLevel::Info, "maru-resource-manager starting");
    maru::logf(maru::LogLevel::Info, "  listen     : %s:%u", cfg.host.c_str(), cfg.port);
    maru::logf(maru::LogLevel::Info, "  state dir  : %s", cfg.stateDir.c_str());
    maru::logf(maru::LogLevel::Info, "  log level  : %s", maru::logLevelStr(cfg.logLevel));
    maru::logf(maru::LogLevel::Info, "  workers    : %d", cfg.numWorkers);
    maru::logf(maru::LogLevel::Info, "  grace period: %ds", cfg.gracePeriodSec);
    maru::logf(maru::LogLevel::Info, "  max clients : %d", cfg.maxClients);

    // Signal handlers
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::signal(SIGHUP, onRescan);
    std::signal(SIGPIPE, SIG_IGN);

    // Initialize components with explicit config injection
    maru::PoolManager pm(cfg.stateDir, cfg.gracePeriodSec);
    int rc = pm.loadPools();
    if (rc != 0) {
        maru::logf(maru::LogLevel::Error, "loadPools failed (rc=%d)", rc);
    }
    // loadPools returns 0 even when no devices found (scanDevices succeeds
    // with empty list). Check actual pool state for auto-rescan decision.
    bool needsRescan = !pm.hasPools();
    if (needsRescan) {
        maru::logf(maru::LogLevel::Warn,
                    "no CXL/DAX devices found — starting with empty pool");
    }

    maru::TcpServer server(pm, cfg.host, cfg.port, cfg.numWorkers, cfg.maxClients);
    rc = server.start();
    if (rc != 0) {
        if (rc == -EADDRINUSE) {
            maru::logf(maru::LogLevel::Error,
                        "port %u is already in use — "
                        "another maru-resource-manager may be running. "
                        "Use --port to specify a different port.",
                        cfg.port);
        } else {
            maru::logf(maru::LogLevel::Error,
                        "failed to start server on %s:%u: %s",
                        cfg.host.c_str(), cfg.port, std::strerror(-rc));
        }
        return 1;
    }

    maru::Reaper reaper(pm);
    reaper.start();

    maru::logf(maru::LogLevel::Info, "ready — listening on %s:%u",
               cfg.host.c_str(), cfg.port);

    // Main loop — runs until SIGINT/SIGTERM
    auto lastRescan = std::chrono::steady_clock::now();
    while (!gStop) {
        if (gRescan) {
            gRescan = 0;
            pm.rescanDevices();
            lastRescan = std::chrono::steady_clock::now();
        }

        if (needsRescan) {
            auto now = std::chrono::steady_clock::now();
            if (now - lastRescan > std::chrono::seconds(10)) {
                maru::logf(maru::LogLevel::Info,
                           "pools empty, rescanning for CXL/DAX devices...");
                int ret = pm.rescanIfEmpty();
                lastRescan = now;
                if (ret > 0) {
                    maru::logf(maru::LogLevel::Info,
                               "CXL/DAX devices found, auto-rescan complete");
                    needsRescan = false;
                } else if (ret < 0) {
                    maru::logf(maru::LogLevel::Warn,
                               "auto-rescan failed (rc=%d), will retry", ret);
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Graceful shutdown
    maru::logf(maru::LogLevel::Info, "shutting down...");
    reaper.stop();
    server.stop();
    pm.checkpoint();
    maru::logf(maru::LogLevel::Info, "shutdown complete");
    return 0;
}
