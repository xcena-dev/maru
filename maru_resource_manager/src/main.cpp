#include <getopt.h>
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "log.h"
#include "pool_manager.h"
#include "reaper.h"
#include "uds_server.h"
#include "util.h"

static volatile std::sig_atomic_t gStop = 0;
static volatile std::sig_atomic_t gRescan = 0;

static void onSignal(int) { gStop = 1; }
static void onRescan(int) { gRescan = 1; }

struct ServerConfig {
    std::string socketPath = "/tmp/maru-resourced/maru-resourced.sock";
    std::string stateDir = "/var/lib/maru-resourced";
    maru::LogLevel logLevel = maru::LogLevel::Info;
    int idleTimeout = 60;  // seconds, 0=disable
};

static void writeConfigFile(const ServerConfig &cfg) {
    std::string confDir = maru::parentDir(cfg.socketPath);
    std::string confPath = confDir + "/rm.conf";
    std::string tmpPath = confPath + ".tmp";

    FILE *fp = std::fopen(tmpPath.c_str(), "w");
    if (!fp) {
        maru::logf(maru::LogLevel::Warn,
                    "failed to write config to %s: %s",
                    confPath.c_str(), std::strerror(errno));
        return;
    }
    std::fprintf(fp, "state_dir=%s\n", cfg.stateDir.c_str());
    std::fprintf(fp, "log_level=%s\n", maru::logLevelStr(cfg.logLevel));
    std::fprintf(fp, "idle_timeout=%d\n", cfg.idleTimeout);
    bool writeOk = (std::ferror(fp) == 0);
    std::fclose(fp);

    if (!writeOk) {
        maru::logf(maru::LogLevel::Warn,
                    "write error for config %s, not applying",
                    tmpPath.c_str());
        std::remove(tmpPath.c_str());
        return;
    }

    if (std::rename(tmpPath.c_str(), confPath.c_str()) != 0) {
        maru::logf(maru::LogLevel::Warn,
                    "failed to rename config %s -> %s: %s",
                    tmpPath.c_str(), confPath.c_str(), std::strerror(errno));
    }
}

static void printUsage(const char *prog) {
    std::fprintf(stderr,
        "Usage: %s [OPTIONS]\n\n"
        "Maru Resource Manager — CXL/DAX shared memory pool server.\n\n"
        "Options:\n"
        "  -s, --socket-path PATH    UDS socket path (default: /tmp/maru-resourced/maru-resourced.sock)\n"
        "  -d, --state-dir PATH      State directory for WAL/metadata (default: /var/lib/maru-resourced)\n"
        "  -l, --log-level LEVEL     Log level: debug, info, warn, error (default: info)\n"
        "  -t, --idle-timeout SECS   Auto-shutdown after N seconds idle (default: 60, 0=disable)\n"
        "  -h, --help                Show this help\n",
        prog);
}

static ServerConfig parseArgs(int argc, char **argv) {
    ServerConfig cfg;
    static struct option longOpts[] = {
        {"socket-path",  required_argument, nullptr, 's'},
        {"state-dir",    required_argument, nullptr, 'd'},
        {"log-level",    required_argument, nullptr, 'l'},
        {"idle-timeout", required_argument, nullptr, 't'},
        {"help",         no_argument,       nullptr, 'h'},
        {nullptr, 0, nullptr, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "s:d:l:t:h", longOpts, nullptr)) != -1) {
        switch (opt) {
        case 's': cfg.socketPath = optarg; break;
        case 'd': cfg.stateDir = optarg; break;
        case 'l': cfg.logLevel = maru::parseLogLevel(optarg); break;
        case 't': cfg.idleTimeout = std::atoi(optarg); break;
        case 'h': printUsage(argv[0]); std::exit(0);
        default:  printUsage(argv[0]); std::exit(1);
        }
    }
    return cfg;
}

int main(int argc, char **argv) {
    ServerConfig cfg = parseArgs(argc, argv);
    maru::setLogLevel(cfg.logLevel);

    // Ensure directories exist
    maru::ensureDirExists(maru::parentDir(cfg.socketPath));
    maru::ensureDirExists(cfg.stateDir);

    // Persist config so auto-start can recover the same settings
    writeConfigFile(cfg);

    // Startup banner
    maru::logf(maru::LogLevel::Info, "maru-resource-manager starting");
    maru::logf(maru::LogLevel::Info, "  socket     : %s", cfg.socketPath.c_str());
    maru::logf(maru::LogLevel::Info, "  state dir  : %s", cfg.stateDir.c_str());
    maru::logf(maru::LogLevel::Info, "  log level  : %s", maru::logLevelStr(cfg.logLevel));
    maru::logf(maru::LogLevel::Info, "  idle timeout: %ds%s", cfg.idleTimeout,
               cfg.idleTimeout == 0 ? " (disabled)" : "");

    // Signal handlers
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::signal(SIGHUP, onRescan);

    // Initialize components with explicit config injection
    maru::PoolManager pm(cfg.stateDir);
    int rc = pm.loadPools();
    if (rc != 0) {
        maru::logf(maru::LogLevel::Warn,
                    "no CXL/DAX devices found — starting with empty pool");
    }

    maru::UdsServer server(pm, cfg.socketPath);
    rc = server.start();
    if (rc != 0) {
        maru::logf(maru::LogLevel::Error,
                    "failed to start server: %d", rc);
        return 1;
    }

    maru::Reaper reaper(pm);
    reaper.start();

    maru::logf(maru::LogLevel::Info, "ready — listening on %s", cfg.socketPath.c_str());

    // Main loop with idle timeout
    int idleSeconds = 0;
    while (!gStop) {
        if (gRescan) {
            gRescan = 0;
            pm.rescanDevices();
        }

        // Idle timeout check
        if (cfg.idleTimeout > 0) {
            if (pm.allocationCount() == 0 && pm.registeredServerCount() == 0) {
                ++idleSeconds;
            } else {
                idleSeconds = 0;
            }
            if (idleSeconds >= cfg.idleTimeout) {
                maru::logf(maru::LogLevel::Info,
                           "no active allocations for %ds, shutting down",
                           cfg.idleTimeout);
                break;
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
