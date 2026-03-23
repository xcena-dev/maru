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
#include "tcp_server.h"
#include "util.h"

static volatile std::sig_atomic_t gStop = 0;
static volatile std::sig_atomic_t gRescan = 0;

static void onSignal(int) { gStop = 1; }
static void onRescan(int) { gRescan = 1; }

struct ServerConfig {
    std::string host = "0.0.0.0";
    uint16_t port = 9850;
    std::string stateDir = "/var/lib/maru-resourced";
    maru::LogLevel logLevel = maru::LogLevel::Info;
    int idleTimeout = 60;  // seconds, 0=disable
    int numWorkers = 32;
};

static void writeConfigFile(const ServerConfig &cfg) {
    std::string confPath = cfg.stateDir + "/rm.conf";
    std::string tmpPath = confPath + ".tmp";

    FILE *fp = std::fopen(tmpPath.c_str(), "w");
    if (!fp) {
        maru::logf(maru::LogLevel::Warn,
                    "failed to write config to %s: %s",
                    confPath.c_str(), std::strerror(errno));
        return;
    }
    std::fprintf(fp, "host=%s\n", cfg.host.c_str());
    std::fprintf(fp, "port=%u\n", cfg.port);
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
        "  -H, --host ADDR           TCP bind address (default: 0.0.0.0)\n"
        "  -p, --port PORT           TCP port (default: 9850)\n"
        "  -d, --state-dir PATH      State directory for WAL/metadata (default: /var/lib/maru-resourced)\n"
        "  -l, --log-level LEVEL     Log level: debug, info, warn, error (default: info)\n"
        "  -t, --idle-timeout SECS   Auto-shutdown after N seconds idle (default: 60, 0=disable)\n"
        "  -w, --num-workers N       Worker thread pool size (default: 32)\n"
        "  -h, --help                Show this help\n",
        prog);
}

static ServerConfig parseArgs(int argc, char **argv) {
    ServerConfig cfg;
    static struct option longOpts[] = {
        {"host",         required_argument, nullptr, 'H'},
        {"port",         required_argument, nullptr, 'p'},
        {"state-dir",    required_argument, nullptr, 'd'},
        {"log-level",    required_argument, nullptr, 'l'},
        {"idle-timeout", required_argument, nullptr, 't'},
        {"num-workers",  required_argument, nullptr, 'w'},
        {"help",         no_argument,       nullptr, 'h'},
        {nullptr, 0, nullptr, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "H:p:d:l:t:w:h", longOpts, nullptr)) != -1) {
        switch (opt) {
        case 'H': cfg.host = optarg; break;
        case 'p': cfg.port = static_cast<uint16_t>(std::atoi(optarg)); break;
        case 'd': cfg.stateDir = optarg; break;
        case 'l': cfg.logLevel = maru::parseLogLevel(optarg); break;
        case 't': cfg.idleTimeout = std::atoi(optarg); break;
        case 'w': cfg.numWorkers = std::atoi(optarg); break;
        case 'h': printUsage(argv[0]); std::exit(0);
        default:  printUsage(argv[0]); std::exit(1);
        }
    }
    return cfg;
}

int main(int argc, char **argv) {
    ServerConfig cfg = parseArgs(argc, argv);
    maru::setLogLevel(cfg.logLevel);

    // Ensure state directory exists
    maru::ensureDirExists(cfg.stateDir);

    // Persist config for reference
    writeConfigFile(cfg);

    // Startup banner
    maru::logf(maru::LogLevel::Info, "maru-resource-manager starting");
    maru::logf(maru::LogLevel::Info, "  listen     : %s:%u", cfg.host.c_str(), cfg.port);
    maru::logf(maru::LogLevel::Info, "  state dir  : %s", cfg.stateDir.c_str());
    maru::logf(maru::LogLevel::Info, "  log level  : %s", maru::logLevelStr(cfg.logLevel));
    maru::logf(maru::LogLevel::Info, "  idle timeout: %ds%s", cfg.idleTimeout,
               cfg.idleTimeout == 0 ? " (disabled)" : "");
    maru::logf(maru::LogLevel::Info, "  workers    : %d", cfg.numWorkers);

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

    maru::TcpServer server(pm, cfg.host, cfg.port, cfg.numWorkers);
    rc = server.start();
    if (rc != 0) {
        maru::logf(maru::LogLevel::Error,
                    "failed to start server: %d", rc);
        return 1;
    }

    maru::Reaper reaper(pm);
    reaper.start();

    maru::logf(maru::LogLevel::Info, "ready — listening on %s:%u",
               cfg.host.c_str(), cfg.port);

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
