#include "server_config.h"

#include <getopt.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace maru {

void writeConfigFile(const ServerConfig &cfg) {
    std::string confPath = cfg.stateDir + "/rm.conf";
    std::string tmpPath = confPath + ".tmp";

    FILE *fp = std::fopen(tmpPath.c_str(), "w");
    if (!fp) {
        logf(LogLevel::Warn,
             "failed to write config to %s: %s",
             confPath.c_str(), std::strerror(errno));
        return;
    }
    std::fprintf(fp, "host=%s\n", cfg.host.c_str());
    std::fprintf(fp, "port=%u\n", cfg.port);
    std::fprintf(fp, "log_level=%s\n", logLevelStr(cfg.logLevel));
    bool writeOk = (std::ferror(fp) == 0);
    std::fclose(fp);

    if (!writeOk) {
        logf(LogLevel::Warn,
             "write error for config %s, not applying",
             tmpPath.c_str());
        std::remove(tmpPath.c_str());
        return;
    }

    if (std::rename(tmpPath.c_str(), confPath.c_str()) != 0) {
        logf(LogLevel::Warn,
             "failed to rename config %s -> %s: %s",
             tmpPath.c_str(), confPath.c_str(), std::strerror(errno));
    }
}

void printUsage(const char *prog) {
    std::fprintf(stderr,
        "Usage: %s [OPTIONS]\n\n"
        "Maru Resource Manager — CXL/DAX shared memory pool server.\n\n"
        "Options:\n"
        "  -H, --host ADDR           TCP bind address (default: 127.0.0.1)\n"
        "  -p, --port PORT           TCP port (default: 9850)\n"
        "  -d, --state-dir PATH      State directory for WAL/metadata (default: /var/lib/maru-resourced)\n"
        "  -l, --log-level LEVEL     Log level: debug, info, warn, error (default: info)\n"
        "  -w, --num-workers N       Worker thread pool size (default: 32)\n"
        "  -g, --grace-period SEC    Disconnect grace period in seconds (default: 30)\n"
        "  -m, --max-clients N       Maximum concurrent client connections (default: 256)\n"
        "  -h, --help                Show this help\n",
        prog);
}

ServerConfig parseArgs(int argc, char **argv) {
    ServerConfig cfg;
    static struct option longOpts[] = {
        {"host",         required_argument, nullptr, 'H'},
        {"port",         required_argument, nullptr, 'p'},
        {"state-dir",    required_argument, nullptr, 'd'},
        {"log-level",    required_argument, nullptr, 'l'},
        {"num-workers",  required_argument, nullptr, 'w'},
        {"grace-period", required_argument, nullptr, 'g'},
        {"max-clients",  required_argument, nullptr, 'm'},
        {"help",         no_argument,       nullptr, 'h'},
        {nullptr, 0, nullptr, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "H:p:d:l:w:g:m:h", longOpts, nullptr)) != -1) {
        switch (opt) {
        case 'H': cfg.host = optarg; break;
        case 'p': {
            int p = std::atoi(optarg);
            if (p <= 0 || p > 65535) {
                std::fprintf(stderr, "invalid port: %s (must be 1-65535)\n", optarg);
                std::exit(1);
            }
            cfg.port = static_cast<uint16_t>(p);
            break;
        }
        case 'd': cfg.stateDir = optarg; break;
        case 'l': cfg.logLevel = parseLogLevel(optarg); break;
        case 'w': {
            int w = std::atoi(optarg);
            if (w <= 0) {
                std::fprintf(stderr, "invalid num-workers: %s (must be >= 1)\n", optarg);
                std::exit(1);
            }
            cfg.numWorkers = w;
            break;
        }
        case 'g': {
            int g = std::atoi(optarg);
            if (g < 0) {
                std::fprintf(stderr, "invalid grace-period: %s (must be >= 0)\n", optarg);
                std::exit(1);
            }
            cfg.gracePeriodSec = g;
            break;
        }
        case 'm': {
            int m = std::atoi(optarg);
            if (m <= 0) {
                std::fprintf(stderr, "invalid max-clients: %s (must be >= 1)\n", optarg);
                std::exit(1);
            }
            cfg.maxClients = m;
            break;
        }
        case 'h': printUsage(argv[0]); std::exit(0);
        default:  printUsage(argv[0]); std::exit(1);
        }
    }
    return cfg;
}

}  // namespace maru
