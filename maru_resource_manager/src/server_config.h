#pragma once

#include <cstdint>
#include <string>

#include "log.h"

namespace maru {

struct ServerConfig {
    std::string host = "127.0.0.1";
    uint16_t port = 9850;
    std::string stateDir = "/var/lib/maru-resourced";
    LogLevel logLevel = LogLevel::Info;
    int numWorkers = 32;
    int gracePeriodSec = 30;
    int maxClients = 256;
};

ServerConfig parseArgs(int argc, char **argv);
void printUsage(const char *prog);
void writeConfigFile(const ServerConfig &cfg);

}  // namespace maru
