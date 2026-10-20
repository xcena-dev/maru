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

/// The boot_id= line of <stateDir>/rm.conf, or "" if there is none. Earlier
/// RMs rewrite rm.conf at every start without it, so a value means the last
/// RM started in this state directory recorded that boot.
std::string readBootMarker(const std::string &stateDir);
/// Replace the boot_id= line (remove it if bootId is empty) durably.
int writeBootMarker(const std::string &stateDir, const std::string &bootId);

}  // namespace maru
