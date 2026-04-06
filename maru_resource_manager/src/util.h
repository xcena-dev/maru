#pragma once

#include <cstdint>
#include <string>
#include <sys/types.h>

#include "types.h"

namespace maru {

int writeFull(int fd, const void *buf, size_t len);
int readFull(int fd, void *buf, size_t len);

uint64_t nowSec();

int initSecret(const std::string &stateDir, bool hasExistingAllocations);
uint64_t computeAuthToken(const Handle &h, uint64_t nonce,
                          const std::string &clientId);
uint64_t generateNonce();

uint32_t crc32(const void *data, size_t len);

// Read process start time from /proc/[pid]/stat (field 22).
// Returns 0 on failure (pid not found or unreadable).
uint64_t getPidStartTime(pid_t pid);

// Directory utilities
bool ensureDirExists(const std::string &path);
std::string parentDir(const std::string &path);

static constexpr uint32_t kAnyPoolId = 0xFFFFFFFFu;

} // namespace maru
