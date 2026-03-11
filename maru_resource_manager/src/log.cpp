#include "log.h"

#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <cstring>

namespace maru {

static std::atomic<int> gLogLevel{0};

void setLogLevel(LogLevel level) { gLogLevel.store(static_cast<int>(level)); }

void logf(LogLevel level, const char *fmt, ...) {
  if (static_cast<int>(level) > gLogLevel.load()) {
    return;
  }
  va_list ap;
  va_start(ap, fmt);
  std::vfprintf(stderr, fmt, ap);
  std::fprintf(stderr, "\n");
  va_end(ap);
}

LogLevel parseLogLevel(const char *s) {
  if (std::strcmp(s, "debug") == 0) return LogLevel::Debug;
  if (std::strcmp(s, "info") == 0)  return LogLevel::Info;
  if (std::strcmp(s, "warn") == 0)  return LogLevel::Warn;
  if (std::strcmp(s, "error") == 0) return LogLevel::Error;
  std::fprintf(stderr, "unknown log level: %s (using info)\n", s);
  return LogLevel::Info;
}

const char *logLevelStr(LogLevel level) {
  switch (level) {
  case LogLevel::Debug: return "debug";
  case LogLevel::Info:  return "info";
  case LogLevel::Warn:  return "warn";
  case LogLevel::Error: return "error";
  default: return "unknown";
  }
}

} // namespace maru
