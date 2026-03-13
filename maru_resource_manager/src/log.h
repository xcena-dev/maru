#pragma once

namespace maru {

// Simple log levels for runtime logging control
enum class LogLevel {
  Error = 0,
  Warn = 1,
  Info = 2,
  Debug = 3,
};

// Runtime control for logging from resource manager components. Call `setLogLevel` at
// startup to enable more verbose output. Use `logf` to emit formatted
// messages; the implementation will only print when the message level is
// <= current log level.
void setLogLevel(LogLevel level);
void logf(LogLevel level, const char *fmt, ...);

LogLevel parseLogLevel(const char *s);
const char *logLevelStr(LogLevel level);

} // namespace maru
