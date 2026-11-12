#pragma once

#include <cstddef>
#include <string>

// Test-only syscall interception. The target must be an exact pathname (a
// temporary checkpoint or the WAL), so unrelated I/O passes through.
namespace checkpoint_fault {
enum class Operation { Write, Fsync, Fdatasync };

void arm(const std::string &path, Operation operation, int error,
         size_t bytesBeforeFailure = 0);
// Independent of arm(): fail ftruncate() on path, e.g. a WAL rollback.
void armTruncate(const std::string &path, int error);
void clear();
unsigned failures();
unsigned shortWrites();
} // namespace checkpoint_fault
