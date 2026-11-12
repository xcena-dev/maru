// Private /dev and /sys for tests: the wrapped stat, open, opendir and
// readlink resolve those paths under fakeRoot, and stat reports /dev/dax*
// files as character devices. Link with ${TEST_SYSCALL_WRAPS} (CMakeLists.txt),
// together with checkpoint_fault.cpp, which wraps the write calls.
#pragma once

#include <string>

// A /dev or /sys path while this is empty aborts: tests never reach a real
// device.
extern std::string fakeRoot;
// If set, sees every path passed to open() first. A nonzero return fails the
// call with -1 and the errno the hook set.
extern int (*fakeOpenHook)(const char *path);
