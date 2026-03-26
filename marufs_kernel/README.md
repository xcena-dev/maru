# marufs kernel module

Linux kernel filesystem module for CXL shared memory. Provides per-region access control via VFS.

## Build & Install

```bash
sudo ./install.sh                              # build + load module
sudo ./install.sh --mount /dev/dax6.0 --format # build + load + format + mount
sudo ./uninstall.sh                            # unmount + unload module
```

## Auto-load on Boot

```bash
sudo ./setup-autoload.sh                     # module auto-load only
sudo ./setup-autoload.sh --mount /dev/dax6.0 # + auto-mount at boot
sudo ./setup-autoload.sh --status            # check current config
sudo ./setup-autoload.sh --uninstall         # remove all config
```

## Tests

Tests require a CXL DAX device.

```bash
# setup → test → teardown
sudo ./tests/setup_local_multinode.sh --teardown
sudo ./tests/setup_local_multinode.sh --device /dev/dax6.0
sudo ./tests/setup_local_multinode.sh --status
sudo ./tests/test_local_multinode.sh --no-cleanup --skip-setup
sudo ./tests/setup_local_multinode.sh --teardown
```

Individual test binaries (built automatically by the test suite):

| Binary | Description |
|--------|-------------|
| `test_ioctl` | Two-phase create, name-ref, permission delegation |
| `test_mmap` | mmap data integrity (single + cross-node) |
| `test_mmap_cuda` | mmap permission + cudaHostRegister (requires CUDA) |
| `test_cross_process` | Cross-process create/truncate/mmap/unlink visibility |
| `test_chown_race` | CHOWN concurrency and race condition tests |
| `test_overlap` | Concurrent ftruncate physical overlap check |
