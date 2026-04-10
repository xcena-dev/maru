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

## Documentation

Architecture docs are in `docs/`:

| Document | Description |
|----------|-------------|
| [1_arch_metadata_layout](docs/1_arch_metadata_layout.md) | CXL memory layout, superblock/shard/RAT/NRHT structs |
| [2_arch_entry_lifecycle](docs/2_arch_entry_lifecycle.md) | State machines for index, RAT, delegation entries |
| [3_arch_gc](docs/3_arch_gc.md) | GC thread: tombstone sweep, dead process reclaim, orphan tracking |
| [4_arch_nrht](docs/4_arch_nrht.md) | NRHT (Name-Ref Hash Table) structure and operations |
| [5_arch_acl](docs/5_arch_acl.md) | Permission model: owner/default_perms/delegation |
| [6_arch_mount_io](docs/6_arch_mount_io.md) | Mount/unmount flow, read/write/mmap I/O paths |
