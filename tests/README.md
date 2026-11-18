# Tests

## Structure

- `tests/unit/` — Unit tests (mocked dependencies, no external services)
- `tests/integration/` — Integration tests (require running MaruServer + ZMQ)
- `tests/conftest.py` — Shared fixtures including `maru_shm` mock for CXL hardware

## Running

CI runs the core unit tests on Python 3.12, 3.13, and 3.14. Each version uploads its own coverage report; Python 3.12 retains the `coverage-report` artifact name for PR comments, while newer versions use a version suffix. Integration tests are excluded, and connector tests requiring optional dependencies may be skipped.

To reproduce a CI test run locally, choose a version from the matrix:

```bash
uv venv --python 3.13 .venv-3.13
uv pip install --python .venv-3.13/bin/python -e ".[dev]"
.venv-3.13/bin/python -m pytest -m "not integration"
```

SSD tests share a filesystem probe that checks alignment reporting,
preallocation, and an actual `O_DIRECT` write/read roundtrip. By default they
try pytest's temporary directory, `/data`, and `/var/tmp`, and skip if none
supports these operations. To require SSD coverage on a particular filesystem:

```bash
mkdir -p /path/on/local-disk/maru-tests
export MARU_TEST_SSD_ROOT=/path/on/local-disk/maru-tests
PYTHONPATH="$PWD" python tests/ssd_test_storage.py "$MARU_TEST_SSD_ROOT"
pytest tests/unit/test_ms_ssd_driver.py tests/unit/test_ms_ssd_tier.py
```

An explicitly configured root must pass the probe; it never falls back or skips.
CI uses the host-mounted `RUNNER_TEMP` directory and runs the probe before the
suite. Linux 6.1+ can report alignment through `statx`; older kernels use XFS
`DIOINFO` or conservative filesystem-block alignment for ordinary ext4 files.
The ext4 fallback rejects encrypted, verity, inline-data and data-journaled
files instead of accepting their buffered-I/O fallback. Unknown filesystems
still fail. If the probe fails, inspect the logged host kernel and filesystem;
changing the container image does not change its host kernel. Each test uses
its own temporary pool directory under the verified root.

The maintenance latency benchmark runs separately, without coverage and on an
otherwise idle host: `python -m pytest benchmarks/test_ms_maintenance.py -q`.
It retains the 10 ms p99 increase limit. The unit suite checks the corresponding
concurrency guarantees deterministically: bounded cleanup batches release the
lock and allow real lookups to complete between them.

```bash
# Unit tests only
pytest -v tests/ -m "not integration"

# Integration tests only
pytest -v tests/ -m integration

# All tests
pytest -v tests/

# With coverage
pytest tests/ --cov=maru --cov=maru_common --cov=maru_handler --cov=maru_server --cov=maru_shm --cov-report=html
```

## Test Coverage by Module

### maru_common

| File | Target | Description |
|------|--------|-------------|
| `test_protocol.py` | `MessageType`, `MessageHeader` | Protocol constants, header pack/unpack, message dataclasses |
| `test_serializer.py` | `Serializer` | MessagePack encoding/decoding, dataclass conversion |
| `test_logging_setup.py` | `setup_package_logging` | Log levels, handlers, `MARU_LOG_LEVEL` precedence |

### maru_shm

| File | Target | Description |
|------|--------|-------------|
| `test_maru_shm.py` | `MaruHandle`, `MaruPoolInfo` | Type pack/unpack, from_dict, validation |
| `test_shm_client.py` | `MaruShmClient` | UDS-based alloc/free, error handling |

### maru_server

| File | Target | Description |
|------|--------|-------------|
| `test_allocation_manager.py` | `AllocationManager` | Alloc/free, KV ref counting, client disconnect |
| `test_kv_manager.py` | `KVManager` | KV register/lookup/delete |
| `test_batch_operations.py` | `MaruServer` batch | Batch register/lookup/exists |
| `test_maru_server.py` | `MaruServer` | Edge cases and error paths |
| `test_server.py` | `MaruServer` | Core server logic |

### maru_handler

| File | Target | Description |
|------|--------|-------------|
| `test_maru_handler.py` | `MaruHandler` | Config, connect, store/retrieve, batch ops |
| `test_mapper.py` | `DaxMapper` | map/unmap, idempotency, query, close |
| `test_owned_region_manager.py` | `OwnedRegionManager` | Add/allocate/free, region scan, stats |
| `test_allocator.py` | `PagedMemoryAllocator` | Page alloc/free, boundary conditions |
| `test_memory_types.py` | `MappedRegion`, `AllocHandle`, `MemoryInfo` | Type construction and buffer views |
| `test_thread_safety.py` | `MaruHandler` | Concurrent retrieve, store/retrieve interleaving |
| `test_rpc_client.py` | `RpcClient` | Sync RPC request/response |
| `test_rpc_async_client.py` | `RpcAsyncClient` | Async RPC with DEALER-ROUTER |
| `test_rpc_server.py` | `RpcServer` + `RpcClient` | Server-client integration (unit-level) |

### integration/

| File | Target | Description |
|------|--------|-------------|
| `test_handler.py` | `MaruHandler` end-to-end | Store/retrieve round-trip, batch ops, cross-instance sharing |
| `test_rpc_async.py` | Async RPC | Connection, alloc, KV CRUD, batch, concurrency |
| `test_rpc_sync.py` | Sync RPC | Connection, alloc, KV CRUD, batch |
