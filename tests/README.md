# Tests

## Structure

- `tests/unit/` — Unit tests (mocked dependencies, no external services)
- `tests/integration/` — Integration tests (require running MaruServer + ZMQ)
- `tests/conftest.py` — Shared fixtures including `maru_shm` mock for CXL hardware

## Running

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
