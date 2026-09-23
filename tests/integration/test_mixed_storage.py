# SPDX-License-Identifier: Apache-2.0
"""Real RM/DAX + metadata server + mixed CPU/CXL GPU transfers.

Uses a small temporary allocation from an already-running RM; never changes
its pool configuration. Skips if no writable DAX device/RM is available.
"""

import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from maru_common.config import MaruConfig
from maru_handler import MaruHandler
from maru_handler.rpc_client import RpcClient

pytestmark = pytest.mark.integration


@pytest.fixture
def mixed_server(server_port, tmp_path):
    from maru_shm import MaruShmClient

    rm_address = os.environ.get("MARU_TEST_RM_ADDRESS", "127.0.0.1:9850")
    rm = MaruShmClient(address=rm_address)
    try:
        pools = rm.stats()
    except Exception as exc:
        pytest.skip(f"Resource manager unavailable: {exc}")
    finally:
        rm.close()
    devices = [
        p
        for p in pools
        if p.free_size >= max(p.align_bytes, 2 * 1024**2)
        and os.access(p.dax_path, os.R_OK | os.W_OK)
    ]
    if not devices:
        pytest.skip("No accessible DAX pool with free capacity")
    root = Path(__file__).resolve().parents[2]
    url = f"tcp://127.0.0.1:{server_port}"
    log_path = tmp_path / "mixed-server.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "maru_server",
                "--host",
                "127.0.0.1",
                "--port",
                str(server_port),
                "--rm-address",
                rm_address,
                "--dax-path",
                devices[0].dax_path,
            ],
            cwd=root,
            env={**os.environ, "PYTHONPATH": str(root)},
            stdout=log,
            stderr=log,
        )
        rpc = RpcClient(url, timeout_ms=100)
        rpc.connect()
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if rpc.handshake().get("success"):
                    break
                if process.poll() is not None:
                    pytest.fail(log_path.read_text())
                time.sleep(0.05)
            else:
                pytest.fail("Mixed server startup failed: " + log_path.read_text())
            yield url
            assert rpc.get_stats().allocation_manager.num_allocations == 0
        finally:
            rpc.close()
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@pytest.mark.parametrize("async_rpc", [False, True])
@pytest.mark.parametrize("order", [("cpu", "cxl"), ("cxl", "cpu")])
def test_mixed_handler_actual_dax_capacity_and_restore(mixed_server, async_rpc, order):
    config = {
        "server_url": mixed_server,
        "storage_backend": "mixed",
        "engine_id": "mixed",
        "cache_namespace": "v1",
        "pool_size": 4096,
        "cxl_pool_size": 4096,
        "chunk_size_bytes": 4096,
        "write_order": order,
        "use_async_rpc": async_rpc,
    }
    worker = MaruHandler(MaruConfig(**config))
    scheduler = MaruHandler(
        MaruConfig(**{**config, "metadata_only": True, "pool_size": 0})
    )
    try:
        assert worker.connect() and scheduler.connect()
        for key in ("first", "second"):
            h = worker.alloc(4)
            h.buf[:] = key[:4].encode()
            assert worker.store(key, h)
        assert scheduler.batch_exists(["first", "second", "missing"]) == [
            True,
            True,
            False,
        ]
        infos = worker.batch_retrieve(["second", "first"])
        try:
            assert [i.location.medium for i in infos] == list(reversed(order))
            assert [bytes(i.view) for i in infos] == [b"seco", b"firs"]
        finally:
            worker.release_retrieved(infos)
        with pytest.raises(MemoryError, match="Both CPU and CXL"):
            worker.alloc(4)
        usage = scheduler._rpc.get_usage()
        assert len(usage.l1_storage["pools"]) == 2
        assert all(
            p["replicas"] == 1 and p["read_leases"] == 0
            for p in usage.l1_storage["pools"]
        )
        assert sum(i.used for i in usage.instances) == 4
        worker.close()
        assert not scheduler._rpc.get_usage().l1_storage["pools"]
        assert scheduler._rpc.get_stats().allocation_manager.num_allocations == 0
    finally:
        worker.close()
        scheduler.close()


@pytest.mark.parametrize("order", [("cpu", "cxl"), ("cxl", "cpu")])
@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_mixed_connector_restores_a_prefix_split_across_both_media(
    mixed_server, order, device
):
    torch = pytest.importorskip("torch")
    pytest.importorskip("vllm")
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    from maru_vllm.connector import MaruConnectorMetadata, MaruReqMeta
    from tests.unit.vllm_connector_helpers import (
        make_flash_attn_metadata,
        make_scheduler,
        make_worker,
        store_metadata,
    )

    extra = {
        "maru_storage_backend": "mixed",
        "maru_server_url": mixed_server,
        "maru_engine_id": "mixed",
        "maru_cache_namespace": "model-v1",
        "maru_cpu_pool_size": "1K",
        "maru_cxl_pool_size": "1K",
        "maru_write_order": list(order),
    }
    worker = make_worker(4, 4, extra, num_kv_heads=2, head_size=8)
    scheduler = make_scheduler(4, 4, extra)
    caches = {
        f"model.layers.{i}.self_attn": torch.arange(
            8 * 2 * 4 * 2 * 8, device=device, dtype=torch.float32
        ).reshape(8, 2, 4, 2, 8)
        + i * 2048
        for i in range(2)
    }
    originals = {name: tensor.clone() for name, tensor in caches.items()}
    tokens = list(range(9))
    attn = make_flash_attn_metadata()
    try:
        worker.register_kv_caches(caches)
        assert worker._handler is not None
        meta = store_metadata(
            token_ids=tokens, block_ids=[0, 1, 2], num_scheduled_tokens=9
        )
        for name, tensor in reversed(list(caches.items())):
            worker.save_kv_layer(name, tensor, attn, meta)
        assert scheduler.get_num_new_matched_tokens(
            SimpleNamespace(request_id="load", prompt_token_ids=tokens), 0
        ) == (8, False)
        before = worker._handler._rpc.get_usage().l1_storage["pools"]
        assert {p["medium"]: p["replicas"] for p in before} == {"cpu": 1, "cxl": 1}
        for tensor in caches.values():
            tensor.zero_()
        context = SimpleNamespace(
            attn_metadata=attn,
            no_compile_layers={
                name: SimpleNamespace(kv_cache=t) for name, t in caches.items()
            },
        )
        load = MaruConnectorMetadata(
            requests=[
                MaruReqMeta(
                    req_id="load",
                    token_ids=tokens,
                    block_ids=[4, 5, 6],
                    is_store=False,
                    num_matched_chunks=2,
                )
            ]
        )
        worker.start_load_kv(context, load)
        for name, tensor in caches.items():
            torch.testing.assert_close(
                tensor[4:6], originals[name][0:2], rtol=0, atol=0
            )
        assert not worker.take_failed_load_blocks()
        after = worker._handler._rpc.get_usage().l1_storage["pools"]
        assert all(p["acquired_objects"] == 1 and p["read_leases"] == 0 for p in after)
        assert not scheduler._known_keys and not worker._stored_keys
    finally:
        worker.shutdown()
        if scheduler._handler:
            scheduler._handler.close()
