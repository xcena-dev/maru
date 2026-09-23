# SPDX-License-Identifier: Apache-2.0
"""Actual CPU-only server process and GPU transfer, without the unit SHM mock."""

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
def cpu_server(server_port, tmp_path):
    root = Path(__file__).resolve().parents[2]
    url = f"tcp://127.0.0.1:{server_port}"
    log_path = tmp_path / "cpu-server.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "maru_server",
                "--cpu-only",
                "--port",
                str(server_port),
                "--rm-address",
                "127.0.0.1:1",
                "--cpu-capacity-limit",
                str(1024 * 1024),
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
                pytest.fail("CPU-only server failed to start: " + log_path.read_text())
            yield url
        finally:
            rpc.close()
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@pytest.mark.parametrize("async_rpc", [False, True])
def test_cpu_handler_without_rm_or_dax(cpu_server, async_rpc, monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("CPU mode attempted DAX/RM initialization")

    monkeypatch.setattr("maru_shm.device_scanner.scan_dax_devices", forbidden)
    monkeypatch.setattr("maru_handler.handler.DaxMapper", forbidden)
    config = {
        "server_url": cpu_server,
        "storage_backend": "cpu",
        "engine_id": "one",
        "cache_namespace": "model-revision-1",
        "pool_size": 128,
        "chunk_size_bytes": 64,
        "use_async_rpc": async_rpc,
    }
    worker = MaruHandler(MaruConfig(**config))
    scheduler = MaruHandler(
        MaruConfig(**{**config, "metadata_only": True, "pool_size": 0})
    )
    other = MaruHandler(MaruConfig(**{**config, "engine_id": "two"}))
    try:
        for h in (worker, scheduler, other):
            assert h.connect()
            assert h._mapper is None
        assert scheduler._cpu.pool is None
        handle = worker.alloc(4)
        handle.buf[:] = b"test"
        assert worker.store("prefix", handle)
        assert scheduler.batch_exists(["prefix", "missing"]) == [True, False]
        assert not other.exists("prefix")
        with worker.retrieve("prefix") as info:
            assert bytes(info.view) == b"test"
        usage = scheduler._rpc.get_usage()
        assert usage.pool_total == 0
        assert usage.cpu_storage["pools"][0]["ready_bytes"] == 4
        assert worker.get_stats()["cpu_storage"]["pools"][0]["replicas"] == 1
        with pytest.raises(RuntimeError, match="metadata-only"):
            scheduler.alloc(1)
        worker.close()
        assert not scheduler.exists("prefix")
    finally:
        worker.close()
        scheduler.close()
        other.close()


def test_metadata_only_cxl_client_does_not_allocate(cpu_server, monkeypatch):
    monkeypatch.setattr(
        "maru_handler.handler.DaxMapper", lambda **_: pytest.fail("DAX mapping")
    )
    handler = MaruHandler(
        MaruConfig(server_url=cpu_server, metadata_only=True, pool_size=0)
    )
    try:
        assert handler.connect()
        assert handler.batch_exists(["missing"]) == [False]
        assert handler._mapper is None
        with pytest.raises(RuntimeError, match="metadata-only"):
            handler.alloc(1)
    finally:
        handler.close()


@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_connector_roundtrip_and_lost_hit_recomputes(cpu_server, device):
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
        "maru_storage_backend": "cpu",
        "maru_server_url": cpu_server,
        "maru_engine_id": "engine",
        "maru_cache_namespace": "test-model-v1",
        "maru_cpu_pool_size": "1K",
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
    attn = make_flash_attn_metadata()
    tokens = list(range(5))  # One full chunk plus one token of compute.
    try:
        worker.register_kv_caches(caches)
        assert worker._handler is not None
        assert worker._handler._mapper is None
        meta = store_metadata(
            token_ids=tokens, block_ids=[0, 1], num_scheduled_tokens=5
        )
        # Reverse callback order exercises completion across all layers.
        for name, tensor in reversed(list(caches.items())):
            worker.save_kv_layer(name, tensor, attn, meta)
        request = SimpleNamespace(request_id="load", prompt_token_ids=tokens)
        assert scheduler.get_num_new_matched_tokens(request, 0) == (4, False)
        assert not scheduler._known_keys
        assert not worker._stored_keys
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
                    block_ids=[2, 3],
                    is_store=False,
                    num_matched_chunks=1,
                )
            ]
        )
        worker.start_load_kv(context, load)
        for name, tensor in caches.items():
            torch.testing.assert_close(tensor[2], originals[name][0], rtol=0, atol=0)
            assert torch.count_nonzero(tensor[0]) == 0
        assert not worker.take_failed_load_blocks()
        assert (
            worker._handler._rpc.get_usage().cpu_storage["pools"][0]["read_leases"] == 0
        )
        # A full prompt hit must leave compute for vLLM (upstream #82).
        request.prompt_token_ids = tokens[:4]
        assert scheduler.get_num_new_matched_tokens(request, 0) == (0, False)
        # Cache fills at one page: admitting another object cannot overwrite it.
        assert worker._handler._cpu.pool.usage()["allocated_bytes"] == 1024
        different = store_metadata(
            token_ids=list(range(10, 15)), block_ids=[0, 1], num_scheduled_tokens=5
        )
        for name, tensor in caches.items():
            worker.save_kv_layer(name, tensor, attn, different)
        assert scheduler._count_matched_chunks(tokens) == 1
        # The worker session disappears after scheduler discovery.
        worker._handler.close()
        worker.start_load_kv(context, load)
        assert worker.take_failed_load_blocks() == {2, 3}
        assert scheduler._count_matched_chunks(tokens) == 0
    finally:
        worker.shutdown()
        if scheduler._handler:
            scheduler._handler.close()
