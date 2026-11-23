# SPDX-License-Identifier: Apache-2.0
"""Connector settings and GPU ownership for direct worker reads."""

from types import SimpleNamespace

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch

import maru
import maru_vllm.connector as connector
from tests.unit.test_vllm_node import node_extra, node_vllm_config, pinning_worker
from tests.unit.test_worker_rdma import PAGE
from tests.unit.test_worker_rdma import env as env


@pytest.mark.parametrize(
    "knob", ["maru_use_layerwise", "maru_overlap_load_with_compute"]
)
@pytest.mark.parametrize("role", ["worker", "scheduler"])
def test_remote_attach_rejects_unsupported_load_modes_at_startup(knob, role):
    handler = SimpleNamespace(
        _cpu=SimpleNamespace(
            attach_info=SimpleNamespace(capabilities=["worker_rdma_v1"])
        ),
        wait_attached=lambda _: True,
        wait_connected=lambda _: True,
        connected=True,
    )
    with pytest.raises(ValueError, match="layerwise or overlap"):
        connector._wait_node_handler(handler, role, extra_config={knob: True})


def test_local_attach_keeps_layerwise_and_overlap_modes():
    handler = SimpleNamespace(
        _cpu=SimpleNamespace(attach_info=SimpleNamespace(capabilities=[]))
    )
    connector._check_node_modes(
        handler, {"maru_use_layerwise": True, "maru_overlap_load_with_compute": True}
    )


def test_node_worker_uses_the_vllm_instance_and_its_staging_settings(monkeypatch):
    made = []

    def handler(config):
        made.append(config)
        return SimpleNamespace(connect=lambda **_: None)

    monkeypatch.setattr(maru, "MaruHandler", handler)
    config = node_vllm_config(2)
    config.instance_id = "vllm-instance"
    extra = connector._node_engine_config(
        node_extra(
            maru_rdma_staging_size="64M",
            maru_rdma_device="test-device",
            maru_rdma_timeout_s=0.1,
            maru_async_load=True,
        ),
        config,
    )
    for rank in (0, 1):
        connector._create_node_handler({**extra, "_maru_tp_rank": rank}, False, False)
    assert made[0].instance_id == made[1].instance_id == config.instance_id
    assert all(c.rdma_staging_size == 64 << 20 for c in made)
    assert all(c.rdma_device == "test-device" and c.rdma_timeout_s == 0.1 for c in made)
    assert made[0].tp_rank != made[1].tp_rank


def test_node_slot_admission_never_retries_or_sleeps(monkeypatch):
    worker = pinning_worker()
    worker._handler.retrieve_capacity.return_value = 0
    monkeypatch.setattr(connector.time, "sleep", lambda _: pytest.fail("slot wait"))
    assert worker._leased_batch_size(worker._handler, 8, True) == 0
    worker._handler.retrieve_capacity.assert_called_once_with(8)


def test_cuda_stop_freezes_the_node_client():
    from tests.unit.vllm_connector_helpers import cuda_error

    worker = pinning_worker()
    assert worker._stop_if_unconfirmed(cuda_error())
    worker._handler.stop.assert_called_once_with()
    worker._handler.release_retrieved.assert_not_called()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA required")
def test_vllm_direct_slot_waits_for_the_h2d_completion(env):
    request = env.start()
    env.reader.progress()
    read = request.result()
    host = torch.frombuffer(read.view, dtype=torch.uint8)
    refs = [read, host]
    stream = torch.cuda.Stream()
    device = torch.empty(PAGE, dtype=torch.uint8, device="cuda")
    with torch.cuda.stream(stream):
        device.copy_(host, non_blocking=True)
        copied = stream.record_event()
    assert refs[0] is read and env.reader.capacity() == 3
    copied.synchronize()
    assert torch.equal(device.cpu(), torch.full((PAGE,), ord("a"), dtype=torch.uint8))
    read.release()
    assert env.reader.capacity() == 4
