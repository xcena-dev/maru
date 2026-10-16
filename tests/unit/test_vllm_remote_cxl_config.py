# SPDX-License-Identifier: Apache-2.0
"""Remote CXL storage opt-in at the vLLM connector boundary."""

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import maru
from maru_vllm.connector import (
    _create_maru_handler,
    _rdma_staging_size,
    _validate_storage_config,
)


def extra(**kwargs):
    return {
        "maru_storage_backend": "remote_cxl",
        "maru_pool_size": "16M",
        "maru_engine_id": "worker-0",
        "maru_cache_namespace": "model-revision-1",
        **kwargs,
    }


def test_remote_cxl_is_a_storage_session_backend():
    assert _validate_storage_config(extra()) is True


@pytest.mark.parametrize(
    "key", ["maru_engine_id", "maru_cache_namespace", "maru_pool_size"]
)
def test_required_rdma_settings(key):
    config = extra()
    del config[key]
    with pytest.raises(ValueError, match=key):
        _validate_storage_config(config)


def test_remote_cxl_takes_its_region_size_from_maru_pool_size():
    # The region size, as for the cxl backend; a full worker gets one more.
    with pytest.raises(ValueError, match="maru_pool_size"):
        _validate_storage_config(extra(maru_cpu_pool_size="1G"))


def test_cpu_storage_still_rejects_maru_pool_size():
    config = {
        "maru_storage_backend": "cpu",
        "maru_cpu_pool_size": "1M",
        "maru_engine_id": "e",
        "maru_cache_namespace": "n",
        "maru_pool_size": "1G",
    }
    with pytest.raises(ValueError, match="maru_cpu_pool_size"):
        _validate_storage_config(config)


def test_a_retryable_open_refusal_backs_off_like_a_cxl_handler(monkeypatch):
    import maru_vllm.connector as connector
    from maru_common.storage_types import StorageUnavailableError
    from tests.unit.vllm_connector_helpers import make_worker

    def refuse(config):
        raise StorageUnavailableError("Remote CXL region reservation failed")

    monkeypatch.setattr(connector, "_create_maru_handler", refuse)
    worker = make_worker(16, 256, extra())
    worker._ensure_handler()  # the worker keeps running without the cache
    assert worker._handler is None and worker._handler_retry_after > 0


@pytest.mark.parametrize("knob", ["maru_async_load", "maru_async_store"])
def test_remote_cxl_takes_asynchronous_loads_and_stores(knob):
    # The staging buffer is page-locked for CUDA (design note 3.11).
    assert _validate_storage_config(extra(**{knob: True})) is True


def test_remote_cxl_has_no_load_compute_overlap():
    with pytest.raises(ValueError, match="maru_overlap_load_with_compute"):
        _validate_storage_config(
            extra(maru_async_load=True, maru_overlap_load_with_compute=True)
        )


def test_remote_cxl_stores_chunkwise_only():
    with pytest.raises(ValueError, match="chunkwise"):
        _validate_storage_config(extra(maru_use_layerwise=True))


@pytest.mark.parametrize("knob", ["maru_async_load", "maru_async_store"])
def test_cpu_storage_still_rejects_asynchronous_transfers(knob):
    from tests.unit.test_vllm_cpu_config import extra as cpu_extra

    with pytest.raises(ValueError, match=knob):
        _validate_storage_config({**cpu_extra(), knob: True})


def _engine(async_scheduling):
    from types import SimpleNamespace

    from tests.unit.test_vllm_cpu_config import engine_config

    config = engine_config()
    config.scheduler_config = SimpleNamespace(async_scheduling=async_scheduling)
    return config


def test_sync_remote_cxl_loads_need_vllm_async_scheduling_off():
    from maru_vllm.connector import _cpu_engine_config

    with pytest.raises(ValueError, match="--no-async-scheduling"):
        _cpu_engine_config(extra(), _engine(True))
    assert _cpu_engine_config(extra(), _engine(False))["maru_cache_namespace"]
    # An asynchronous load reports its failure while the request is parked.
    assert _cpu_engine_config(extra(maru_async_load=True), _engine(True))


def test_cpu_storage_keeps_its_async_scheduling_behavior():
    from maru_vllm.connector import _cpu_engine_config
    from tests.unit.test_vllm_cpu_config import extra as cpu_extra

    assert _cpu_engine_config(cpu_extra(), _engine(True))


@pytest.mark.parametrize("backend", ["cxl", "cpu"])
def test_rdma_settings_need_the_remote_cxl_backend(backend):
    config = {
        "maru_storage_backend": backend,
        "maru_cpu_pool_size": "1M",
        "maru_engine_id": "e",
        "maru_cache_namespace": "n",
        "maru_rdma_device": "mlx5_0:1",
    }
    with pytest.raises(ValueError, match="RDMA settings"):
        _validate_storage_config(config)


def test_handler_gets_the_remote_cxl_pool_and_transfer_settings(monkeypatch):
    seen = {}

    class FakeHandler:
        def __init__(self, cfg):
            seen["cfg"] = cfg

        def connect(self):
            return True

    monkeypatch.setattr(maru, "MaruHandler", FakeHandler)
    _create_maru_handler(
        extra(
            maru_rdma_device="mlx5_0:1",
            maru_rdma_staging_size="8M",
            maru_rdma_timeout_s=5,
            maru_chunk_size="1M",
        )
    )
    cfg = seen["cfg"]
    assert cfg.storage_backend == "remote_cxl"
    assert cfg.pool_size == 16 * 1024**2
    assert cfg.rdma_device == "mlx5_0:1"
    assert cfg.rdma_staging_size == 8 * 1024**2
    assert cfg.rdma_timeout_s == 5.0


MIB = 1024**2


def test_default_staging_fits_one_longest_load_and_store():
    # 40 chunks of 64 MiB: one request needs 2.5 GiB, more than the 1 GiB floor.
    assert (
        _rdma_staging_size({"_maru_request_chunks": 40}, 64 * MIB) == 2 * 40 * 64 * MIB
    )
    assert _rdma_staging_size({"_maru_request_chunks": 4}, MIB) == 1024 * MIB
    assert _rdma_staging_size({}, MIB) is None


def test_async_staging_holds_a_request_for_loads_and_one_for_stores():
    config = {
        "maru_storage_backend": "remote_cxl",
        "maru_async_load": True,
        "_maru_request_chunks": 40,
        "maru_rdma_staging_size": "3G",
    }
    with pytest.raises(ValueError, match="one for loads, one for stores"):
        _rdma_staging_size(config, 64 * MIB)
    config["maru_rdma_staging_size"] = "5G"
    assert _rdma_staging_size(config, 64 * MIB) == 5 * 1024 * MIB
    # The default already gives each half one longest request.
    del config["maru_rdma_staging_size"]
    assert _rdma_staging_size(config, 64 * MIB) == 2 * 40 * 64 * MIB


@pytest.mark.parametrize(
    "knobs,reserve",
    [({}, 0.0), ({"maru_async_load": True}, 0.5), ({"maru_async_store": True}, 0.5)],
)
def test_async_transfers_keep_half_the_staging_for_loads(monkeypatch, knobs, reserve):
    seen = {}

    class FakeHandler:
        def __init__(self, cfg):
            seen["cfg"] = cfg

        def connect(self):
            return True

    monkeypatch.setattr(maru, "MaruHandler", FakeHandler)
    _create_maru_handler(extra(maru_chunk_size="1M", **knobs))
    assert seen["cfg"].rdma_load_reserve == reserve
    _create_maru_handler(extra(maru_chunk_size="1M", **knobs), metadata_only=True)
    assert seen["cfg"].rdma_load_reserve == 0.0


def test_explicit_staging_must_hold_one_longest_request():
    config = {"_maru_request_chunks": 40, "maru_rdma_staging_size": "1G"}
    with pytest.raises(ValueError, match="fewer chunks than one max_model_len"):
        _rdma_staging_size(config, 64 * MIB)
    config["maru_rdma_staging_size"] = "3G"
    assert _rdma_staging_size(config, 64 * MIB) == 3 * 1024 * MIB


def test_metadata_only_scheduler_gets_no_staging(monkeypatch):
    seen = {}

    class FakeHandler:
        def __init__(self, cfg):
            seen["cfg"] = cfg

        def connect(self):
            return True

    monkeypatch.setattr(maru, "MaruHandler", FakeHandler)
    _create_maru_handler(
        extra(_maru_request_chunks=4, maru_chunk_size="1M"), metadata_only=True
    )
    assert seen["cfg"].rdma_staging_size is None


def test_worker_sizes_staging_by_its_longest_request(monkeypatch):
    import maru_vllm.connector as connector
    from tests.unit.vllm_connector_helpers import make_worker

    seen = {}
    monkeypatch.setattr(
        connector, "_create_maru_handler", lambda config: seen.update(config)
    )
    worker = make_worker(16, 256, extra(_maru_max_model_len=1000))
    worker._ensure_handler()
    assert seen["_maru_request_chunks"] == 4  # ceil(1000 / 256)


def test_worker_reports_a_staging_misconfiguration_instead_of_retrying(monkeypatch):
    from tests.unit.vllm_connector_helpers import make_worker

    class FakeHandler:
        def __init__(self, cfg):
            pass

        def connect(self):
            return True

    monkeypatch.setattr(maru, "MaruHandler", FakeHandler)
    worker = make_worker(
        16,
        256,
        extra(
            _maru_max_model_len=1000, maru_rdma_staging_size="1M", maru_chunk_size="1M"
        ),
    )
    with pytest.raises(ValueError, match="fewer chunks than one max_model_len"):
        worker._ensure_handler()
