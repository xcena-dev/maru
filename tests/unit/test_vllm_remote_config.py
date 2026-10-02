# SPDX-License-Identifier: Apache-2.0
"""maru_storage_backend='remote' at the vLLM connector boundary."""

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

from maru_vllm import connector as conn
from tests.unit.test_vllm_cpu_config import engine_config, extra
from tests.unit.vllm_connector_helpers import make_scheduler, make_worker


def remote_extra(**kwargs):
    return {
        "maru_storage_backend": "remote",
        "maru_remote_url": "tcp://pool:6600",
        "maru_cache_namespace": "model-revision-1",
        **kwargs,
    }


def test_remote_is_a_lease_backend():
    assert conn._validate_storage_config(remote_extra()) is True


@pytest.mark.parametrize("key", ["maru_remote_url", "maru_cache_namespace"])
def test_required_remote_settings(key):
    config = remote_extra()
    del config[key]
    with pytest.raises(ValueError, match=key):
        conn._validate_storage_config(config)


@pytest.mark.parametrize(
    "key",
    ["maru_pool_size", "maru_cpu_pool_size", "maru_cxl_pool_size", "maru_write_order"],
)
def test_local_capacity_settings_do_not_apply(key):
    with pytest.raises(ValueError, match="maru_remote_staging_size"):
        conn._validate_storage_config(remote_extra(**{key: "1G"}))


@pytest.mark.parametrize("backend", ["cxl", "cpu", "mixed"])
def test_remote_settings_require_the_remote_backend(backend):
    config = {**extra(), "maru_storage_backend": backend, "maru_remote_url": "tcp://x"}
    if backend == "mixed":
        config["maru_cxl_pool_size"] = "2M"
    with pytest.raises(ValueError, match="maru_storage_backend='remote'"):
        conn._validate_storage_config(config)


@pytest.mark.parametrize(
    "knob", ["maru_overlap_load_with_compute", "maru_use_layerwise"]
)
def test_remote_stays_chunkwise_without_layer_overlap(knob):
    with pytest.raises(ValueError, match="Remote storage"):
        conn._validate_storage_config(remote_extra(**{knob: True}))


@pytest.mark.parametrize("knob", ["maru_async_load", "maru_async_store"])
def test_remote_accepts_async_loads_and_stores(knob):
    assert conn._validate_storage_config(remote_extra(**{knob: True})) is True


@pytest.mark.parametrize("name", ["maru_remote_timeout_s", "maru_remote_retry_s"])
def test_remote_durations_must_be_positive(name):
    with pytest.raises(ValueError, match=name):
        conn._validate_storage_config(remote_extra(**{name: 0}))


def test_remote_namespace_binding_uses_the_remote_label():
    config = engine_config()
    config.model_config.enforce_eager = False
    with pytest.raises(ValueError, match="Remote storage requires enforce_eager"):
        conn._bind_lease_namespace(remote_extra(), config)


def test_engines_with_equal_geometry_share_one_namespace():
    a = conn._bind_lease_namespace(remote_extra(), engine_config())
    b = conn._bind_lease_namespace(remote_extra(), engine_config())
    assert a["maru_cache_namespace"] == b["maru_cache_namespace"]
    other = engine_config()
    other.cache_config.block_size = 32
    c = conn._bind_lease_namespace(remote_extra(), other)
    assert c["maru_cache_namespace"] != a["maru_cache_namespace"]


def test_handler_creation_maps_remote_settings(monkeypatch):
    seen = {}

    class FakeHandler:
        def __init__(self, cfg):
            seen["cfg"] = cfg

        def connect(self):
            return True

    import maru

    monkeypatch.setattr(maru, "MaruHandler", FakeHandler)
    conn._create_maru_handler(
        remote_extra(
            maru_remote_ucx_device="mlx5_0:1",
            maru_remote_staging_size="64M",
            maru_remote_timeout_s=12,
            maru_remote_retry_s=7,
            maru_chunk_size=3 * 1024 * 1024,
        )
    )
    cfg = seen["cfg"]
    assert cfg.storage_backend == "remote" and cfg.remote_url == "tcp://pool:6600"
    assert cfg.remote_ucx_device == "mlx5_0:1" and cfg.pool_size == 64 * 1024**2
    assert cfg.remote_transfer_timeout_s == 12.0 and cfg.remote_retry_s == 7.0
    assert cfg.chunk_size_bytes == 3 * 1024 * 1024
    assert cfg.cache_namespace == "model-revision-1" and cfg.engine_id is None
    conn._create_maru_handler(remote_extra(), metadata_only=True)
    assert seen["cfg"].metadata_only and seen["cfg"].pool_size == 0


def test_staging_must_hold_one_kv_object():
    worker = make_worker(
        4, 4, remote_extra(maru_remote_staging_size="512"), num_kv_heads=2, head_size=8
    )
    import torch

    caches = {
        f"model.layers.{i}.self_attn": torch.zeros(8, 2, 4, 2, 8) for i in range(2)
    }
    with pytest.raises(ValueError, match="maru_remote_staging_size"):
        worker.register_kv_caches(caches)


def test_scheduler_probes_the_server_every_time():
    scheduler = make_scheduler(4, 4, remote_extra())
    scheduler._known_keys = {"stale"}
    scheduler._handler = type(
        "H", (), {"batch_exists": lambda self, keys: [True, False]}
    )()
    assert scheduler._count_matched_chunks(list(range(8))) == 1


def test_namespace_ignores_where_and_with_which_library_a_node_loaded_weights():
    a, b = engine_config(), engine_config()
    a.model_config.model, b.model_config.model = "/data/models/x", "/mnt/nfs/x"
    a.model_config.hf_config = type(
        "C",
        (),
        {
            "to_dict": lambda self: {
                "heads": 4,
                "_name_or_path": "/data/models/x",
                "transformers_version": "5.11.0",
            }
        },
    )()
    b.model_config.hf_config = type(
        "C",
        (),
        {
            "to_dict": lambda self: {
                "heads": 4,
                "_name_or_path": "/mnt/nfs/x",
                "transformers_version": "5.12.0",
            }
        },
    )()
    assert (
        conn._bind_lease_namespace(remote_extra(), a)["maru_cache_namespace"]
        == conn._bind_lease_namespace(remote_extra(), b)["maru_cache_namespace"]
    )


def test_default_staging_holds_at_least_64_objects():
    assert conn._remote_staging_bytes({}, 3 * 1024**2) == 1024**3
    assert conn._remote_staging_bytes({}, 32 * 1024**2) == 64 * 32 * 1024**2
    assert (
        conn._remote_staging_bytes({"maru_remote_staging_size": "64M"}, 32 * 1024**2)
        == 64 * 1024**2
    )


def test_unreachable_pool_retries_on_the_remote_retry_period():
    assert conn._handler_backoff_s(remote_extra()) == 30.0
    assert conn._handler_backoff_s(remote_extra(maru_remote_retry_s=7)) == 7.0
    assert conn._handler_backoff_s({"maru_storage_backend": "cpu"}) == 5.0
