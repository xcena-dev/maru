# SPDX-License-Identifier: Apache-2.0
"""Removed backends identify their replacement store."""

import pytest

from maru_common.config import MaruConfig

BACKENDS = [
    ("cpu", "a dram store"),
    ("mixed", "a dram or local_cxl store"),
    ("remote_cxl", "a remote store"),
]


@pytest.mark.parametrize(("backend", "store"), BACKENDS)
def test_removed_handler_backend_names_replacement(backend, store):
    with pytest.raises(ValueError) as exc:
        MaruConfig(storage_backend=backend)
    assert str(exc.value) == (
        f"storage_backend={backend!r} was removed; use maru_node with {store} "
        "(docs/source/getting_started/quick_start_node.md)"
    )


@pytest.mark.parametrize(("backend", "store"), BACKENDS)
@pytest.mark.parametrize("node", [False, True])
def test_removed_connector_backend_names_replacement(backend, store, node):
    pytest.importorskip("torch")
    pytest.importorskip("vllm")
    from maru_vllm.connector import _validate_storage_config

    extra = {"maru_storage_backend": backend}
    if node:
        extra["maru_node"] = "/unused"
    with pytest.raises(ValueError) as exc:
        _validate_storage_config(extra)
    assert str(exc.value) == (
        f"maru_storage_backend={backend!r} was removed; use maru_node with {store} "
        "(docs/source/getting_started/quick_start_node.md)"
    )
