# SPDX-License-Identifier: Apache-2.0
"""Removed backend settings point to the node migration guide."""

import pytest

from maru_common.config import MaruConfig


@pytest.mark.parametrize("backend", ["cpu", "mixed", "remote_cxl"])
def test_removed_handler_backend_names_the_migration_guide(backend):
    with pytest.raises(
        ValueError, match="docs/source/getting_started/migrating_to_maru_node.md"
    ):
        MaruConfig(storage_backend=backend)


@pytest.mark.parametrize("backend", ["cpu", "mixed", "remote_cxl"])
@pytest.mark.parametrize("node", [False, True])
def test_removed_connector_backend_names_the_migration_guide(backend, node):
    pytest.importorskip("torch")
    pytest.importorskip("vllm")
    from maru_vllm.connector import _validate_storage_config

    extra = {"maru_storage_backend": backend}
    if node:
        extra["maru_node"] = "/unused"
    with pytest.raises(
        ValueError, match="docs/source/getting_started/migrating_to_maru_node.md"
    ):
        _validate_storage_config(extra)
