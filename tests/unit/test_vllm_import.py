# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The public connector export is lazy; utilities need no optional packages."""

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


def test_public_connector_export_is_the_real_class_and_is_cached():
    pytest.importorskip("torch")
    pytest.importorskip("vllm")
    import maru_vllm
    from maru_vllm import MaruKVConnector
    from maru_vllm.connector import MaruKVConnector as Connector

    assert MaruKVConnector is Connector
    assert vars(maru_vllm)["MaruKVConnector"] is Connector


def test_utilities_import_without_optional_dependencies():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent("""\
                import importlib.abc
                import sys

                class NoOptionalPackages(importlib.abc.MetaPathFinder):
                    def find_spec(self, fullname, path=None, target=None):
                        if fullname.split('.')[0] in ('torch', 'vllm'):
                            raise ModuleNotFoundError(fullname, name=fullname)

                sys.meta_path.insert(0, NoOptionalPackages())
                import maru_vllm
                from maru_vllm.copy_tracker import CopyTracker

                assert 'maru_vllm.connector' not in sys.modules
                assert 'MaruKVConnector' in dir(maru_vllm)
                assert not hasattr(maru_vllm, 'unknown_export')
                tracker = CopyTracker()
                assert tracker.confirm(tracker.register(object()), True)

                # Requesting the real connector still reports missing deps.
                try:
                    from maru_vllm import MaruKVConnector
                except ModuleNotFoundError as error:
                    assert error.name == 'torch'
                else:
                    raise AssertionError('connector must require torch')
                """),
        ],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
