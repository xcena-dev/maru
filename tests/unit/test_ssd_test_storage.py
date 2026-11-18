# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""An explicit SSD test filesystem must not silently turn into a skip."""

import os
import subprocess
import sys
from pathlib import Path


def test_unavailable_configured_ssd_root_fails_the_test_run(tmp_path):
    root = tmp_path / "missing"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "tests/unit/test_ms_ssd_driver.py::test_pool_contract_and_preallocation",
        ],
        cwd=Path(__file__).resolve().parents[2],
        env={**os.environ, "MARU_TEST_SSD_ROOT": str(root)},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "no direct I/O test filesystem" in result.stdout
    assert str(root) in result.stdout
    assert "1 error" in result.stdout
