# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for maru_common.resource_manager_installer."""

import argparse
import io
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from maru_common.resource_manager_installer import (
    _check_root,
    _do_install,
    _do_uninstall,
    _find_resource_manager_source,
    _run,
    fprintf,
    main,
)

# =============================================================================
# fprintf tests
# =============================================================================


class TestFprintf:
    """Tests for fprintf helper."""

    def test_with_format_args(self):
        buf = io.StringIO()
        fprintf(buf, "hello %s %d\n", "world", 42)
        assert buf.getvalue() == "hello world 42\n"

    def test_without_format_args(self):
        buf = io.StringIO()
        fprintf(buf, "plain message")
        assert buf.getvalue() == "plain message"


# =============================================================================
# _check_root tests
# =============================================================================


class TestCheckRoot:
    """Tests for _check_root."""

    def test_returns_1_when_not_root(self):
        with patch(
            "maru_common.resource_manager_installer.os.getuid", return_value=1000
        ):
            assert _check_root() == 1

    def test_returns_none_when_root(self):
        with patch("maru_common.resource_manager_installer.os.getuid", return_value=0):
            assert _check_root() is None


# =============================================================================
# _find_resource_manager_source tests
# =============================================================================


class TestFindResourceManagerSource:
    """Tests for _find_resource_manager_source."""

    def test_finds_source_directory(self):
        """Source directory should be found from the package location."""
        result = _find_resource_manager_source()
        # In our dev environment, this finds the real source
        if result.is_dir():
            assert (result / "CMakeLists.txt").is_file()

    def test_returns_empty_path_when_not_found(self):
        """Returns empty Path when source cannot be found."""
        with patch(
            "maru_common.resource_manager_installer.__file__",
            "/tmp/_maru_test_isolated/fake.py",
        ):
            result = _find_resource_manager_source()
            assert result == Path()


# =============================================================================
# _run tests
# =============================================================================


class TestRun:
    """Tests for _run."""

    def test_runs_command_with_check(self):
        with patch("maru_common.resource_manager_installer.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=["echo", "test"], returncode=0
            )
            result = _run(["echo", "test"])
            mock_run.assert_called_once_with(["echo", "test"], check=True)
            assert result.returncode == 0

    def test_runs_command_without_check(self):
        with patch("maru_common.resource_manager_installer.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=["cmd"], returncode=1
            )
            _run(["cmd"], check=False)
            mock_run.assert_called_once_with(["cmd"], check=False)


# =============================================================================
# _do_uninstall tests
# =============================================================================


class TestDoUninstall:
    """Tests for _do_uninstall."""

    def test_removes_existing_binary(self, tmp_path):
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        binary = bin_dir / "maru-resource-manager"
        binary.write_text("fake binary")

        result = _do_uninstall(str(tmp_path))
        assert result == 0
        assert not binary.exists()

    def test_nothing_to_remove(self, tmp_path):
        result = _do_uninstall(str(tmp_path))
        assert result == 0


# =============================================================================
# _do_install tests
# =============================================================================


class TestDoInstall:
    """Tests for _do_install."""

    def test_cmake_not_found(self):
        args = argparse.Namespace(prefix="/usr/local", clean=False)
        with patch(
            "maru_common.resource_manager_installer.shutil.which", return_value=None
        ):
            assert _do_install(args) == 1

    def test_source_not_found(self):
        args = argparse.Namespace(prefix="/usr/local", clean=False)
        with (
            patch(
                "maru_common.resource_manager_installer.shutil.which",
                return_value="/usr/bin/cmake",
            ),
            patch(
                "maru_common.resource_manager_installer._find_resource_manager_source",
                return_value=Path("/nonexistent_maru_test_dir"),
            ),
        ):
            assert _do_install(args) == 1

    def test_successful_build(self, tmp_path):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        build_dir = src_dir / "build"

        args = argparse.Namespace(prefix=str(tmp_path / "install"), clean=False)

        def mock_run_side_effect(*a, **kw):
            build_dir.mkdir(exist_ok=True)
            binary = build_dir / "maru-resource-manager"
            binary.write_text("fake binary")
            return subprocess.CompletedProcess(args=a, returncode=0)

        with (
            patch(
                "maru_common.resource_manager_installer.shutil.which",
                return_value="/usr/bin/cmake",
            ),
            patch(
                "maru_common.resource_manager_installer._find_resource_manager_source",
                return_value=src_dir,
            ),
            patch(
                "maru_common.resource_manager_installer._run",
                side_effect=mock_run_side_effect,
            ),
        ):
            assert _do_install(args) == 0

    def test_build_output_missing(self, tmp_path):
        src_dir = tmp_path / "src"
        src_dir.mkdir()

        args = argparse.Namespace(prefix=str(tmp_path / "install"), clean=False)

        with (
            patch(
                "maru_common.resource_manager_installer.shutil.which",
                return_value="/usr/bin/cmake",
            ),
            patch(
                "maru_common.resource_manager_installer._find_resource_manager_source",
                return_value=src_dir,
            ),
            patch(
                "maru_common.resource_manager_installer._run",
                return_value=subprocess.CompletedProcess(args=[], returncode=0),
            ),
        ):
            assert _do_install(args) == 1

    def test_build_output_empty(self, tmp_path):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        build_dir = src_dir / "build"

        args = argparse.Namespace(prefix=str(tmp_path / "install"), clean=False)

        def mock_run_side_effect(*a, **kw):
            build_dir.mkdir(exist_ok=True)
            binary = build_dir / "maru-resource-manager"
            binary.write_bytes(b"")  # Empty file
            return subprocess.CompletedProcess(args=a, returncode=0)

        with (
            patch(
                "maru_common.resource_manager_installer.shutil.which",
                return_value="/usr/bin/cmake",
            ),
            patch(
                "maru_common.resource_manager_installer._find_resource_manager_source",
                return_value=src_dir,
            ),
            patch(
                "maru_common.resource_manager_installer._run",
                side_effect=mock_run_side_effect,
            ),
        ):
            assert _do_install(args) == 1

    def test_clean_removes_build_dir(self, tmp_path):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        build_dir = src_dir / "build"
        build_dir.mkdir()
        old_artifact = build_dir / "old_artifact"
        old_artifact.write_text("old")

        args = argparse.Namespace(prefix=str(tmp_path / "install"), clean=True)

        def mock_run_side_effect(*a, **kw):
            build_dir.mkdir(exist_ok=True)
            binary = build_dir / "maru-resource-manager"
            binary.write_text("fake binary")
            return subprocess.CompletedProcess(args=a, returncode=0)

        with (
            patch(
                "maru_common.resource_manager_installer.shutil.which",
                return_value="/usr/bin/cmake",
            ),
            patch(
                "maru_common.resource_manager_installer._find_resource_manager_source",
                return_value=src_dir,
            ),
            patch(
                "maru_common.resource_manager_installer._run",
                side_effect=mock_run_side_effect,
            ),
        ):
            assert _do_install(args) == 0
            # Old artifact should be gone (dir was cleaned before rebuild)
            assert not old_artifact.exists()

    def test_stale_cmake_cache_removed(self, tmp_path):
        """Stale CMake cache is removed when probe fails."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        build_dir = src_dir / "build"
        build_dir.mkdir()
        cmake_cache = build_dir / "CMakeCache.txt"
        cmake_cache.write_text("stale cache")

        args = argparse.Namespace(prefix=str(tmp_path / "install"), clean=False)

        def mock_run_side_effect(*a, **kw):
            binary = build_dir / "maru-resource-manager"
            binary.write_text("fake binary")
            return subprocess.CompletedProcess(args=a, returncode=0)

        with (
            patch(
                "maru_common.resource_manager_installer.shutil.which",
                return_value="/usr/bin/cmake",
            ),
            patch(
                "maru_common.resource_manager_installer._find_resource_manager_source",
                return_value=src_dir,
            ),
            patch(
                "maru_common.resource_manager_installer._run",
                side_effect=mock_run_side_effect,
            ),
            patch(
                "subprocess.run",
                return_value=subprocess.CompletedProcess(
                    args=[], returncode=1, stdout=b"", stderr=b"error"
                ),
            ),
        ):
            assert _do_install(args) == 0
            # Stale cache should have been removed
            assert not cmake_cache.exists()

    def test_valid_cmake_cache_kept(self, tmp_path):
        """Valid CMake cache is kept when probe succeeds."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        build_dir = src_dir / "build"
        build_dir.mkdir()
        cmake_cache = build_dir / "CMakeCache.txt"
        cmake_cache.write_text("valid cache")

        args = argparse.Namespace(prefix=str(tmp_path / "install"), clean=False)

        def mock_run_side_effect(*a, **kw):
            binary = build_dir / "maru-resource-manager"
            binary.write_text("fake binary")
            return subprocess.CompletedProcess(args=a, returncode=0)

        with (
            patch(
                "maru_common.resource_manager_installer.shutil.which",
                return_value="/usr/bin/cmake",
            ),
            patch(
                "maru_common.resource_manager_installer._find_resource_manager_source",
                return_value=src_dir,
            ),
            patch(
                "maru_common.resource_manager_installer._run",
                side_effect=mock_run_side_effect,
            ),
            patch(
                "subprocess.run",
                return_value=subprocess.CompletedProcess(args=[], returncode=0),
            ),
        ):
            assert _do_install(args) == 0
            # Cache not removed (probe succeeded)
            assert cmake_cache.is_file()


# =============================================================================
# main tests
# =============================================================================


class TestMain:
    """Tests for main entry point."""

    def test_not_root(self):
        with patch(
            "maru_common.resource_manager_installer.os.getuid", return_value=1000
        ):
            assert main([]) == 1

    def test_uninstall(self, tmp_path):
        with patch("maru_common.resource_manager_installer.os.getuid", return_value=0):
            assert main(["--uninstall", "--prefix", str(tmp_path)]) == 0

    def test_install_delegates_to_do_install(self):
        with (
            patch("maru_common.resource_manager_installer.os.getuid", return_value=0),
            patch(
                "maru_common.resource_manager_installer._do_install", return_value=0
            ) as mock_install,
        ):
            assert main([]) == 0
            mock_install.assert_called_once()

    def test_install_with_custom_prefix(self):
        with (
            patch("maru_common.resource_manager_installer.os.getuid", return_value=0),
            patch(
                "maru_common.resource_manager_installer._do_install", return_value=0
            ) as mock_install,
        ):
            main(["--prefix", "/opt/maru"])
            args = mock_install.call_args[0][0]
            assert args.prefix == "/opt/maru"

    def test_install_with_clean(self):
        with (
            patch("maru_common.resource_manager_installer.os.getuid", return_value=0),
            patch(
                "maru_common.resource_manager_installer._do_install", return_value=0
            ) as mock_install,
        ):
            main(["--clean"])
            args = mock_install.call_args[0][0]
            assert args.clean is True

    def test_default_prefix(self):
        with (
            patch("maru_common.resource_manager_installer.os.getuid", return_value=0),
            patch(
                "maru_common.resource_manager_installer._do_install", return_value=0
            ) as mock_install,
        ):
            main([])
            args = mock_install.call_args[0][0]
            assert args.prefix == "/usr/local"

    def test_main_module_entry_point(self):
        """Test the if __name__ == '__main__' block."""
        import runpy
        import sys

        # Run without root — _check_root() returns 1 immediately.
        with patch.object(sys, "argv", ["install-maru-resource-manager"]):
            with pytest.raises(SystemExit) as exc_info:
                runpy.run_module(
                    "maru_common.resource_manager_installer",
                    run_name="__main__",
                    alter_sys=True,
                )
            assert exc_info.value.code == 1
