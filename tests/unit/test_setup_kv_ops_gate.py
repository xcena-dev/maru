# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The optional KV-ops extension must never take the install down with it.

``optional=True`` only covers a compile that has already started. Everything
PyTorch settles before that — the CUDA and ABI preflight, and which target
architectures to emit — raises out of the whole command, so ``setup.py``
runs those checks itself and drops the extension when one refuses. These
pin that behaviour, including which advice each refusal prints, because the
message is what an operator acts on.
"""

import pathlib
import runpy
from types import SimpleNamespace

import pytest

_SETUP = pathlib.Path(__file__).resolve().parents[2] / "setup.py"


def _setup_namespace(monkeypatch):
    """Run setup.py for its definitions, without letting it install anything."""
    import setuptools

    monkeypatch.setattr(setuptools, "setup", lambda **kwargs: None)
    return runpy.run_path(str(_SETUP), run_name="maru_setup_under_test")


def _refusal_for(monkeypatch, *, arch_flags, check_cuda_version=lambda *a: None):
    """Build the command class over a stub base and ask it for a refusal."""
    from torch.utils import cpp_extension

    namespace = _setup_namespace(monkeypatch)
    monkeypatch.setattr(
        cpp_extension, "_check_cuda_version", check_cuda_version, raising=False
    )
    monkeypatch.setattr(
        cpp_extension, "_get_cuda_arch_flags", arch_flags, raising=False
    )

    base = type("_StubBuild", (), {"_check_abi": lambda self: ("g++", "11")})
    command_cls = namespace["_optional_kv_ops_build"](base)
    command = command_cls.__new__(command_cls)
    command.extensions = [SimpleNamespace(name=namespace["_KV_OPS_NAME"])]
    return command._kv_ops_refusal(), namespace


def test_no_target_architecture_drops_the_extension(monkeypatch):
    """A toolchain host with no GPU and no arch list must still install.

    PyTorch reads the target architectures from ``TORCH_CUDA_ARCH_LIST`` or,
    failing that, from the visible devices. With neither it has nothing to
    name and raises, per source file, inside the loop ``optional=True``
    guards — so without this check the whole install dies.
    """
    pytest.importorskip("torch")

    def raises_like_torch():
        raise IndexError("list index out of range")

    refusal, namespace = _refusal_for(monkeypatch, arch_flags=raises_like_torch)

    assert refusal is not None, "the build would have failed instead of skipping"
    error, note = refusal
    assert isinstance(error, IndexError)
    # The advice has to name what this host is missing. It has PyTorch and a
    # toolkit, so the generic "reinstall with the toolkit" note misdirects.
    assert note == namespace["_NO_TARGET_ARCH_NOTE"]
    assert "TORCH_CUDA_ARCH_LIST" in note


def test_named_architectures_keep_the_extension_on_a_gpu_less_host(monkeypatch):
    """Naming the targets is what lets a build host without a GPU build it."""
    pytest.importorskip("torch")

    refusal, _ = _refusal_for(
        monkeypatch, arch_flags=lambda: ["-gencode=arch=compute_90,code=sm_90"]
    )

    assert refusal is None


def test_cuda_preflight_refusal_keeps_its_own_advice(monkeypatch):
    """A toolkit mismatch is a different fix, so it keeps the reinstall note."""
    pytest.importorskip("torch")

    def mismatched(*args):
        raise RuntimeError("The detected CUDA version mismatches")

    refusal, namespace = _refusal_for(
        monkeypatch, arch_flags=lambda: [], check_cuda_version=mismatched
    )

    assert refusal is not None
    error, note = refusal
    assert isinstance(error, RuntimeError)
    assert note == namespace["_FALLBACK_NOTE"]
