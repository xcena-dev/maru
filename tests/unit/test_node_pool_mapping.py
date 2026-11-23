# SPDX-License-Identifier: Apache-2.0
"""Mapped node pool registration and memory limits without CUDA hardware."""

import ctypes
import errno
import mmap
from unittest.mock import MagicMock, patch

import pytest

from maru_handler.storage.cpu import CpuPool

PAGE = 4096
CAPACITY = 4 * PAGE


def _mock_torch_cuda(register_rc=0):
    """A torch stand-in whose cudart answers host (un)registration."""
    mock_torch = MagicMock()
    mock_torch.cuda.is_available.return_value = True
    cudart = MagicMock()
    cudart.cudaHostRegister.return_value = (register_rc,)
    cudart.cudaHostUnregister.return_value = (0,)
    mock_torch.cuda.cudart.return_value = cudart
    return mock_torch, cudart


def _address(mapping):
    return ctypes.addressof(ctypes.c_char.from_buffer(mapping))


def _locked(pool, keepalive=None):
    pool.page_lock(keepalive)
    return pool


class _RecordingMap(mmap.mmap):
    """An anonymous mapping that records madvise and single-byte writes.

    ``fail_madvise`` is the errno madvise fails with, 0 for none.
    """

    def __init__(self, *args, fail_madvise=0):
        self.calls = []
        self.fail_madvise = fail_madvise

    def __new__(cls, *args, fail_madvise=0):
        return super().__new__(cls, *args)

    def madvise(self, option, *args):
        self.calls.append(("madvise", option, *args))
        if self.fail_madvise:
            raise OSError(self.fail_madvise, errno.errorcode[self.fail_madvise])
        return super().madvise(option, *args)

    def __setitem__(self, index, value):
        if isinstance(index, int):
            self.calls.append(("write", index))
        super().__setitem__(index, value)


def test_pinned_pool_registers_its_whole_mapping_until_close():
    mock_torch, cudart = _mock_torch_cuda()
    with patch.dict("sys.modules", {"torch": mock_torch}):
        pool = _locked(CpuPool("p", CAPACITY, PAGE))
        address = _address(pool._mapping)
        assert pool.cuda_registered
        cudart.cudaHostRegister.assert_called_once_with(address, CAPACITY, 0)
        pool.close()
    cudart.cudaHostUnregister.assert_called_once_with(address)
    assert not pool.cuda_registered


def test_pool_is_prefaulted_before_registration():
    from maru_handler.memory.mapper import _MADV_POPULATE_WRITE

    mapping = _RecordingMap(-1, CAPACITY)
    mock_torch, cudart = _mock_torch_cuda()
    cudart.cudaHostRegister.side_effect = lambda *args: (
        mapping.calls.append(("register",)) or (0,)
    )
    with patch.dict("sys.modules", {"torch": mock_torch}):
        pool = _locked(CpuPool("p", CAPACITY, PAGE, mapping=mapping))
        pool.close()
    assert mapping.calls == [
        ("madvise", _MADV_POPULATE_WRITE, 0, CAPACITY),
        ("register",),
    ]


def test_prefault_without_madvise_writes_each_page_back_unchanged():
    """Kernels before 5.14 have no MADV_POPULATE_WRITE; a read would only map
    the zero page of an anonymous pool, so each page is written instead."""
    from maru_handler.memory.mapper import _PAGE_SIZE

    mapping = _RecordingMap(-1, CAPACITY, fail_madvise=errno.EINVAL)
    mapping[:] = bytes(range(256)) * (CAPACITY // 256)
    expected = bytes(mapping[:])
    mapping.calls.clear()
    mock_torch, _ = _mock_torch_cuda()
    with patch.dict("sys.modules", {"torch": mock_torch}):
        pool = _locked(CpuPool("p", CAPACITY, PAGE, mapping=mapping))
        assert mapping.calls[1:] == [
            ("write", offset) for offset in range(0, CAPACITY, _PAGE_SIZE)
        ]
        assert bytes(mapping[:]) == expected
        assert pool.cuda_registered
        pool.close()


def test_maru_prefault_off_skips_the_prefault_only(monkeypatch):
    import maru_handler.memory.mapper as mapper_mod

    monkeypatch.setattr(mapper_mod, "_PREFAULT_ENABLED", False)
    mapping = _RecordingMap(-1, CAPACITY)
    mock_torch, cudart = _mock_torch_cuda()
    with patch.dict("sys.modules", {"torch": mock_torch}):
        pool = _locked(CpuPool("p", CAPACITY, PAGE, mapping=mapping))
        assert mapping.calls == []
        assert pool.cuda_registered
        pool.close()
    cudart.cudaHostRegister.assert_called_once()


def test_failed_registration_leaves_a_working_pageable_pool(monkeypatch):
    import maru_handler.memory.mapper as mapper_mod

    cleared = []
    monkeypatch.setattr(
        mapper_mod, "_clear_cuda_sticky_error", lambda: cleared.append(1)
    )
    mock_torch, cudart = _mock_torch_cuda(register_rc=2)  # cudaErrorMemoryAllocation
    with patch.dict("sys.modules", {"torch": mock_torch}):
        pool = _locked(CpuPool("p", CAPACITY, PAGE))
        assert not pool.cuda_registered
        view = pool.view(0, 5)
        view[:] = b"bytes"
        view.release()
        pool.close()
    assert cleared == [1]
    cudart.cudaHostUnregister.assert_not_called()


def test_registration_error_leaves_the_pool_pageable():
    mock_torch, cudart = _mock_torch_cuda()
    cudart.cudaHostRegister.side_effect = RuntimeError("CUDA error")
    with patch.dict("sys.modules", {"torch": mock_torch}):
        pool = _locked(CpuPool("p", CAPACITY, PAGE))
        assert not pool.cuda_registered
        pool.close()
    cudart.cudaHostUnregister.assert_not_called()


def test_without_cuda_or_pin_the_pool_is_not_registered():
    mock_torch, cudart = _mock_torch_cuda()
    with patch.dict("sys.modules", {"torch": mock_torch}):
        CpuPool("p", CAPACITY, PAGE).close()
        mock_torch.cuda.is_available.return_value = False
        _locked(CpuPool("q", CAPACITY, PAGE)).close()
    cudart.cudaHostRegister.assert_not_called()


def test_close_unregisters_once_when_an_exported_view_delays_it():
    mock_torch, cudart = _mock_torch_cuda()
    with patch.dict("sys.modules", {"torch": mock_torch}):
        pool = _locked(CpuPool("p", CAPACITY, PAGE))
        exported = memoryview(pool._mapping)[:8]
        with pytest.raises(RuntimeError, match="exported CPU buffers"):
            pool.close()
        exported.release()
        pool.close()
    cudart.cudaHostUnregister.assert_called_once()


def test_page_lock_is_idempotent_and_refuses_a_closed_pool():
    mock_torch, cudart = _mock_torch_cuda()
    with patch.dict("sys.modules", {"torch": mock_torch}):
        pool = _locked(CpuPool("p", CAPACITY, PAGE))
        pool.page_lock()
        cudart.cudaHostRegister.assert_called_once()
        pool.close()
        with pytest.raises(RuntimeError, match="closed"):
            pool.page_lock()


def test_the_prefault_runs_in_steps_and_calls_keepalive(monkeypatch):
    """One madvise call holds the GIL throughout, and page-locking tens of GB
    outlasts the session TTL: the prefault runs in steps, with a keepalive
    after each one and after the registration."""
    import maru_handler.storage.cpu as cpu_mod
    from maru_handler.memory.mapper import _MADV_POPULATE_WRITE

    monkeypatch.setattr(cpu_mod, "_PREFAULT_STEP", 2 * PAGE)
    mapping = _RecordingMap(-1, CAPACITY)
    mock_torch, cudart = _mock_torch_cuda()
    cudart.cudaHostRegister.side_effect = lambda *args: (
        mapping.calls.append(("register",)) or (0,)
    )
    with patch.dict("sys.modules", {"torch": mock_torch}):
        pool = CpuPool("p", CAPACITY, PAGE, mapping=mapping)
        pool.page_lock(lambda: mapping.calls.append(("keepalive",)))
        assert pool.cuda_registered
        pool.close()
    assert mapping.calls == [
        ("madvise", _MADV_POPULATE_WRITE, 0, 2 * PAGE),
        ("keepalive",),
        ("madvise", _MADV_POPULATE_WRITE, 2 * PAGE, 2 * PAGE),
        ("keepalive",),
        ("register",),
        ("keepalive",),
    ]


def test_a_prefault_error_other_than_an_old_kernels_leaves_the_pool_pageable():
    """Only EINVAL (no MADV_POPULATE_WRITE before 5.14) falls back to writing
    each page; after another error, writing would commit the pool anyway."""
    mapping = _RecordingMap(-1, CAPACITY, fail_madvise=errno.ENOMEM)
    mock_torch, cudart = _mock_torch_cuda()
    with patch.dict("sys.modules", {"torch": mock_torch}):
        pool = _locked(CpuPool("p", CAPACITY, PAGE, mapping=mapping))
        assert not pool.cuda_registered
        pool.close()
    assert [call[0] for call in mapping.calls] == ["madvise"]
    cudart.cudaHostRegister.assert_not_called()


def test_a_cpu_pool_larger_than_the_memory_available_stays_pageable(monkeypatch):
    """The kernel answers a prefault past the memory available with the OOM
    killer, not with an error, so such a pool is not page-locked at all."""
    import maru_handler.storage.cpu as cpu_mod

    monkeypatch.setattr(cpu_mod, "_available_memory", lambda: CAPACITY - PAGE)
    mapping = _RecordingMap(-1, CAPACITY)
    mock_torch, cudart = _mock_torch_cuda()
    with patch.dict("sys.modules", {"torch": mock_torch}):
        pool = _locked(CpuPool("p", CAPACITY, PAGE, mapping=mapping))
        assert not pool.cuda_registered
        assert mapping.calls == []
        pool.close()
        # A CXL pool is device memory, whatever the host has available
        cxl = _locked(CpuPool("c", CAPACITY, PAGE, medium="cxl"))
        assert cxl.cuda_registered
        cxl.close()
    cudart.cudaHostRegister.assert_called_once()


def test_available_memory_is_capped_by_each_cgroup_that_limits_it(
    monkeypatch, tmp_path
):
    import maru_handler.storage.cpu as cpu_mod

    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal: 4000 kB\nMemAvailable: 1000 kB\n")
    own = tmp_path / "cgroup"
    own.write_text("0::/pod/worker\n")
    root = tmp_path / "fs"
    (root / "pod" / "worker").mkdir(parents=True)
    (root / "pod" / "worker" / "memory.max").write_text("max\n")
    monkeypatch.setattr(cpu_mod, "_MEMINFO", str(meminfo))
    monkeypatch.setattr(cpu_mod, "_SELF_CGROUP", str(own))
    monkeypatch.setattr(cpu_mod, "_CGROUP_ROOT", root)
    assert cpu_mod._available_memory() == 1000 << 10
    # The pod's limit, less what the pod holds apart from its file cache
    (root / "pod" / "memory.max").write_text("600000\n")
    (root / "pod" / "memory.current").write_text("550000\n")
    (root / "pod" / "memory.stat").write_text(
        "anon 450000\nactive_file 60000\ninactive_file 40000\n"
    )
    assert cpu_mod._available_memory() == 600000 - 550000 + 100000
    meminfo.unlink()
    assert cpu_mod._available_memory() is None
