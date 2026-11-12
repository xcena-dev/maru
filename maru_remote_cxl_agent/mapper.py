# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Map resource-manager regions read/write for NIC registration.

The remote CXL agent maps a region with libc ``mmap`` so it gets a plain address
to register with the NIC, without CUDA registration or prefaulting: a memory
node has no GPU, and NIC registration pins every page of the range anyway.

An unmapped region's address range is reserved for the rest of this process:
it is replaced by an inaccessible placeholder. A NIC can hand out the same
remote key again when the same address range is registered again. A worker
paused before posting a WRITE can resume arbitrarily late with the old key
and address; a timeout cannot fence it. Keeping the old address unused makes
that WRITE fail instead of landing in a new region.
"""

import ctypes
import logging
import os
import threading

from maru_shm.device_scanner import read_device_uuid, scan_dax_devices

logger = logging.getLogger(__name__)

# Linux mmap(2) flags.
_PROT_NONE = 0x0
_PROT_READ = 0x1
_PROT_WRITE = 0x2
_MAP_SHARED = 0x01
_MAP_PRIVATE = 0x02
_MAP_FIXED = 0x10
_MAP_ANONYMOUS = 0x20
_MAP_NORESERVE = 0x4000

_libc = ctypes.CDLL(None, use_errno=True)
_libc.mmap.restype = ctypes.c_void_p
_libc.mmap.argtypes = (
    ctypes.c_void_p,
    ctypes.c_size_t,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_long,
)
_libc.munmap.restype = ctypes.c_int
_libc.munmap.argtypes = (ctypes.c_void_p, ctypes.c_size_t)
_MAP_FAILED = ctypes.c_void_p(-1).value


class DaxRegionMapper:
    """Maps resource-manager regions read/write on this node.

    The device is found as ``maru_shm`` finds it: by the UUID in its header
    when this node has the device under another path, else by the path the
    resource manager gave.
    """

    def __init__(
        self,
        device_table: dict[str, str] | None = None,
        clock=None,
    ) -> None:
        """Create the mapper.

        Args:
            device_table: UUID -> local device path. Scanned from the local
                DAX devices on first use when omitted.
            clock: Retained for callers that supplied a clock. Address
                reservations no longer expire with elapsed time.
        """
        self._device_table = device_table
        # Placeholders over unmapped regions: (address, length). They remain
        # reserved even after this object is discarded; the OS removes them
        # when the process exits. They consume address space, not CXL pages.
        self._quarantine: list[tuple[int, int]] = []
        self._lock = threading.Lock()

    def resolve(self, dax_path: str, device_uuid: str) -> str:
        """Return the local path of a region's device.

        Args:
            dax_path: Device (DEV_DAX) or file (FS_DAX) path from the RM.
            device_uuid: Device UUID from the RM; empty for FS_DAX.

        Returns:
            The path to open on this node.

        Raises:
            RuntimeError: if no device here has the UUID. The RM's path may
                name another device on this node, so it is opened only when
                its header carries the UUID, as ``MaruShmClient`` does.
        """
        if not device_uuid:
            return dax_path
        if self._device_table is None or device_uuid not in self._device_table:
            self._device_table = dict(scan_dax_devices())  # devices come and go
        local = self._device_table.get(device_uuid)
        if local:
            return local
        found = read_device_uuid(dax_path)
        if found == device_uuid:
            return dax_path
        raise RuntimeError(
            f"device UUID {device_uuid} (RM path {dax_path}) not found on this "
            f"node; the header at {dax_path} has UUID {found}"
        )

    def map(self, dax_path: str, device_uuid: str, offset: int, length: int) -> int:
        """Map ``length`` bytes at ``offset`` of the region's device read/write.

        Args:
            dax_path: Device or file path from the RM.
            device_uuid: Device UUID from the RM (may be empty).
            offset: mmap offset of the region.
            length: Region length in bytes.

        Returns:
            The address of the region's first byte.

        Raises:
            OSError: if the device cannot be opened or mapped.
            RuntimeError: if no device here has the region's UUID (resolve).
        """
        path = self.resolve(dax_path, device_uuid)
        fd = os.open(path, os.O_RDWR)
        try:
            addr = _libc.mmap(
                None, length, _PROT_READ | _PROT_WRITE, _MAP_SHARED, fd, offset
            )
        finally:
            os.close(fd)
        if addr is None or addr == _MAP_FAILED:
            err = ctypes.get_errno()
            raise OSError(err, f"mmap {path}+{offset:#x}: {os.strerror(err)}")
        return addr

    def unmap(self, addr: int, length: int) -> None:
        """Drop a mapping made by :meth:`map`; its addresses stay unused.

        The device mapping is replaced in one step by an inaccessible
        anonymous placeholder, so no later mapping in this process can take
        the range. No timer, object destruction or service cleanup removes
        it; only process exit releases these reservations.

        Args:
            addr: Address returned by :meth:`map`.
            length: Length passed to :meth:`map`.

        Raises:
            OSError: if the range cannot be replaced.
        """
        placeholder = _libc.mmap(
            addr,
            length,
            _PROT_NONE,
            _MAP_PRIVATE | _MAP_ANONYMOUS | _MAP_FIXED | _MAP_NORESERVE,
            -1,
            0,
        )
        if placeholder is None or placeholder == _MAP_FAILED:
            err = ctypes.get_errno()
            raise OSError(err, f"unmap {addr:#x}+{length}: {os.strerror(err)}")
        with self._lock:
            self._quarantine.append((addr, length))

    def discard(self, addr: int, length: int) -> None:
        """Drop a mapping that was never registered with the NIC.

        No NIC key ever named these addresses, so they may be reused at once.

        Args:
            addr: Address returned by :meth:`map`.
            length: Length passed to :meth:`map`.

        Raises:
            OSError: if the range cannot be unmapped.
        """
        if _libc.munmap(addr, length) != 0:
            err = ctypes.get_errno()
            raise OSError(err, f"munmap {addr:#x}+{length}: {os.strerror(err)}")

    def held_addresses(self) -> list[tuple[int, int]]:
        """Address ranges kept out of use (for tests and status)."""
        with self._lock:
            return list(self._quarantine)
