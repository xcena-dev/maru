# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Map resource-manager regions read/write for NIC registration.

The region mapper maps a region with libc ``mmap`` so it gets a plain address
to register with the NIC, without CUDA registration or prefaulting: a memory
node has no GPU, and NIC registration pins every page of the range anyway.

An unmapped region's address range is not given to a later mapping for a
while (:data:`ADDRESS_QUARANTINE_S`): it is replaced by an inaccessible
placeholder. A NIC can hand out the same remote key again when the same
address range is registered again, and a late RDMA WRITE that still carries
the old key and address would then land in the new region. With a new
address, the old key and address match no registration and the WRITE fails.
"""

import ctypes
import logging
import os
import threading
import time

from maru_shm.device_scanner import scan_dax_devices

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

# How long an unmapped region's address range stays out of use. Far longer
# than an RDMA connection retransmits a request (seconds).
ADDRESS_QUARANTINE_S = 600.0

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
        clock=time.monotonic,
    ) -> None:
        """Create the mapper.

        Args:
            device_table: UUID -> local device path. Scanned from the local
                DAX devices on first use when omitted.
            clock: Monotonic clock (tests pass a fake one).
        """
        self._device_table = device_table
        self._clock = clock
        # Placeholders over unmapped regions: (expiry, address, length)
        self._quarantine: list[tuple[float, int, int]] = []
        self._lock = threading.Lock()

    def resolve(self, dax_path: str, device_uuid: str) -> str:
        """Return the local path of a region's device.

        Args:
            dax_path: Device (DEV_DAX) or file (FS_DAX) path from the RM.
            device_uuid: Device UUID from the RM; empty for FS_DAX.

        Returns:
            The path to open on this node.
        """
        if device_uuid:
            if self._device_table is None or device_uuid not in self._device_table:
                self._device_table = dict(scan_dax_devices())  # devices come and go
            local = self._device_table.get(device_uuid)
            if local:
                return local
        return dax_path

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
        """
        self._expire_placeholders()
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
        anonymous placeholder, so no other mapping can take the range before
        :data:`ADDRESS_QUARANTINE_S` has passed.

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
            self._quarantine.append(
                (self._clock() + ADDRESS_QUARANTINE_S, addr, length)
            )
        self._expire_placeholders()

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
        self._expire_placeholders()

    def held_addresses(self) -> list[tuple[int, int]]:
        """Address ranges kept out of use (for tests and status)."""
        with self._lock:
            return [(addr, length) for _, addr, length in self._quarantine]

    def _expire_placeholders(self) -> None:
        now = self._clock()
        with self._lock:
            expired = [q for q in self._quarantine if q[0] <= now]
            self._quarantine = [q for q in self._quarantine if q[0] > now]
        for _, addr, length in expired:
            if _libc.munmap(addr, length) != 0:
                err = ctypes.get_errno()
                logger.warning(
                    "munmap of placeholder %#x+%d failed: %s",
                    addr,
                    length,
                    os.strerror(err),
                )
