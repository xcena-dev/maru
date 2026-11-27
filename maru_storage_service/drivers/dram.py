# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""DRAM driver: memfd pools, optionally on huge pages.

Pools are fully allocated and size-sealed at creation, so a shortage fails at
start rather than as a SIGBUS in a worker. A pool ends with its run, so it is
never opened again.
"""

import errno
import fcntl
import mmap
import os
import re
from dataclasses import dataclass

from maru_common.storage_types import PoolDescriptor, PoolInfo, PoolKind

from ..config_fields import TierConfig, TierContext, TierSpec, _bool
from ..tiers import L1Pool, L1Tier, Level, TierEnv
from . import PoolAccess

_MEMINFO = "/proc/meminfo"
# Linux UAPI (include/uapi/linux/fcntl.h). Some Python builds omit these
# names even on kernels supporting memfd seals. Keep the actual kernel check.
_F_ADD_SEALS = getattr(fcntl, "F_ADD_SEALS", 1033)
_SIZE_SEALS = (
    getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
    | getattr(fcntl, "F_SEAL_GROW", 0x0004)
    | getattr(fcntl, "F_SEAL_SEAL", 0x0001)
)


class DramPoolError(RuntimeError):
    """The DRAM pool could not be created as configured. ``short``: memory
    ran short, and may come free (an earlier run's pool still mapped)."""

    def __init__(self, message: str, short: bool = False):
        super().__init__(message)
        self.short = short


def dram_pool_id(node_id: str, node_epoch: str) -> str:
    return f"dram:{node_id}:{node_epoch}"


def _meminfo() -> dict[str, int]:
    """Fields of /proc/meminfo; sizes in bytes, counts as they are."""
    out: dict[str, int] = {}
    try:
        with open(_MEMINFO) as f:
            for line in f:
                m = re.match(r"^(\S+):\s+(\d+)(\s+kB)?", line)
                if m:
                    value = int(m.group(2))
                    out[m.group(1)] = value << 10 if m.group(3) else value
    except OSError:
        pass
    return out


def default_huge_page_size() -> int | None:
    """Size of the default huge page (what MFD_HUGETLB uses), or None."""
    return _meminfo().get("Hugepagesize")


@dataclass
class DramPool:
    """One memfd pool; ``fd`` stays open for the ATTACH replies. An L1Pool."""

    pool_id: str
    node_id: str
    capacity: int
    page_size: int
    fd: int
    hugepage: bool
    pool_generation: int = 1

    @property
    def access(self) -> PoolAccess:
        return PoolAccess.MAPPED

    def info(self) -> PoolInfo:
        return PoolInfo(
            pool_id=self.pool_id,
            kind=PoolKind.DRAM,
            node_id=self.node_id,
            pool_generation=self.pool_generation,
            capacity=self.capacity,
            page_size=self.page_size,
        )

    def descriptor(self, memfd: int | None = None) -> PoolDescriptor:
        return PoolDescriptor(pool=self.info(), memfd=memfd)

    def worker_fd(self) -> int | None:
        return self.fd if self.fd >= 0 else None

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1


def create_dram_pool(
    node_id: str, node_epoch: str, size: int, page_size: int, hugepage: bool
) -> DramPool:
    """Create the pool (whole pages). With ``hugepage`` it must get huge pages;
    there is no fallback to normal pages."""
    if page_size <= 0:
        raise DramPoolError(f"page_size must be positive, got {page_size}")
    capacity = size // page_size * page_size
    if capacity <= 0:
        raise DramPoolError(
            f"the dram tier ({size} bytes) holds no page of {page_size} bytes"
        )
    flags = os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING
    huge = None
    if hugepage:
        huge = default_huge_page_size()
        if not huge:
            raise DramPoolError(
                "the dram tier asks for huge pages (hugepage: true), but this "
                "kernel reports no huge page size; set hugepage to false"
            )
        if capacity % huge:
            raise DramPoolError(
                f"the dram pool ({capacity} bytes) is not a multiple of the huge "
                f"page size ({huge} bytes); change size or page_size, or set "
                "hugepage to false"
            )
        flags |= os.MFD_HUGETLB
    try:
        fd = os.memfd_create(f"maru-dram:{node_id}", flags)
    except OSError as e:
        raise DramPoolError(f"memfd_create failed for the dram pool: {e}") from e
    try:
        os.ftruncate(fd, capacity)
        try:
            os.posix_fallocate(fd, 0, capacity)
        except OSError as e:
            if huge is not None and e.errno in (
                errno.ENOSPC,
                errno.ENOMEM,
                errno.EINVAL,
            ):
                info = _meminfo()
                raise DramPoolError(
                    f"cannot get {capacity // huge} huge pages of {huge} bytes for "
                    f"the dram pool ({capacity} bytes): HugePages_Free is "
                    f"{info.get('HugePages_Free', 'unknown')}. Reserve huge pages "
                    "(vm.nr_hugepages) or set hugepage to false",
                    # Short only if the reserved huge pages could hold it
                    short=info.get("HugePages_Total", 0) * huge >= capacity,
                ) from e
            raise DramPoolError(
                f"cannot allocate the dram pool ({capacity} bytes): {e}",
                short=e.errno in (errno.ENOSPC, errno.ENOMEM),
            ) from e
        # No holder of the fd may resize it under the others' mappings

        try:
            fcntl.fcntl(fd, _F_ADD_SEALS, _SIZE_SEALS)
        except OSError as e:
            raise DramPoolError(
                f"cannot seal the dram pool against resizing: {e}"
            ) from e
    except BaseException:
        os.close(fd)
        raise
    return DramPool(
        pool_id=dram_pool_id(node_id, node_epoch),
        node_id=node_id,
        capacity=capacity,
        page_size=page_size,
        fd=fd,
        hugepage=hugepage,
    )


class DramDriver:
    """Creates the dram tier's pool."""

    def create(
        self, tier: TierConfig, *, node_id: str, node_epoch: str, page_size: int
    ) -> DramPool:
        if tier.size is None:
            raise DramPoolError("the dram tier needs a size")
        return create_dram_pool(
            node_id, node_epoch, tier.size, page_size, bool(tier.hugepage)
        )

    def open(self, info: PoolInfo) -> DramPool:
        raise DramPoolError(f"{info.pool_id}: a DRAM pool ends with its run")


class DramTier(L1Tier):
    """The run's memfd pool, created by open(); DramPoolError (``short``:
    memory may come free) if it cannot be."""

    name = "dram"

    def __init__(
        self, tier: TierConfig, *, node_id: str, node_epoch: str, page_size: int
    ):
        self.tier = tier
        self.node_id = node_id
        self.node_epoch = node_epoch
        self.page_size = page_size
        self.pools: list[DramPool] = []

    @property
    def pool(self) -> DramPool | None:
        return self.pools[0] if self.pools else None

    def open(self) -> None:
        self.pools = [
            DramDriver().create(
                self.tier,
                node_id=self.node_id,
                node_epoch=self.node_epoch,
                page_size=self.page_size,
            )
        ]

    def map_for_transfers(self, pool: L1Pool) -> mmap.mmap:
        fd = pool.worker_fd()  # the memfd
        assert fd is not None
        return mmap.mmap(
            fd,
            pool.capacity,
            flags=mmap.MAP_SHARED,
            prot=mmap.PROT_READ | mmap.PROT_WRITE,
        )

    def close(self) -> None:
        for pool in self.pools:
            pool.close()

    def describe(self, adopted: set[str]) -> str:
        pool = self.pool
        assert pool is not None
        return f"pool {pool.pool_id} ({pool.capacity >> 20} MiB)"


def _make(tier: TierConfig, env: TierEnv) -> DramTier:
    return DramTier(
        tier, node_id=env.node_id, node_epoch=env.node_epoch, page_size=env.page_size
    )


def _parse(raw: dict, ctx: TierContext) -> dict:
    return {"hugepage": _bool(raw, "hugepage", ctx.what, True)}


SPEC = TierSpec(
    store="dram",
    level=Level.L1,
    local=True,
    kind=PoolKind.DRAM,
    keys=frozenset(
        {
            "store",
            "size",
            "high_watermark",
            "low_watermark",
            "hugepage",
        }
    ),
    parse=_parse,
    make=_make,
)
