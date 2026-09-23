# SPDX-License-Identifier: Apache-2.0
"""Two fixed, owner-local L1 pools with independent placement/read policy.

Typed CXL pools never publish keys through legacy KVManager. Their regions are
reserved by ReplicaDirectory, and returned only after the owner unmaps them.
Cross-engine CXL mapping, replication and eviction are later capabilities.
"""

from maru_common.storage_policy import FixedOrderPolicy

from .cpu import CpuPool


class CxlPool(CpuPool):
    """The same page/ownership contract backed by an actual RM DAX mapping."""

    def __init__(self, grant: dict, page_size: int, rm_address: str):
        from maru_shm import PROT_READ, PROT_WRITE, MaruHandle, MaruShmClient
        from maru_shm.device_scanner import scan_dax_devices

        self._shm = MaruShmClient(
            address=rm_address, device_table=dict(scan_dax_devices())
        )
        self._handle = MaruHandle.from_dict(grant["handle"])
        try:
            mapping = self._shm.mmap(self._handle, PROT_READ | PROT_WRITE)
            super().__init__(
                grant["pool_id"],
                grant["capacity"],
                page_size,
                medium="cxl",
                mapping=mapping,
            )
        except Exception:
            self._shm.close()
            raise

    def close(self):
        # mmap.close raises while exported buffers still exist. Do not send
        # drained ACK / release the RM region until this actually succeeds.
        super().close()
        self._shm.close()


class MixedPool:
    def __init__(
        self,
        grants: list[dict],
        page_size: int,
        rm_address: str,
        write_order: tuple[str, ...],
    ):
        self.pools: dict[str, CpuPool] = {}
        self.policy = FixedOrderPolicy(write_order)
        try:
            for grant in grants:
                medium = grant["medium"]
                self.pools[medium] = (
                    CpuPool(grant["pool_id"], grant["capacity"], page_size)
                    if medium == "cpu"
                    else CxlPool(grant, page_size, rm_address)
                )
        except Exception:
            self.close()
            raise

    def alloc(self, size: int):
        for medium in self.policy.candidates(set(self.pools)):
            try:
                return self.pools[medium].alloc(size)
            except MemoryError:
                continue
        raise MemoryError(
            "Both CPU and CXL pools are full; skipping new cache admission"
        )

    def _pool(self, location):
        pool = self.pools.get(location.medium)
        if pool is None or pool.pool_id != location.pool_id:
            raise ValueError("Stale or foreign storage location")
        return pool

    def validate(self, handle):
        self._pool(handle.location).validate(handle)

    def free(self, handle):
        self._pool(handle.location).free(handle)

    def resolve(self, location):
        return self._pool(location).resolve(location)

    def usage(self):
        return {
            "pools": [{"pool_id": p.pool_id, **p.usage()} for p in self.pools.values()]
        }

    def close(self):
        for pool in self.pools.values():
            pool.close()
