# SPDX-License-Identifier: Apache-2.0
"""Test doubles for remote storage: an in-process NIXL agent and a fake clock."""

from __future__ import annotations

import ctypes


class FakeNixlAgent:
    """Stands in for ``nixl._api.nixl_agent``.

    Every agent registers itself in a class-level registry keyed by name so
    that ``add_remote_agent`` can resolve metadata to a peer. Transfers copy
    bytes with ``ctypes.memmove``.

    A registration of an address range gets the key the same range had
    before, as the RoCE NIC this was checked on does (design note 5.4); a new
    range gets a new key. As with the real agent, a
    transfer can only name remote ranges whose metadata was loaded: full
    metadata (``get_agent_metadata``) covers every current registration of
    the peer, partial metadata only the registrations it lists (with their
    keys), and partial metadata without connection info cannot introduce a
    new peer. A transfer carries the key it was set up with; if the peer no
    longer holds that registration when the transfer reaches it, the
    transfer ends in ``ERR`` and moves nothing, even if the same range was
    registered again under a new key. As a reliable connection does after a
    remote access error, the connection breaks: every later transfer to that
    peer ends in ``ERR`` until the peer is removed and loaded again.

    ``stall`` keeps every new transfer in progress (no bytes move) until
    :meth:`finish_stalled` completes them, which is how a test reproduces a
    transfer that misses its deadline and lands later.
    """

    registry: dict[str, FakeNixlAgent] = {}

    def __init__(self, name: str, *args, **kwargs) -> None:
        self.name = name
        self.regs: list[tuple[int, int, int]] = []  # (addr, size, key)
        self.peers: set[str] = set()
        # peer -> registrations loaded from its metadata (addr, size, key);
        # None = every current registration (full metadata)
        self.known: dict[str, set[tuple[int, int, int]] | None] = {}
        self.broken: set[str] = set()
        self.backends: list[tuple[str, dict]] = []
        self.stall = False
        self.fail_next = False
        self.released: list[tuple] = []
        self._stalled: list[list] = []
        FakeNixlAgent.registry[name] = self

    def create_backend(self, name: str, params: dict) -> None:
        self.backends.append((name, dict(params)))

    def get_xfer_descs(self, descs, mem_type=None):
        assert mem_type == "DRAM", mem_type
        return list(descs)

    _next_key = 1

    @property
    def registered(self) -> list[tuple[int, int]]:
        return [(addr, size) for addr, size, _key in self.regs]

    _keys_by_range: dict[tuple[int, int], int] = {}

    def register_memory(self, descs, mem_type=None):
        assert mem_type == "DRAM", mem_type
        reg = []
        for addr, size, _dev, _tag in descs:
            key = FakeNixlAgent._keys_by_range.get((addr, size))
            if key is None:
                key = FakeNixlAgent._next_key
                FakeNixlAgent._next_key += 1
                FakeNixlAgent._keys_by_range[(addr, size)] = key
            self.regs.append((addr, size, key))
            reg.append((addr, size, key))
        return reg

    def deregister_memory(self, reg) -> None:
        for entry in reg:
            self.regs.remove(tuple(entry))

    def get_agent_metadata(self) -> bytes:
        return f"{self.name}|1|*".encode()

    def get_partial_agent_metadata(self, descs, inc_conn_info=False, backends=()):
        ranges = ";".join(f"{addr}:{size}:{key}" for addr, size, key in descs)
        return f"{self.name}|{int(bool(inc_conn_info))}|{ranges}".encode()

    def add_remote_agent(self, md: bytes) -> bytes:
        name, conn, ranges = md.decode().split("|")
        assert name in FakeNixlAgent.registry, name
        if conn != "1" and name not in self.peers:
            raise RuntimeError(f"no connection info for new agent {name}")
        self.peers.add(name)
        if ranges == "*":
            self.known[name] = None
        else:
            loaded = {
                (int(a), int(n), int(k))
                for a, n, k in (r.split(":") for r in ranges.split(";") if r)
            }
            if self.known.get(name, set()) is not None:
                self.known[name] = self.known.get(name, set()) | loaded
        return name.encode()  # the real binding returns bytes too

    def remove_remote_agent(self, name: str) -> None:
        self.peers.discard(name)
        self.known.pop(name, None)
        self.broken.discard(name)

    @staticmethod
    def _covered(agent: FakeNixlAgent, addr: int, size: int) -> bool:
        return any(a <= addr and addr + size <= a + s for a, s in agent.registered)

    @staticmethod
    def _key_for(regs, addr: int, size: int) -> int | None:
        for a, n, key in regs:
            if a <= addr and addr + size <= a + n:
                return key
        return None

    def initialize_xfer(self, op, local, remote, peer, notif=b""):
        peer_name = peer.decode() if isinstance(peer, bytes) else peer
        assert peer_name in self.peers, f"unknown peer {peer_name}"
        known = self.known.get(peer_name)
        target = FakeNixlAgent.registry[peer_name]
        keys = []
        for (la, ln, _), (ra, rn, _) in zip(local, remote, strict=True):
            assert ln == rn, "descriptor length mismatch"
            assert self._covered(self, la, ln), f"local {la:#x}+{ln} unregistered"
            regs = target.regs if known is None else known
            key = self._key_for(regs, ra, rn)
            if key is None and known is not None:
                raise RuntimeError("NIXL_ERR_NOT_FOUND")  # no loaded descriptor
            keys.append(key)
        return [op, list(local), list(remote), "NEW", peer_name, keys]

    def _remote_ok(self, handle) -> bool:
        """Whether the peer still holds every registration the transfer names."""
        target = FakeNixlAgent.registry.get(handle[4])
        return target is not None and all(
            key is not None
            and any(
                k == key and a <= ra and ra + rn <= a + n for a, n, k in target.regs
            )
            for (ra, rn, _), key in zip(handle[2], handle[5], strict=True)
        )

    def _complete(self, handle) -> None:
        if handle[4] in self.broken or not self._remote_ok(handle):
            self.broken.add(handle[4])  # remote access error breaks the connection
            handle[3] = "ERR"
            return
        self._move(handle)
        handle[3] = "DONE"

    @staticmethod
    def _move(handle) -> None:
        op, local, remote = handle[:3]
        for (la, ln, _), (ra, _rn, _) in zip(local, remote, strict=True):
            if op == "READ":
                ctypes.memmove(la, ra, ln)
            else:
                ctypes.memmove(ra, la, ln)

    def transfer(self, handle, notif=b"") -> str:
        if self.fail_next:
            self.fail_next = False
            handle[3] = "ERR"
            return "ERR"
        if self.stall:
            handle[3] = "PROC"
            self._stalled.append(handle)
            return "PROC"
        self._complete(handle)
        return handle[3]

    def finish_stalled(self) -> int:
        """Complete every stalled transfer now; returns how many moved bytes.

        A stalled transfer whose remote range was deregistered meanwhile ends
        in ``ERR`` and moves nothing, as on a NIC.
        """
        n = 0
        for handle in self._stalled:
            self._complete(handle)
            n += handle[3] == "DONE"
        self._stalled.clear()
        return n

    def check_xfer_state(self, handle) -> str:
        return handle[3]

    def release_xfer_handle(self, handle) -> None:
        self.released.append(tuple(handle[:1]))


class FakeClock:
    """Manually advanced monotonic clock."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def reset_fake_agents() -> None:
    """Forget every fake agent and NIC key (call from fixtures between tests)."""
    FakeNixlAgent.registry.clear()
    FakeNixlAgent._keys_by_range.clear()
    FakeNixlAgent._next_key = 1


class FakeCxl:
    """One CXL device and its resource manager.

    The device is a temporary file that region mappers map with the real
    ``DaxRegionMapper``. Regions are reserved first fit, so a released range
    is handed out again under a new region id, as the real resource manager
    does. ``get_access`` checks a handle the way ``GET_ACCESS`` does. Every
    region mapper of the pool maps the same bytes.
    """

    def __init__(self, size: int = 1 << 20) -> None:
        import mmap
        import tempfile

        self._file = tempfile.TemporaryFile(prefix="fake-cxl-")
        self._file.truncate(size)
        self.path = f"/proc/self/fd/{self._file.fileno()}"
        self._view = mmap.mmap(self._file.fileno(), size)
        self.size = size
        self.next_id = 1
        self.regions: dict[int, object] = {}
        self.released: list[int] = []
        self.accesses: list[int] = []
        self.fail_release = False

    @property
    def used(self) -> int:
        return sum(h.length for h in self.regions.values())

    def _fit(self, size: int) -> int | None:
        start = 0
        for h in sorted(self.regions.values(), key=lambda h: h.offset):
            if h.offset - start >= size:
                return start
            start = h.offset + h.length
        return start if self.size - start >= size else None

    def reserve(self, owner, size):
        from maru_shm.types import MaruHandle

        offset = self._fit(size)
        if offset is None:
            return None
        handle = MaruHandle(
            region_id=self.next_id,
            offset=offset,
            length=size,
            auth_token=1000 + self.next_id,
        )
        self.regions[self.next_id] = handle
        self.next_id += 1
        return handle

    def release(self, owner, region_id):
        if self.fail_release:
            return False
        self.regions.pop(region_id)
        self.released.append(region_id)
        return True

    def get_access(self, handle):
        from types import SimpleNamespace

        self.accesses.append(handle.region_id)
        if self.regions.get(handle.region_id) != handle:
            raise RuntimeError("GetAccess failed with status -13")
        return SimpleNamespace(
            dax_path=self.path,
            device_uuid="",
            offset=handle.offset,
            length=handle.length,
            status=0,
        )

    def page(self, offset: int, length: int) -> bytes:
        return bytes(self._view[offset : offset + length])


class FakeDeviceMapper:
    """Maps a region as a made-up address; nothing reads or writes it."""

    base = 0x7F0000000000

    def __init__(self, cxl: FakeCxl) -> None:
        self.cxl = cxl
        self.mapped: list[int] = []
        self.unmapped: list[int] = []  # unmapped and kept out of use
        self.discarded: list[int] = []  # never registered: free at once

    def map(self, dax_path, device_uuid, offset, length):
        self.mapped.append(offset)
        return self.base + offset

    def unmap(self, addr, length):
        self.unmapped.append(addr - self.base)

    def discard(self, addr, length):
        self.discarded.append(addr - self.base)


class FakeMemoryNode:
    """A memory node's region mapper served on loopback TCP.

    The service, server, client and device mapper are the real ones; only the
    resource manager, the device (a file) and NIXL are fakes.
    """

    def __init__(self, cxl: FakeCxl, name: str = "mem1", port: int = 0) -> None:
        self.cxl = cxl
        self.name = name
        self._port = port
        self.restarts = 0
        self.boots = 0
        self._start()

    def _start(self) -> None:
        import threading

        from maru_region_mapper.mapper import DaxRegionMapper
        from maru_region_mapper.server import RegionMapServer
        from maru_region_mapper.service import RegionMapService
        from maru_remote.transport import NixlTransport

        epoch = f"{self.name}-e{self.restarts}"
        self.agent = FakeNixlAgent(f"maru-region-mapper-{epoch}")
        self.transport = NixlTransport(self.agent.name, agent=self.agent)
        self.mapper = DaxRegionMapper({})
        self.service = RegionMapService(
            self.cxl,
            self.mapper,
            self.transport,
            node_id=self.name,
            epoch=epoch,
            boot_id=f"{self.name}-boot{self.boots}",
        )
        self.server = RegionMapServer(self.service, "127.0.0.1", self._port)
        self._port = self.server.address[1]
        self.address = f"127.0.0.1:{self._port}"
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self.server.stop()
        self._thread.join(timeout=5)
        self.service.close()

    def restart(self, reboot: bool = False) -> None:
        """End the process (and boot the node again if ``reboot``): every
        registration and connection goes."""
        self.stop()
        self.restarts += 1
        self.boots += int(reboot)
        self._start()
