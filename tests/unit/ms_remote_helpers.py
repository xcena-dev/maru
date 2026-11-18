# SPDX-License-Identifier: Apache-2.0
"""Real-node remote gate setup and byte/quarantine invariants."""

import hashlib
import mmap
import os

from maru_common.ms_rpc import PLACEHOLDER_OP, OpOrigin
from maru_common.remote_protocol import (
    REMOTE_WIRE_VERSION,
    CancelRequest,
    DropRequest,
    HelloRequest,
    PingRequest,
    RemoteWriteDoneRequest,
    ReserveRequest,
    TransferEnd,
    Written,
    peer_origin,
)
from maru_common.remote_protocol import (
    RemoteMessageType as R,
)
from maru_common.storage_types import (
    REMOTE_CAPABILITY,
    ItemResult,
    Key,
    PoolKind,
    Status,
)
from maru_remote.region_map import load_key
from maru_remote.transport import TransferState, buffer_address
from maru_storage_service.config import parse_node_config
from maru_storage_service.drivers.ssd_alignment import aligned_buffer
from maru_storage_service.page_table import PageState
from maru_storage_service.remote_client import RemoteConnection
from tests.unit.ms_node_helpers import (
    PAGE,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)
from tests.unit.remote_fakes import key_file
from tests.unit.remote_transport_fakes import FakeTransport
from tests.unit.test_ms_cxl_tier import FakeRm, anon_region
from tests.unit.test_ms_demotion import Regions
from tests.unit.test_ms_fetch import Loader
from tests.unit.test_ms_remote_tier import fake_transport, free_port

NS = "ns"


def sentinel(name, revision=1):
    seed = hashlib.sha256(f"{name}:{revision}".encode()).digest()
    return (seed * 94)[:3000]


def state(node, location):
    ref = node.pools.find(location)
    return None if ref is None else node.pools.page(ref).state


class PersistentRegions(Regions):
    def node_map(self, pool):
        handle = pool.region.handle
        if handle.region_id not in self.fds:
            fd = os.memfd_create(f"gate-cxl-{handle.region_id}")
            os.ftruncate(fd, handle.length)
            self.fds[handle.region_id] = fd
        return mmap.mmap(self.fds[handle.region_id], handle.length)


class Peer:
    """RemoteConnection client, without a GPU StorageService."""

    def __init__(self, gate, memory, node_id="raw-gpu", epoch="run1"):
        self.gate, self.memory = gate, memory
        self.node_id, self.epoch = node_id, epoch
        self.transport = FakeTransport(f"gate-{node_id}-{epoch}")
        self.local = mmap.mmap(-1, PAGE)
        self.registration = self.transport.register(buffer_address(self.local), PAGE)
        self.ops = OpOrigin(peer_origin(node_id, epoch))
        self.generation = 0
        self.conn = None
        self.connect()

    def connect(self):
        self.generation += 1
        request = HelloRequest(
            REMOTE_WIRE_VERSION,
            self.node_id,
            self.epoch,
            "gate-boot",
            os.getpid(),
            self.memory.server.server_epoch,
            self.transport.agent.name,
            self.transport.metadata(),
            PAGE,
            self.generation,
            [REMOTE_CAPABILITY],
        )
        self.conn, self.hello = RemoteConnection.open(
            self.memory.config.remote_server.listen, load_key(key_file()), request, 2
        )
        assert self.hello.status == Status.OK, self.hello
        for pool in self.hello.pools:
            if pool.metadata:  # a staged pool (SSD) is never reached by RDMA
                self.transport.add_peer(
                    pool.metadata, peer=self.hello.agent_name, timeout_s=2
                )

    def call(self, code, request):
        if hasattr(request, "op"):
            request = self.ops.stamp(request)
        answer = self.conn.call(code, request, 3)
        if hasattr(request, "op"):
            self.ops.resolved(request.op.seq)
        return answer

    def reserve(self, pages=1):
        return self.call(R.RESERVE, ReserveRequest(PLACEHOLDER_OP, pages))

    def done(self, location, name, end=TransferEnd.DONE):
        return self.call(
            R.WRITE_DONE,
            RemoteWriteDoneRequest(
                PLACEHOLDER_OP,
                [Written(Key(NS, name), location, len(sentinel(name)), end)],
            ),
        )

    def write(self, name, revision=1):
        (credit,) = self.reserve().credits
        value = sentinel(name, revision)
        self.gate.values[Key(NS, name)] = value
        self.local[: len(value)] = value
        pool = next(p for p in self.hello.pools if p.pool_id == credit.pool_id)
        pending = self.transport.post(
            "WRITE",
            self.hello.agent_name,
            [(buffer_address(self.local), pool.base + credit.offset, len(value))],
        )
        assert wait_until(
            lambda: pending.poll_state() == TransferState.DONE,
            timeout=2,
            what="client WRITE DONE",
        )
        assert self.done(credit, name).items[0].result == ItemResult.ADDED
        (location,) = self.gate.locations(name, self.memory)
        return location

    def cancel(self, locations):
        for location in locations:
            self.gate.permit(self.memory, location, "not posted")
        return self.call(R.CANCEL, CancelRequest(PLACEHOLDER_OP, locations=locations))

    def drop(self, name, location):
        return self.call(R.DROP, DropRequest(PLACEHOLDER_OP, Key(NS, name), location))

    def ping(self):
        return self.call(R.PING, PingRequest())

    def close(self):
        if self.conn is not None:
            self.conn.close()
        self.transport.deregister(self.registration)
        self.transport.close()
        self.local.close()


class Gate:
    def __init__(self, server, url, workers, monkeypatch, ssd_dir):
        self.server, self.url = server, url
        self.workers, self.monkeypatch = workers, monkeypatch
        self.root = short_tmpdir()
        self.ssd_dir = ssd_dir
        self.nodes, self.clients, self.cleanups = [], [], []
        self.values, self.generations = {}, {}
        self.quarantined, self.evidence, self.violations = set(), {}, []
        self.regions = PersistentRegions()
        self.monkeypatch.setattr(
            "maru_handler.client.pool_maps.CxlRegionMapping", self.regions.worker_map
        )

    def watch(self, node):
        for table in node.pools._tables.values():
            original = table.move

            def move(page, src, dst, table=table, original=original):
                token = (id(table), page.index, page.generation)
                if dst == PageState.QUARANTINED:
                    self.quarantined.add(token)
                if src == PageState.QUARANTINED and dst == PageState.FREE:
                    if token not in self.evidence:
                        self.violations.append((table.pool_id, token))
                return original(page, src, dst)

            self.monkeypatch.setattr(table, "move", move)
        return node

    def permit(self, node, location, reason):
        table = node.pools._tables[location.pool_id]
        token = (id(table), location.offset // PAGE, location.generation)
        self.evidence[token] = reason

    def memory(self, tiers=("dram",), *, node_id="mem1", port=None, rm=None, **options):
        port = port or free_port()
        stores = []
        for kind in tiers:
            raw = {
                "store": kind,
                "size": 32 * PAGE,
                "high_watermark": 1.0,
                "low_watermark": 0.9,
            }
            if kind == "dram":
                raw.update(hugepage=False)
            elif kind == "cxl":
                raw.update(region_size=32 * PAGE, rm="127.0.0.1:1")
            else:
                raw.update(path=f"{self.ssd_dir}/{node_id}-ssd")
            stores.append(raw)
        raw = {
            "node_id": node_id,
            "role": "memory",
            "server": self.url,
            "page_size": PAGE,
            "remote_server": {
                "listen": f"127.0.0.1:{port}",
                "key_file": key_file(),
                "peer_quarantine_max": 8 * PAGE,
                "quarantine_max": 16 * PAGE,
            },
            "tiers": stores,
            "budgets": self.budgets(),
        }
        kwargs = {"make_transport": fake_transport}
        if "cxl" in tiers:
            kwargs.update(region_source=rm or FakeRm(), map_region=anon_region)
        kwargs.update(options)
        node = start_node(parse_node_config(raw), **kwargs)
        self.nodes.append(node)
        return self.watch(node)

    @staticmethod
    def budgets():
        return {
            "credit_max_bytes": 4 * PAGE,
            "credit_total_max_bytes": 8 * PAGE,
            "credit_refill_low": PAGE,
            "credit_refill_high": 2 * PAGE,
            "commit_pending_max_bytes": 8 * PAGE,
            "transfer_reserve_bytes": PAGE,
        }

    def gpu(self, memory, tiers=("dram",), *, node_id="gpu1", timeout_s=3, **options):
        stores = []
        for kind in tiers:
            raw = {
                "store": kind,
                "size": 32 * PAGE,
                "high_watermark": 1.0,
                "low_watermark": 0.9,
            }
            if kind == "dram":
                raw.update(hugepage=False)
            else:
                raw.update(region_size=32 * PAGE, rm="127.0.0.1:1")
            stores.append(raw)
        stores.append(
            {
                "store": "remote",
                "url": [memory.config.remote_server.listen],
                "key_file": key_file(),
                "timeout_s": timeout_s,
            }
        )
        raw = {
            "node_id": node_id,
            "socket": f"{self.root}/{node_id}.sock",
            "server": self.url,
            "page_size": PAGE,
            "tiers": stores,
            "budgets": self.budgets(),
        }
        kwargs = {"make_transport": fake_transport}
        if "cxl" in tiers:
            kwargs.update(region_source=FakeRm(), map_region=self.regions.node_map)
        kwargs.update(options)
        node = start_node(parse_node_config(raw), **kwargs)
        self.nodes.append(node)
        self.watch(node)
        remote = node.tier("remote")
        if remote.capabilities():
            wait_until(lambda: remote.ready(memory.config.node_id), what="remote ready")
        return node

    def peer(self, memory, *args, **kwargs):
        client = Peer(self, memory, *args, **kwargs)
        self.clients.append(client)
        return client

    def loader(self, node):
        client = Loader(node, self.server)
        self.clients.append(client)
        return client

    def put(self, node, name, revision=1):
        handler = self.workers(node)
        value = sentinel(name, revision)
        self.values[Key(NS, name)] = value
        handle = handler.alloc(len(value))
        handle.buf[:] = value
        assert handler.store(name, handle)
        return handler

    def locations(self, name, node=None):
        service = self.server.ms_service
        with service.lock:
            return [
                e.location
                for e in service.catalog.locations(Key(NS, name))
                if node is None or e.pool.node_id == node.config.node_id
            ]

    def pressure(self, node, tier):
        placement = node.manager.placement
        wait_until(
            lambda: all(
                t <= placement._clock() for t in list(placement._backoff.values())
            ),
            timeout=2,
            what="placement backoff",
        )
        free = sum(
            table.bytes[PageState.FREE]
            for pool_id, table in node.pools._tables.items()
            if node.pools.tier_of(pool_id) == tier
        )
        node.manager.want_room(tier, free + PAGE)

    def moved(self, node, name):
        wait_until(
            lambda: not self.locations(name, node), what=f"{name} source removed"
        )

        def applied():
            with node.pools._lock:
                return all(
                    page.key != Key(NS, name) or page.state != PageState.READY
                    for table in node.pools._tables.values()
                    for page in table.pages
                )

        wait_until(applied, what="local move result applied")
        wait_until(lambda: node.manager._remote_writing == 0, what="writes ended")

    def check(self):
        assert not self.violations, self.violations
        for node in self.nodes:
            node.pools.check()
        service = self.server.ms_service

        def swept():
            with service.lock:
                return not service.catalog.sweeping()

        wait_until(swept, what="Catalog retired pool sweep")
        with service.lock:
            entries = [
                e
                for by_pool in service.catalog._by_key.values()
                for e in by_pool.values()
            ]
            for entry in entries:
                location = entry.location
                value = self.values[entry.key]
                token = (entry.key, location.pool_id, location.generation)
                assert self.generations.setdefault(token, value) == value
                owners = [
                    n
                    for n in reversed(self.nodes)
                    if n.transfers is not None
                    and location.pool_id in (n.transfers._maps | n.transfers._kept)
                    and not (n.transfers._maps | n.transfers._kept)[
                        location.pool_id
                    ].closed
                ]
                if location.kind == PoolKind.SSD:
                    owners = [
                        n
                        for n in self.nodes
                        if n.tier("ssd") is not None
                        and n.tier("ssd").pool.pool_id == location.pool_id
                    ]
                    assert owners, location
                    pool = owners[-1].tier("ssd").pool
                    with aligned_buffer(
                        pool.io_size(location.length), pool.alignment.memory
                    ) as buffer:
                        pool.read(location.offset, location.length, buffer)
                        actual = bytes(buffer[: location.length])
                else:
                    assert owners, location
                    actual = bytes(
                        (owners[0].transfers._maps | owners[0].transfers._kept)[
                            location.pool_id
                        ][location.offset : location.offset + location.length]
                    )
                assert actual == value, (entry.key, location)
        for table_id, index, generation in self.quarantined:
            for node in self.nodes:
                for table in node.pools._tables.values():
                    if id(table) == table_id:
                        page = table.pages[index]
                        if (table_id, index, generation) not in self.evidence:
                            assert (
                                page.generation == generation
                                and page.state != PageState.FREE
                            )

    def close(self):
        for client in reversed(self.clients):
            client.close()
        for node in reversed(self.nodes):
            remote = node.tier("remote")
            if remote is not None and remote.transport is not None:
                remote.transport.stall(False)
                remote.transport.finish_stalled()
            node.stop()
        for cleanup in reversed(self.cleanups):
            cleanup()
        self.regions.close()
        remove_tree(self.root)
