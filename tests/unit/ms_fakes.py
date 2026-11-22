# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Fake StorageServices and workers for MsService tests; every request and
response goes through the wire encoding, so op digests use raw payloads."""

import dataclasses
import itertools
import json
import os
import uuid
from typing import Any

from maru_common.protocol import (
    AdminDeleteRequest,
    CommitItem,
    CommitRequest,
    LookupAndPinRequest,
    MessageType,
    NodeCloseRequest,
    NodeHeartbeatRequest,
    NodeOpenRequest,
    NodeRecoverListRequest,
    ProbeRequest,
    RetireRequest,
    UnpinRequest,
    WorkerCloseRequest,
    WorkerExitRequest,
    WorkerHeartbeatRequest,
    WorkerOpenRequest,
)
from maru_common.serializer import Serializer, op_digest
from maru_common.storage_types import (
    MS_PROTOCOL,
    CloseMode,
    ExitCause,
    Key,
    KeyLocation,
    Location,
    OpRef,
    PoolInfo,
    PoolKind,
    RmDomain,
    SessionRole,
    Visibility,
    make_generation,
)
from maru_server.ms_service import MsService
from maru_server.node_rpc import ms_handlers

PAGE = 4096
POOL_PAGES = 4096


class FakeClock:
    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_service(clock: FakeClock | None = None, **kwargs) -> MsService:
    kwargs.setdefault("session_ttl_s", 30.0)
    kwargs.setdefault("node_give_up_s", 600.0)
    return MsService(
        server_epoch=uuid.uuid4().hex, clock=clock or FakeClock(), **kwargs
    )


class Wire:
    """In-process transport through the serializer."""

    def __init__(self, service: MsService):
        self.service = service
        self.serializer = Serializer()
        self.handlers = ms_handlers(service)

    def call(self, code: MessageType, request: Any) -> Any:
        frame = self.serializer.encode(code, request)
        decoded = self.serializer.decode_request_payload(frame)
        response = self.handlers[int(code)](decoded.request, decoded.payload)
        reply = self.serializer.encode_response(decoded.header, response)
        _, out = self.serializer.decode_response(reply)
        return out


class RpcWire:
    """Like Wire, over a real ZMQ REQ socket."""

    def __init__(self, address: str, service: MsService, timeout_ms: int = 5000):
        import zmq

        self.service = service
        self.serializer = Serializer()
        self._context = zmq.Context()
        self.socket = self._context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
        self.socket.connect(address)

    def exchange(self, frame: bytes):
        """Send a raw frame; returns (header, response)."""
        self.socket.send(frame)
        return self.serializer.decode_response(self.socket.recv())

    def call(self, code: MessageType, request: Any) -> Any:
        return self.exchange(self.serializer.encode(code, request))[1]

    def close(self) -> None:
        self.socket.close()
        self._context.term()


def policy_service(tmp_path, clock: FakeClock | None = None):
    """A service with a policy file; ``preserve(*keys)`` rewrites it and
    reloads, as the maintenance thread would."""
    path = tmp_path / "policy.json"
    path.write_text("{}")
    service = make_service(clock or FakeClock(), policy_file=str(path))
    bumps = itertools.count(1)

    def preserve(*keys: Key) -> None:
        entries = [{"namespace": k.namespace, "key": k.key} for k in keys]
        path.write_text(json.dumps({"preserve": entries}))
        stat = os.stat(path)
        mtime = stat.st_mtime_ns + next(bumps) * 1_000_000
        os.utime(path, ns=(stat.st_atime_ns, mtime))
        service.check_policy()

    return service, Wire(service), preserve


def admin_delete(wire, *keys):
    return wire.call(MessageType.ADMIN_DELETE, AdminDeleteRequest(items=list(keys)))


def key(name: str, namespace: str = "ns") -> Key:
    return Key(namespace=namespace, key=name)


class _OpSender:
    """Numbers ops per origin and acks each result."""

    origin: str
    seq: int
    ack: int
    # sent instead of the service's server_epoch when set
    server_epoch: str | None = None

    def build(self, cls: type, **fields) -> Any:
        """A request with the next op and its digest."""
        self.seq += 1
        request = cls(op=OpRef(self.origin, self.seq, b"", self.ack), **fields)
        digest = op_digest(request)
        return dataclasses.replace(
            request, op=OpRef(self.origin, self.seq, digest, self.ack)
        )

    def send(self, code: MessageType, request: Any) -> Any:
        response = self.wire.call(code, request)
        self.ack = max(self.ack, request.op.seq)
        return response


class FakeNode(_OpSender):
    """A StorageService as MaruServer sees it."""

    def __init__(self, wire: Wire, node_id: str, boot: str = "boot-1"):
        self.wire = wire
        self.node_id = node_id
        self.boot = boot
        self.epoch = uuid.uuid4().hex
        self.origin = self.epoch
        self.seq = 0
        self.ack = 0
        self.token = ""
        self.epoch_seq = 0
        self.counter = 0
        self.last_open = None
        # NODE_OPEN capabilities it asks for
        self.capabilities: list[str] = []

    # -------------------------------------------------------------- pools

    @property
    def dram_id(self) -> str:
        return f"dram:{self.node_id}:{self.epoch}"

    def dram(self, generation: int = 1, pages: int = POOL_PAGES) -> PoolInfo:
        return PoolInfo(
            pool_id=self.dram_id,
            kind=PoolKind.DRAM,
            node_id=self.node_id,
            pool_generation=generation,
            capacity=pages * PAGE,
            page_size=PAGE,
        )

    def cxl(self, rm_domain: str = "rm1", region_id: int = 7) -> PoolInfo:
        return PoolInfo(
            pool_id=f"cxl:{rm_domain}:{region_id}",
            kind=PoolKind.CXL,
            node_id=self.node_id,
            pool_generation=1,
            capacity=POOL_PAGES * PAGE,
            page_size=PAGE,
            rm_domain=rm_domain,
            region_id=region_id,
            device_id="dev0",
            visibility=Visibility.COHERENT,
        )

    def loc(self, pool: PoolInfo | str, page: int, length: int = PAGE) -> Location:
        """A page of this run in ``pool`` with a fresh generation."""
        if isinstance(pool, str):
            kind = PoolKind(pool.split(":", 1)[0])
            pool_id = pool
        else:
            kind, pool_id = pool.kind, pool.pool_id
        self.counter += 1
        return Location(
            pool_id=pool_id,
            allocation_id=f"a{page}",
            generation=make_generation(self.epoch_seq, self.counter),
            offset=page * PAGE,
            length=length,
            kind=kind,
        )

    # ---------------------------------------------------------- requests

    def open_request(self, pools=None, rm_domains=None, **kw) -> NodeOpenRequest:
        return NodeOpenRequest(
            node_id=self.node_id,
            node_epoch=self.epoch,
            host_boot_id=self.boot,
            protocol=MS_PROTOCOL,
            capabilities=list(self.capabilities),
            pools=[self.dram()] if pools is None else list(pools),
            rm_domains=[RmDomain("rm1", ("dev0",))]
            if rm_domains is None
            else list(rm_domains),
            **kw,
        )

    def open(self, pools=None, rm_domains=None, **kw):
        response = self.wire.call(
            MessageType.NODE_OPEN, self.open_request(pools, rm_domains, **kw)
        )
        if response.status == "OK":
            self.token = response.node_session
            self.epoch_seq = response.epoch_seq
        self.last_open = response
        return response

    def restart(self, boot: str | None = None) -> None:
        """A new process: new node_epoch and op numbering."""
        self.epoch = uuid.uuid4().hex
        self.origin = self.epoch
        self.seq = self.ack = 0
        self.token = ""
        if boot is not None:
            self.boot = boot

    def envelope(self) -> dict:
        epoch = self.server_epoch or self.wire.service.server_epoch
        return {"session": self.token, "server_epoch": epoch}

    def heartbeat(self, usage=()):
        return self.wire.call(
            MessageType.NODE_HEARTBEAT,
            NodeHeartbeatRequest(**self.envelope(), usage=list(usage)),
        )

    def close(self, mode: CloseMode):
        return self.wire.call(
            MessageType.NODE_CLOSE, NodeCloseRequest(**self.envelope(), mode=mode)
        )

    def recover_list(self, pool_id: str, cursor: str | None = None):
        return self.wire.call(
            MessageType.NODE_RECOVER_LIST,
            NodeRecoverListRequest(**self.envelope(), pool_id=pool_id, cursor=cursor),
        )

    def commit(self, *items):
        """commit((key, location[, logical_bytes]), ...)"""
        commit_items = [
            CommitItem(
                key=it[0],
                location=it[1],
                logical_bytes=it[2] if len(it) > 2 else it[1].length,
            )
            for it in items
        ]
        return self.send(
            MessageType.COMMIT,
            self.build(CommitRequest, **self.envelope(), items=commit_items),
        )

    def retire(self, *items):
        return self.send(
            MessageType.RETIRE,
            self.build(
                RetireRequest,
                **self.envelope(),
                items=[KeyLocation(k, loc) for k, loc in items],
            ),
        )

    def worker_exit(self, *workers, cause: ExitCause = ExitCause.EXIT):
        return self.send(
            MessageType.WORKER_EXIT,
            self.build(
                WorkerExitRequest, **self.envelope(), workers=list(workers), cause=cause
            ),
        )

    def unpin(self, *pin_ids):
        return self.wire.call(
            MessageType.UNPIN,
            UnpinRequest(
                **self.envelope(),
                pin_ids=list(pin_ids),
            ),
        )


class FakeWorker(_OpSender):
    """A worker (or a probe-only scheduler) as MaruServer sees it."""

    def __init__(
        self,
        wire: Wire,
        node: FakeNode,
        role: SessionRole = SessionRole.WORKER,
        tp_rank: int = 0,
    ):
        self.wire = wire
        self.node = node
        self.role = role
        self.tp_rank = tp_rank
        self.incarnation = uuid.uuid4().hex
        self.origin = self.incarnation
        self.pid = 4242 + tp_rank
        self.pid_start = 99
        self.seq = 0
        self.ack = 0
        self.token = ""
        self.node_epoch = ""

    def open(self, node_epoch: str | None = None, **overrides):
        self.node_epoch = node_epoch or self.node.epoch
        fields = {
            "node_id": self.node.node_id,
            "node_epoch": self.node_epoch,
            "engine_id": "engine-a",
            "dp_index": 0,
            "tp_rank": self.tp_rank,
            "pid": self.pid,
            "pid_start": self.pid_start,
            "incarnation": self.incarnation,
            "namespace": "ns",
            "role": self.role,
            "protocol": MS_PROTOCOL,
            "capabilities": [],
        }
        fields.update(overrides)
        response = self.wire.call(MessageType.WORKER_OPEN, WorkerOpenRequest(**fields))
        if response.status == "OK":
            self.token = response.worker_session
        return response

    def ref(self):
        from maru_common.storage_types import WorkerRef

        return WorkerRef(
            engine_id="engine-a",
            dp_index=0,
            tp_rank=self.tp_rank,
            pid=self.pid,
            pid_start=self.pid_start,
            incarnation=self.incarnation,
            worker_session=self.token,
        )

    def envelope(self) -> dict:
        epoch = self.server_epoch or self.wire.service.server_epoch
        return {"session": self.token, "server_epoch": epoch}

    def heartbeat(self):
        return self.wire.call(
            MessageType.WORKER_HEARTBEAT, WorkerHeartbeatRequest(**self.envelope())
        )

    def close(self):
        return self.wire.call(
            MessageType.WORKER_CLOSE, WorkerCloseRequest(**self.envelope())
        )

    def probe(self, *keys, with_locations: bool = False):
        return self.wire.call(
            MessageType.PROBE,
            ProbeRequest(
                **self.envelope(), items=list(keys), with_locations=with_locations
            ),
        )

    def lookup(self, *keys):
        return self.send(
            MessageType.LOOKUP_AND_PIN,
            self.build(
                LookupAndPinRequest,
                **self.envelope(),
                items=list(keys),
            ),
        )

    def unpin(self, *pin_ids):
        return self.wire.call(
            MessageType.UNPIN, UnpinRequest(**self.envelope(), pin_ids=list(pin_ids))
        )
