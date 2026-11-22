# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Node configs and a worker speaking the node protocol directly."""

import itertools
import json
import os
import select
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from maru_common.ms_rpc import PLACEHOLDER_OP, OpOrigin
from maru_common.node_protocol import (
    AttachRequest,
    CreditRequest,
    GroupLayout,
    NodeMessageType,
    WriteDoneItem,
    WriteDoneRequest,
    process_start_time,
)
from maru_common.storage_types import MS_PROTOCOL, Key, Location
from maru_handler.client.storage_conn import StorageConn, close_fds
from maru_server.node_rpc import NodeRpcServer
from maru_server.node_server import NodeServer
from maru_shm.types import (
    MaruHandle,
    NodeClaim,
    NodeRegion,
    RegionHolder,
    StaleNodeEpochError,
)
from maru_storage_service.config import parse_node_config
from maru_storage_service.service import StorageService

KIB = 1 << 10
PAGE = 64 * KIB


def wait_until(pred, timeout: float = 10.0, interval: float = 0.01, what: str = ""):
    deadline = time.monotonic() + timeout
    while True:
        value = pred()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what or pred}")
        time.sleep(interval)


def listening(path: str) -> bool:
    """A probe connect succeeds (the file exists before listen())."""
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.connect(path)
        return True
    except OSError:
        return False
    finally:
        probe.close()


def short_tmpdir() -> str:
    """A short directory: AF_UNIX paths hold 108 bytes."""
    return tempfile.mkdtemp(prefix="maru-s2-", dir="/tmp")


def node_config(
    server_url: str,
    socket_path: str,
    *,
    node_id="gpu-test",
    page_size=PAGE,
    pages=16,
    high=0.9,
    low=0.8,
    budgets=None,
    hugepage=False,
    cxl_pages=None,
    region_pages=4,
    role="gpu",
):
    defaults = {
        "credit_max_bytes": 4 * page_size,
        "credit_total_max_bytes": 8 * page_size,
        "credit_refill_low": page_size,
        "credit_refill_high": 4 * page_size,
        "commit_pending_max_bytes": 4 * page_size,
        "transfer_reserve_bytes": page_size,
    }
    count = pages if cxl_pages is None else cxl_pages
    if count < 16:
        total = max(1, count // 2) * page_size
        defaults.update(
            credit_max_bytes=total,
            credit_total_max_bytes=total,
            credit_refill_high=total,
            commit_pending_max_bytes=max(1, count // 4) * page_size,
        )
    defaults.update(budgets or {})
    if cxl_pages is None:
        tier = {"store": "dram", "size": pages * page_size, "hugepage": hugepage}
    else:
        tier = {
            "store": "local_cxl",
            "size": cxl_pages * page_size,
            "region_size": region_pages * page_size,
        }
    if role == "memory":
        tier.update(high_watermark=high, low_watermark=low)
    return parse_node_config(
        {
            "node_id": node_id,
            "role": role,
            "socket": socket_path,
            "server": server_url,
            "page_size": page_size,
            "tiers": [tier],
            "budgets": defaults,
        }
    )


def start_node(config, **kwargs) -> StorageService:
    options = {
        "rpc_timeout_s": 0.3,
        "gpu_drain_grace_s": 0.3,
        "evict_interval_s": 0.02,
        "kept_backoff_s": 0.2,
    }
    options.update(kwargs)
    service = StorageService(config, **options)
    service.start()
    return service


class RawWorker:
    """A worker that sends node messages itself (no MaruClient)."""

    def __init__(self, path: str, incarnation: str | None = None):
        self.path = path
        self.incarnation = incarnation or os.urandom(16).hex()
        self.notices = []
        self.lost = False
        self.conn = StorageConn(
            path, on_notify=lambda n, c: self.notices.append(n), on_lost=self._lost
        )
        self.origin = OpOrigin(self.incarnation)
        self.response = None
        self.fds: list[int] = []

    def _lost(self, _conn) -> None:
        self.lost = True

    def attach_request(self, **overrides) -> AttachRequest:
        pid = os.getpid()
        fields = {
            "engine_id": "raw",
            "dp_index": 0,
            "tp_rank": 0,
            "pid": pid,
            "pid_start": process_start_time(pid),
            "incarnation": self.incarnation,
            "namespace": "ns",
            "groups": [GroupLayout(object_bytes=PAGE, layout="opaque")],
            "protocol": MS_PROTOCOL,
            "capabilities": [],
        }
        fields.update(overrides)
        return AttachRequest(**fields)

    def attach(self, **overrides):
        self.response, self.fds = self.conn.connect(
            self.attach_request(**overrides), 10.0
        )
        self.conn.start_notifications()
        return self.response

    def request(self, code: NodeMessageType, request, timeout: float = 10.0):
        return self.conn.request(code, request, timeout)

    def stamp(self, request):
        return self.origin.stamp(request)

    def credit(self, want: int, session: str = "session", stamped=None):
        request = stamped or self.stamp(
            CreditRequest(op=PLACEHOLDER_OP, worker_session=session, want_bytes=want)
        )
        response = self.request(NodeMessageType.CREDIT, request)
        if response.status.value == "OK":
            self.origin.resolved(request.op.seq)
        return response

    def write_done_request(
        self, credits: list[Location], keys: list[str], size: int = 8
    ):
        return self.stamp(
            WriteDoneRequest(
                op=PLACEHOLDER_OP,
                items=[
                    WriteDoneItem(key=Key("ns", k), credit=c, logical_bytes=size)
                    for c, k in zip(credits, keys, strict=True)
                ],
            )
        )

    def write_done(self, credits, keys, size: int = 8, timeout: float = 10.0):
        request = self.write_done_request(credits, keys, size)
        response = self.request(NodeMessageType.WRITE_DONE, request, timeout)
        self.origin.resolved(request.op.seq)
        return response

    def close(self) -> None:
        self.conn.close()
        close_fds(self.fds)
        self.fds = []


def remove_tree(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)


def publish(node, key, logical_bytes=8):
    """Write and publish one page through the manager."""
    import threading

    from maru_common.protocol import CommitItem

    credits = node.pools.credit("publisher", node.config.page_size)
    assert len(credits) == 1, node.pools.usage()
    ref, loc, why = node.pools.write_done(
        "publisher", credits[0], Key("ns", key), logical_bytes
    )
    assert ref is not None, why
    done, outcomes = threading.Event(), []

    def finish(items):
        outcomes.extend(items)
        done.set()

    node.manager.commit_written(
        [None], {0: (ref, CommitItem(Key("ns", key), loc, logical_bytes))}, finish
    )
    assert done.wait(10)
    return outcomes[0]


DOMAIN = "rm-test-id"


class FakeRm:
    """The RM's node requests over an in-memory owner table."""

    def __init__(self, lease_s: float = 900.0):
        self.lease_s = lease_s
        self.regions: dict[str, list[NodeRegion]] = {}
        self.epochs: dict[str, str] = {}
        self.next_id = 1
        self.unreachable = 0  # requests that fail before one gets through
        self.capacity: int | None = None  # bytes it may still give
        self.renew_error: Exception | None = None
        self.released: list[tuple[str, str]] = []
        self.freed: list[int] = []
        self.calls: list[str] = []
        # region_id -> holder -> its record; the RM keeps them across runs
        self.holds: dict[int, dict[str, RegionHolder]] = {}
        self.unholds: list[tuple[int, str]] = []
        self.unhold_errors = 0  # unholds that fail before one gets through
        self.holds_error: Exception | None = None  # what NODE_HOLDS raises
        self.dax_path = "/dev/dax9.0"  # what NODE_HOLD answers

    def _check(self, node_id: str, node_epoch: str) -> None:
        if self.epochs.get(node_id) != node_epoch:
            raise StaleNodeEpochError(f"node {node_id}: not the current run")

    def region(self, size: int) -> NodeRegion:
        rid = self.next_id
        self.next_id += 1
        return NodeRegion(
            handle=MaruHandle(
                region_id=rid, offset=rid << 30, length=size, auth_token=7
            ),
            requested_size=size,
            dax_path="/dev/dax9.0",
            device_uuid="dev-uuid",
        )

    def node_claim(self, node_id, node_epoch):
        self.calls.append("claim")
        if self.unreachable:
            self.unreachable -= 1
            raise ConnectionRefusedError("RM down")
        self.epochs[node_id] = node_epoch
        regions = [
            NodeRegion(
                r.handle,
                r.requested_size,
                r.dax_path,
                r.device_uuid,
                holders=list(self.holds.get(r.handle.region_id, {}).values()),
            )
            for r in self.regions.get(node_id, [])
        ]
        return NodeClaim(rm_domain=DOMAIN, lease_s=self.lease_s, regions=regions)

    def hold(self, region: NodeRegion, holder: str, epoch: str):
        """This process holds ``region`` as ``holder`` in run ``epoch`` (set
        up by a test); a remote agent's hold has no epoch."""
        import socket

        from maru_common.node_protocol import process_identity

        boot, pid, pid_start, pid_ns = process_identity()
        record = RegionHolder(
            holder, epoch, socket.gethostname(), boot, pid, pid_start, pid_ns
        )
        self.holds.setdefault(region.handle.region_id, {})[holder] = record

    def node_hold(self, node_id, node_epoch, handle, holder, **who):
        from types import SimpleNamespace

        self.calls.append("hold")
        self._check(node_id, node_epoch)
        region = next(
            r
            for r in self.regions.get(node_id, [])
            if r.handle.region_id == handle.region_id
        )
        self.hold(region, holder, node_epoch)
        return SimpleNamespace(dax_path=self.dax_path)

    def node_holds(self, node_id, node_epoch):
        self.calls.append("holds")
        if self.holds_error is not None:
            raise self.holds_error
        self._check(node_id, node_epoch)
        return [
            NodeRegion(
                r.handle,
                r.requested_size,
                r.dax_path,
                r.device_uuid,
                holders=list(self.holds.get(r.handle.region_id, {}).values()),
            )
            for r in self.regions.get(node_id, [])
        ]

    def unhold(self, handle, holder, instance):
        if self.unhold_errors:
            self.unhold_errors -= 1
            raise ConnectionRefusedError("RM down")
        self.unholds.append((handle.region_id, holder))
        self.holds.get(handle.region_id, {}).pop(holder, None)

    def node_alloc(self, node_id, node_epoch, size, dax_path=""):
        self.calls.append("alloc")
        self._check(node_id, node_epoch)
        if self.capacity is not None:
            if self.capacity < size:
                raise RuntimeError("alloc failed (-28): no space")
            self.capacity -= size
        region = self.region(size)
        self.regions.setdefault(node_id, []).append(region)
        return region

    def node_free(self, node_id, node_epoch, handle):
        self._check(node_id, node_epoch)
        self.freed.append(handle.region_id)
        self.regions[node_id] = [
            r for r in self.regions[node_id] if r.handle.region_id != handle.region_id
        ]

    def node_renew(self, node_id, node_epoch):
        self.calls.append("renew")
        if self.renew_error is not None:
            raise self.renew_error
        self._check(node_id, node_epoch)
        return self.lease_s

    def node_release(self, node_id, node_epoch):
        self._check(node_id, node_epoch)
        self.released.append((node_id, node_epoch))
        freed = len(self.regions.pop(node_id, []))
        del self.epochs[node_id]
        return freed


def anon_region(pool):
    """Stands in for the region's device in the node's own mapping."""
    import mmap

    return mmap.mmap(-1, pool.region.handle.length)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class MetadataServer:
    """A NodeRpcServer serving on a thread, and REQ wires to it."""

    def __init__(self, server=None, **kwargs):
        self.server = server or NodeServer()
        port = free_port()
        self.address = f"tcp://127.0.0.1:{port}"
        self.rpc = NodeRpcServer(self.server, host="127.0.0.1", port=port, **kwargs)
        self.thread = threading.Thread(target=self.rpc.start, daemon=True)
        self.thread.start()
        self.wires = []

    def wire(self, timeout_ms=5000):
        from tests.unit.ms_fakes import RpcWire

        wire = RpcWire(self.address, self.server.ms_service, timeout_ms=timeout_ms)
        self.wires.append(wire)
        return wire

    def close(self):
        for wire in self.wires:
            wire.close()
        self.rpc.stop()
        self.thread.join(5)
        self.server.close()
        assert not self.thread.is_alive()


WORKTREE = str(Path(__file__).resolve().parents[2])
AGENT = str(Path(__file__).resolve().parent / "ms_worker_agent.py")


class Agent:
    """A MaruHandler in a separate worker process."""

    def __init__(
        self,
        socket_path: str,
        *,
        engine: str = "eng",
        namespace: str = "ns",
        chunk: int = PAGE,
        log_dir: str | None = None,
        read_only: bool = False,
        store_timeout_s: float | None = None,
        cxl_paths: dict[int, str] | None = None,
    ):
        """``cxl_paths``: files standing in for CXL regions, by region id."""
        env = dict(os.environ)
        env["PYTHONPATH"] = WORKTREE + os.pathsep + env.get("PYTHONPATH", "")
        if read_only:
            env["AGENT_READ_ONLY"] = "1"
        if cxl_paths:
            env["AGENT_CXL_PATHS"] = json.dumps(
                {str(k): v for k, v in cxl_paths.items()}
            )
        if store_timeout_s is not None:
            env["AGENT_STORE_TIMEOUT_S"] = str(store_timeout_s)
        self.log_path = os.path.join(
            log_dir or tempfile.gettempdir(), f"agent-{os.urandom(4).hex()}.log"
        )
        self._log = open(self.log_path, "wb")
        self.proc = subprocess.Popen(
            [sys.executable, AGENT, socket_path, engine, namespace, str(chunk)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._log,
            env=env,
            cwd=WORKTREE,
        )
        self.pid = self.proc.pid
        self.incarnation = None

    def call(self, op: str, timeout: float = 30.0, **args):
        self.proc.stdin.write((json.dumps({"op": op, **args}) + "\n").encode())
        self.proc.stdin.flush()
        ready, _, _ = select.select([self.proc.stdout], [], [], timeout)
        if not ready:
            raise AssertionError(f"agent {self.pid}: no answer to {op} ({self.log()})")
        line = self.proc.stdout.readline()
        if not line:
            raise AssertionError(f"agent {self.pid} ended ({self.log()})")
        reply = json.loads(line)
        if not reply["ok"]:
            raise AgentError(reply["error"])
        return reply["result"]

    def connect(self):
        info = self.call("connect")
        self.incarnation = info["incarnation"]
        assert info["module"].startswith(WORKTREE), info["module"]
        return info

    def log(self) -> str:
        self._log.flush()
        try:
            with open(self.log_path, "rb") as f:
                return f.read()[-4000:].decode(errors="replace")
        except OSError:
            return ""

    def kill(self) -> None:
        self.proc.kill()
        self.proc.wait(10)

    def stop(self) -> None:
        if self.proc.poll() is None:
            try:
                self.call("exit", timeout=10)
            except (AssertionError, AgentError, OSError, ValueError):
                pass
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(10)
        self._log.close()
        try:
            os.unlink(self.log_path)
        except OSError:
            pass


class AgentError(RuntimeError):
    """The agent's operation raised."""


class FakeFetchService:
    """Lease FETCH replies over a node's real UDS endpoint."""

    def __init__(self, node, capabilities=None):
        from maru_common.storage_types import FETCH_CAPABILITY, LEASE_CAPABILITY

        self.node = node
        node.local.capabilities = frozenset(
            [FETCH_CAPABILITY, LEASE_CAPABILITY]
            if capabilities is None
            else capabilities
        )
        self._dispatch = node.local.dispatch
        node.local.dispatch = self.dispatch
        self.lock = threading.RLock()
        self.fetches = {}
        self._next_lease = itertools.count(1)
        self.busy_fetches = 0
        self.leases = set()
        self.requests = []
        self.releases = []
        self.drop_release_reply = False
        self.release_gate = None
        self.release_results = []

    def dispatch(self, conn, decoded):
        from maru_common.node_protocol import (
            FetchCancelResponse,
            FetchReleaseResponse,
            FetchResponse,
        )
        from maru_common.storage_types import ItemOutcome, ItemResult, Status

        code, request = NodeMessageType(decoded.header.msg_type), decoded.request
        with self.lock:
            self.requests.append((code, request))
            if code == NodeMessageType.FETCH:
                key = (request.op.origin, request.op.seq)
                entry = self.fetches.setdefault(
                    key,
                    {
                        "request": request,
                        "waiters": [],
                        "response": None,
                        "cancelled": False,
                    },
                )
                assert entry["request"].op.digest == request.op.digest
                if self.busy_fetches:
                    self.busy_fetches -= 1
                    conn.respond(decoded.header, FetchResponse(Status.BUSY))
                    return
                entry["waiters"].append((conn, decoded.header))
                if entry["response"] is not None:
                    conn.respond(decoded.header, entry["response"])
                elif entry["cancelled"]:
                    self.answer(key)
                return
            if code == NodeMessageType.FETCH_CANCEL:
                entry = self.fetches.get((request.target.origin, request.target.seq))
                if entry is not None and entry["response"] is None:
                    entry["cancelled"] = True
                conn.respond(decoded.header, FetchCancelResponse(Status.OK))
                return
            if code == NodeMessageType.FETCH_RELEASE:
                outcomes = []
                for lease in request.lease_ids:
                    outcomes.append(
                        ItemOutcome(
                            ItemResult.OK if lease in self.leases else ItemResult.STALE
                        )
                    )
                    self.leases.discard(lease)
                self.releases.extend(request.lease_ids)
                self.release_results.append(outcomes)
                if self.drop_release_reply:
                    return
                if self.release_gate is not None:
                    # Tests control this response without blocking UDS reception.
                    self.release_gate.append(
                        (
                            conn,
                            decoded.header,
                            FetchReleaseResponse(Status.OK, items=outcomes),
                        )
                    )
                else:
                    conn.respond(
                        decoded.header, FetchReleaseResponse(Status.OK, items=outcomes)
                    )
                return
        self._dispatch(conn, decoded)

    def answer(self, seq, *, deliver=True, timeout=False):
        from maru_common.node_protocol import FetchResponse
        from maru_common.storage_types import ItemOutcome, ItemResult, MissReason

        with self.lock:
            entry = self.fetches[seq]
            if entry["response"] is None:
                items = []
                for i, _ in enumerate(entry["request"].items):
                    if entry["cancelled"] or timeout:
                        items.append(
                            ItemOutcome(
                                ItemResult.MISS,
                                reason=MissReason.TIMEOUT if timeout else None,
                            )
                        )
                    else:
                        lease = next(self._next_lease)
                        self.leases.add(lease)
                        pool = self.node.local.pools[0][0].pool
                        location = Location(
                            pool.pool_id, "buffer", 1, i * PAGE, PAGE, pool.kind
                        )
                        items.append(
                            ItemOutcome(
                                ItemResult.HIT, location=location, lease_id=lease
                            )
                        )
                entry["response"] = FetchResponse("OK", items=items)
            if deliver:
                for conn, header in entry["waiters"]:
                    conn.respond(header, entry["response"])
                entry["waiters"].clear()
            return entry["response"]
