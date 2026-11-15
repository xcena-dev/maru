# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Helpers for StorageService and MaruClient tests: node configs, RawWorker
(node protocol by hand) and Agent (MaruHandler in a separate process)."""

import json
import os
import select
import shutil
import socket
import subprocess
import sys
import tempfile
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
from maru_storage_service.config import parse_node_config
from maru_storage_service.service import StorageService

WORKTREE = str(Path(__file__).resolve().parents[2])
AGENT = str(Path(__file__).resolve().parent / "ms_worker_agent.py")
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


def wait_demotions_applied(manager, timeout: float = 10.0) -> None:
    """Waits until demotions are quiet: no write in flight or waiting for
    its result at the start and the end of two eviction passes, and no
    result applied in between (COMMIT, FINALIZE_MOVE, RETIRE)."""
    passes = getattr(manager, "_test_passes", None)
    if passes is None:
        passes = manager._test_passes = [0]
        ended = manager._remote_writes_ended  # once a pass, on its thread

        def counted():
            try:
                ended()
            finally:
                passes[0] += 1

        manager._remote_writes_ended = counted

    def idle() -> bool:
        with manager._lock:
            return not (
                manager._writing
                or manager._remote_writing
                or manager._written
                or manager._remote_written
                or manager._remote_settled
            )

    def applied() -> tuple[int, ...]:
        with manager._lock:
            return tuple(
                sum(c.values())
                for c in (
                    manager.move_results,
                    manager.through_results,
                    manager.retire_results,
                )
            )

    deadline = time.monotonic() + timeout
    while True:
        left = max(0.1, deadline - time.monotonic())
        wait_until(idle, left, what="demotion writes ended")
        before, seen = applied(), passes[0]
        wait_until(
            lambda seen=seen: passes[0] >= seen + 2, left, what="eviction passes"
        )
        if idle() and applied() == before:
            return
        if time.monotonic() > deadline:
            raise AssertionError("demotions did not settle")


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
    node_id: str = "gpu-test",
    page_size: int = PAGE,
    pages: int = 16,
    high: float = 0.9,
    low: float = 0.8,
    budgets: dict | None = None,
    hugepage: bool = False,
    cxl_pages: int | None = None,
    region_pages: int = 4,
    cxl_fallback: bool = True,
    ssd_path: str | None = None,
    ssd_pages: int = 32,
):
    default_budgets = {
        "credit_max_bytes": 4 * page_size,
        "credit_total_max_bytes": 8 * page_size,
        "credit_refill_low": 1 * page_size,
        "credit_refill_high": 4 * page_size,
        "commit_pending_max_bytes": 4 * page_size,
        "transfer_reserve_bytes": 1 * page_size,
    }
    default_budgets.update(budgets or {})
    return parse_node_config(
        {
            "node_id": node_id,
            "socket": socket_path,
            "server": server_url,
            "page_size": page_size,
            "tiers": [
                {
                    "store": "dram",
                    "size": pages * page_size,
                    "hugepage": hugepage,
                    "high_watermark": high,
                    "low_watermark": low,
                }
            ]
            + (
                []
                if cxl_pages is None
                else [
                    {
                        "store": "cxl",
                        "size": cxl_pages * page_size,
                        "region_size": region_pages * page_size,
                        "credit_fallback": cxl_fallback,
                    }
                ]
            )
            + (
                []
                if ssd_path is None
                else [
                    {
                        "store": "ssd",
                        "size": ssd_pages * page_size,
                        "path": ssd_path,
                    }
                ]
            ),
            "budgets": default_budgets,
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


def remove_tree(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)
