# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""LocalServer: the UDS endpoint for this node's workers.

Per connection a reader answers in order, and what other threads finish later
(WRITE_DONE answers, NOTIFY) goes out in the order posted: from the
posting thread when nothing posted is ahead and the socket takes it without
waiting, else from a writer thread. All send under the send lock, so a worker
that stops reading holds up only its own threads.
"""

import collections
import grp
import logging
import os
import socket
import stat
import threading

from maru_common.node_protocol import (
    NODE_MESSAGE_CLASSES,
    NOTIFY_SEQUENCE,
    AttachRequest,
    AttachResponse,
    Budgets,
    NodeMessageType,
    Notify,
    TransferDefaults,
    peer_credentials,
    process_start_time,
    recv_frame,
    same_pid_namespace,
    send_frame,
    send_frame_nowait,
)
from maru_common.protocol import MessageHeader
from maru_common.serializer import DecodedRequest, MessageDecodeError, Serializer
from maru_common.storage_types import (
    MS_PROTOCOL,
    WORKER_RDMA_CAPABILITY,
    PoolDescriptor,
    SessionRole,
    Status,
)

from ..ownership import FileLock, LocalServerError, NodeOwnership
from .worker_registry import AttachIdentity, WorkerRecord
from .workers import WorkerOps

logger = logging.getLogger("maru_storage_service.local_server")

_LISTEN_BACKLOG = 128
_DISABLED = {
    NodeMessageType.LOCAL_PIN: "local pins are not supported",
    NodeMessageType.LOCAL_UNPIN: "local pins are not supported",
    NodeMessageType.HINT: "hints are not served",
    NodeMessageType.READ_JOIN: "worker RDMA is not served",
}


class Connection:
    """One worker connection."""

    def __init__(self, server: "LocalServer", sock: socket.socket):
        self.server = server
        self.sock = sock
        self.send_lock = threading.Lock()
        self.worker: WorkerRecord | None = None
        self.peer_pid, self.peer_uid, _ = peer_credentials(sock)
        self._closed = threading.Event()
        self._outbox: collections.deque = collections.deque()
        self._outbox_cv = threading.Condition()
        # A thread is sending a posted frame
        self._sending = False
        # The end of a frame a posting thread began: send_lock stays held for
        # the writer, which sends it before anything else
        self._rest: memoryview | None = None
        self._reader = threading.Thread(
            target=self._read_loop, name="maru-local-conn", daemon=True
        )
        self._writer = threading.Thread(
            target=self._write_loop, name="maru-local-send", daemon=True
        )

    def start(self) -> None:
        self._writer.start()
        self._reader.start()

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    def send(self, frame: bytes, fds=()) -> bool:
        """Send now; any error closes the connection (the stream is out of step)."""
        if self.closed:
            return False
        try:
            with self.send_lock:
                send_frame(self.sock, frame, fds)
            return True
        except (OSError, ValueError) as e:
            logger.debug("send to pid %d failed: %s", self.peer_pid, e)
            self.close()
            return False

    def respond(self, header: MessageHeader, response, fds=()) -> bool:
        return self.send(self.server.serializer.encode_response(header, response), fds)

    def post_response(self, header: MessageHeader, response) -> None:
        """Send from the writer thread."""
        self._post(self.server.serializer.encode_response(header, response))

    def post_notify(self, notify: Notify) -> None:
        self._post(
            self.server.serializer.encode(
                NodeMessageType.NOTIFY, notify, seq=NOTIFY_SEQUENCE
            )
        )

    def _post(self, frame: bytes) -> None:
        """Sent by this thread if nothing posted is ahead and the socket
        takes it at once; the writer sends the rest, in order."""
        with self._outbox_cv:
            if self.closed:
                return
            if self._outbox or self._sending or self._rest is not None:
                self._outbox.append(frame)
                self._outbox_cv.notify()
                return
            self._sending = True
        sent = None
        try:
            sent = self._send_nowait(frame)
        finally:
            with self._outbox_cv:
                self._sending = False
                if sent is None:
                    if not self.closed:
                        self._outbox.appendleft(frame)
                elif sent < len(frame):
                    if self.closed:
                        self.send_lock.release()
                    else:
                        self._rest = memoryview(frame)[sent:]
                if self._outbox or self._rest is not None:
                    self._outbox_cv.notify()

    def _send_nowait(self, frame: bytes) -> int | None:
        """The bytes the socket took at once; None if none went (the send
        lock is busy, or the socket full). After a part the send lock
        stays held."""
        if not self.send_lock.acquire(blocking=False):
            return None
        sent, failed = 0, False
        try:
            sent = send_frame_nowait(self.sock, frame)
        except (OSError, ValueError) as e:
            logger.debug("send to pid %d failed: %s", self.peer_pid, e)
            sent, failed = len(frame), True
        finally:
            if not 0 < sent < len(frame):
                self.send_lock.release()
        if failed:
            self.close()  # the stream is out of step
        return sent or None

    def _send_rest(self, rest: memoryview) -> None:
        """End a frame a posting thread began; it left the send lock held."""
        try:
            self.sock.sendall(rest)
        except (OSError, ValueError) as e:
            logger.debug("send to pid %d failed: %s", self.peer_pid, e)
            self.close()
        finally:
            self.send_lock.release()

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        with self._outbox_cv:
            self._outbox.clear()
            if self._rest is not None:  # never sent: its send lock goes
                self._rest = None
                self.send_lock.release()
            self._outbox_cv.notify_all()
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def join(self, timeout: float) -> None:
        for thread in (self._reader, self._writer):
            if thread.is_alive() and thread is not threading.current_thread():
                thread.join(timeout)

    def _write_loop(self) -> None:
        while True:
            with self._outbox_cv:
                while not self.closed and (
                    self._sending or (not self._outbox and self._rest is None)
                ):
                    self._outbox_cv.wait()
                if self.closed:
                    return
                rest, self._rest = self._rest, None
                frame = self._outbox.popleft() if rest is None else b""
                self._sending = True
            if rest is not None:
                self._send_rest(rest)
            else:
                self.send(frame)
            with self._outbox_cv:
                self._sending = False

    def _read_loop(self) -> None:
        try:
            while not self.closed:
                try:
                    got = recv_frame(self.sock, max_fds=0)
                except (OSError, ValueError) as e:
                    logger.debug("connection of pid %d broke: %s", self.peer_pid, e)
                    break
                if got is None:
                    break
                frame, _ = got
                try:
                    decoded = self.server.serializer.decode_request_payload(frame)
                except MessageDecodeError as e:
                    if e.header is None:
                        logger.warning("undecodable message from pid %d", self.peer_pid)
                        break
                    self.respond(
                        e.header, {"status": Status.REJECTED.value, "error": str(e)}
                    )
                    continue
                try:
                    self.server.dispatch(self, decoded)
                except Exception as e:
                    logger.exception("request 0x%02X failed", decoded.header.msg_type)
                    self.respond(
                        decoded.header,
                        {
                            "status": Status.REJECTED.value,
                            "error": f"internal error: {e}",
                        },
                    )
        finally:
            self.close()
            self.server.forget(self)
            try:
                self.sock.close()
            except OSError:
                pass


class LocalServer:
    def __init__(
        self,
        path: str,
        manager: WorkerOps | None,
        *,
        node_id: str,
        node_epoch: str,
        server_address: str,
        page_size: int,
        budgets: Budgets,
        pools: list[tuple[PoolDescriptor, int | None]],
        transfer_defaults: TransferDefaults | None = None,
        socket_group: str | None = None,
        capabilities: frozenset[str] = frozenset(),
        ownership: NodeOwnership | None = None,
    ):
        self.path = path
        # ATTACH capabilities this endpoint serves
        self.capabilities = capabilities
        self.socket_group = socket_group
        self.manager = manager
        self.node_id = node_id
        self.node_epoch = node_epoch
        self.server_address = server_address
        self.page_size = page_size
        self.budgets = budgets
        # (descriptor, fd or None); memfd indexes the fds sent with the reply
        self.pools = pools
        self.transfer_defaults = transfer_defaults or TransferDefaults()
        self.serializer = Serializer(NODE_MESSAGE_CLASSES)
        self._lock = threading.Lock()
        self._conns: set[Connection] = set()
        self._sock: socket.socket | None = None
        self._ownership = ownership or FileLock(f"{path}.lock")
        self._owns_ownership = ownership is None
        self._bound: tuple[int, int] | None = None  # (st_dev, st_ino) we created
        self._accept: threading.Thread | None = None
        self._stopping = False

    # ------------------------------------------------------------- lifecycle

    def claim(self) -> None:
        """Claim local ownership and bind (0660); start() accepts workers."""
        self._ownership.claim()
        self._remove_stale_socket()
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM | socket.SOCK_CLOEXEC)
        try:
            sock.bind(self.path)
            st = os.lstat(self.path)
            self._bound = (st.st_dev, st.st_ino)
            # The kernel checks write permission on connect: never world-writable
            os.chmod(self.path, 0o660)
            if self.socket_group is not None:
                os.chown(self.path, -1, grp.getgrnam(self.socket_group).gr_gid)
        except (OSError, KeyError) as e:
            sock.close()
            raise LocalServerError(f"cannot bind {self.path}: {e}") from e
        self._sock = sock

    @property
    def started(self) -> bool:
        """Whether it accepted connections (workers may have attached)."""
        return getattr(self, "_accept", None) is not None

    def start(self) -> None:
        """Listen and accept; claim() came first."""
        if self._sock is None or self.manager is None:
            raise LocalServerError("start() needs claim() and the StorageManager")
        try:
            self._sock.listen(_LISTEN_BACKLOG)
        except OSError as e:
            raise LocalServerError(f"cannot listen on {self.path}: {e}") from e
        self._accept = threading.Thread(
            target=self._accept_loop, name="maru-local-accept", daemon=True
        )
        self._accept.start()
        logger.info("LocalServer listening on %s", self.path)

    def _remove_stale_socket(self) -> None:
        try:
            st = os.lstat(self.path)
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(st.st_mode):
            raise LocalServerError(f"{self.path} exists and is not a socket")
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.connect(self.path)
        except (ConnectionRefusedError, FileNotFoundError):
            os.unlink(self.path)  # left by a StorageService that ended
            return
        finally:
            probe.close()
        raise LocalServerError(f"another StorageService is serving {self.path}")

    def stop(self) -> None:
        with self._lock:
            self._stopping = True
            conns = list(self._conns)
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()
        if self._accept is not None:
            self._accept.join(timeout=5.0)
        for conn in conns:
            conn.close()
        for conn in conns:
            conn.join(timeout=5.0)
        if self._bound is not None:
            # Unlink only the socket this instance made
            try:
                st = os.lstat(self.path)
                if (st.st_dev, st.st_ino) == self._bound:
                    os.unlink(self.path)
            except FileNotFoundError:
                pass
            self._bound = None
        if self._owns_ownership:
            self._ownership.close()

    def connections(self) -> list[Connection]:
        with self._lock:
            return list(self._conns)

    def forget(self, conn: Connection) -> None:
        with self._lock:
            self._conns.discard(conn)
        if conn.worker is not None and self.manager is not None:
            self.manager.workers.connection_lost(conn.worker, conn)

    def _accept_loop(self) -> None:
        listener = self._sock
        while listener is not None:
            try:
                sock, _ = listener.accept()
            except OSError:
                return
            sock.set_inheritable(False)
            try:
                conn = Connection(self, sock)
            except OSError:
                sock.close()
                continue
            with self._lock:
                if self._stopping:
                    sock.close()
                    return
                self._conns.add(conn)
            conn.start()

    # -------------------------------------------------------------- requests

    def _manager(self) -> WorkerOps:
        """Requests come after start(), which checked it is set."""
        if self.manager is None:
            raise LocalServerError("the StorageService is still starting")
        return self.manager

    def dispatch(self, conn: Connection, decoded: DecodedRequest) -> None:
        header = decoded.header
        code = NodeMessageType(header.msg_type)  # decoding checked the code
        _, response_cls = NODE_MESSAGE_CLASSES[code]
        if code in (NodeMessageType.NOTIFY, NodeMessageType.PEERS):
            conn.respond(
                header,
                {
                    "status": Status.REJECTED.value,
                    "error": f"{code.name} is not a request",
                },
            )
            return
        if code == NodeMessageType.ATTACH:
            self._attach(conn, decoded)
            return
        if code in _DISABLED:
            conn.respond(
                header, response_cls(status=Status.DISABLED, error=_DISABLED[code])
            )
            return
        rec = conn.worker
        if rec is None or rec.detached or rec.exited:
            conn.respond(
                header, response_cls(status=Status.REJECTED, error="not attached")
            )
            return
        request = decoded.request
        manager = self._manager()
        if rec.role == SessionRole.PROBE and code != NodeMessageType.DETACH:
            conn.respond(
                header,
                response_cls(status=Status.REJECTED, error="a probe only detaches"),
            )
            return
        if code == NodeMessageType.CREDIT:
            conn.respond(header, manager.credit(rec, decoded))
        elif code == NodeMessageType.CREDIT_RETURN:
            conn.respond(header, manager.credit_return(rec, request))
        elif code == NodeMessageType.WRITE_DONE:
            manager.write_done(
                rec,
                decoded,
                lambda response: conn.post_response(header, response),
                waiter=conn,
            )
        elif code == NodeMessageType.ABORT:
            conn.respond(header, manager.abort(rec, decoded))
        elif code == NodeMessageType.UNMAP_ACK:
            conn.respond(header, manager.unmap_ack(rec, request))
        elif code == NodeMessageType.DETACH:
            conn.respond(header, manager.workers.detach(rec))
            manager.workers.after_detach_reply(rec)
        else:  # every code of the table is handled above
            conn.respond(header, response_cls(status=Status.REJECTED, error="unknown"))

    def _attach(self, conn: Connection, decoded: DecodedRequest) -> None:
        header = decoded.header
        req: AttachRequest = decoded.request

        def reject(why: str) -> None:
            logger.warning("ATTACH from pid %d refused: %s", conn.peer_pid, why)
            conn.respond(header, AttachResponse(status=Status.REJECTED, error=why))

        if conn.worker is not None:
            return reject("this connection is already attached")
        if req.protocol != MS_PROTOCOL:
            return reject(f"protocol {req.protocol!r}; this node speaks {MS_PROTOCOL}")
        if not req.incarnation:
            return reject("incarnation is empty")
        if req.step_objects < 0:
            return reject(f"step_objects {req.step_objects} is negative")
        probe = req.role == SessionRole.PROBE
        if not probe:
            if not req.groups:
                return reject("groups is empty")
            if any(g.object_bytes <= 0 for g in req.groups):
                return reject("a group has no object bytes")
            largest = max(g.object_bytes for g in req.groups)
            if largest > self.page_size:
                return reject(
                    f"object_bytes {largest} is larger than the page size "
                    f"{self.page_size}"
                )
        try:
            same_ns = same_pid_namespace(conn.peer_pid)
            node_pid = conn.peer_pid
            pid_start = process_start_time(node_pid)
        except OSError as e:
            return reject(f"cannot read the peer process: {e}")
        if (
            not probe
            and WORKER_RDMA_CAPABILITY in req.capabilities
            and WORKER_RDMA_CAPABILITY in self.capabilities
            and not same_ns
        ):
            return reject("worker_rdma_v1 needs the same pid namespace")
        if same_ns and (req.pid != node_pid or req.pid_start != pid_start):
            return reject(
                f"pid {req.pid} (start {req.pid_start}) is not the peer, pid "
                f"{node_pid} (start {pid_start})"
            )
        ident = AttachIdentity(
            incarnation=req.incarnation,
            engine_id=req.engine_id,
            dp_index=req.dp_index,
            tp_rank=req.tp_rank,
            pid=node_pid,
            pid_start=pid_start,
            namespace=req.namespace,
        )
        manager = self._manager()
        if probe:
            rec, why = manager.workers.attach_probe(conn, ident)
        else:
            rec, why = manager.workers.attach(conn, ident)
        if rec is None:
            return reject(why)
        agreed = [c for c in req.capabilities if c in self.capabilities]
        rec.capabilities = frozenset(agreed)
        conn.worker = rec
        budgets = None if probe else self.budgets
        response = AttachResponse(
            status=Status.OK,
            node_id=self.node_id,
            node_epoch=self.node_epoch,
            server=self.server_address,
            page_size=self.page_size,
            pools=[] if probe else [descriptor for descriptor, _ in self.pools],
            budgets=budgets,
            transfer_defaults=self.transfer_defaults,
            local_pin=False,
            pid=node_pid,
            pid_start=pid_start,
            capabilities=agreed,
        )
        fds = [] if probe else [fd for _, fd in self.pools if fd is not None]
        if not conn.respond(header, response, fds):
            return
        logger.info(
            "%s %s attached: engine %s dp %d tp %d pid %d%s",
            req.role.value,
            req.incarnation,
            req.engine_id,
            req.dp_index,
            req.tp_rank,
            node_pid,
            ""
            if budgets is None
            else f"; credit {budgets.credit_refill_high // self.page_size} pages "
            f"at hand, up to {budgets.credit_max_bytes // self.page_size} pages",
        )
