# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruShmClient holds every region it maps (MAP_HOLD) until it unmaps it.

The resource manager keeps a freed or reclaimed region's range out of use
while it is held, so the hold must be there before the mapping and must
outlive it. A mock resource manager records what the client sends.
"""

import errno
import logging
import os
import struct
import threading

import pytest

from maru_shm.client import MaruShmClient
from maru_shm.constants import PROT_READ, PROT_WRITE
from maru_shm.ipc import (
    HEADER_SIZE,
    ErrorResp,
    FreeResp,
    GetAccessResp,
    MsgHeader,
    MsgType,
)
from maru_shm.types import MaruHandle
from maru_shm.uds_helpers import read_full, write_full

from .test_shm_client import MockResourceManagerServer

_RW = PROT_READ | PROT_WRITE


class HoldingRm:
    """A resource manager that answers MAP_HOLD, UNHOLD, GET_ACCESS and FREE.

    ``requests`` lists what arrived, in order: ("map_hold", region, holder,
    boot_id, pid, pid_start, pid_ns, client_id), ("unhold", region, holder,
    instance), ("get_access", region), ("free", region).
    """

    def __init__(self, tmp_path, map_hold_status=0, supports_holds=True):
        self.path = str(tmp_path / "device")
        with open(self.path, "wb") as f:
            f.write(b"\xa5" * 8192)
        self.map_hold_status = map_hold_status
        self.supports_holds = supports_holds
        self.requests = []
        self._lock = threading.Lock()
        self._server = MockResourceManagerServer(self._serve)
        self.address = self._server.start()

    def stop(self):
        self._server.stop()

    def kinds(self):
        with self._lock:
            return [r[0] for r in self.requests]

    def _record(self, *request):
        with self._lock:
            self.requests.append(request)

    def _reply(self, sock, msg_type, payload):
        hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
        write_full(sock, hdr.pack() + payload)

    def _access(self, status=0):
        if status != 0:
            return GetAccessResp(status=status).pack()
        return GetAccessResp(status=0, dax_path=self.path, offset=0, length=4096).pack()

    def _serve(self, sock):
        while True:
            try:
                hdr = MsgHeader.unpack(read_full(sock, HEADER_SIZE))
                payload = read_full(sock, hdr.payload_len)
            except (ConnectionError, OSError):
                return
            region = struct.unpack_from("<Q", payload)[0] if payload else 0
            if hdr.msg_type == MsgType.MAP_HOLD_REQ:
                if not self.supports_holds:
                    self._record("map_hold", region)
                    err = ErrorResp(status=-errno.ENOSYS, message="unknown request")
                    self._reply(sock, MsgType.ERROR_RESP, err.pack())
                    return  # As the RM does: it closes the connection.
                holder_len, boot_len, pid, _, pid_start, pid_ns = struct.unpack_from(
                    "<IIIIQQ", payload, 32
                )
                off = 64
                holder = payload[off : off + holder_len].decode()
                off += holder_len
                boot_id = payload[off : off + boot_len].decode()
                off += boot_len
                (cid_len,) = struct.unpack_from("<H", payload, off)
                client_id = payload[off + 2 : off + 2 + cid_len].decode()
                assert len(payload) == off + 2 + cid_len + 8  # Then the request ID.
                self._record(
                    "map_hold",
                    region,
                    holder,
                    boot_id,
                    pid,
                    pid_start,
                    pid_ns,
                    client_id,
                )
                self._reply(
                    sock, MsgType.MAP_HOLD_RESP, self._access(self.map_hold_status)
                )
            elif hdr.msg_type == MsgType.UNHOLD_REQ:
                holder_len, instance_len = struct.unpack_from("<II", payload, 32)
                holder = payload[40 : 40 + holder_len].decode()
                instance = payload[
                    40 + holder_len : 40 + holder_len + instance_len
                ].decode()
                self._record("unhold", region, holder, instance)
                self._reply(sock, MsgType.UNHOLD_RESP, FreeResp(status=0).pack())
            elif hdr.msg_type == MsgType.GET_ACCESS_REQ:
                self._record("get_access", region)
                self._reply(sock, MsgType.GET_ACCESS_RESP, self._access())
            elif hdr.msg_type == MsgType.FREE_REQ:
                self._record("free", region)
                self._reply(sock, MsgType.FREE_RESP, FreeResp(status=0).pack())


@pytest.fixture
def rm(tmp_path):
    server = HoldingRm(tmp_path)
    yield server
    server.stop()


def _handle(region_id=7):
    return MaruHandle(region_id=region_id, offset=0, length=4096, auth_token=42)


def _pid_start():
    with open("/proc/self/stat") as f:
        return int(f.read().rsplit(")", 1)[1].split()[19])


def test_mmap_holds_first_and_munmap_unholds(rm):
    client = MaruShmClient(address=rm.address)
    try:
        mm = client.mmap(_handle(), _RW)
        assert mm[0:1] == b"\xa5"
        # One request: the hold answers what GET_ACCESS would.
        assert rm.kinds() == ["map_hold"]
        _, region, holder, boot_id, pid, pid_start, pid_ns, client_id = rm.requests[0]
        assert region == 7
        assert client_id == client._client_id
        assert pid == os.getpid()
        assert pid_start == _pid_start()
        assert pid_ns == os.stat("/proc/self/ns/pid").st_ino
        with open("/proc/sys/kernel/random/boot_id") as f:
            assert boot_id == f.read().strip()
        # A name the resource manager accepts: printable ASCII, no spaces.
        assert holder.startswith("mmap:") and len(holder) <= 128
        assert all(0x21 <= ord(c) <= 0x7E for c in holder)

        assert client.mmap(_handle(), _RW) is mm  # Cached: no second hold.
        assert rm.kinds() == ["map_hold"]

        client.munmap(_handle())
        assert mm.closed
        assert rm.requests[1:] == [("unhold", 7, holder, holder)]
        client.munmap(_handle())  # Not mapped: nothing to drop.
        assert len(rm.requests) == 2
    finally:
        client.close()


def test_close_and_free_drop_the_holds(rm):
    client = MaruShmClient(address=rm.address)
    client.mmap(_handle(1), _RW)
    client.mmap(_handle(2), _RW)
    client.mmap(_handle(3), _RW)
    # The owner frees first; the resource manager drains until the unhold.
    client.free(_handle(2))
    assert rm.kinds()[3:] == ["free", "unhold"]
    assert rm.requests[4][1] == 2
    client.close()
    assert sorted(r[1] for r in rm.requests[5:]) == [1, 3]
    assert rm.kinds()[5:] == ["unhold", "unhold"]
    client.close()
    assert len(rm.requests) == 7


def test_two_clients_of_one_process_hold_separately(rm):
    a = MaruShmClient(address=rm.address)
    b = MaruShmClient(address=rm.address)
    try:
        a.mmap(_handle(), _RW)
        b.mmap(_handle(), _RW)
        holders = [r[2] for r in rm.requests]
        assert holders[0] != holders[1]
        a.munmap(_handle())
        # b still maps the region under its own hold.
        assert rm.requests[2] == ("unhold", 7, holders[0], holders[0])
    finally:
        a.close()
        b.close()


def test_a_mapping_that_cannot_be_closed_keeps_its_hold(rm):
    client = MaruShmClient(address=rm.address)
    mm = client.mmap(_handle(), _RW)
    exported = memoryview(mm)
    try:
        with pytest.raises(BufferError):
            client.munmap(_handle())
        client.close()
        # The process can still write through `exported`: no unhold, also
        # not when the region is mapped and unmapped once more.
        assert rm.kinds() == ["map_hold"]
        client.mmap(_handle(), _RW)
        client.munmap(_handle())
        client.close()
        assert rm.kinds() == ["map_hold", "map_hold"]
    finally:
        exported.release()
        mm.close()


@pytest.mark.parametrize("status", [-errno.ENOENT, -errno.EIO])
def test_a_refused_hold_maps_nothing(tmp_path, status):
    rm = HoldingRm(tmp_path, map_hold_status=status)
    client = MaruShmClient(address=rm.address)
    try:
        with pytest.raises(RuntimeError, match=f"Map hold failed with status {status}"):
            client.mmap(_handle(), _RW)
        assert client._mmap_cache == {}
        # A hold that may be recorded but is not durable is dropped again.
        assert rm.kinds() == (
            ["map_hold"] if status == -errno.ENOENT else ["map_hold", "unhold"]
        )
    finally:
        client.close()
        rm.stop()


def test_a_failed_mapping_drops_its_hold(rm):
    os.unlink(rm.path)
    client = MaruShmClient(address=rm.address)
    try:
        with pytest.raises(FileNotFoundError):
            client.mmap(_handle(), _RW)
        assert rm.kinds() == ["map_hold", "unhold"]
    finally:
        client.close()


def test_a_resource_manager_without_map_holds_is_asked_once(tmp_path, caplog):
    rm = HoldingRm(tmp_path, supports_holds=False)
    client = MaruShmClient(address=rm.address)
    try:
        with caplog.at_level(logging.WARNING, logger="maru_shm.client"):
            client.mmap(_handle(1), _RW)
            client.mmap(_handle(2), _RW)
        assert rm.kinds() == ["map_hold", "get_access", "get_access"]
        warnings = [r for r in caplog.records if "does not hold mapped" in r.message]
        assert len(warnings) == 1
        client.munmap(_handle(1))
        client.close()
        assert "unhold" not in rm.kinds()
    finally:
        rm.stop()


def test_unhold_failure_does_not_fail_munmap(rm, caplog):
    client = MaruShmClient(address=rm.address)
    mm = client.mmap(_handle(), _RW)
    # The resource manager goes away: nothing listens at its address.
    client._address = "127.0.0.1:1"
    with client._conn_lock:
        client._close_conn()
    with caplog.at_level(logging.WARNING, logger="maru_shm.client"):
        client.munmap(_handle())
        client.close()
    assert mm.closed
    assert any("could not drop the hold" in r.message for r in caplog.records)


def test_a_forked_child_does_not_drop_the_parents_hold(rm):
    parent = MaruShmClient(address=rm.address)
    parent.mmap(_handle(), _RW)
    # The same client as a forked child sees it: another pid.
    parent._pid += 1
    parent.munmap(_handle())
    parent.close()
    assert rm.kinds() == ["map_hold"]


def test_munmap_cannot_unhold_a_region_mapped_again(rm):
    """An unhold never lands after another thread mapped the region again."""
    client = MaruShmClient(address=rm.address)
    try:
        for _ in range(50):
            client.mmap(_handle(), _RW)
            other = threading.Thread(target=client.mmap, args=(_handle(), _RW))
            other.start()
            client.munmap(_handle())
            other.join()
            # Mapped at the end means held at the end.
            if 7 in client._mmap_cache:
                last = [k for k in rm.kinds() if k != "get_access"][-1]
                assert last == "map_hold"
                client.munmap(_handle())
    finally:
        client.close()
