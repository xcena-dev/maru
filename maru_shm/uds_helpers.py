# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""UDS (Unix Domain Socket) helper utilities.

Provides SCM_RIGHTS FD passing, SO_PEERCRED peer credentials,
and reliable read/write helpers over sockets.
"""

import array
import socket


def send_with_fd(sock: socket.socket, data: bytes, fd: int) -> None:
    """Send data with a file descriptor via SCM_RIGHTS.

    Args:
        sock: Connected AF_UNIX socket.
        data: Payload bytes to send.
        fd: File descriptor to pass.
    """
    fds = array.array("i", [fd])
    sock.sendmsg(
        [data],
        [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)],
    )


def recv_with_fd(sock: socket.socket, bufsize: int) -> tuple[bytes, int | None]:
    """Receive data with an optional file descriptor via SCM_RIGHTS.

    Args:
        sock: Connected AF_UNIX socket.
        bufsize: Maximum bytes to receive.

    Returns:
        Tuple of (data, fd) where fd is None if no FD was received.
    """
    # ancbufsize: enough for one int FD
    fds = array.array("i")
    msg, ancdata, flags, addr = sock.recvmsg(
        bufsize,
        socket.CMSG_LEN(fds.itemsize),
    )
    fd = None
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
            received_fds = array.array("i")
            received_fds.frombytes(
                cmsg_data[: len(cmsg_data) - (len(cmsg_data) % fds.itemsize)]
            )
            if received_fds:
                fd = received_fds[0]
    return msg, fd


def read_full(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from socket.

    Args:
        sock: Connected socket.
        n: Number of bytes to read.

    Returns:
        Exactly n bytes.

    Raises:
        ConnectionError: If connection closed before n bytes received.
    """
    parts: list[bytes] = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("Connection closed before all data received")
        parts.append(chunk)
        remaining -= len(chunk)
    return b"".join(parts)


def write_full(sock: socket.socket, data: bytes) -> None:
    """Write all bytes to socket.

    Args:
        sock: Connected socket.
        data: Bytes to send.
    """
    sock.sendall(data)
