# SPDX-License-Identifier: Apache-2.0
"""Length-prefixed MessagePack frames and TCP addresses."""

from __future__ import annotations

import socket
import struct

import msgpack

MAX_FRAME = 16 * 1024 * 1024
_LEN = struct.Struct("!I")


def send_frame(sock: socket.socket, message: dict) -> None:
    """Send one length-prefixed MessagePack message.

    Args:
        sock: Connected socket.
        message: Map to send.

    Raises:
        ValueError: if the encoded message exceeds :data:`MAX_FRAME`.
        OSError: if the socket fails.
    """
    data = msgpack.packb(message, use_bin_type=True)
    if len(data) > MAX_FRAME:
        raise ValueError(f"remote message of {len(data)} bytes is too large")
    sock.sendall(_LEN.pack(len(data)) + data)


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    chunks = []
    while n:
        chunk = sock.recv(n)
        if not chunk:
            return None
        chunks.append(chunk)
        n -= len(chunk)
    return b"".join(chunks)


def recv_frame(sock: socket.socket) -> dict | None:
    """Receive one message; None when the peer closed the connection.

    Args:
        sock: Connected socket.

    Returns:
        The decoded map, or None at end of stream.

    Raises:
        ValueError: if the frame is too large or not a map.
        OSError: if the socket fails.
    """
    head = _recv_exact(sock, _LEN.size)
    if head is None:
        return None
    (length,) = _LEN.unpack(head)
    if length > MAX_FRAME:
        raise ValueError(f"remote frame of {length} bytes is too large")
    body = _recv_exact(sock, length)
    if body is None:
        return None
    message = msgpack.unpackb(body, raw=False)
    if not isinstance(message, dict):
        raise ValueError("remote message must be a map")
    return message


def parse_address(address: str) -> tuple[str, int]:
    """Split ``host:port``.

    Args:
        address: Remote peer address.

    Returns:
        ``(host, port)``.

    Raises:
        ValueError: if the address has no port.
    """
    host, sep, port = address.rpartition(":")
    if not sep or not host or not port.isdigit():
        raise ValueError(f"remote address must be host:port, got {address!r}")
    return host, int(port)
