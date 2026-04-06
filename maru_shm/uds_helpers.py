# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Socket helper utilities.

Provides reliable read/write helpers over sockets.
"""

import socket


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
