# SPDX-License-Identifier: Apache-2.0
"""Shared keys, connection authentication and socket limits."""

from __future__ import annotations

import hashlib
import hmac
import os
import resource
import secrets
import socket
import tempfile

# The deployment key installed on memory nodes.
DEFAULT_KEY_FILE = "/etc/maru/maru.key"
NONCE_BYTES = 16
# TCP keepalive: probe after 10 s idle, every 5 s, give up after 3 misses.
_KEEPALIVE = (10, 5, 3)
# A connection must prove the key within this long; a proven one may idle.
AUTH_TIMEOUT_S = 10.0
# Connections served at once (each holds a thread and a file descriptor), and
# connections from one source address that have not proved the key yet; more
# are closed at accept. The first limit is also kept below the process's
# file-descriptor limit, minus what the server itself uses.
MAX_CONNECTIONS = 1024
MAX_PER_SOURCE = 64
FD_RESERVE = 64


class KeyFileError(ValueError):
    """The memory node key file is missing, malformed or open to everyone."""


def new_key() -> str:
    """A new memory node key: 32 random bytes as hex."""
    return secrets.token_hex(32)


def load_key(path: str) -> bytes:
    """Read the memory node key.

    Args:
        path: Key file; one line of hex digits (at least 32). ``~`` is the
            user's home.

    Returns:
        The key bytes.

    Raises:
        KeyFileError: if the file cannot be read, is readable by every user,
            or does not hold a key.
    """
    path = os.path.expanduser(path)
    try:
        with open(path) as f:
            mode = os.fstat(f.fileno()).st_mode
            text = f.read().strip()
    except OSError as exc:
        raise KeyFileError(f"cannot read the memory node key {path}: {exc}") from exc
    if mode & 0o004:
        raise KeyFileError(
            f"the memory node key {path} is readable by every user; chmod o-r {path}"
        )
    try:
        key = bytes.fromhex(text)
    except ValueError:
        key = b""
    if len(key) < 16:
        raise KeyFileError(
            f"{path} does not hold a memory node key (at least 32 hex digits)"
        )
    return key


def write_key(path: str) -> None:
    """Create a key file readable by its owner only; an existing one stays.

    The key is written to a temporary file that is then linked into place,
    so an interrupted write never leaves an empty key behind.

    Args:
        path: Key file to create.

    Raises:
        FileExistsError: if the file exists.
    """
    directory = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(prefix=".maru-key-", dir=directory)
    try:
        with os.fdopen(fd, "w") as f:
            f.write(new_key() + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.link(tmp, path)
    finally:
        os.unlink(tmp)


def prove(key: bytes, role: str, nonce: bytes) -> bytes:
    """Proof that ``role`` holds ``key``, bound to the peer's ``nonce``.

    Args:
        key: Remote CXL agent key.
        role: ``"handler"`` for a client, ``"agent"`` for the memory node.
        nonce: The nonce the other side chose for this connection.

    Returns:
        HMAC-SHA256 digest.
    """
    return hmac.new(key, role.encode() + b"|" + bytes(nonce), hashlib.sha256).digest()


def set_keepalive(sock: socket.socket) -> None:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    idle, interval, count = _KEEPALIVE
    for name, value in (
        ("TCP_KEEPIDLE", idle),
        ("TCP_KEEPINTVL", interval),
        ("TCP_KEEPCNT", count),
    ):
        option = getattr(socket, name, None)
        if option is not None:
            sock.setsockopt(socket.IPPROTO_TCP, option, value)


class ConnectionAuth:
    """Whether one connection proved the memory node key."""

    def __init__(self, key: bytes | None) -> None:
        """Start a connection.

        Args:
            key: Remote CXL agent key; None runs without one (every request is
                accepted).
        """
        self.key = key
        self.nonce = secrets.token_bytes(NONCE_BYTES) if key is not None else b""
        self.proved = key is None

    def check(self, message: dict) -> dict:
        """Check an ``auth`` request and build its reply."""
        if self.key is None:
            return {
                "ok": False,
                "error": "this memory node runs without a key",
                "code": "invalid",
            }
        proof, nonce = message.get("proof"), message.get("nonce")
        if (
            not isinstance(proof, bytes)
            or not isinstance(nonce, bytes)
            or not (hmac.compare_digest(proof, prove(self.key, "handler", self.nonce)))
        ):
            return {
                "ok": False,
                "error": "wrong memory node key",
                "code": "denied",
            }
        self.proved = True
        return {"ok": True, "proof": prove(self.key, "agent", nonce)}


def connection_limit() -> int:
    """Connections the file-descriptor limit leaves room for."""
    soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft == resource.RLIM_INFINITY:
        return MAX_CONNECTIONS
    return max(1, min(MAX_CONNECTIONS, soft - FD_RESERVE))
