# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Node identity and worker socket ownership."""

import fcntl
import os


class LocalServerError(RuntimeError):
    """The endpoint cannot start (socket in use, bad path)."""


class FileLock:
    """A nonblocking flock held until close()."""

    def __init__(self, path: str):
        self.path = path
        self._fd: int | None = None

    def claim(self) -> None:
        if self._fd is not None:
            return
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o660)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise LocalServerError(
                f"another StorageService holds {self.path}"
            ) from None
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    def close(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None


class NodeOwnership:
    """Node and socket locks shared by every run."""

    def __init__(self, state_dir: str, node_id: str, socket: str | None = None):
        self.state_dir = state_dir
        self._node = FileLock(os.path.join(state_dir, f"{node_id}.lock"))
        self._socket = FileLock(f"{socket}.lock") if socket is not None else None
        if self._socket is not None and os.path.realpath(
            self._socket.path
        ) == os.path.realpath(self._node.path):
            self._socket = self._node

    def claim(self) -> None:
        try:
            self._node.claim()
        except OSError as e:
            raise LocalServerError(
                f"cannot use state_dir {self.state_dir}: {e}; "
                "create a writable directory or set config.state_dir"
            ) from e
        if self._socket is not None:
            self._socket.claim()

    def close(self) -> None:
        if self._socket is not None:
            self._socket.close()
        self._node.close()
