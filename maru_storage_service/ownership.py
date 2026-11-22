# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Exclusive node ownership at the configured socket path."""

import fcntl
import os


class LocalServerError(RuntimeError):
    """The endpoint cannot start (socket in use, bad path)."""


class NodeOwnership:
    """Exclusive local ownership of a node's socket path."""

    def __init__(self, path: str):
        self.path = path
        self._fd: int | None = None

    def claim(self) -> None:
        if self._fd is not None:
            return
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        fd = os.open(f"{self.path}.lock", os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o660)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise LocalServerError(
                f"another StorageService holds {self.path}.lock"
            ) from None
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    def close(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
