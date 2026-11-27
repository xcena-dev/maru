# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Serialized memory-generation checks and boot identity."""

import os
import threading
import time
from collections.abc import Callable
from pathlib import Path

from .region_extension import RegionExtensionError


class MemoryGenerationError(RegionExtensionError):
    """The memory generation cannot be used."""


class MemoryGeneration:
    def __init__(
        self,
        extension,
        boot_id: str,
        *,
        grace_s: float,
        state_path: Path,
        on_change: Callable[[], None],
        clock: Callable[[], float] = time.monotonic,
    ):
        self.boot_id = boot_id
        self._read = getattr(extension, "memory_generation", None)
        self._clock, self._grace_s = clock, grace_s
        self._state_path, self._on_change = state_path, on_change
        self._lock = threading.Lock()
        self._failed_since: float | None = None
        self._unknown = False
        self._changed = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._periods = int(state_path.read_text()) if state_path.exists() else 0
        try:
            self._generation = self._value() if self._read is not None else None
        except Exception as exc:
            raise MemoryGenerationError(str(exc)) from exc
        self.boot = self._boot()

    def _value(self) -> str:
        value = self._read()
        if not isinstance(value, str) or not value or value == "unknown":
            raise MemoryGenerationError("memory generation is unknown")
        return value

    def _boot(self) -> str:
        if self._read is None:
            return self.boot_id
        if self._unknown:
            return f"{self.boot_id}:unknown"
        suffix = f"~{self._periods}" if self._periods else ""
        return f"{self.boot_id}:{self._generation}{suffix}"

    def check(self) -> bool:
        if self._read is None:
            return True
        with self._lock:
            changed = False
            try:
                generation = self._value()
            except Exception:
                now = self._clock()
                if self._failed_since is None:
                    self._failed_since = now
                if not self._unknown and now - self._failed_since >= self._grace_s:
                    self._unknown = True
                    self._periods += 1
                    # Keep the new identity across run and process restarts.
                    temporary = self._state_path.with_suffix(".tmp")
                    try:
                        with temporary.open("w") as stream:
                            stream.write(str(self._periods))
                            stream.flush()
                            os.fsync(stream.fileno())
                        temporary.replace(self._state_path)
                    finally:
                        self._changed = changed = True
            else:
                changed = self._unknown or generation != self._generation
                self._generation = generation
                self._unknown = False
                self._failed_since = None
            finally:
                self.boot = self._boot()
                if changed:
                    self._changed = True
                    self._on_change()
            return not self._changed

    def start(self, interval_s: float) -> None:
        if self._read is None:
            return

        def watch():
            while not self._stop.wait(interval_s):
                if not self.check():
                    return

        self._thread = threading.Thread(
            target=watch, name="maru-memory-generation", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=1.0)
