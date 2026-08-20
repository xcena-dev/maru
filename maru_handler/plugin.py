# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Out-of-tree (OOT) plugin support for MaruHandler.

Maru core stays vendor-neutral: hardware- or vendor-specific behaviour
(e.g. a CXL device's prefetch/pin hints) lives in a *separate* pip package
that registers itself under the ``maru.handler_plugins`` entry-point group.
MaruHandler discovers those packages at construction time and calls them at a
few well-defined seams — it never imports them directly.

The design mirrors vLLM's plugin system (soft-fail + name allowlist) rather
than PyTorch's device-backend autoload (hard-fail at ``import torch``): a Maru
plugin is an *optional optimization*. A missing or broken plugin must never
break KV-cache operation, so a load/hook failure is logged and skipped, never
raised.

Registering a plugin (in the plugin package's ``pyproject.toml``)::

    [project.entry-points."maru.handler_plugins"]
    my_plugin = "my_pkg.plugin:MyPlugin"

The entry-point value is a zero-argument callable (a class or factory
function) returning an object that implements any subset of
:class:`MaruHandlerPlugin`.

Selecting plugins at runtime::

    MARU_PLUGINS=my_plugin,other   # only these names load; unset → all load
    MARU_PLUGINS=none              # explicitly disable every plugin
"""

from __future__ import annotations

import importlib.metadata
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from maru_common import BatchLookupKVResponse

    from .handler import MaruHandler

logger = logging.getLogger(__name__)

#: Entry-point group scanned for handler plugins.
PLUGIN_GROUP = "maru.handler_plugins"

#: Comma-separated allowlist of plugin *names* to load. Unset/empty → load all;
#: the reserved value ``none`` disables all plugins.
PLUGIN_ALLOWLIST_ENV = "MARU_PLUGINS"

#: Process-level cache of the discovered entry points. The installed
#: distribution set is immutable within a process, so scan once (the scan is
#: not free) and reuse across every MaruHandler construction.
_discovered_entry_points: list | None = None


@dataclass(frozen=True)
class StageResult:
    """Outcome of preparing one key batch for a future local read.

    ``ready`` is deliberately strict: every requested key must exist, map to
    an eligible device range, and complete its blocking preparation call.
    Callers may still fall back to a normal demand read when it is false.

    Attributes:
        requested_keys: Number of keys in the stage request.
        found_keys: Number of keys found by the metadata lookup.
        eligible_keys: Found keys that resolved to a live device range.
        prepared_bytes: Bytes whose blocking preparation completed.
        issued_ranges: Coalesced device ranges submitted successfully.
        failed_ranges: Device ranges whose preparation returned an error.
        skipped_keys: Found keys that could not resolve to a live range.
        wait_ms: Wall time spent in the blocking device preparation. When the
            plugin yields to demand reads mid-batch, that pause is included
            here as well — subtract ``yielded_ms`` for pure device time.
        yielded_ms: Wall time the preparation spent paused between sub-calls,
            yielding the device to concurrent demand reads.
        probe_checks: Demand-probe consultations during this batch (wiring
            echo — zero means the probe was never even asked).
        probe_hits: Consultations that observed an in-flight demand read.
        error: Optional lookup/plugin error summary.
    """

    requested_keys: int
    found_keys: int
    eligible_keys: int = 0
    prepared_bytes: int = 0
    issued_ranges: int = 0
    failed_ranges: int = 0
    skipped_keys: int = 0
    wait_ms: float = 0.0
    yielded_ms: float = 0.0
    probe_checks: int = 0
    probe_hits: int = 0
    error: str | None = None

    @property
    def ready(self) -> bool:
        """Return whether every requested key is ready for local consumption."""
        return (
            self.requested_keys > 0
            and self.found_keys == self.requested_keys
            and self.eligible_keys == self.found_keys
            and self.issued_ranges > 0
            and self.failed_ranges == 0
            and self.skipped_keys == 0
            and self.error is None
        )


def _discover_entry_points() -> list:
    """Scan (once) the installed ``maru.handler_plugins`` entry points."""
    global _discovered_entry_points
    if _discovered_entry_points is None:
        try:
            _discovered_entry_points = list(
                importlib.metadata.entry_points(group=PLUGIN_GROUP)
            )
        except Exception:
            logger.exception("failed to enumerate %s entry points", PLUGIN_GROUP)
            _discovered_entry_points = []
    return _discovered_entry_points


@runtime_checkable
class MaruHandlerPlugin(Protocol):
    """Extension interface invoked by MaruHandler at defined lifecycle seams.

    Every hook is **optional** — a plugin implements only the ones it needs;
    MaruHandler skips any that are absent. All hooks are best-effort: an
    exception raised inside one is logged and swallowed so it can never break
    the surrounding core operation. That isolation covers *raised exceptions*
    only — a hook that blocks or hangs still stalls the calling core path, so
    every hook must return promptly.
    """

    def on_init(self, handler: MaruHandler) -> None:
        """Called once, at the end of ``MaruHandler.__init__``.

        The handler is constructed but **not yet connected** (``_mapper`` is
        ``None`` until ``connect()``). Use this only for cheap setup; defer any
        work that needs mapped regions to :meth:`on_batch_retrieve`.

        Runs synchronously inside the constructor, so keep it fast and
        non-blocking. Note this hook is **not paired** with
        :meth:`on_close`: ``on_close`` fires only if the handler was connected
        (see its note), so do not acquire in ``on_init`` a resource whose
        release you rely on ``on_close`` to perform — a construct-then-never-
        connect sequence would leak it. Acquire lazily in ``on_batch_retrieve``
        instead.
        """
        ...

    def on_batch_retrieve(
        self,
        handler: MaruHandler,
        keys: list[str],
        batch_resp: BatchLookupKVResponse,
    ) -> None:
        """Called at the end of ``batch_retrieve``, after regions are mapped.

        ``keys[i]`` corresponds to ``batch_resp.entries[i]``. For a found
        entry, the KV bytes live at ``entry.kv_offset`` (offset within the
        allocation) for ``entry.kv_length`` bytes; ``entry.handle.offset`` is
        only the region's mmap base, **not** the payload location, so a
        hardware hint must target ``handle.offset + kv_offset`` for
        ``kv_length`` bytes (mirror ``get_buffer_view``). The region is already
        mapped, so a plugin may issue hints (prefetch/pin) against live memory.

        Runs on the retrieval hot path — keep it cheap and non-blocking.
        **Not serialized against** :meth:`on_close`: ``batch_retrieve`` takes
        no lock, so this hook can still be in flight while another thread's
        ``close()`` unmaps the very regions referenced here. A plugin issuing
        HW hints must be thread-safe and treat pin/unpin as idempotent (or
        reference-counted) so a hint that lands after the unmap is harmless.
        """
        ...

    def on_prefetch(
        self,
        handler: MaruHandler,
        keys: list[str],
        batch_resp: BatchLookupKVResponse,
    ) -> None:
        """Called at the end of ``prefetch_batch``, after a lookup-only RPC.

        This is the *lookahead* counterpart of :meth:`on_batch_retrieve`: it
        fires ahead of demand so the plugin can start an SSD->DRAM migration
        while the caller still waits (e.g. a request's admission wait), leaving
        the data warm for a later ``batch_retrieve`` of the same keys. The
        argument shape and the ``keys[i]`` ↔ ``batch_resp.entries[i]``
        correspondence are identical to :meth:`on_batch_retrieve`, and a
        hardware hint targets the same ``handle.offset + kv_offset`` for
        ``kv_length`` bytes.

        The one difference from :meth:`on_batch_retrieve` is that
        ``prefetch_batch`` performs **no data read and maps no regions** — it
        only looks the keys up. A region that is not already mapped therefore
        has no live address to hint against; the plugin must issue hints only
        for regions where :meth:`MaruHandler.is_region_mapped` is true and skip
        the rest (they are prefaulted on their own ``map_region`` at demand
        time). On a cache-hit pass the regions are already mapped from the
        populate pass, so the common case still hints every key.

        Runs on the caller's path — keep it cheap and non-blocking. Same
        soft-fail isolation and non-serialization against :meth:`on_close` as
        :meth:`on_batch_retrieve`.
        """
        ...

    def on_stage(
        self,
        handler: MaruHandler,
        keys: list[str],
        batch_resp: BatchLookupKVResponse,
    ) -> StageResult | None:
        """Synchronously prepare a lookup batch for a future local read.

        Unlike every hot-path hook above, this hook is explicitly allowed to
        block. :meth:`MaruHandler.stage_batch` must therefore be called only
        from a dedicated executor or helper process, never from a model or
        scheduler critical path. A non-``None`` result is a completion
        contract, not a fire-and-forget hint: ``result.ready`` is true only
        after all requested ranges are locally consumable.

        Plugins that do not provide a materialization/readiness operation omit
        this hook. Exceptions are converted to a failed :class:`StageResult`
        by the handler so demand reads can safely fall back.
        """
        ...

    def on_stage_release(
        self,
        handler: MaruHandler,
        keys: list[str],
    ) -> None:
        """Release per-stage device resources held for ``keys``.

        Called via :meth:`MaruHandler.stage_release` when the consumer of a
        prior :meth:`on_stage` batch is done with it (or will never arrive) —
        the counterpart that ends a stage's residency lease. ``keys`` is the
        same list the matching :meth:`on_stage` received. Must be idempotent
        and a cheap no-op for batches that hold nothing (never staged, stage
        failed, or lease already released). Plugins whose stages hold no
        releasable resource omit this hook.
        """
        ...

    def on_close(self, handler: MaruHandler) -> None:
        """Called during ``MaruHandler.close``, while regions are still mapped.

        Runs before regions are unmapped and the RPC connection is torn down,
        so a plugin can safely release resources tied to mapped memory (e.g.
        unpin device ranges) here. Fires **only if the handler was connected** —
        a handler that is constructed but never connected does not invoke this
        hook (see :meth:`on_init`).

        Dispatched while holding the handler's write lock, so a slow or
        blocking implementation stalls ``close()`` and every concurrent writer:
        keep it cheap and non-blocking, same as the other hooks.
        """
        ...

    def contribute_stats(self) -> dict | None:
        """Return a JSON-serializable stats dict merged into ``get_stats``.

        The result is placed under ``stats["plugins"][<plugin-class-name>]``.
        The key is the plugin's class name, so two plugins sharing a class name
        (from different packages) would overwrite each other here — give the
        class a distinctive name if that collision is possible. Return ``None``
        (or an empty dict) to contribute nothing.
        """
        ...


def _get_allowlist() -> set[str] | None:
    """Parse ``MARU_PLUGINS``; ``none`` is the explicit disable sentinel."""
    raw = os.environ.get(PLUGIN_ALLOWLIST_ENV, "").strip()
    if not raw:
        return None
    if raw.lower() == "none":
        return set()
    return {name.strip() for name in raw.split(",") if name.strip()}


def load_handler_plugins(
    entry_points_iter=None,
    allowlist: set[str] | None = None,
) -> list[object]:
    """Discover and instantiate handler plugins, isolating every failure.

    Args:
        entry_points_iter: Iterable of entry-point objects (each with a
            ``name`` attribute and a ``load()`` method). Defaults to the
            installed ``maru.handler_plugins`` entry points; injectable for
            testing.
        allowlist: Explicit set of plugin names to load. When ``None``, the
            ``MARU_PLUGINS`` env var is consulted (unset → load all).

    Returns:
        Instantiated plugin objects. Never raises: a plugin that fails to
        enumerate, load, or instantiate is logged and skipped.
    """
    if allowlist is None:
        allowlist = _get_allowlist()

    if entry_points_iter is None:
        entry_points_iter = _discover_entry_points()

    plugins: list[object] = []
    seen_names: set[str] = set()
    for ep in entry_points_iter:
        if allowlist is not None and ep.name not in allowlist:
            logger.debug(
                "skipping handler plugin %r (not in %s)", ep.name, PLUGIN_ALLOWLIST_ENV
            )
            continue
        if ep.name in seen_names:
            # Two installed dists can register the same name (e.g. a stale and
            # a fresh copy). First wins — running both would double every hook.
            logger.warning(
                "duplicate handler plugin name %r, ignoring the later entry", ep.name
            )
            continue
        seen_names.add(ep.name)
        try:
            factory = ep.load()
            plugin = factory()
        except Exception:
            logger.exception("handler plugin %r failed to load, skipping", ep.name)
            continue
        if plugin is None:
            # A factory that self-disables should return a no-op object, not
            # None — a None here would sit in the list as a dead "plugin".
            logger.warning("handler plugin %r factory returned None, skipping", ep.name)
            continue
        plugins.append(plugin)
        logger.info("loaded handler plugin: %s", ep.name)

    # Surface typos: an allowlist name that matched no installed plugin is
    # almost always a misspelling, and otherwise loads nothing silently.
    if allowlist is not None:
        missing = allowlist - seen_names
        if missing:
            logger.warning(
                "%s names matched no installed plugin: %s",
                PLUGIN_ALLOWLIST_ENV,
                ", ".join(sorted(missing)),
            )

    return plugins
