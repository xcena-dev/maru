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
"""

from __future__ import annotations

import importlib.metadata
import logging
import os
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from maru_common import BatchLookupKVResponse

    from .handler import MaruHandler

logger = logging.getLogger(__name__)

#: Entry-point group scanned for handler plugins.
PLUGIN_GROUP = "maru.handler_plugins"

#: Comma-separated allowlist of plugin *names* to load. Unset/empty → load all.
PLUGIN_ALLOWLIST_ENV = "MARU_PLUGINS"


@runtime_checkable
class MaruHandlerPlugin(Protocol):
    """Extension interface invoked by MaruHandler at defined lifecycle seams.

    Every hook is **optional** — a plugin implements only the ones it needs;
    MaruHandler skips any that are absent. All hooks are best-effort: an
    exception raised inside one is logged and swallowed so it can never break
    the surrounding core operation.
    """

    def on_init(self, handler: MaruHandler) -> None:
        """Called once, at the end of ``MaruHandler.__init__``.

        The handler is constructed but **not yet connected** (``_mapper`` is
        ``None`` until ``connect()``). Use this only for cheap setup; defer any
        work that needs mapped regions to :meth:`on_batch_retrieve`.
        """
        ...

    def on_batch_retrieve(
        self,
        handler: MaruHandler,
        keys: list[str],
        batch_resp: BatchLookupKVResponse,
    ) -> None:
        """Called at the end of ``batch_retrieve``, after regions are mapped.

        ``keys[i]`` corresponds to ``batch_resp.entries[i]``. Found entries
        expose a ``handle`` (region/offset) whose region is already mapped, so
        a plugin may issue hardware hints (prefetch/pin) against live memory.
        Runs on the retrieval hot path — keep it cheap and non-blocking.
        """
        ...

    def on_close(self, handler: MaruHandler) -> None:
        """Called during ``MaruHandler.close``, while regions are still mapped.

        Runs before regions are unmapped and the RPC connection is torn down,
        so a plugin can safely release resources tied to mapped memory (e.g.
        unpin device ranges) here.
        """
        ...

    def contribute_stats(self) -> dict | None:
        """Return a JSON-serializable stats dict merged into ``get_stats``.

        The result is placed under ``stats["plugins"][<plugin-class-name>]``.
        Return ``None`` (or an empty dict) to contribute nothing.
        """
        ...


def _get_allowlist() -> set[str] | None:
    """Parse ``MARU_PLUGINS`` into a name set, or ``None`` when unset/empty."""
    raw = os.environ.get(PLUGIN_ALLOWLIST_ENV, "").strip()
    if not raw:
        return None
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
        try:
            entry_points_iter = importlib.metadata.entry_points(group=PLUGIN_GROUP)
        except Exception:
            logger.exception("failed to enumerate %s entry points", PLUGIN_GROUP)
            return []

    plugins: list[object] = []
    for ep in entry_points_iter:
        if allowlist is not None and ep.name not in allowlist:
            logger.debug(
                "skipping handler plugin %r (not in %s)", ep.name, PLUGIN_ALLOWLIST_ENV
            )
            continue
        try:
            factory = ep.load()
            plugin = factory()
        except Exception:
            logger.exception("handler plugin %r failed to load, skipping", ep.name)
            continue
        plugins.append(plugin)
        logger.info("loaded handler plugin: %s", ep.name)

    return plugins
