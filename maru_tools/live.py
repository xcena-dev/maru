# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unified live view — the default ``marutop`` screen.

Fuses backends into one ``htop``-style curses TUI, the analog of XCENA's
``pxltop`` (a DEVICES gauge section + a per-owner section):

- **DEVICES**   — physical DAX pools from the Resource Manager (:9850)
- **INSTANCES** — per-instance allocated/used/slack from MaruServer(s) (:5555)
- **STATS**     — select an instance + ``enter`` to drill into a full
  per-instance dashboard: op table (count/delta/avg/min/max + activity
  sparkline), a detail box for the selected op (hit-rate bar, throughput), and a
  min/avg/max latency graph over time. Populated only when clients run with
  ``MARU_STAT=1``. (``marutop stats`` remains for a port-pinned dashboard.)

With no ``-p``/``--host``, the INSTANCES section **auto-discovers** local
``maru-server`` processes (scanning ``/proc`` for their ``--port``) and polls
every one — so multiple servers are covered without knowing their ports. Pass
``-p``/``--host`` to pin a single (possibly remote) server instead.

Backends are polled on a background thread, so the UI stays smooth and keys
stay responsive even when a server is slow or unreachable.

    marutop                    # auto-discover local servers + RM
    marutop -w 2               # refresh every 2s
    marutop -p 11011           # pin one server (e.g. a naru run) — disables discovery
    marutop --once             # single plain-text snapshot then exit (scriptable)

Keys (overview):  ↑↓ select instance · enter → per-instance STATS · s sort · i interval · q quit
Keys (STATS view): ↑↓ select op · esc/← back · i interval · q quit
"""

import argparse
import curses
import logging
import threading
from datetime import datetime
from pathlib import Path

from maru_tools._common import clear_screen, fmt_size, usage_bar
from maru_tools.stats import (
    _HISTORY_WIDTH,
    _SPARK_TABLE_WIDTH,
    _ZERO_LAT,
    _sparkline,
)

# ── color pairs (pxltop palette: cyan header, green/yellow/red gauge, blue dim) ─
_CP_HEADER = 1
_CP_OK = 2
_CP_WARN = 3
_CP_CRIT = 4
_CP_DIM = 5

# Instance-table sort keys, cycled with 's'.
_SORT_KEYS = ["name", "allocated", "used", "slack"]


def _color_for_ratio(ratio: float) -> int:
    """Green < 60% < yellow < 85% < red (pxltop thresholds)."""
    if ratio >= 85.0:
        return _CP_CRIT
    if ratio >= 60.0:
        return _CP_WARN
    return _CP_OK


def _make_bar(ratio: float, width: int) -> str:
    """``||||||        `` fill for inside ``[ ]`` (pxltop style)."""
    ratio = max(0.0, min(100.0, ratio))
    filled = int(ratio / 100.0 * width + 0.5)
    return "|" * filled + " " * (width - filled)


# =============================================================================
# maru-server auto-discovery (scan /proc for `maru_server` cmdlines + --port)
# =============================================================================


def _extract_opt(tokens: list[str], name: str) -> str | None:
    """Read ``--name VALUE`` or ``--name=VALUE`` from an argv token list."""
    for i, tok in enumerate(tokens):
        if tok == name and i + 1 < len(tokens):
            return tokens[i + 1]
        if tok.startswith(name + "="):
            return tok.split("=", 1)[1]
    return None


def discover_servers(self_pid: int) -> list[tuple[str, int, int]]:
    """Find local maru-server processes → list of (host, port, pid).

    Matches processes whose argv mentions ``maru_server``/``maru-server`` and
    carries a ``--port`` (both the ``maru-server`` console script and
    ``python -m maru_server.server`` qualify). Best-effort and local-only.
    """
    found: list[tuple[str, int, int]] = []
    try:
        proc_entries = list(Path("/proc").iterdir())
    except OSError:
        return found
    for entry in proc_entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid == self_pid:
            continue
        try:
            raw = (entry / "cmdline").read_bytes()
        except OSError:
            continue  # process gone or unreadable
        tokens = [t.decode("utf-8", "replace") for t in raw.split(b"\0") if t]
        if not tokens:
            continue
        joined = " ".join(tokens)
        if "maru_server" not in joined and "maru-server" not in joined:
            continue
        port = _extract_opt(tokens, "--port")
        if not port:
            continue
        try:
            port_i = int(port)
        except ValueError:
            continue
        host = _extract_opt(tokens, "--host") or "127.0.0.1"
        if host in ("0.0.0.0", "::"):
            host = "127.0.0.1"  # connect locally to a wildcard-bound server
        found.append((host, port_i, pid))
    return found


# =============================================================================
# Stats summary (compact per-op view for the live screen)
# =============================================================================


def _summarize_ops(stats_manager: dict, client_key: str = "_all") -> list[dict]:
    """Compact per-op rows for one client (or ``_all``), busiest first.

    ``stats_manager`` is the raw ``{"clients": {...}}`` dict from a
    GetStatsResponse. ``client_key`` selects an instance_id (or ``_all`` for the
    server aggregate). Only ops with activity are returned. Stats are populated
    only when clients run with ``MARU_STAT=1`` (else empty; caller shows a hint).
    """
    sm = stats_manager or {}
    clients = sm.get("clients", {}) if isinstance(sm, dict) else {}
    ops = clients.get(client_key, {}).get("operations", {})
    rows = []
    for name, o in ops.items():
        count = o.get("count", 0)
        if count <= 0:
            continue
        rows.append(
            {
                "op": name,
                "count": count,
                "avg_us": o.get("avg_latency_us", 0.0),
                "min_us": o.get("min_latency_us", 0.0),
                "max_us": o.get("max_latency_us", 0.0),
                "bytes": o.get("total_bytes", 0),
                "hit": o.get("hit_count", 0),
                "miss": o.get("miss_count", 0),
            }
        )
    return sorted(rows, key=lambda r: -r["count"])


_HIT_MISS_OPS = {"retrieve", "batch_retrieve", "exists", "batch_exists"}


# =============================================================================
# Background poller — owns all clients so no network I/O blocks the UI thread
# =============================================================================


class _Poller:
    """Polls the RM and every target maru-server on its own thread.

    All ZMQ clients live on this thread (never shared with the UI thread), so a
    slow/dead backend delays only polling, never key handling. In auto mode the
    server set is re-discovered each cycle, so servers coming and going (e.g. a
    benchmark cycling backends) are picked up automatically.
    """

    def __init__(self, args):
        self._args = args
        self._interval = max(0.1, args.watch)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # Fixed mode iff the user pinned a host or port.
        self._fixed = args.host is not None or args.port is not None
        self._fixed_host = args.host or "127.0.0.1"
        self._fixed_port = args.port or 5555
        self.rm_addr = args.address or "127.0.0.1:9850"
        self.auto = not self._fixed

        # Shared snapshot (guarded by _lock).
        self.pool = (None, None)  # (list[MaruPoolInfo] | None, err | None)
        # servers: list of {label, port, pid, usage:(resp|None, err|None)}
        self.servers: list[dict] = []
        self.fetch_count = 0

        # Per-(server_label, client_id) op history for the STATS dashboard.
        # op -> list; bounded to _HISTORY_WIDTH (counts/latency) / _SPARK_TABLE_WIDTH.
        self._tick = 0
        self._h_count: dict[tuple, dict[str, list]] = {}
        self._h_lat: dict[tuple, dict[str, list]] = {}
        self._h_spark: dict[tuple, dict[str, list]] = {}
        self._h_accum: dict[tuple, dict[str, int]] = {}

    @property
    def interval(self) -> float:
        with self._lock:
            return self._interval

    def set_interval(self, value: float) -> None:
        with self._lock:
            self._interval = max(0.1, value)

    def snapshot(self):
        with self._lock:
            return self.pool, list(self.servers), self.fetch_count

    def get_history(self, label: str, client_id: str):
        """Copy of (count_hist, lat_hist, spark_hist) for one client's ops."""
        key = (label, client_id)
        with self._lock:
            return (
                {op: list(v) for op, v in self._h_count.get(key, {}).items()},
                {op: list(v) for op, v in self._h_lat.get(key, {}).items()},
                {op: list(v) for op, v in self._h_spark.get(key, {}).items()},
            )

    def _update_history(self, label: str, sm: dict, flush_spark: bool) -> None:
        """Accumulate per-op interval history from one server's stats dict.

        Server interval counters reset on each get_stats, so each tick's
        ``interval_*`` values are the delta since the previous poll.
        """
        clients = sm.get("clients", {}) if isinstance(sm, dict) else {}
        for cid, cdata in clients.items():
            ops = cdata.get("operations", {})
            key = (label, cid)
            ch = self._h_count.setdefault(key, {})
            lh = self._h_lat.setdefault(key, {})
            sh = self._h_spark.setdefault(key, {})
            ac = self._h_accum.setdefault(key, {})
            for op, o in ops.items():
                ch.setdefault(op, [0] * _HISTORY_WIDTH)
                lh.setdefault(op, [_ZERO_LAT] * _HISTORY_WIDTH)
                sh.setdefault(op, [0.0] * _SPARK_TABLE_WIDTH)
                ac.setdefault(op, 0)
                ivc = o.get("interval_count", 0)
                if ivc > 0:
                    ch[op].append(ivc)
                    lh[op].append(
                        (
                            o.get("interval_min_us", 0.0),
                            o.get("interval_avg_us", 0.0),
                            o.get("interval_max_us", 0.0),
                        )
                    )
                    ac[op] += ivc
                else:
                    ch[op].append(0)
                    lh[op].append(_ZERO_LAT)
                if len(ch[op]) > _HISTORY_WIDTH:
                    del ch[op][: len(ch[op]) - _HISTORY_WIDTH]
                if len(lh[op]) > _HISTORY_WIDTH:
                    del lh[op][: len(lh[op]) - _HISTORY_WIDTH]
            if flush_spark:
                for op in ac:
                    sh.setdefault(op, [0.0] * _SPARK_TABLE_WIDTH)
                    sh[op].append(float(ac[op]))
                    if len(sh[op]) > _SPARK_TABLE_WIDTH:
                        del sh[op][: len(sh[op]) - _SPARK_TABLE_WIDTH]
                    ac[op] = 0

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3.0)

    # -- polling thread ------------------------------------------------------

    def _targets(self, self_pid: int) -> list[tuple[str, int, int | None]]:
        if self._fixed:
            return [(self._fixed_host, self._fixed_port, None)]
        return list(discover_servers(self_pid))

    def _run(self) -> None:
        import os

        from maru_handler.rpc_client import RpcClient
        from maru_shm import MaruShmClient

        self_pid = os.getpid()
        rm = MaruShmClient(address=self._args.address)
        clients: dict[tuple[str, int], RpcClient] = {}

        try:
            while not self._stop.is_set():
                try:
                    pool = (rm.stats(), None)
                except Exception as e:  # noqa: BLE001 - surface any backend error
                    pool = (None, e)

                targets = self._targets(self_pid)
                live_keys = {(h, p) for h, p, _ in targets}
                # Drop clients for servers that disappeared.
                for key in list(clients):
                    if key not in live_keys:
                        try:
                            clients.pop(key).close()
                        except Exception:
                            pass

                results = []
                for host, port, pid in sorted(targets, key=lambda t: (t[1], t[0])):
                    cli = clients.get((host, port))
                    if cli is None:
                        cli = RpcClient(
                            server_url=f"tcp://{host}:{port}", timeout_ms=1500
                        )
                        try:
                            cli.connect()
                        except Exception:
                            pass
                        clients[(host, port)] = cli
                    try:
                        usage = (cli.get_usage(), None)
                    except Exception as e:  # noqa: BLE001
                        usage = (None, e)
                    try:
                        stats = (cli.get_stats().stats_manager, None)
                    except Exception as e:  # noqa: BLE001
                        stats = (None, e)
                    label = f"{host}:{port}" if self._fixed else f":{port}"
                    results.append(
                        {
                            "label": label,
                            "port": port,
                            "pid": pid,
                            "usage": usage,
                            "stats": stats,
                        }
                    )

                with self._lock:
                    self.pool = pool
                    self.servers = results
                    self.fetch_count += 1
                    self._tick += 1
                    flush_spark = self._tick % 2 == 0
                    for r in results:
                        sm, serr = r["stats"]
                        if serr is None and sm is not None:
                            self._update_history(r["label"], sm, flush_spark)
                    # Drop history for servers that disappeared (keys are
                    # (label, client_id)); otherwise auto-discovery churn grows
                    # the key set unbounded over a long run.
                    live_labels = {r["label"] for r in results}
                    for hist in (
                        self._h_count,
                        self._h_lat,
                        self._h_spark,
                        self._h_accum,
                    ):
                        for k in [k for k in hist if k[0] not in live_labels]:
                            del hist[k]

                waited = 0.0
                while waited < self.interval and not self._stop.is_set():
                    self._stop.wait(0.05)
                    waited += 0.05
        finally:
            for c in (*clients.values(), rm):
                try:
                    c.close()
                except Exception:
                    pass


# =============================================================================
# Plain-text renderer (used by --once; also handy for piping/logging)
# =============================================================================


def _devices_lines(pools) -> list[str]:
    if not pools:
        return ["  (no pools found)"]
    lines = [
        f"  {'Device':<16}  {'Type':<8}  {'Used':>8}  {'Free':>8}  "
        f"{'Total':>8}  {'Usage'}"
    ]
    for p in sorted(pools, key=lambda x: x.dax_path):
        used = p.total_size - p.free_size
        lines.append(
            f"  {(p.dax_path or '(unknown)'):<16}  {p.dax_type.name:<8}  "
            f"{fmt_size(used):>8}  {fmt_size(p.free_size):>8}  "
            f"{fmt_size(p.total_size):>8}  {usage_bar(used, p.total_size)}"
        )
    return lines


def _instance_rows(usage, sort_key: str) -> list[str]:
    lines = []
    instances = _sort_instances(usage.instances, sort_key)
    if not instances:
        return ["  (no active instances)"]
    lines.append(
        f"  {'owner_instance_id':<38}  {'regions':>7}  "
        f"{'allocated':>9}  {'used':>9}  {'slack':>9}"
    )
    for inst in instances:
        slack = inst.allocated - inst.used
        lines.append(
            f"  {inst.instance_id:<38}  {inst.regions:>7}  "
            f"{fmt_size(inst.allocated):>9}  {fmt_size(inst.used):>9}  "
            f"{fmt_size(slack):>9}"
        )
        for dax_path, nbytes in sorted(inst.devices.items()):
            lines.append(f"      └ {_dax_short(dax_path):<12}  {fmt_size(nbytes):>9}")
    total_alloc = sum(i.allocated for i in instances)
    total_used = sum(i.used for i in instances)
    lines.append(
        f"  {'TOTAL':<38}  {len(instances):>7}  "
        f"{fmt_size(total_alloc):>9}  {fmt_size(total_used):>9}  "
        f"{fmt_size(total_alloc - total_used):>9}"
    )
    return lines


def _render_text(poller: _Poller, sort_key: str) -> str:
    (pools, perr), servers, _ = poller.snapshot()
    ts = datetime.now().isoformat(timespec="seconds")
    out = [f"  marutop  —  {ts}  (Ctrl+C to quit)", ""]

    out.append(f"  DEVICES  (resource manager @ {poller.rm_addr})")
    out += [f"  (unavailable: {perr})"] if perr is not None else _devices_lines(pools)
    out.append("")

    title = "INSTANCES  (auto-discovered)" if poller.auto else "INSTANCES"
    out.append(f"  {title}")
    if poller.auto and not servers:
        out.append(
            "  (no maru-server found on localhost; pass -p PORT for a remote one)"
        )
    for s in servers:
        if poller.auto or len(servers) > 1:
            pid = f" (pid {s['pid']})" if s["pid"] else ""
            out.append(f"  ── server {s['label']}{pid} ──")
        resp, err = s["usage"]
        out += (
            [f"  (unavailable: {err})"]
            if err is not None
            else _instance_rows(resp, sort_key)
        )

    footer = _shared_pool_line(servers)
    if footer:
        out += ["", footer]

    # STATS section (per-op summary; needs MARU_STAT=1 on clients).
    out += ["", "  STATS  (ops; needs MARU_STAT=1 on clients)"]
    for s in servers:
        if poller.auto or len(servers) > 1:
            out.append(f"  ── server {s['label']} ──")
        sm, err = s["stats"]
        rows = _summarize_ops(sm, "_all") if sm is not None else None
        out += _stats_lines(rows, err)
    return "\n".join(out + [""])


def _stats_lines(rows, err) -> list[str]:
    if err is not None:
        return [f"  (unavailable: {err})"]
    if not rows:
        return ["  (no op stats — enable MARU_STAT=1 on the vLLM clients)"]
    lines = [f"  {'op':<16}  {'count':>10}  {'avg_us':>9}  {'hit%':>6}"]
    for r in rows:
        hitpct = ""
        if r["op"] in _HIT_MISS_OPS and r["count"]:
            hitpct = f"{r['hit'] / r['count'] * 100:.1f}"
        lines.append(
            f"  {r['op']:<16}  {r['count']:>10}  {r['avg_us']:>9.1f}  {hitpct:>6}"
        )
    return lines


def _shared_pool_line(servers) -> str | None:
    """Shared-pool footer from the first server that answered (all share one RM)."""
    for s in servers:
        resp, err = s["usage"]
        if err is None and resp is not None:
            used = resp.pool_total - resp.pool_free
            return (
                f"  Pool (shared): {fmt_size(resp.pool_free)} free / "
                f"{fmt_size(resp.pool_total)} total  {usage_bar(used, resp.pool_total)}"
            )
    return None


# =============================================================================
# Curses TUI (the default, pxltop-style)
# =============================================================================


def _safe_addstr(win, y: int, x: int, text: str, attr: int = 0) -> None:
    """addstr that silently clips writes outside window bounds."""
    max_y, max_x = win.getmaxyx()
    if y < 0 or y >= max_y or x >= max_x:
        return
    available = max_x - x
    if available <= 0:
        return
    try:
        win.addnstr(y, x, text, available, attr)
    except curses.error:
        pass  # writing the last cell raises on some terminals


def _sort_instances(instances, sort_key: str):
    if sort_key == "allocated":
        return sorted(instances, key=lambda i: (-i.allocated, i.instance_id))
    if sort_key == "used":
        return sorted(instances, key=lambda i: (-i.used, i.instance_id))
    if sort_key == "slack":
        return sorted(instances, key=lambda i: (-(i.allocated - i.used), i.instance_id))
    return sorted(instances, key=lambda i: i.instance_id)


def _draw_gauge(
    stdscr,
    y: int,
    x: int,
    label: str,
    ratio: float,
    used_str: str,
    total_str: str,
    colors: bool,
    bar_width: int = 24,
) -> None:
    """One labelled gauge row: ``LBL [||||    ]  75.0%   used / total``."""
    cp = _color_for_ratio(ratio)
    dim = curses.color_pair(_CP_DIM) if colors else 0
    col = curses.color_pair(cp) if colors else 0
    _safe_addstr(stdscr, y, x, f"{label:<16}", dim)
    bx = x + 17
    _safe_addstr(stdscr, y, bx, "[")
    _safe_addstr(stdscr, y, bx + 1, _make_bar(ratio, bar_width), col)
    _safe_addstr(stdscr, y, bx + 1 + bar_width, "]")
    _safe_addstr(stdscr, y, bx + bar_width + 4, f"{ratio:5.1f}%", col)
    _safe_addstr(stdscr, y, bx + bar_width + 12, f"{used_str:>9} / {total_str}")


def _dax_short(path: str) -> str:
    """'/dev/dax0.0' -> 'dax0.0' for compact labels."""
    return path.rsplit("/", 1)[-1] if path else "(unknown)"


def _draw_instance_rows(
    stdscr,
    y: int,
    usage,
    sort_key: str,
    colors: bool,
    dev_totals: dict[str, int],
    sel_id: str | None = None,
) -> int:
    hdr = (
        f"    {'owner_instance_id':<38}  {'regions':>7}  "
        f"{'allocated':>9}  {'used':>9}  {'slack':>9}"
    )
    _safe_addstr(stdscr, y, 0, hdr, curses.A_BOLD)
    y += 1
    instances = _sort_instances(usage.instances, sort_key)
    if not instances:
        _safe_addstr(stdscr, y, 2, "(no active instances)", _dim(colors))
        return y + 1
    for inst in instances:
        slack = inst.allocated - inst.used
        selected = inst.instance_id == sel_id
        # Color a row when its reservation is nearly all live data (little
        # slack) — used/allocated is the meaningful ratio here.
        ratio = (inst.used / inst.allocated * 100) if inst.allocated else 0.0
        if selected:
            attr = curses.A_REVERSE | curses.A_BOLD
        else:
            cp = _color_for_ratio(ratio) if (colors and ratio >= 60.0) else 0
            attr = curses.color_pair(cp) if cp else 0
        prefix = "  ▶ " if selected else "    "
        _safe_addstr(
            stdscr,
            y,
            0,
            f"{prefix}{inst.instance_id:<38}  {inst.regions:>7}  "
            f"{fmt_size(inst.allocated):>9}  {fmt_size(inst.used):>9}  "
            f"{fmt_size(slack):>9}",
            attr,
        )
        y += 1
        # Per-device gauge(s): how much of each DAX device this instance holds.
        for dax_path, nbytes in sorted(inst.devices.items()):
            total = dev_totals.get(dax_path, 0)
            dratio = (nbytes / total * 100) if total else 0.0
            total_str = fmt_size(total) if total else "?"
            _draw_gauge(
                stdscr,
                y,
                6,
                f"└ {_dax_short(dax_path)}",
                dratio,
                fmt_size(nbytes),
                total_str,
                colors,
                bar_width=18,
            )
            y += 1
    total_alloc = sum(i.allocated for i in instances)
    total_used = sum(i.used for i in instances)
    _safe_addstr(
        stdscr,
        y,
        0,
        f"    {'TOTAL':<38}  {len(instances):>7}  "
        f"{fmt_size(total_alloc):>9}  {fmt_size(total_used):>9}  "
        f"{fmt_size(total_alloc - total_used):>9}",
        curses.A_BOLD,
    )
    return y + 1


def _dim(colors: bool) -> int:
    return curses.color_pair(_CP_DIM) if colors else 0


def _flatten_instances(servers, sort_key: str) -> list[tuple[dict, object]]:
    """Flat, ordered [(server, instance)] across all servers with usage data.

    Matches the on-screen draw order so a selection index lines up with rows.
    """
    flat: list[tuple[dict, object]] = []
    for s in servers:
        resp, err = s["usage"]
        if err is None and resp is not None:
            for inst in _sort_instances(resp.instances, sort_key):
                flat.append((s, inst))
    return flat


def _draw_main(stdscr, poller: _Poller, sort_key: str, selected: int) -> int:
    """Overview screen: DEVICES + INSTANCES (selectable). Returns instance count."""
    stdscr.erase()
    max_y, _ = stdscr.getmaxyx()
    colors = curses.has_colors()
    header_attr = (
        (curses.color_pair(_CP_HEADER) | curses.A_BOLD) if colors else curses.A_BOLD
    )
    dim = _dim(colors)

    (pools, perr), servers, count = poller.snapshot()
    fetched = count > 0
    # Device capacities (for per-instance device gauges), keyed by dax_path.
    dev_totals = {p.dax_path: p.total_size for p in pools} if pools else {}
    flat = _flatten_instances(servers, sort_key)
    sel_id = flat[selected][1].instance_id if 0 <= selected < len(flat) else None

    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    _safe_addstr(stdscr, 0, 0, "  marutop", header_attr)
    _safe_addstr(stdscr, 0, 11, f"{ts}   interval {poller.interval:.1f}s")

    y = 2

    # ── DEVICES ─────────────────────────────────────────────────────────────
    _safe_addstr(stdscr, y, 0, "  DEVICES", header_attr)
    _safe_addstr(stdscr, y, 12, f"(resource manager @ {poller.rm_addr})", dim)
    y += 1
    if not fetched:
        _safe_addstr(stdscr, y, 2, "(connecting...)", dim)
        y += 1
    elif perr is not None:
        _safe_addstr(stdscr, y, 2, f"(unavailable: {perr})", dim)
        y += 1
    elif not pools:
        _safe_addstr(stdscr, y, 2, "(no pools found)", dim)
        y += 1
    else:
        for p in sorted(pools, key=lambda x: x.dax_path):
            used = p.total_size - p.free_size
            ratio = (used / p.total_size * 100) if p.total_size else 0.0
            _draw_gauge(
                stdscr,
                y,
                2,
                p.dax_path or "(unknown)",
                ratio,
                fmt_size(used),
                fmt_size(p.total_size),
                colors,
            )
            y += 1
    y += 1

    # ── INSTANCES ─────────────────────────────────────────────────────────────
    title = "  INSTANCES"
    _safe_addstr(stdscr, y, 0, title, header_attr)
    hint = "(auto-discovered)" if poller.auto else "(pinned)"
    _safe_addstr(stdscr, y, 14, hint, dim)
    y += 1
    if not fetched:
        _safe_addstr(stdscr, y, 2, "(connecting...)", dim)
        y += 1
    elif poller.auto and not servers:
        _safe_addstr(
            stdscr,
            y,
            2,
            "(no maru-server found on localhost; pass -p PORT for a remote one)",
            dim,
        )
        y += 1
    else:
        multi = poller.auto or len(servers) > 1
        for s in servers:
            if multi:
                pid = f" (pid {s['pid']})" if s["pid"] else ""
                _safe_addstr(stdscr, y, 2, f"── server {s['label']}{pid} ──", dim)
                y += 1
            resp, err = s["usage"]
            if err is not None:
                _safe_addstr(stdscr, y, 2, f"(unavailable: {err})", dim)
                y += 1
            else:
                y = _draw_instance_rows(
                    stdscr, y, resp, sort_key, colors, dev_totals, sel_id
                )
        # Shared-pool footer (all servers share one RM) — from first that answered.
        for s in servers:
            resp, err = s["usage"]
            if err is None and resp is not None:
                used = resp.pool_total - resp.pool_free
                ratio = (used / resp.pool_total * 100) if resp.pool_total else 0.0
                y += 1
                _draw_gauge(
                    stdscr,
                    y,
                    2,
                    "Pool (shared)",
                    ratio,
                    fmt_size(used),
                    fmt_size(resp.pool_total),
                    colors,
                )
                y += 1
                break

    # ── footer key hints (pinned to last row, htop-style) ──────────────────────
    if flat:
        footer = "↑↓:select  enter:stats  s:sort  i:interval  q:quit"
    else:
        footer = "s:sort  i:interval  q:quit"
    _safe_addstr(stdscr, max_y - 1, 0, "  " + footer, dim)
    stdscr.refresh()
    return len(flat)


_GRAPH_HEIGHT = 7


def _draw_lat_graph(stdscr, y, x, values, width, height=_GRAPH_HEIGHT):
    """3-line min/avg/max latency graph over time (max=red ▴, avg=green •,
    min=blue ▾). Adapted from the stats dashboard, using the live palette."""
    data = values[-width:] if len(values) >= 2 else []
    all_vals = [v for tup in data for v in tup if v > 0]
    lo, hi = 0.0, (max(all_vals) if all_vals else 1.0)
    if hi == lo:
        hi = lo + 1
    label_w = 8
    plot_w = min(len(data), width) if data else width
    has = curses.has_colors()
    max_attr = curses.color_pair(_CP_CRIT) if has else 0
    avg_attr = curses.color_pair(_CP_OK) if has else 0
    min_attr = curses.color_pair(_CP_DIM) if has else 0

    def row_of(v):
        r = int((v - lo) / (hi - lo) * (height - 1))
        return max(0, min(height - 1, height - 1 - r))

    grid: list[list] = [[None] * plot_w for _ in range(height)]
    for col, (mn, avg, mx) in enumerate(data[-plot_w:]):
        if mx > 0:
            grid[row_of(mx)][col] = ("▴", max_attr)
        if mn > 0:
            grid[row_of(mn)][col] = ("▾", min_attr)
        if avg > 0:
            grid[row_of(avg)][col] = ("•", avg_attr)
    for r in range(height):
        fmt = ".0f" if hi >= 100 else ".1f"
        if r == 0:
            label = f"{hi:>{label_w - 2}{fmt}} ┤"
        elif r == height - 1:
            label = f"{lo:>{label_w - 2}{fmt}} ┤"
        else:
            label = " " * (label_w - 1) + "│"
        _safe_addstr(stdscr, y + r, x, label)
        for col in range(plot_w):
            cell = grid[r][col]
            if cell is not None:
                _safe_addstr(stdscr, y + r, x + label_w + col, cell[0], cell[1])
    _safe_addstr(stdscr, y + height, x, " " * (label_w - 1) + "└" + "─" * plot_w)
    return height + 1


def _draw_stats_view(
    stdscr, poller: _Poller, sort_key: str, selected: int, op_sel: int
) -> int:
    """Full per-instance dashboard: op table (+sparkline/delta), a detail box
    and a min/avg/max latency graph for the selected op. Returns the op count."""
    stdscr.erase()
    max_y, max_x = stdscr.getmaxyx()
    colors = curses.has_colors()
    header_attr = (
        (curses.color_pair(_CP_HEADER) | curses.A_BOLD) if colors else curses.A_BOLD
    )
    accent = (curses.color_pair(_CP_WARN) | curses.A_BOLD) if colors else curses.A_BOLD
    dim = _dim(colors)
    (_pool, servers, _count) = poller.snapshot()
    flat = _flatten_instances(servers, sort_key)

    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    _safe_addstr(stdscr, 0, 0, "  marutop · STATS", header_attr)
    _safe_addstr(stdscr, 0, 20, f"{ts}   interval {poller.interval:.1f}s")

    def _footer(nops: int) -> None:
        _safe_addstr(
            stdscr,
            max_y - 1,
            0,
            f"  esc/←:back  ↑↓:op  i:interval  q:quit"
            f"   [inst {selected + 1}/{len(flat)}]",
            dim,
        )
        stdscr.refresh()

    if not flat:
        _safe_addstr(stdscr, 2, 2, "(no instances)", dim)
        _footer(0)
        return 0
    if selected >= len(flat):
        selected = len(flat) - 1

    s, inst = flat[selected]
    slack = inst.allocated - inst.used
    _safe_addstr(stdscr, 2, 2, f"instance {inst.instance_id}  @ {s['label']}", accent)
    _safe_addstr(
        stdscr,
        3,
        2,
        f"regions {inst.regions}   allocated {fmt_size(inst.allocated)}   "
        f"used {fmt_size(inst.used)}   slack {fmt_size(slack)}   "
        f"devices {', '.join(_dax_short(d) for d in sorted(inst.devices))}",
        dim,
    )

    sm, err = s["stats"]
    if err is not None:
        _safe_addstr(stdscr, 5, 2, f"(stats unavailable: {err})", dim)
        _footer(0)
        return 0
    rows = _summarize_ops(sm, inst.instance_id) if sm is not None else []
    if not rows:
        _safe_addstr(
            stdscr,
            5,
            2,
            "(no op stats for this instance — enable MARU_STAT=1 on the client)",
            dim,
        )
        _footer(0)
        return 0

    op_names = [r["op"] for r in rows]
    if op_sel >= len(op_names):
        op_sel = len(op_names) - 1
    c_hist, l_hist, s_hist = poller.get_history(s["label"], inst.instance_id)

    # ── op table ──────────────────────────────────────────────────────────
    y = 5
    _safe_addstr(
        stdscr,
        y,
        2,
        f"{'op':<16} {'count':>9} {'delta':>6} {'avg_us':>8} {'min_us':>8} "
        f"{'max_us':>8}  {'activity':>{_SPARK_TABLE_WIDTH}}",
        curses.A_BOLD,
    )
    y += 1
    for i, r in enumerate(rows):
        op = r["op"]
        ch = c_hist.get(op, [])
        delta = f"+{ch[-1]}" if ch and ch[-1] > 0 else ""
        sh = s_hist.get(op, [])
        spark = _sparkline(sh, _SPARK_TABLE_WIDTH) if any(v > 0 for v in sh) else ""
        prefix = "▶ " if i == op_sel else "  "
        line = (
            f"{prefix}{op:<16} {r['count']:>9} {delta:>6} {r['avg_us']:>8.1f} "
            f"{r['min_us']:>8.1f} {r['max_us']:>8.1f}  {spark:>{_SPARK_TABLE_WIDTH}}"
        )
        attr = (curses.A_REVERSE | curses.A_BOLD) if i == op_sel else 0
        _safe_addstr(stdscr, y, 2, line, attr)
        y += 1

    # ── detail for the selected op ────────────────────────────────────────
    sel = rows[op_sel]
    y += 1
    _safe_addstr(stdscr, y, 2, f"── {sel['op']} ──", accent)
    y += 1
    parts = [f"count: {sel['count']}"]
    if sel["op"] in _HIT_MISS_OPS:
        parts += [f"hit: {sel['hit']}", f"miss: {sel['miss']}"]
    parts.append(f"bytes: {fmt_size(sel['bytes'])}")
    _safe_addstr(stdscr, y, 2, "   ".join(parts))
    y += 1
    if sel["op"] in _HIT_MISS_OPS and sel["count"]:
        pct = sel["hit"] / sel["count"]
        bar = "█" * int(pct * 20) + "░" * (20 - int(pct * 20))
        _safe_addstr(stdscr, y, 2, f"hit rate: {bar} {pct * 100:.1f}%")
        y += 1
    _safe_addstr(
        stdscr,
        y,
        2,
        f"latency (us): avg={sel['avg_us']:.1f}  min={sel['min_us']:.1f}  "
        f"max={sel['max_us']:.1f}",
    )
    y += 1
    if sel["count"] and sel["avg_us"] > 0 and sel["bytes"]:
        total_s = sel["count"] * sel["avg_us"] / 1e6
        if total_s > 0:
            _safe_addstr(
                stdscr, y, 2, f"throughput:   {sel['bytes'] / 1e6 / total_s:.1f} MB/s"
            )
            y += 1

    # ── latency graph for the selected op ─────────────────────────────────
    lat = l_hist.get(sel["op"], [])
    if any(a > 0 for _, a, _ in lat) and y + _GRAPH_HEIGHT + 2 < max_y:
        y += 1
        _safe_addstr(stdscr, y, 2, "latency (us) over time:", dim)
        y += 1
        gw = min(_HISTORY_WIDTH, max_x - 14)
        y += _draw_lat_graph(stdscr, y, 4, lat, gw)
        _safe_addstr(
            stdscr, y, 12, "▴=max ", curses.color_pair(_CP_CRIT) if colors else 0
        )
        _safe_addstr(
            stdscr, y, 18, "•=avg ", curses.color_pair(_CP_OK) if colors else 0
        )
        _safe_addstr(
            stdscr, y, 24, "▾=min", curses.color_pair(_CP_DIM) if colors else 0
        )

    _footer(len(op_names))
    return len(op_names)


def _init_colors() -> None:
    if not curses.has_colors():
        return
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(_CP_HEADER, curses.COLOR_CYAN, -1)
    curses.init_pair(_CP_OK, curses.COLOR_GREEN, -1)
    curses.init_pair(_CP_WARN, curses.COLOR_YELLOW, -1)
    curses.init_pair(_CP_CRIT, curses.COLOR_RED, -1)
    curses.init_pair(_CP_DIM, curses.COLOR_BLUE, -1)


def _prompt_interval(stdscr, poller: _Poller) -> None:
    """Prompt for a new refresh interval (pxltop 'i' behaviour).

    getstr() must block for typed input, so the per-frame timeout is disabled
    for the duration of the prompt and restored afterwards.
    """
    max_y, _ = stdscr.getmaxyx()
    curses.echo()
    curses.curs_set(1)
    stdscr.timeout(-1)  # blocking: let the user actually type
    _safe_addstr(stdscr, max_y - 2, 0, "  Enter new interval (seconds): ")
    stdscr.clrtoeol()
    stdscr.refresh()
    try:
        raw = stdscr.getstr(max_y - 2, 32, 8)
        poller.set_interval(float(raw.decode().strip()))
    except Exception:
        pass
    finally:
        curses.noecho()
        curses.curs_set(0)
        stdscr.timeout(100)


def _tui_loop(stdscr, poller: _Poller, max_count: int) -> None:
    curses.curs_set(0)
    stdscr.timeout(100)  # UI never blocks longer than this on input
    _init_colors()
    sort_key = "name"
    view = "main"  # "main" (overview) or "stats" (per-instance dashboard)
    selected = 0  # instance index (overview)
    op_sel = 0  # op index (stats view)
    poller.start()
    try:
        while True:
            # `n` is the count for the CURRENT view: instances (main) or ops (stats).
            if view == "stats":
                n = _draw_stats_view(stdscr, poller, sort_key, selected, op_sel)
                if n <= 0:
                    op_sel = 0
                elif op_sel >= n:
                    op_sel = n - 1
            else:
                n = _draw_main(stdscr, poller, sort_key, selected)
                if n <= 0:
                    selected = 0
                elif selected >= n:
                    selected = n - 1

            if max_count:
                _, _, fetched_n = poller.snapshot()
                if fetched_n >= max_count:
                    return

            key = stdscr.getch()
            if key in (ord("q"), ord("Q"), 3):  # q / Ctrl-C
                return
            if key == 27:  # ESC: leave stats view, or quit from main
                if view == "stats":
                    view = "main"
                else:
                    return
                continue

            if view == "main":
                if key == curses.KEY_UP:
                    selected = max(0, selected - 1)
                elif key == curses.KEY_DOWN:
                    selected = min(max(n - 1, 0), selected + 1)
                elif key in (curses.KEY_ENTER, 10, 13, curses.KEY_RIGHT):
                    if n > 0:
                        view, op_sel = "stats", 0
                elif key in (ord("s"), ord("S")):
                    i = _SORT_KEYS.index(sort_key)
                    sort_key = _SORT_KEYS[(i + 1) % len(_SORT_KEYS)]
                elif key in (ord("i"), ord("I")):
                    _prompt_interval(stdscr, poller)
            else:  # stats view — ↑↓ selects an operation
                if key == curses.KEY_LEFT:
                    view = "main"
                elif key == curses.KEY_UP:
                    op_sel = max(0, op_sel - 1)
                elif key == curses.KEY_DOWN:
                    op_sel = min(max(n - 1, 0), op_sel + 1)
                elif key in (ord("i"), ord("I")):
                    _prompt_interval(stdscr, poller)
    finally:
        poller.stop()


# =============================================================================
# Entry points
# =============================================================================


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--address",
        type=str,
        default=None,
        help="Resource manager address (host:port, default: 127.0.0.1:9850)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Pin one MaruServer host (default: auto-discover local servers)",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=None,
        help="Pin one MaruServer port (default: auto-discover local servers)",
    )
    parser.add_argument(
        "-w",
        "--watch",
        type=float,
        default=1.0,
        help="Refresh interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "-c",
        "--count",
        type=int,
        default=0,
        help="Quit after N refreshes (0 = run until 'q'); useful for screenshots",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Single plain-text snapshot then exit (no curses; scriptable)",
    )


def _quiet_loggers() -> None:
    # A down/slow server logs an ERROR per timeout; in a refreshing screen that
    # is pure noise (we already render "(unavailable)"), so silence the loggers.
    logging.getLogger("maru").setLevel(logging.CRITICAL)
    logging.getLogger("maru_handler").setLevel(logging.CRITICAL)
    logging.getLogger("maru_common").setLevel(logging.CRITICAL)


def run(args: argparse.Namespace) -> None:
    _quiet_loggers()
    poller = _Poller(args)

    if args.once:
        # Do one polling pass synchronously, render text, done.
        poller.start()
        import time as _time

        for _ in range(100):  # wait up to ~5s for the first fetch
            if poller.snapshot()[2] > 0:
                break
            _time.sleep(0.05)
        clear_screen()
        print(_render_text(poller, "name"))
        poller.stop()
        return

    try:
        curses.wrapper(lambda stdscr: _tui_loop(stdscr, poller, args.count))
    except KeyboardInterrupt:
        pass


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Maru unified live view (pool + per-instance)"
    )
    add_arguments(parser)
    run(parser.parse_args(argv))


if __name__ == "__main__":
    main()
