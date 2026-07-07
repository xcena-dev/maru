"""Unified live view — the default ``marutop`` screen.

Fuses backends into one ``htop``-style curses TUI, the analog of XCENA's
``pxltop`` (a DEVICES gauge section + a per-owner section):

- **DEVICES**   — physical DAX pools from the Resource Manager (:9850)
- **INSTANCES** — per-instance allocated/used/slack from MaruServer(s) (:5555)
- **STATS**     — compact per-op summary (count/avg_us/hit%) from the same
  server(s); populated only when clients run with ``MARU_STAT=1``. The full
  latency-graph dashboard remains ``marutop stats``.

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

Keys:  [s]ort instances   [i]nterval   [q]uit
"""

import argparse
import curses
import logging
import threading
from datetime import datetime
from pathlib import Path

from maru_tools._common import clear_screen, fmt_size, usage_bar

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


def _summarize_ops(stats_resp) -> list[dict]:
    """Compact per-op rows from a GetStatsResponse, busiest first.

    Uses the server-aggregated ``_all`` client. Only ops with activity are
    returned. Stats are populated only when clients run with ``MARU_STAT=1``;
    otherwise this is empty (the caller shows a hint).
    """
    sm = getattr(stats_resp, "stats_manager", None) or {}
    clients = sm.get("clients", {}) if isinstance(sm, dict) else {}
    ops = clients.get("_all", {}).get("operations", {})
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
                        stats = (_summarize_ops(cli.get_stats()), None)
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
        rows, err = s["stats"]
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
    stdscr, y: int, usage, sort_key: str, colors: bool, dev_totals: dict[str, int]
) -> int:
    hdr = (
        f"  {'owner_instance_id':<38}  {'regions':>7}  "
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
        # Color a row when its reservation is nearly all live data (little
        # slack) — used/allocated is the meaningful ratio here.
        ratio = (inst.used / inst.allocated * 100) if inst.allocated else 0.0
        cp = _color_for_ratio(ratio) if (colors and ratio >= 60.0) else 0
        _safe_addstr(
            stdscr,
            y,
            0,
            f"  {inst.instance_id:<38}  {inst.regions:>7}  "
            f"{fmt_size(inst.allocated):>9}  {fmt_size(inst.used):>9}  "
            f"{fmt_size(slack):>9}",
            curses.color_pair(cp) if cp else 0,
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
        f"  {'TOTAL':<38}  {len(instances):>7}  "
        f"{fmt_size(total_alloc):>9}  {fmt_size(total_used):>9}  "
        f"{fmt_size(total_alloc - total_used):>9}",
        curses.A_BOLD,
    )
    return y + 1


def _dim(colors: bool) -> int:
    return curses.color_pair(_CP_DIM) if colors else 0


def _draw_screen(stdscr, poller: _Poller, sort_key: str) -> None:
    stdscr.erase()
    colors = curses.has_colors()
    header_attr = (
        (curses.color_pair(_CP_HEADER) | curses.A_BOLD) if colors else curses.A_BOLD
    )
    dim = _dim(colors)

    (pools, perr), servers, count = poller.snapshot()
    fetched = count > 0
    # Device capacities (for per-instance device gauges), keyed by dax_path.
    dev_totals = {p.dax_path: p.total_size for p in pools} if pools else {}

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
                y = _draw_instance_rows(stdscr, y, resp, sort_key, colors, dev_totals)
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

    # ── STATS (compact per-op summary; needs MARU_STAT=1 on clients) ───────────
    if fetched and servers:
        y += 1
        _safe_addstr(stdscr, y, 0, "  STATS", header_attr)
        _safe_addstr(stdscr, y, 8, "(ops; needs MARU_STAT=1 on clients)", dim)
        y += 1
        multi = poller.auto or len(servers) > 1
        for s in servers:
            if multi:
                _safe_addstr(stdscr, y, 2, f"── server {s['label']} ──", dim)
                y += 1
            rows, err = s["stats"]
            y = _draw_stats_rows(stdscr, y, rows, err, colors)

    # ── footer key hints (pinned to last row, htop-style) ──────────────────────
    max_y, _ = stdscr.getmaxyx()
    footer = f"sort: {sort_key}   [s]ort  [i]nterval  [q]uit"
    _safe_addstr(stdscr, max_y - 1, 0, "  " + footer, dim)
    stdscr.refresh()


def _draw_stats_rows(stdscr, y: int, rows, err, colors: bool) -> int:
    dim = _dim(colors)
    if err is not None:
        _safe_addstr(stdscr, y, 2, f"(unavailable: {err})", dim)
        return y + 1
    if not rows:
        _safe_addstr(
            stdscr, y, 2, "(no op stats — enable MARU_STAT=1 on the vLLM clients)", dim
        )
        return y + 1
    _safe_addstr(
        stdscr,
        y,
        0,
        f"  {'op':<16}  {'count':>10}  {'avg_us':>9}  {'hit%':>6}",
        curses.A_BOLD,
    )
    y += 1
    for r in rows:
        hitpct = ""
        if r["op"] in _HIT_MISS_OPS and r["count"]:
            hitpct = f"{r['hit'] / r['count'] * 100:.1f}"
        _safe_addstr(
            stdscr,
            y,
            0,
            f"  {r['op']:<16}  {r['count']:>10}  {r['avg_us']:>9.1f}  {hitpct:>6}",
        )
        y += 1
    return y


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
    poller.start()
    try:
        while True:
            _draw_screen(stdscr, poller, sort_key)

            if max_count:
                _, _, count = poller.snapshot()
                if count >= max_count:
                    return

            key = stdscr.getch()
            if key in (ord("q"), ord("Q"), 27, 3):  # q/Q/ESC/Ctrl-C
                return
            if key in (ord("s"), ord("S")):
                i = _SORT_KEYS.index(sort_key)
                sort_key = _SORT_KEYS[(i + 1) % len(_SORT_KEYS)]
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
