#!/usr/bin/env python3
"""Maru server operation stats monitor.

Connects to MaruServer via RPC and displays real-time operation metrics
with per-operation sparkline latency graphs and interactive detail panel.

Usage:
    python -m tools.stats_monitor -p 11008
    python -m tools.stats_monitor --host 10.0.0.1 -p 5556
"""

import argparse
import curses
import logging
from datetime import datetime

from maru_handler.rpc_client import RpcClient

# Sparkline block characters: space (0) + U+2581..U+2587 (1-7)
_SPARK_CHARS = " \u2581\u2582\u2583\u2584\u2585\u2586\u2587"
_HISTORY_WIDTH = 50
_SPARK_TABLE_WIDTH = _HISTORY_WIDTH // 2  # sparkline shows last 25 ticks
_KNOWN_OPS = [
    "alloc", "free", "store", "retrieve", "exists", "pin", "unpin", "delete",
    "batch_store", "batch_retrieve", "batch_exists", "batch_pin", "batch_unpin",
]
_ZERO_LAT = (0.0, 0.0, 0.0)
_HIT_MISS_OPS = {"retrieve", "batch_retrieve", "exists", "batch_exists"}
_DATA_OPS = {"store", "retrieve", "batch_store", "batch_retrieve"}
_EMPTY_OP = {
    "count": 0, "hit_count": 0, "miss_count": 0, "total_bytes": 0,
    "latency_sum_us": 0, "avg_latency_us": 0, "min_latency_us": 0, "max_latency_us": 0,
    "interval_count": 0, "interval_avg_us": 0, "interval_min_us": 0, "interval_max_us": 0,
}


def _fmt_size(nbytes: int) -> str:
    if nbytes == 0:
        return "0B"
    if nbytes >= 1024**3:
        return f"{nbytes / 1024**3:.1f}G"
    if nbytes >= 1024**2:
        return f"{nbytes / 1024**2:.1f}M"
    if nbytes >= 1024:
        return f"{nbytes / 1024:.1f}K"
    return f"{nbytes}B"


def _sparkline(values: list[float], width: int = _HISTORY_WIDTH) -> str:
    """Render a list of floats as a sparkline string."""
    if not values:
        return ""
    hi = max(values)
    if hi <= 0:
        return " " * min(len(values), width)
    chars = []
    for v in values[-width:]:
        idx = int(v / hi * 6) + 1 if v > 0 else 0
        idx = min(idx, 7)
        chars.append(_SPARK_CHARS[idx])
    return "".join(chars)


_GRAPH_HEIGHT = 8


def _draw_line_graph(
    win, y: int, x: int, values: list[int],
    width: int = _HISTORY_WIDTH, height: int = _GRAPH_HEIGHT,
) -> int:
    """Draw a single-line count graph. Returns rows consumed."""
    if len(values) < 2:
        _safe_addstr(win, y, x, "(waiting for data...)")
        return 1

    data = values[-width:]
    hi = max(data)
    if hi <= 0:
        _safe_addstr(win, y, x, "(no activity in window)")
        return 1
    lo = 0  # count always starts from 0

    label_w = 8
    plot_w = min(len(data), width)
    point_attr = curses.color_pair(2) if curses.has_colors() else 0

    def _val_to_row(v: int) -> int:
        r = int(v / hi * (height - 1))
        return max(0, min(height - 1, height - 1 - r))

    # Build grid
    grid = [[" "] * plot_w for _ in range(height)]
    for col, v in enumerate(data[-plot_w:]):
        if v > 0:
            grid[_val_to_row(v)][col] = "•"

    # Draw rows
    for r in range(height):
        mid = hi // 2
        show_mid = r == height // 2 and mid != hi and mid != lo
        if r == 0:
            label = f"{hi:>{label_w - 2}} ┤"
        elif r == height - 1:
            label = f"{lo:>{label_w - 2}} ┤"
        elif show_mid:
            label = f"{mid:>{label_w - 2}} ┤"
        else:
            label = " " * (label_w - 1) + "│"

        _safe_addstr(win, y + r, x, label)
        col_x = x + label_w
        for col in range(plot_w):
            if grid[r][col] != " ":
                _safe_addstr(win, y + r, col_x + col, grid[r][col], point_attr)

    # X-axis
    _safe_addstr(win, y + height, x, " " * (label_w - 1) + "└" + "─" * plot_w)

    return height + 1


def _draw_latency_graph(
    win, y: int, x: int, values: list[tuple[float, float, float]],
    width: int = _HISTORY_WIDTH, height: int = _GRAPH_HEIGHT,
) -> int:
    """Draw 3-line (min/avg/max) latency graph. Returns rows consumed.

    Colors: max=red(▴), avg=green(•), min=blue(▾).
    """
    data = values[-width:] if len(values) >= 2 else []
    all_vals = [v for tup in data for v in tup if v > 0]

    lo = 0.0
    hi = max(all_vals) if all_vals else 1.0
    if hi == lo:
        hi = lo + 1

    label_w = 8
    plot_w = min(len(data), width) if data else width
    has_colors = curses.has_colors()
    max_attr = curses.color_pair(5) if has_colors else 0
    avg_attr = curses.color_pair(2) if has_colors else 0
    min_attr = curses.color_pair(6) if has_colors else 0

    def _val_to_row(v: float) -> int:
        r = int((v - lo) / (hi - lo) * (height - 1))
        return max(0, min(height - 1, height - 1 - r))

    grid: list[list[tuple[str, int] | None]] = [[None] * plot_w for _ in range(height)]
    for col, (mn, avg, mx) in enumerate(data[-plot_w:]):
        if mx > 0:
            grid[_val_to_row(mx)][col] = ("▴", max_attr)
        if mn > 0:
            grid[_val_to_row(mn)][col] = ("▾", min_attr)
        if avg > 0:
            grid[_val_to_row(avg)][col] = ("•", avg_attr)

    for r in range(height):
        mid = (hi + lo) / 2
        show_mid = r == height // 2 and mid != hi and mid != lo
        fmt = ".0f" if hi >= 100 else ".1f"
        if r == 0:
            label = f"{hi:>{label_w - 2}{fmt}} ┤"
        elif r == height - 1:
            label = f"{lo:>{label_w - 2}{fmt}} ┤"
        elif show_mid:
            label = f"{mid:>{label_w - 2}{fmt}} ┤"
        else:
            label = " " * (label_w - 1) + "│"

        _safe_addstr(win, y + r, x, label)
        col_x = x + label_w
        for col in range(plot_w):
            cell = grid[r][col]
            if cell is not None:
                _safe_addstr(win, y + r, col_x + col, cell[0], cell[1])

    _safe_addstr(win, y + height, x, " " * (label_w - 1) + "└" + "─" * plot_w)

    legend_y = y + height + 1
    _safe_addstr(win, legend_y, x + label_w, "▴", max_attr)
    _safe_addstr(win, legend_y, x + label_w + 1, "=max  ")
    _safe_addstr(win, legend_y, x + label_w + 7, "•", avg_attr)
    _safe_addstr(win, legend_y, x + label_w + 8, "=avg  ")
    _safe_addstr(win, legend_y, x + label_w + 14, "▾", min_attr)
    _safe_addstr(win, legend_y, x + label_w + 15, "=min")

    return height + 2


def fetch_stats(client: RpcClient) -> dict:
    resp = client.get_stats()
    return {
        "kv_manager": {
            "total_entries": resp.kv_manager.total_entries,
            "total_size": resp.kv_manager.total_size,
        },
        "allocation_manager": {
            "num_allocations": resp.allocation_manager.num_allocations,
            "total_allocated": resp.allocation_manager.total_allocated,
            "active_clients": resp.allocation_manager.active_clients,
        },
        "stats_manager": resp.stats_manager,
    }


def _get_client_ids(stats: dict) -> list[str]:
    """Get sorted client IDs (excluding _all)."""
    clients = stats.get("stats_manager", {}).get("clients", {})
    return sorted(k for k in clients if k != "_all")


def _get_ops(stats: dict, client_key: str = "_all") -> dict:
    """Get operations dict for a specific client, with known ops defaulted."""
    clients = stats.get("stats_manager", {}).get("clients", {})
    raw_ops = clients.get(client_key, {}).get("operations", {})
    ops = {name: raw_ops.get(name, _EMPTY_OP) for name in _KNOWN_OPS}
    for name in raw_ops:
        if name not in ops:
            ops[name] = raw_ops[name]
    return ops


def _extract_interval(
    ops: dict,
) -> dict[str, tuple[float, float, float, int]]:
    """Extract interval (min, avg, max, count) from server-provided stats.

    Returns:
        dict mapping op_name -> (min_us, avg_us, max_us, count)
    """
    result: dict[str, tuple[float, float, float, int]] = {}
    for name, o in ops.items():
        iv_count = o.get("interval_count", 0)
        if iv_count > 0:
            result[name] = (
                o.get("interval_min_us", 0.0),
                o.get("interval_avg_us", 0.0),
                o.get("interval_max_us", 0.0),
                iv_count,
            )
    return result


# =============================================================================
# Curses TUI
# =============================================================================

def _safe_addstr(win, y: int, x: int, text: str, attr: int = 0) -> None:
    """addstr that silently ignores writes outside window bounds."""
    max_y, max_x = win.getmaxyx()
    if y < 0 or y >= max_y or x >= max_x:
        return
    # Truncate to fit
    available = max_x - x
    if available <= 0:
        return
    win.addnstr(y, x, text, available, attr)


def _handle_key(
    key: int, selected: int, client_idx: int,
    num_ops: int, num_clients: int, need_redraw: bool,
) -> tuple[int, int, bool]:
    """Process a key press and return updated (selected, client_idx, need_redraw)."""
    if key == curses.KEY_UP:
        return max(0, selected - 1), client_idx, True
    elif key == curses.KEY_DOWN:
        return min(num_ops - 1, selected + 1), client_idx, True
    elif key == curses.KEY_LEFT:
        return selected, max(0, client_idx - 1), True
    elif key == curses.KEY_RIGHT:
        return selected, min(num_clients - 1, client_idx + 1), True
    return selected, client_idx, need_redraw


_tui_client: RpcClient | None = None  # module-level ref for cleanup


def _run_tui(server_url: str, interval: float, max_count: int) -> None:
    """Run the interactive curses TUI."""
    global _tui_client
    logging.getLogger("maru_handler").setLevel(logging.WARNING)
    logging.getLogger("maru_common").setLevel(logging.WARNING)
    try:
        curses.wrapper(lambda stdscr: _tui_loop(stdscr, server_url, interval, max_count))
    except KeyboardInterrupt:
        pass
    finally:
        if _tui_client is not None:
            _tui_client.close()
            _tui_client = None


def _try_connect(server_url: str) -> RpcClient | None:
    """Try to connect, return client or None."""
    try:
        client = RpcClient(server_url=server_url)
        client.connect()
        # Test with a real request
        client.get_stats()
        return client
    except Exception:
        return None


def _tui_loop(
    stdscr, server_url: str, interval: float, max_count: int
) -> None:
    import time as _time
    curses.curs_set(0)
    stdscr.timeout(100)  # 100ms for responsive key handling
    curses.use_default_colors()
    last_fetch = 0.0

    # Init color pairs — XCENA brand: yellow accent (#FFC300), navy (#222835)
    if curses.has_colors():
        curses.start_color()
        # Try 256-color for closer XCENA yellow (color 220 ≈ #FFD700)
        if curses.COLORS >= 256:
            curses.init_pair(1, 255, -1)                # header: bright white (website title)
            curses.init_pair(2, 156, -1)                # avg graph: light green
            curses.init_pair(3, 220, -1)                # box title/accent: XCENA gold (Contact btn)
            curses.init_pair(4, 252, -1)                # dim text: light gray (nav menu)
            curses.init_pair(5, 203, -1)                # max: soft red
            curses.init_pair(6, 75, -1)                 # min: soft blue
            curses.init_pair(7, 60, -1)                 # border: slate navy (website bg tone)
            curses.init_pair(8, 232, 220)               # selected: navy on gold (Contact btn style)
        else:
            curses.init_pair(1, curses.COLOR_YELLOW, -1)
            curses.init_pair(2, curses.COLOR_GREEN, -1)
            curses.init_pair(3, curses.COLOR_YELLOW, -1)
            curses.init_pair(4, curses.COLOR_WHITE, -1)
            curses.init_pair(5, curses.COLOR_RED, -1)
            curses.init_pair(6, curses.COLOR_BLUE, -1)
            curses.init_pair(7, curses.COLOR_WHITE, -1)
            curses.init_pair(8, curses.COLOR_BLACK, curses.COLOR_YELLOW)

    global _tui_client
    client: RpcClient | None = None
    selected = 0

    # Per-client histories: client_key -> {op_name -> list}
    all_count_hist: dict[str, dict[str, list[int]]] = {}
    all_lat_hist: dict[str, dict[str, list[tuple[float, float, float]]]] = {}
    all_spark_hist: dict[str, dict[str, list[float]]] = {}
    all_spark_accum: dict[str, dict[str, int]] = {}

    def _get_hist(client_key: str) -> tuple[dict, dict, dict, dict]:
        """Get or create per-client history dicts."""
        if client_key not in all_count_hist:
            all_count_hist[client_key] = {n: [0] * _HISTORY_WIDTH for n in _KNOWN_OPS}
            all_lat_hist[client_key] = {n: [_ZERO_LAT] * _HISTORY_WIDTH for n in _KNOWN_OPS}
            all_spark_hist[client_key] = {n: [0.0] * _SPARK_TABLE_WIDTH for n in _KNOWN_OPS}
            all_spark_accum[client_key] = dict.fromkeys(_KNOWN_OPS, 0)
        return (all_count_hist[client_key], all_lat_hist[client_key],
                all_spark_hist[client_key], all_spark_accum[client_key])

    iteration = 0
    op_names: list[str] = list(_KNOWN_OPS)
    client_idx = 0
    client_keys: list[str] = ["_all"]

    stats = None
    need_redraw = True

    while True:
        now = _time.monotonic()
        ts = datetime.now().strftime("%H:%M:%S")

        # Connect / reconnect
        if client is None:
            stdscr.erase()
            header_attr = curses.color_pair(1) | curses.A_BOLD if curses.has_colors() else curses.A_BOLD
            _safe_addstr(stdscr, 0, 0, f"  Maru Stats Monitor  --  {ts}  (q=quit)", header_attr)
            _safe_addstr(stdscr, 2, 0, f"  Connecting to {server_url} ...")
            stdscr.refresh()
            client = _try_connect(server_url)
            _tui_client = client
            key = stdscr.getch()
            if key in (ord("q"), 27, 3):  # q, ESC, Ctrl+C
                return
            if client is None:
                continue
            last_fetch = 0.0  # force immediate fetch

        # Fetch on timer only
        if now - last_fetch >= interval:
            last_fetch = now
            try:
                stats = fetch_stats(client)
            except Exception:
                client = None
                continue

            # Update client list
            cids = _get_client_ids(stats)
            client_keys = ["_all"] + cids
            if client_idx >= len(client_keys):
                client_idx = 0
            cur_client = client_keys[client_idx]

            # Update histories for ALL clients (so switching is instant)
            for ck in client_keys:
                ck_ops = _get_ops(stats, ck)
                c_hist, l_hist, s_hist, s_accum = _get_hist(ck)
                iv_data = _extract_interval(ck_ops)

                for name in sorted(ck_ops.keys()):
                    if name not in c_hist:
                        c_hist[name] = [0] * _HISTORY_WIDTH
                    if name not in l_hist:
                        l_hist[name] = [_ZERO_LAT] * _HISTORY_WIDTH
                    if name not in s_hist:
                        s_hist[name] = [0.0] * _SPARK_TABLE_WIDTH
                    if name not in s_accum:
                        s_accum[name] = 0

                    if name in iv_data:
                        mn, avg, mx, cnt = iv_data[name]
                        c_hist[name].append(cnt)
                        l_hist[name].append((mn, avg, mx))
                        s_accum[name] += cnt
                    else:
                        c_hist[name].append(0)
                        l_hist[name].append(_ZERO_LAT)

                    if len(c_hist[name]) > _HISTORY_WIDTH:
                        del c_hist[name][: len(c_hist[name]) - _HISTORY_WIDTH]
                    if len(l_hist[name]) > _HISTORY_WIDTH:
                        del l_hist[name][: len(l_hist[name]) - _HISTORY_WIDTH]

                # Every 2 ticks, flush sparkline accumulators
                if iteration > 0 and iteration % 2 == 0:
                    for name in s_accum:
                        s_hist[name].append(float(s_accum[name]))
                        if len(s_hist[name]) > _SPARK_TABLE_WIDTH:
                            del s_hist[name][: len(s_hist[name]) - _SPARK_TABLE_WIDTH]
                        s_accum[name] = 0

            ops = _get_ops(stats, cur_client)
            op_names = sorted(ops.keys())

            iteration += 1
            if max_count > 0 and iteration >= max_count:
                return
            need_redraw = True

        # Clamp selection
        if selected >= len(op_names):
            selected = len(op_names) - 1
        if selected < 0:
            selected = 0

        if stats is None or not need_redraw:
            key = stdscr.getch()
            if key in (ord("q"), 27, 3):
                return
            selected, client_idx, need_redraw = _handle_key(
                key, selected, client_idx, len(op_names), len(client_keys), need_redraw,
            )
            continue

        need_redraw = False
        cur_client = client_keys[client_idx] if client_idx < len(client_keys) else "_all"
        ops = _get_ops(stats, cur_client)
        count_history, latency_history, spark_history, _ = _get_hist(cur_client)

        # Draw
        stdscr.erase()
        row = 0

        # Header — compact single line with global info
        kv = stats["kv_manager"]
        alloc = stats["allocation_manager"]
        header_attr = curses.color_pair(1) | curses.A_BOLD if curses.has_colors() else curses.A_BOLD
        _safe_addstr(stdscr, row, 0,
            f"  Maru Stats Monitor  {ts}    "
            f"KV:{kv['total_entries']} ({_fmt_size(kv['total_size'])})  "
            f"Alloc:{alloc['num_allocations']} ({_fmt_size(alloc['total_allocated'])})  "
            f"Inst:{alloc['active_clients']}",
            header_attr,
        )
        row += 1

        # Instance tabs
        col = 2
        for ci, ck in enumerate(client_keys):
            label = "ALL" if ck == "_all" else f"Instance {ci}"
            if ci == client_idx:
                _safe_addstr(stdscr, row, col, "▐", curses.color_pair(3) if curses.has_colors() else 0)
                col += 1
                _safe_addstr(stdscr, row, col, f" {label} ",
                    curses.color_pair(3) | curses.A_BOLD if curses.has_colors() else curses.A_REVERSE)
                col += len(label) + 2
                _safe_addstr(stdscr, row, col, "▌", curses.color_pair(3) if curses.has_colors() else 0)
                col += 2
            else:
                _safe_addstr(stdscr, row, col, f" {label} ")
                col += len(label) + 3
        row += 2

        # Table header
        table_hdr = (
            f"  {'operation':<22}│{'count':>8}│{'delta':>7}│"
            f"{'avg_us':>9}│{'min_us':>9}│{'max_us':>9}"
            f"│ {'activity':>{_SPARK_TABLE_WIDTH}}"
        )
        border_attr = curses.color_pair(7) if curses.has_colors() else 0
        _safe_addstr(stdscr, row, 0, table_hdr, curses.A_BOLD)
        row += 1
        _safe_addstr(stdscr, row, 0,
            f"  {'─' * 22}┼{'─' * 8}┼{'─' * 7}┼"
            f"{'─' * 9}┼{'─' * 9}┼{'─' * 9}"
            f"┼─{'─' * _SPARK_TABLE_WIDTH}",
            border_attr,
        )
        row += 1

        # Table rows (windowed stats)
        for i, name in enumerate(op_names):
            c_hist = count_history.get(name, [0] * _HISTORY_WIDTH)
            win_count = sum(c_hist)

            delta = ""
            if c_hist:
                last = c_hist[-1]
                if last > 0:
                    delta = f"+{last}"

            # Sparkline from dedicated 2-tick history
            s_hist = spark_history.get(name, [0.0] * _SPARK_TABLE_WIDTH)
            if any(v > 0 for v in s_hist):
                rendered = _sparkline(s_hist, _SPARK_TABLE_WIDTH)
                pad = _SPARK_TABLE_WIDTH - len(rendered)
                spark = f"  {' ' * pad}{rendered}"
            else:
                spark = f"  {' ' * _SPARK_TABLE_WIDTH}"

            # Windowed latency from interval history
            lat_hist = latency_history.get(name, [_ZERO_LAT] * _HISTORY_WIDTH)
            active = [(mn, avg, mx) for mn, avg, mx in lat_hist if avg > 0]
            win_avg = sum(t[1] for t in active) / len(active) if active else 0.0
            win_min = min(t[0] for t in active) if active else 0.0
            win_max = max(t[2] for t in active) if active else 0.0

            prefix = "▶ " if i == selected else "  "
            line = (
                f"{prefix}{name:<22}│{win_count:>8}│{delta:>7}│"
                f"{win_avg:>9.1f}│"
                f"{win_min:>9.1f}│"
                f"{win_max:>9.1f}"
                f"│{spark}"
            )

            if i == selected:
                attr = curses.color_pair(8) | curses.A_BOLD if curses.has_colors() else curses.A_REVERSE
            else:
                attr = 0
            _safe_addstr(stdscr, row, 0, line, attr)
            row += 1

        # Detail panel
        row += 2
        if op_names:
            sel_name = op_names[selected]
            sel_op = ops[sel_name]


            accent = curses.color_pair(3) | curses.A_BOLD if curses.has_colors() else curses.A_BOLD
            # Total line width = 98 (matches table). Layout: "  ╭─...─╮" / "  │ ... │"
            total_w = 98  # must match table header len
            # ╭ and ╮ at positions 2 and total_w-1, so fill = total_w - 4
            fill_w = total_w - 4  # dashes between ╭─ and ─╮
            inner = total_w - 6  # text between │_ and _│

            # === Info box (rounded + bold) ===
            bb = border_attr | curses.A_BOLD

            def _box_top(title: str, _fw: int = fill_w, _bb: int = bb, _ac: int = accent) -> None:
                nonlocal row
                pad = _fw - 1 - len(title)
                _safe_addstr(stdscr, row, 2, "╭─", _bb)
                _safe_addstr(stdscr, row, 4, title, _ac | curses.A_BOLD)
                _safe_addstr(stdscr, row, 4 + len(title), f"{'─' * max(pad, 0)}╮", _bb)
                row += 1

            def _box_bottom(_fw: int = fill_w, _bb: int = bb) -> None:
                nonlocal row
                _safe_addstr(stdscr, row, 2, f"╰{'─' * _fw}╯", _bb)
                row += 1

            def _box_line(text: str, _in: int = inner, _bb: int = bb) -> None:
                nonlocal row
                _safe_addstr(stdscr, row, 2, f"│ {text:<{_in}} │", _bb)
                row += 1

            _box_top(f" {sel_name} ")

            hit_c = sel_op.get("hit_count", 0)
            miss_c = sel_op.get("miss_count", 0)
            count = sel_op["count"]
            avg = sel_op["avg_latency_us"]

            # Line 1: count + optional bytes + optional hit/miss
            line1_parts = [f"count: {count}"]
            if sel_name in _HIT_MISS_OPS:
                line1_parts.append(f"hit: {hit_c}")
                line1_parts.append(f"miss: {miss_c}")
            if sel_name in _DATA_OPS:
                line1_parts.append(f"bytes: {_fmt_size(sel_op['total_bytes'])}")
            _box_line("    ".join(line1_parts))

            # Line 2: hit rate bar (for hit/miss ops) or blank
            if sel_name in _HIT_MISS_OPS and count > 0:
                pct = hit_c / count
                bar_w = 20
                filled = int(pct * bar_w)
                bar = "█" * filled + "░" * (bar_w - filled)
                _box_line(f"hit rate: {bar} {hit_c}/{count} ({pct * 100:.1f}%)")
            else:
                _box_line("")

            # Line 3: latency
            _box_line(f"latency (us):  avg={avg:.1f}  min={sel_op['min_latency_us']:.1f}  max={sel_op['max_latency_us']:.1f}")

            # Line 4: throughput (only for data ops)
            if sel_name in _DATA_OPS and count > 0 and avg > 0:
                total_time_s = sel_op["latency_sum_us"] / 1e6
                if total_time_s > 0:
                    mbps = (sel_op["total_bytes"] / 1e6) / total_time_s
                    _box_line(f"throughput:    {mbps:.1f} MB/s")
                else:
                    _box_line("")
            else:
                _box_line("")

            _box_bottom()

            # === Latency graph box ===
            _box_top(" latency (us) ")
            _box_line("")  # spacing between title and graph

            sel_lat_hist = latency_history.get(sel_name, [_ZERO_LAT] * _HISTORY_WIDTH)
            # graph at x=4 (indent+border+space), y-label=8
            # right border at total_w-1, so plot_w = total_w - 4(left) - 8(label) - 2(right pad+border)
            graph_plot_w = total_w - 14
            graph_rows = _draw_latency_graph(stdscr, row, 4, sel_lat_hist, graph_plot_w, _GRAPH_HEIGHT)
            # Draw side borders for each graph row
            for gr in range(graph_rows):
                _safe_addstr(stdscr, row + gr, 2, "│", bb)
                _safe_addstr(stdscr, row + gr, total_w - 1, "│", bb)
            row += graph_rows

            _box_bottom()

        # Key hints at bottom
        row += 1
        hint_attr = curses.color_pair(4) if curses.has_colors() else 0
        _safe_addstr(stdscr, row, 2, "q:quit  ↑↓:select  ←→:instance", hint_attr)

        stdscr.refresh()

        # Input handling (non-blocking, 100ms timeout)
        key = stdscr.getch()
        if key in (ord("q"), 27, 3):
            return
        selected, client_idx, need_redraw = _handle_key(
            key, selected, client_idx, len(op_names), len(client_keys), need_redraw,
        )




# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Maru server operation stats monitor")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="MaruServer host (default: 127.0.0.1)")
    parser.add_argument("-p", "--port", type=int, default=5555, help="MaruServer port (default: 5555)")
    parser.add_argument("-i", "--interval", type=float, default=1.0, help="Refresh interval in seconds (default: 1.0)")
    args = parser.parse_args()

    server_url = f"tcp://{args.host}:{args.port}"
    _run_tui(server_url, args.interval, 0)


if __name__ == "__main__":
    main()
