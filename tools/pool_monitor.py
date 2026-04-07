#!/usr/bin/env python3
"""Maru pool usage monitor.

Usage:
    python tools/pool_monitor.py              # one-shot snapshot
    python tools/pool_monitor.py -w 1         # top-style refresh every 1s
    python tools/pool_monitor.py -w 1 -c 30   # watch 30 times at 1s interval
    python tools/pool_monitor.py --csv         # CSV output for post-processing
    python tools/pool_monitor.py --scroll      # scrolling log (no screen clear)
"""

import argparse
import sys
import time
from datetime import datetime

from maru_shm import MaruPoolInfo, MaruShmClient


def _fmt_size(nbytes: int) -> str:
    if nbytes == 0:
        return "0B"
    if nbytes >= 1024**4:
        return f"{nbytes / 1024**4:.1f}T"
    if nbytes >= 1024**3:
        return f"{nbytes / 1024**3:.1f}G"
    if nbytes >= 1024**2:
        return f"{nbytes / 1024**2:.1f}M"
    return f"{nbytes / 1024:.1f}K"


def _usage_bar(used: int, total: int, width: int = 30) -> str:
    if total == 0:
        return "[" + "?" * width + "]"
    ratio = used / total
    filled = int(ratio * width)
    return "[" + "#" * filled + "-" * (width - filled) + f"] {ratio * 100:.1f}%"


def snapshot(client: MaruShmClient) -> list[MaruPoolInfo]:
    return client.stats()


def _clear_screen() -> None:
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()


def render_table(
    pools: list[MaruPoolInfo], ts: str, prev: dict[str, int] | None = None
) -> str:
    """Render table to string. prev maps device_path -> previous used bytes for delta."""
    lines = []
    lines.append(f"  Maru Pool Monitor  —  {ts}  (Ctrl+C to quit)")
    lines.append("")
    if not pools:
        lines.append("  (no pools found)")
        return "\n".join(lines)
    lines.append(
        f"  {'Device':<16}  {'Type':<8}  {'Used':>8}  {'Free':>8}  "
        f"{'Total':>8}  {'Delta':>8}  {'Usage'}"
    )
    lines.append(
        f"  {'----------------':<16}  {'--------':<8}  {'--------':>8}  {'--------':>8}  "
        f"{'--------':>8}  {'--------':>8}  {'-----'}"
    )
    for p in sorted(pools, key=lambda x: x.device_path):
        used = p.total_size - p.free_size
        bar = _usage_bar(used, p.total_size)
        delta = ""
        if prev and p.device_path in prev:
            diff = used - prev[p.device_path]
            if diff > 0:
                delta = f"+{_fmt_size(diff)}"
            elif diff < 0:
                delta = f"-{_fmt_size(-diff)}"
        device_label = p.device_path or "(unknown)"
        lines.append(
            f"  {device_label:<16}  {p.dax_type.name:<8}  "
            f"{_fmt_size(used):>8}  {_fmt_size(p.free_size):>8}  "
            f"{_fmt_size(p.total_size):>8}  {delta:>8}  {bar}"
        )
    return "\n".join(lines)


def print_csv_header() -> None:
    print("timestamp,device_path,dax_type,total_bytes,free_bytes,used_bytes,usage_pct")


def print_csv_row(pools: list[MaruPoolInfo], ts: str) -> None:
    for p in sorted(pools, key=lambda x: x.device_path):
        used = p.total_size - p.free_size
        pct = (used / p.total_size * 100) if p.total_size > 0 else 0
        print(
            f"{ts},{p.device_path},{p.dax_type.name},{p.total_size},{p.free_size},{used},{pct:.2f}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Maru pool usage monitor")
    parser.add_argument(
        "-w",
        "--watch",
        type=float,
        default=0,
        help="Refresh interval in seconds (0 = one-shot)",
    )
    parser.add_argument(
        "-c",
        "--count",
        type=int,
        default=0,
        help="Number of iterations (0 = unlimited)",
    )
    parser.add_argument("--csv", action="store_true", help="Output in CSV format")
    parser.add_argument(
        "--scroll",
        action="store_true",
        help="Scrolling log instead of top-style refresh",
    )
    parser.add_argument(
        "--address",
        type=str,
        default=None,
        help="Resource manager address (host:port, default: 127.0.0.1:9850)",
    )
    args = parser.parse_args()

    try:
        client = MaruShmClient(address=args.address)
    except Exception as e:
        print(f"Error: cannot connect to maru_resourced: {e}", file=sys.stderr)
        sys.exit(1)

    if args.watch <= 0:
        # One-shot
        pools = snapshot(client)
        ts = datetime.now().isoformat(timespec="seconds")
        if args.csv:
            print_csv_header()
            print_csv_row(pools, ts)
        else:
            print(render_table(pools, ts))
        return

    # Watch mode
    iteration = 0
    prev_used: dict[int, int] = {}
    if args.csv:
        print_csv_header()

    try:
        while True:
            ts = datetime.now().isoformat(timespec="seconds")
            try:
                pools = snapshot(client)
            except Exception as e:
                print(f"  [error] {e}", file=sys.stderr)
                time.sleep(args.watch)
                continue
            if args.csv:
                print_csv_row(pools, ts)
                sys.stdout.flush()
            else:
                output = render_table(pools, ts, prev_used if prev_used else None)
                if args.scroll:
                    print(output)
                    print()
                else:
                    _clear_screen()
                    print(output)
                sys.stdout.flush()

            # Track previous for delta
            prev_used = {p.device_path: p.total_size - p.free_size for p in pools}

            iteration += 1
            if args.count > 0 and iteration >= args.count:
                break
            time.sleep(args.watch)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
