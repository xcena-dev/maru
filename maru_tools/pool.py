# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Physical DAX pool usage view (Resource Manager, default :9850).

marutop pool              # one-shot snapshot
marutop pool -w 1         # top-style refresh every 1s
marutop pool -w 1 -c 30   # watch 30 times at 1s interval
marutop pool --csv        # CSV output for post-processing
marutop pool --scroll     # scrolling log (no screen clear)
"""

import argparse
import sys
import time
from datetime import datetime

from maru_shm import MaruPoolInfo, MaruShmClient
from maru_tools._common import clear_screen, fmt_size, usage_bar


def snapshot(client: MaruShmClient) -> list[MaruPoolInfo]:
    return client.stats()


def render_table(
    pools: list[MaruPoolInfo], ts: str, prev: dict[str, int] | None = None
) -> str:
    """Render table to string. prev maps dax_path -> previous used bytes for delta."""
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
    for p in sorted(pools, key=lambda x: x.dax_path):
        used = p.total_size - p.free_size
        bar = usage_bar(used, p.total_size)
        delta = ""
        if prev and p.dax_path in prev:
            diff = used - prev[p.dax_path]
            if diff > 0:
                delta = f"+{fmt_size(diff)}"
            elif diff < 0:
                delta = f"-{fmt_size(-diff)}"
        dax_label = p.dax_path or "(unknown)"
        lines.append(
            f"  {dax_label:<16}  {p.dax_type.name:<8}  "
            f"{fmt_size(used):>8}  {fmt_size(p.free_size):>8}  "
            f"{fmt_size(p.total_size):>8}  {delta:>8}  {bar}"
        )
    return "\n".join(lines)


def print_csv_header() -> None:
    print("timestamp,dax_path,dax_type,total_bytes,free_bytes,used_bytes,usage_pct")


def print_csv_row(pools: list[MaruPoolInfo], ts: str) -> None:
    for p in sorted(pools, key=lambda x: x.dax_path):
        used = p.total_size - p.free_size
        pct = (used / p.total_size * 100) if p.total_size > 0 else 0
        print(
            f"{ts},{p.dax_path},{p.dax_type.name},{p.total_size},{p.free_size},{used},{pct:.2f}"
        )


def add_arguments(parser: argparse.ArgumentParser) -> None:
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


def run(args: argparse.Namespace) -> None:
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
    prev_used: dict[str, int] = {}
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
                    clear_screen()
                    print(output)
                sys.stdout.flush()

            # Track previous for delta
            prev_used = {p.dax_path: p.total_size - p.free_size for p in pools}

            iteration += 1
            if args.count > 0 and iteration >= args.count:
                break
            time.sleep(args.watch)
    except KeyboardInterrupt:
        print("\nStopped.")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Maru pool usage monitor")
    add_arguments(parser)
    run(parser.parse_args(argv))


if __name__ == "__main__":
    main()
