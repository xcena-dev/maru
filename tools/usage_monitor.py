#!/usr/bin/env python3
"""Maru per-instance CXL usage monitor.

Shows, per client instance (owner_instance_id), how much CXL memory it
reserved (allocated) versus how much is actually live KV data (used),
plus the shared pool free space. Queries MaruServer over RPC.

Usage:
    python -m tools.usage_monitor                  # one-shot snapshot
    python -m tools.usage_monitor -w 1             # refresh every 1s
    python -m tools.usage_monitor -w 1 -c 30       # 30 iterations then exit
    python -m tools.usage_monitor --csv            # CSV for post-processing
    python -m tools.usage_monitor --host H -p 5555 # target a MaruServer

Note:
    ``instance_id`` identifies one MaruBackend = one process/worker, and the
    default is a random UUID per process. Set ``maru_instance_id`` in the
    integration's extra_config for stable, readable rows.
"""

import argparse
import logging
import sys
import time
from datetime import datetime

from maru_common import GetUsageResponse
from maru_handler.rpc_client import RpcClient


def _fmt_size(nbytes: int) -> str:
    if nbytes < 0:
        return "-" + _fmt_size(-nbytes)
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


def _clear_screen() -> None:
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()


def render_table(usage: GetUsageResponse, ts: str) -> str:
    """Render the per-instance usage table to a string."""
    lines = []
    lines.append(f"  Maru Usage Monitor  —  {ts}  (Ctrl+C to quit)")
    lines.append("")

    instances = sorted(usage.instances, key=lambda i: i.instance_id)
    if not instances:
        lines.append("  (no active instances)")
    else:
        lines.append(
            f"  {'owner_instance_id':<38}  {'regions':>7}  "
            f"{'allocated':>9}  {'used':>9}  {'slack':>9}"
        )
        lines.append(
            f"  {'-' * 38}  {'-' * 7}  {'-' * 9}  {'-' * 9}  {'-' * 9}"
        )
        for inst in instances:
            slack = inst.allocated - inst.used
            lines.append(
                f"  {inst.instance_id:<38}  {inst.regions:>7}  "
                f"{_fmt_size(inst.allocated):>9}  {_fmt_size(inst.used):>9}  "
                f"{_fmt_size(slack):>9}"
            )
        total_alloc = sum(i.allocated for i in instances)
        total_used = sum(i.used for i in instances)
        lines.append(
            f"  {'-' * 38}  {'-' * 7}  {'-' * 9}  {'-' * 9}  {'-' * 9}"
        )
        lines.append(
            f"  {'TOTAL':<38}  {len(instances):>7}  "
            f"{_fmt_size(total_alloc):>9}  {_fmt_size(total_used):>9}  "
            f"{_fmt_size(total_alloc - total_used):>9}"
        )

    lines.append("")
    pool_used = usage.pool_total - usage.pool_free
    lines.append(
        f"  Pool (shared): {_fmt_size(usage.pool_free)} free / "
        f"{_fmt_size(usage.pool_total)} total  "
        f"{_usage_bar(pool_used, usage.pool_total)}"
    )
    return "\n".join(lines)


def print_csv_header() -> None:
    print(
        "timestamp,instance_id,regions,allocated_bytes,used_bytes,"
        "slack_bytes,pool_total_bytes,pool_free_bytes"
    )


def print_csv_rows(usage: GetUsageResponse, ts: str) -> None:
    for inst in sorted(usage.instances, key=lambda i: i.instance_id):
        slack = inst.allocated - inst.used
        print(
            f"{ts},{inst.instance_id},{inst.regions},{inst.allocated},"
            f"{inst.used},{slack},{usage.pool_total},{usage.pool_free}"
        )


def snapshot(client: RpcClient) -> GetUsageResponse:
    return client.get_usage()


def main() -> None:
    parser = argparse.ArgumentParser(description="Maru per-instance CXL usage monitor")
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="MaruServer host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=5555,
        help="MaruServer port (default: 5555)",
    )
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
        help="Scrolling log instead of top-style refresh (table mode only; "
        "ignored with --csv)",
    )
    args = parser.parse_args()

    # Quiet the client libraries so they don't clutter the table.
    logging.getLogger("maru_handler").setLevel(logging.WARNING)
    logging.getLogger("maru_common").setLevel(logging.WARNING)

    server_url = f"tcp://{args.host}:{args.port}"
    client = RpcClient(server_url=server_url)
    try:
        client.connect()
    except Exception as e:
        print(f"Error: cannot connect to MaruServer at {server_url}: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        if args.watch <= 0:
            try:
                usage = snapshot(client)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)
            ts = datetime.now().isoformat(timespec="seconds")
            if args.csv:
                print_csv_header()
                print_csv_rows(usage, ts)
            else:
                print(render_table(usage, ts))
            return

        iteration = 0
        if args.csv:
            print_csv_header()

        while True:
            ts = datetime.now().isoformat(timespec="seconds")
            try:
                usage = snapshot(client)
            except Exception as e:
                print(f"  [error] {e}", file=sys.stderr)
                time.sleep(args.watch)
                continue

            if args.csv:
                print_csv_rows(usage, ts)
                sys.stdout.flush()
            else:
                output = render_table(usage, ts)
                if args.scroll:
                    print(output)
                    print()
                else:
                    _clear_screen()
                    print(output)
                sys.stdout.flush()

            iteration += 1
            if args.count > 0 and iteration >= args.count:
                break
            time.sleep(args.watch)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        client.close()


if __name__ == "__main__":
    main()
