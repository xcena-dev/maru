#!/usr/bin/env python3
"""Maru Resource Manager tool.

Usage:
    python tools/maru_rm_tool.py device init /dev/dax0.0          # Write UUID header
    python tools/maru_rm_tool.py device init --show /dev/dax0.0   # Show existing UUID
    python tools/maru_rm_tool.py device init --force /dev/dax0.0  # Force regenerate UUID
    python tools/maru_rm_tool.py device clear /dev/dax0.0         # Clear header (zero-fill)
    python tools/maru_rm_tool.py device clear --yes /dev/dax0.0   # Clear without prompt
"""

import argparse
import sys


def cmd_device_init(args: argparse.Namespace) -> None:
    """Initialize UUID header on a DEV_DAX device."""
    from maru_shm.device_scanner import read_device_uuid, write_device_header

    path = args.path

    existing = read_device_uuid(path)

    if args.show:
        if existing:
            print(f"UUID: {existing}")
        else:
            print(f"No valid header on {path}")
            sys.exit(1)
        return

    if existing and not args.force:
        print(f"Device {path} already has UUID: {existing}")
        print("Use --force to regenerate.")
        return

    uuid_str = write_device_header(path)
    if existing:
        print(f"Regenerated UUID on {path}: {uuid_str} (was: {existing})")
    else:
        print(f"Initialized UUID on {path}: {uuid_str}")


def cmd_device_clear(args: argparse.Namespace) -> None:
    """Clear UUID header from a DEV_DAX device."""
    from maru_shm.device_scanner import clear_device_header, read_device_uuid

    path = args.path

    existing = read_device_uuid(path)
    if not existing:
        print(f"No valid header on {path}, nothing to clear.")
        return

    if not args.yes:
        answer = input(f"Clear UUID header on {path}? (UUID: {existing}) [y/N] ")
        if answer.lower() not in ("y", "yes"):
            print("Aborted.")
            return

    clear_device_header(path)
    print(f"Cleared header on {path} (was: {existing})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Maru Resource Manager tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="category", help="Command category")

    # device subcommand
    device_parser = subparsers.add_parser("device", help="Device management")
    device_sub = device_parser.add_subparsers(dest="action", help="Device action")

    # device init
    init_parser = device_sub.add_parser("init", help="Initialize UUID header")
    init_parser.add_argument("path", help="DEV_DAX device path (e.g. /dev/dax0.0)")
    init_parser.add_argument(
        "--show", action="store_true", help="Show existing UUID without writing"
    )
    init_parser.add_argument(
        "--force", action="store_true", help="Force regenerate UUID"
    )
    init_parser.set_defaults(func=cmd_device_init)

    # device clear
    clear_parser = device_sub.add_parser("clear", help="Clear UUID header")
    clear_parser.add_argument("path", help="DEV_DAX device path (e.g. /dev/dax0.0)")
    clear_parser.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompt"
    )
    clear_parser.set_defaults(func=cmd_device_clear)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
