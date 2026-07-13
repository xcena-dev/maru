# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""DAX device UUID header management (Resource Manager device layer).

The Resource Manager identifies a DEV_DAX device by a UUID written into a small
header at the front of the device, so that a device keeps a stable identity
across reboots even when the kernel's ``/dev/daxX.Y`` numbering changes.

    marutop device init /dev/dax0.0          # write UUID header
    marutop device init --show /dev/dax0.0   # show existing UUID (alias of `show`)
    marutop device init --force /dev/dax0.0  # force regenerate UUID
    marutop device show /dev/dax0.0          # show existing UUID
    marutop device clear /dev/dax0.0         # clear header (zero-fill)
    marutop device clear --yes /dev/dax0.0   # clear without prompt
"""

import argparse
import sys


def cmd_device_init(args: argparse.Namespace) -> None:
    """Initialize UUID header on a DEV_DAX device."""
    from maru_shm.device_scanner import read_device_uuid, write_device_header

    path = args.path

    existing = read_device_uuid(path)

    if args.show:
        _print_uuid(path, existing)
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


def cmd_device_show(args: argparse.Namespace) -> None:
    """Show the existing UUID header on a DEV_DAX device."""
    from maru_shm.device_scanner import read_device_uuid

    _print_uuid(args.path, read_device_uuid(args.path))


def _print_uuid(path: str, existing: str | None) -> None:
    if existing:
        print(f"UUID: {existing}")
    else:
        print(f"No valid header on {path}")
        sys.exit(1)


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


def add_arguments(parser: argparse.ArgumentParser) -> None:
    action = parser.add_subparsers(dest="device_action", help="Device action")

    init_parser = action.add_parser("init", help="Initialize UUID header")
    init_parser.add_argument("path", help="DEV_DAX device path (e.g. /dev/dax0.0)")
    init_parser.add_argument(
        "--show", action="store_true", help="Show existing UUID without writing"
    )
    init_parser.add_argument(
        "--force", action="store_true", help="Force regenerate UUID"
    )
    init_parser.set_defaults(device_func=cmd_device_init)

    show_parser = action.add_parser("show", help="Show existing UUID header")
    show_parser.add_argument("path", help="DEV_DAX device path (e.g. /dev/dax0.0)")
    show_parser.set_defaults(device_func=cmd_device_show)

    clear_parser = action.add_parser("clear", help="Clear UUID header")
    clear_parser.add_argument("path", help="DEV_DAX device path (e.g. /dev/dax0.0)")
    clear_parser.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompt"
    )
    clear_parser.set_defaults(device_func=cmd_device_clear)


def run(args: argparse.Namespace) -> None:
    func = getattr(args, "device_func", None)
    if func is None:
        print("Error: no device action given (init|show|clear)", file=sys.stderr)
        sys.exit(1)
    func(args)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Maru DAX device UUID management")
    add_arguments(parser)
    run(parser.parse_args(argv))


if __name__ == "__main__":
    main()
