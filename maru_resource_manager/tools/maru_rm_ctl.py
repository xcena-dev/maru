#!/usr/bin/env python3
"""Control which DAX devices maru-resource-manager uses.

Commands talk to the running RM over <state-dir>/admin.sock (root or the RM's
own user only):

  status                       pools, excluded devices, checkpoint state
  allocations [daxN.M]         live allocations and whether their owner runs
  exclude daxN.M               stop using a device for good (e.g. for ROOF);
                               refused while any allocation on it is live.
                               Hand the device over only once this exits 0:
                               until the list is synced to disk it exits 4,
                               and running it again retries the sync
  discard daxN.M [--confirm [--force]]
                               release every allocation the RM records for the
                               device's pool without opening the device;
                               refuses while a local owner process is running
                               unless --force
  include daxN.M [--reinit | --adopt | --restore-header]
                               judge an excluded or refused device again;
                               --reinit archives its pool metadata and writes
                               a fresh header, discarding whatever the device
                               holds; --adopt accepts the Maru header of a
                               device that has no identity record;
                               --restore-header writes the recorded UUID back
                               to a device whose header was cleared, when the
                               identity tags still match
  release REGION --confirm [--force]
                               return one allocation whose owner has exited;
                               --force releases even if a local process with
                               the owner's PID is running (e.g. a reused PID)
  wipe daxN.M --confirm        zero all of an excluded device, header included
                               (e.g. before handing it over, or to reset it for
                               tests); waits until done, which can take minutes
  drop-holds HOLDER [--confirm]
                               list the regions a remote agent (node HOLDER)
                               holds; --confirm drops those holds once that
                               node's agent is gone for good, releasing the
                               regions freed while held

Exit codes: 0 = done, 1 = refused by the RM, 2 = RM unreachable,
3 = sent but no reply in time (the command may still complete; run status),
4 = done but not on disk yet (a power loss could undo it; run it again).
Interrupting a wipe (Ctrl-C) stops it, leaving the device partly zeroed.
"""

from __future__ import annotations  # System python3 may be 3.9.

import argparse
import re
import socket
import sys
from pathlib import Path

DEFAULT_STATE_DIR = "/var/lib/maru-resourced"
DEVICE = re.compile(r"(?:/dev/)?(dax(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*))")
# A remote agent's node ID, as the RM records it: printable ASCII, no spaces.
HOLDER = re.compile(r"[\x21-\x7e]{1,128}")


def device_name(text: str) -> str:
    match = DEVICE.fullmatch(text)
    if not match:
        raise argparse.ArgumentTypeError(f"expected daxN.M or /dev/daxN.M: {text}")
    return match[1]


def holder_name(text: str) -> str:
    if not HOLDER.fullmatch(text):
        raise argparse.ArgumentTypeError(
            f"expected 1-128 printable characters without spaces: {text!r}"
        )
    return text


def region_id(text: str) -> int:
    if not text.isdigit() or len(text) > 20:
        raise argparse.ArgumentTypeError(f"expected a region ID: {text}")
    return int(text)


def request_line(args: argparse.Namespace) -> str:
    if args.command == "status":
        return "status"
    if args.command == "allocations":
        return f"allocations {args.device}" if args.device else "allocations"
    if args.command == "exclude":
        return f"exclude {args.device}"
    if args.command == "include":
        mode = "reinit" if args.reinit else "adopt" if args.adopt else None
        mode = "restore-header" if args.restore_header else mode
        return f"include {args.device}" + (f" --{mode}" if mode else "")
    if args.command == "discard":
        return (
            f"discard {args.device}"
            + (" --confirm" if args.confirm else "")
            + (" --force" if args.force else "")
        )
    if args.command == "wipe":
        return f"wipe {args.device}" + (" --confirm" if args.confirm else "")
    if args.command == "drop-holds":
        return f"drop-holds {args.holder}" + (" --confirm" if args.confirm else "")
    return (
        f"release {args.region}"
        + (" --confirm" if args.confirm else "")
        + (" --force" if args.force else "")
    )


class NoReplyError(Exception):
    """The request was sent, but the RM did not answer in time."""


def send(path: Path, line: str, timeout: float | None = 120.0) -> tuple[str, str]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(timeout)
        conn.connect(str(path))
        conn.sendall(line.encode() + b"\n")
        chunks = []
        try:
            while chunk := conn.recv(65536):
                chunks.append(chunk)
        # System python3 may be 3.9, where socket.timeout is not TimeoutError.
        except socket.timeout as error:  # noqa: UP041
            raise NoReplyError from error
    status, _, body = b"".join(chunks).decode(errors="replace").partition("\n")
    return status, body


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    target = root.add_mutually_exclusive_group()
    target.add_argument(
        "--state-dir", default=DEFAULT_STATE_DIR, help="RM state directory"
    )
    target.add_argument("--socket", help="admin socket path (overrides --state-dir)")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="show pools and excluded devices")
    listing = commands.add_parser("allocations", help="list live allocations")
    listing.add_argument("device", nargs="?", type=device_name)
    exclude = commands.add_parser("exclude", help="stop using a device")
    exclude.add_argument("device", type=device_name)
    include = commands.add_parser(
        "include", help="judge an excluded or refused device again"
    )
    include.add_argument("device", type=device_name)
    mode = include.add_mutually_exclusive_group()
    mode.add_argument(
        "--reinit",
        action="store_true",
        help="discard the device's contents and pool metadata (e.g. after ROOF)",
    )
    mode.add_argument(
        "--adopt",
        action="store_true",
        help="accept the Maru header on an excluded device that has no identity record",
    )
    mode.add_argument(
        "--restore-header",
        action="store_true",
        help="write the recorded UUID back to a device whose header was "
        "cleared, when the identity tags still match",
    )
    discard = commands.add_parser(
        "discard",
        help="release a device's allocations without opening it",
        description="Release every allocation the RM records for the device's "
        "pool without opening the device; refuses while a local owner process "
        "is running unless --force.",
    )
    discard.add_argument("device", type=device_name)
    discard.add_argument(
        "--confirm",
        action="store_true",
        help="no host maps the device's regions any more",
    )
    discard.add_argument(
        "--force",
        action="store_true",
        help="discard even if a local owner process is running (needs --confirm)",
    )
    release = commands.add_parser("release", help="return one allocation")
    release.add_argument("region", type=region_id)
    release.add_argument(
        "--confirm",
        action="store_true",
        help="the owner has exited and unmapped the region",
    )
    release.add_argument(
        "--force",
        action="store_true",
        help="release even if a local process with the owner's PID is running "
        "(e.g. a PID reused after a reboot); needs --confirm",
    )
    wipe = commands.add_parser("wipe", help="zero all of an excluded device")
    wipe.add_argument("device", type=device_name)
    wipe.add_argument(
        "--confirm",
        action="store_true",
        help="no host maps the device and its contents may be destroyed",
    )
    drop = commands.add_parser(
        "drop-holds",
        help="list or drop a remote agent's region holds",
        description="List the regions the remote agent of node HOLDER holds. "
        "Freed regions stay out of use while held; --confirm drops the holds.",
    )
    drop.add_argument("holder", type=holder_name)
    drop.add_argument(
        "--confirm",
        action="store_true",
        help="the agent of that node is gone for good, and its NIC registrations "
        "with it",
    )
    return root


def main(argv=None) -> int:
    cli = parser()
    args = cli.parse_args(argv)
    if getattr(args, "force", False) and not args.confirm:
        cli.error("--force is only valid together with --confirm")
    path = Path(args.socket) if args.socket else Path(args.state_dir) / "admin.sock"
    try:
        # A wipe of a large device takes minutes; wait for it.
        timeout = None if args.command == "wipe" else 120.0
        status, body = send(path, request_line(args), timeout)
    except KeyboardInterrupt:
        # Closing the connection stops a running wipe; nothing else is undone.
        print("interrupted; check the result with 'status'", file=sys.stderr)
        return 130
    except NoReplyError:
        print(
            "no reply from maru-resource-manager in time; the command may still "
            "complete, so check with 'status' before retrying",
            file=sys.stderr,
        )
        return 3
    except OSError as error:
        print(f"cannot reach maru-resource-manager at {path}: {error}", file=sys.stderr)
        return 2
    if status == "OK":
        sys.stdout.write(body)
        return 0
    if status == "INCOMPLETE":
        sys.stdout.write(body)
        print(
            "not finished: a power loss could still undo this; run the same "
            "command again until it exits 0",
            file=sys.stderr,
        )
        return 4
    parts = status.split(" ", 2)
    message = parts[2] if len(parts) == 3 and parts[0] == "ERR" else status
    print(f"refused: {message}", file=sys.stderr)
    sys.stderr.write(body)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
