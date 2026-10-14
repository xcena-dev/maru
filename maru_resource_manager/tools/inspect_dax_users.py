#!/usr/bin/env python3
"""Report local fd/mmap references to DAX character devices without opening them.

Run as root on the host. Exit codes: 0 = no references observed, 1 = references
found, 2 = incomplete inspection/error. This is a non-atomic snapshot, not a
check of RM allocations, remote hosts, or outstanding GPU/DMA registrations.
"""

import argparse
import os
import platform
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path


def inspect(devices: list[str], proc: Path = Path("/proc")) -> dict:
    targets = {}
    for name in devices:
        info = os.stat(name)
        if not stat.S_ISCHR(info.st_mode):
            raise ValueError(f"Not a character device: {name}")
        targets.setdefault(info.st_rdev, []).append(name)

    references = {}
    errors = set()
    vanished = 0
    processes = 0
    threads = 0

    def entries(path):
        nonlocal vanished
        try:
            return list(path.iterdir())
        except (FileNotFoundError, ProcessLookupError) as error:
            # A missing inspection directory for a still-visible task must not
            # be mistaken for an empty directory.
            if path.parent.exists():
                errors.add(f"{path}: {error}")
            else:
                vanished += 1
        except OSError as error:
            errors.add(f"{path}: {error}")
        return []

    for process in entries(proc):
        if not process.name.isdigit():
            continue
        # Numeric /proc entries list process leaders; non-leader /proc/<tid>
        # paths are accessible but not returned by a real /proc directory scan.
        processes += 1
        try:
            name = (process / "comm").read_text().strip()
        except OSError:
            name = "?"
        for thread in entries(process / "task"):
            threads += 1
            # map_files exists here, not at /proc/<pid>/task/<tid>/map_files.
            task = proc / thread.name
            for kind in ("fd", "map_files"):
                mapping_details = None
                for link in entries(task / kind):
                    try:
                        info = link.stat()
                    except (FileNotFoundError, ProcessLookupError):
                        vanished += 1
                        continue
                    except OSError as error:
                        errors.add(f"{link}: {error}")
                        continue
                    if not stat.S_ISCHR(info.st_mode) or info.st_rdev not in targets:
                        continue
                    key = (int(process.name), kind, link.name, info.st_rdev)
                    if key in references:
                        continue  # Shared references otherwise repeat per thread.
                    details = ""
                    if kind == "map_files":
                        if mapping_details is None:
                            try:
                                mapping_details = {
                                    line.split(maxsplit=1)[0]: line
                                    for line in (task / "maps").read_text().splitlines()
                                    if line.strip()
                                }
                            except OSError as error:
                                errors.add(f"{task / 'maps'}: {error}")
                                mapping_details = {}
                        details = mapping_details.get(link.name, "")
                    references[key] = {
                        "pid": int(process.name),
                        "tid": int(thread.name),
                        "name": name,
                        "kind": kind,
                        "device": ", ".join(targets[info.st_rdev]),
                        "reference": str(link),
                        "maps": details,
                    }

    return {
        "devices": targets,
        "references": [references[key] for key in sorted(references)],
        "errors": sorted(errors),
        "processes": processes,
        "threads": threads,
        "vanished": vanished,
    }


def report_status(report: dict) -> tuple[str, int]:
    if report["errors"]:
        return "INCOMPLETE", 2
    if report["references"]:
        return "IN_USE", 1
    return "NO_REFERENCES_OBSERVED", 0


def print_report(report: dict) -> int:
    for rdev, names in report["devices"].items():
        print(
            f"DEVICE {', '.join(names)} major:minor={os.major(rdev)}:{os.minor(rdev)}"
        )
    for reference in report["references"]:
        print(
            f"\n{reference['kind'].upper()} PID={reference['pid']} "
            f"TID={reference['tid']} comm={reference['name']!r} "
            f"device={reference['device']}"
        )
        print(f"  reference: {reference['reference']}")
        if reference["maps"]:
            print(f"  maps: {reference['maps']}")
    for error in report["errors"]:
        print(f"INCOMPLETE: {error}")
    status, code = report_status(report)
    print(
        f"\nSUMMARY processes={report['processes']} threads={report['threads']} "
        f"references={len(report['references'])} errors={len(report['errors'])} "
        f"disappeared_during_scan={report['vanished']}"
    )
    print(f"RESULT={status} exit_code={code}")
    print(
        "Local userspace snapshot only. Keep new clients blocked for maintenance; "
        "check RM allocations, other hosts, and GPU/DMA registrations separately."
    )
    return code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("devices", nargs="+", metavar="/dev/daxN.M")
    args = parser.parse_args()
    if os.geteuid() != 0:
        print("RESULT=INCOMPLETE: run with sudo on the host to inspect all users.")
        return 2
    print(f"DAX reference inspection: host={platform.node()}")
    print(f"Started: {datetime.now(UTC).isoformat()}", flush=True)
    try:
        report = inspect(args.devices)
    except (OSError, ValueError) as error:
        print(f"RESULT=INCOMPLETE: {error}", file=sys.stderr)
        return 2
    print(f"Finished: {datetime.now(UTC).isoformat()}")
    return print_report(report)


if __name__ == "__main__":
    raise SystemExit(main())
