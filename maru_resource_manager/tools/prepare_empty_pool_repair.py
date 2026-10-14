#!/usr/bin/env python3
"""Prepare corrected v6 pool checkpoints from an OFFLINE, empty RM snapshot.

Writes only to a new output directory. Never updates the source state, opens a
DAX device, changes global.meta/secret, or removes WAL. Stop/drain the RM and
take a backup first. Verify device UUID headers and review the output manifest
before replacing any pool checkpoint files.
"""

import argparse
import hashlib
import json
import re
import struct
import sys
import zlib
from pathlib import Path

POOL_HEADER = struct.Struct("<IIIIQQ")
GLOBAL_HEADER = struct.Struct("<IIIIQQ")
EXTENT = struct.Struct("<QQ")


def read_empty_global(state: Path) -> bytes:
    data = (state / "global.meta").read_bytes()
    if len(data) != GLOBAL_HEADER.size:
        raise ValueError(
            "global.meta must contain zero allocations and no trailing bytes"
        )
    magic, version, crc, reserved, next_id, count = GLOBAL_HEADER.unpack(data)
    if (magic, version, crc, reserved, count) != (
        0x474C4F42,
        2,
        0,
        0,
        0,
    ) or next_id == 0:
        raise ValueError(
            "invalid or nonempty global.meta; do not repair a live allocation"
        )
    for name in ("maru-resource-manager.wal", "maru_resourced.wal"):
        path = state / name
        if path.exists() and path.stat().st_size:
            raise ValueError(
                f"{name} is not empty; complete recovery/checkpoint before repair"
            )
    return data


def corrected_pool(data: bytes, pool_id: int, size: int, alignment: int) -> bytes:
    if not 0 < alignment < size < 2**64:
        raise ValueError("invalid device size/alignment")
    if len(data) < POOL_HEADER.size:
        raise ValueError("truncated pool metadata")
    magic, version, stored_id, crc, total, count = POOL_HEADER.unpack_from(data)
    payload = data[POOL_HEADER.size :]
    if (magic, version, stored_id) != (0x4D455441, 6, pool_id):
        raise ValueError("unexpected pool magic/version/ID")
    if len(payload) != count * EXTENT.size or zlib.crc32(payload) != crc:
        raise ValueError("pool payload size/CRC mismatch")
    # This tool only fixes the known fully-empty legacy layout (or no-ops a
    # correct one). Other patterns need investigation, not a blanket reset.
    if count != 1:
        raise ValueError(
            "expected one empty-pool extent; refusing to discard other state"
        )
    offset, length = EXTENT.unpack(payload)
    legacy = (offset, length) == (0, size) and total in (size, size - alignment)
    valid = (offset, length, total) == (alignment, size - alignment, size - alignment)
    if not (legacy or valid):
        raise ValueError(
            "pool is not the known empty legacy layout or an already-correct pool"
        )
    payload = EXTENT.pack(alignment, size - alignment)
    return (
        POOL_HEADER.pack(
            magic, version, pool_id, zlib.crc32(payload), size - alignment, 1
        )
        + payload
    )


def prepare(state: Path, output: Path, devices: list[str], sysfs: Path) -> dict:
    state = state.resolve(strict=True)
    output = output.resolve()
    if output == state or state in output.parents:
        raise ValueError("output must be outside the source state directory")
    if output.exists():
        raise ValueError("output directory must not already exist")
    original_global = read_empty_global(state)
    prepared = {}
    pools = []
    for device in devices:
        match = re.fullmatch(r"/dev/(dax(\d+)\.\d+)", device)
        if not match:
            raise ValueError(f"expected /dev/daxN.M: {device}")
        name, pool_id = match[1], int(match[2])
        filename = f"pool_{pool_id}.meta"
        if filename in prepared:
            raise ValueError(
                "multiple devices share a pool ID; mapping requires manual review"
            )
        size = int((sysfs / name / "size").read_text().strip(), 0)
        alignment = int((sysfs / name / "align").read_text().strip(), 0)
        original = (state / filename).read_bytes()
        corrected = corrected_pool(original, pool_id, size, alignment)
        prepared[filename] = (original, corrected)
        pools.append(
            {
                "device": device,
                "file": filename,
                "size": size,
                "alignment": alignment,
                "data_begin": alignment,
                "data_end": size,
                "changed": original != corrected,
                "source_sha256": hashlib.sha256(original).hexdigest(),
                "prepared_sha256": hashlib.sha256(corrected).hexdigest(),
            }
        )
    # Detect an obvious concurrent change; this does not replace stopping RM.
    if read_empty_global(state) != original_global:
        raise ValueError("global metadata changed while preparing repair")
    for filename, (original, _) in prepared.items():
        if (state / filename).read_bytes() != original:
            raise ValueError(f"{filename} changed while preparing repair")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    for filename, (_, corrected) in prepared.items():
        (output / filename).write_bytes(corrected)
    manifest = {
        "source": str(state),
        "global_sha256": hashlib.sha256(original_global).hexdigest(),
        "next_region_id": GLOBAL_HEADER.unpack(original_global)[4],
        "pools": pools,
        "applied": False,
        "requires": "RM stopped; all clients/mappings drained; valid device UUID headers",
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-dir", type=Path, required=True, help="offline state backup"
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="new directory for repaired copies"
    )
    parser.add_argument(
        "--device", action="append", required=True, help="/dev/daxN.M; repeat per pool"
    )
    args = parser.parse_args()
    try:
        manifest = prepare(
            args.state_dir, args.output, args.device, Path("/sys/bus/dax/devices")
        )
    except (OSError, ValueError, struct.error) as error:
        print(f"Refusing to prepare repair: {error}", file=sys.stderr)
        return 1
    print(json.dumps(manifest, indent=2))
    print(
        f"Prepared copies in {args.output}; source state and devices were NOT changed."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
