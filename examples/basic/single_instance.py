#!/usr/bin/env python3
"""Zero-copy store & retrieve on a single Maru instance.

Demonstrates the three-step zero-copy flow:
  1. alloc()        — allocate a page in CXL shared memory
  2. handle.buf[:]  — write directly to CXL (mmap, no intermediate buffer)
  3. store(handle=) — register metadata only (key → region, offset)

Prerequisites:
    1. maru-resource-manager running
    2. maru-server running (maru-server)

Usage:
    python examples/basic/single_instance.py
"""

import logging

from maru import MaruConfig, MaruHandler

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024  # 1MB — matches default chunk_size_bytes


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    config = MaruConfig(
        server_url="tcp://localhost:5555",
        pool_size=1024 * 1024 * 100,  # 100MB
    )

    with MaruHandler(config) as handler:
        logger.info("Connected (instance_id=%s)", config.instance_id)

        # --- Store ---
        data = b"A" * CHUNK_SIZE
        key = 42

        # 1. Allocate a page in CXL shared memory
        handle = handler.alloc(size=len(data))
        logger.info(
            "Allocated %d bytes in CXL memory (region=%d, page=%d)",
            len(data),
            handle.region_id,
            handle.page_index,
        )

        # 2. Write directly to CXL memory (mmap — no intermediate buffer)
        handle.buf[:] = data
        logger.info("Wrote %d bytes directly to CXL memory", len(data))

        # 3. Register the key — only metadata (key → region, offset) is sent
        handler.store(key=key, handle=handle)
        logger.info("Registered key=%d (metadata only, no data copy)", key)

        # --- Retrieve ---
        result = handler.retrieve(key=key)
        assert result is not None, f"key={key} not found"
        assert len(result.view) == CHUNK_SIZE
        assert bytes(result.view[:5]) == b"AAAAA"
        logger.info(
            "Retrieved key=%d — %d bytes (memoryview into CXL memory)",
            key,
            len(result.view),
        )

        # --- Exists / Stats ---
        assert handler.exists(key=key)
        stats = handler.get_stats()
        logger.info("Server stats: %s", stats)

        logger.info("Done — all operations used zero-copy CXL access")


if __name__ == "__main__":
    main()
