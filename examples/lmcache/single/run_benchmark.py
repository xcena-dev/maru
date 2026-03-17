#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Single-instance KV cache benchmark with TTFT measurement.

Sends the same prompt twice to a single vLLM instance with Maru storage backend.
Query 1 computes and stores KV cache; Query 2 retrieves from cache.
Measures TTFT speedup to validate cache hit.

Usage:
    python run_benchmark.py [--model MODEL] [--port PORT]
                            [--max-tokens N] [--repeat-count N] [--wait-time SEC]
"""

import argparse
import asyncio
import json
import os
import sys
import time

BASE_PROMPT = "Explain the significance of KV cache in language models."
DEFAULT_MODEL = "Qwen/Qwen2.5-0.5B"
DEFAULT_MAX_TOKENS = 32
DEFAULT_REPEAT_COUNT = 1
DEFAULT_WAIT_TIME = 2.0


def build_prompt(base: str = BASE_PROMPT, repeat: int = 100) -> str:
    """Build a long repeated prompt for KV cache generation."""
    return base * repeat


async def stream_completion(
    base_url: str, model: str, prompt: str, max_tokens: int
) -> dict:
    """Send a streaming completion request and measure TTFT.

    Returns dict with: ttft_ms, total_time_ms, text, status.
    """
    from openai import AsyncOpenAI

    client = AsyncOpenAI(base_url=f"{base_url}/v1", api_key="dummy")

    start = time.monotonic()
    first_token_time = None
    text_chunks = []

    try:
        stream = await client.completions.create(
            model=model,
            prompt=prompt,
            max_tokens=max_tokens,
            stream=True,
        )
        async for chunk in stream:
            if first_token_time is None:
                first_token_time = time.monotonic()
            if chunk.choices and chunk.choices[0].text:
                text_chunks.append(chunk.choices[0].text)

        end = time.monotonic()
        ttft = (first_token_time - start) * 1000 if first_token_time else None
        total = (end - start) * 1000

        return {
            "ttft_ms": round(ttft, 2) if ttft else None,
            "total_time_ms": round(total, 2),
            "text": "".join(text_chunks),
            "status": "ok",
        }
    except Exception as e:
        end = time.monotonic()
        return {
            "ttft_ms": None,
            "total_time_ms": round((end - start) * 1000, 2),
            "text": "",
            "status": f"error: {e}",
        }
    finally:
        await client.close()


async def run_session(
    label: str,
    base_url: str,
    model: str,
    prompt: str,
    max_tokens: int,
    repeat_count: int,
) -> list:
    """Run repeat_count requests, return list of results."""
    results = []
    for i in range(repeat_count):
        result = await stream_completion(base_url, model, prompt, max_tokens)
        result["session"] = label
        result["iteration"] = i + 1
        results.append(result)

        ttft_str = f"{result['ttft_ms']:.1f} ms" if result["ttft_ms"] else "N/A"
        print(
            f"  [{label}] iter {i + 1}/{repeat_count}: "
            f"TTFT={ttft_str}, total={result['total_time_ms']:.1f} ms",
            file=sys.stderr,
        )
    return results


_BLUE = "\033[0;34m"
_GREEN = "\033[0;32m"
_CYAN = "\033[0;36m"
_NC = "\033[0m"


def avg_ttft(results: list) -> float | None:
    """Calculate average TTFT from results list."""
    valid = [r["ttft_ms"] for r in results if r["ttft_ms"] is not None]
    return round(sum(valid) / len(valid), 2) if valid else None


def print_box_summary(q1_results: list, q2_results: list, wait_time: float) -> None:
    """Print a box-style human-readable summary to stderr."""

    q1_ttft = avg_ttft(q1_results)
    q2_ttft = avg_ttft(q2_results)
    speedup = (
        round(q1_ttft / q2_ttft, 2) if (q1_ttft and q2_ttft and q2_ttft > 0) else None
    )
    cache_hit = speedup is not None and speedup > 1.5

    print(f"\n{_BLUE}{'=' * 60}{_NC}", file=sys.stderr)
    print(f"{_BLUE}  Single Instance KV Cache - Results{_NC}", file=sys.stderr)
    print(f"{_BLUE}{'=' * 60}{_NC}", file=sys.stderr)
    print(
        f"  {_GREEN}Query 1 (compute+store){_NC}: TTFT = "
        f"{f'{q1_ttft:.1f} ms' if q1_ttft else 'N/A'}",
        file=sys.stderr,
    )
    print(
        f"  {_GREEN}Query 2 (cache hit){_NC}:     TTFT = "
        f"{f'{q2_ttft:.1f} ms' if q2_ttft else 'N/A'}",
        file=sys.stderr,
    )
    if speedup:
        print(
            f"  {_CYAN}TTFT Speedup{_NC}:            {speedup:.2f}x",
            file=sys.stderr,
        )
    print(
        f"  {_CYAN}Cache Hit{_NC}:               {'Yes' if cache_hit else 'No'}",
        file=sys.stderr,
    )
    print(f"  Wait between queries:    {wait_time}s", file=sys.stderr)
    print(f"{_BLUE}{'=' * 60}{_NC}\n", file=sys.stderr)


def build_json_summary(q1_results: list, q2_results: list, wait_time: float) -> dict:
    """Build machine-parseable JSON summary."""
    q1_ttft = avg_ttft(q1_results)
    q2_ttft = avg_ttft(q2_results)
    speedup = (
        round(q1_ttft / q2_ttft, 2) if (q1_ttft and q2_ttft and q2_ttft > 0) else None
    )
    cache_hit = speedup is not None and speedup > 1.5

    return {
        "query1_ttft_ms": q1_ttft,
        "query2_ttft_ms": q2_ttft,
        "ttft_speedup": speedup,
        "cache_hit": cache_hit,
        "wait_time_s": wait_time,
    }


async def main():
    parser = argparse.ArgumentParser(
        description="Single-instance KV cache benchmark with TTFT measurement"
    )
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("LMCACHE_INST_PORT", 8000)),
        help="Instance port (default: $LMCACHE_INST_PORT)",
    )
    parser.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    parser.add_argument(
        "--repeat-count",
        type=int,
        default=DEFAULT_REPEAT_COUNT,
        help="Requests per query session (default: 1)",
    )
    parser.add_argument(
        "--wait-time",
        type=float,
        default=DEFAULT_WAIT_TIME,
        help="Seconds between query 1 and query 2 (default: 2.0)",
    )
    args = parser.parse_args()

    prompt = build_prompt()
    base_url = f"http://localhost:{args.port}"

    print(
        f"\nModel: {args.model}, Port: {args.port}, "
        f"MaxTokens: {args.max_tokens}, Repeat: {args.repeat_count}",
        file=sys.stderr,
    )

    # Query 1: compute and store KV cache
    print(
        f"\n[Query 1] Compute + Store KV cache (port {args.port})",
        file=sys.stderr,
    )
    q1_results = await run_session(
        "query1",
        base_url,
        args.model,
        prompt,
        args.max_tokens,
        args.repeat_count,
    )

    # Wait for KV cache to be fully stored
    print(
        f"\nWaiting {args.wait_time}s for KV cache storage...",
        file=sys.stderr,
    )
    await asyncio.sleep(args.wait_time)

    # Query 2: same prompt, should hit cache
    print(
        f"\n[Query 2] Retrieve from cache (port {args.port})",
        file=sys.stderr,
    )
    q2_results = await run_session(
        "query2",
        base_url,
        args.model,
        prompt,
        args.max_tokens,
        args.repeat_count,
    )

    # Print human-readable summary to stderr
    print_box_summary(q1_results, q2_results, args.wait_time)

    # Print machine-parseable JSON on stdout
    summary = build_json_summary(q1_results, q2_results, args.wait_time)
    print(json.dumps(summary))

    sys.exit(0 if summary["cache_hit"] else 1)


if __name__ == "__main__":
    asyncio.run(main())
