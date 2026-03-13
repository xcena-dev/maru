#!/usr/bin/env python3
"""SGLang KV cache sharing benchmark with TTFT measurement.

Sends identical prompts to two SGLang instances (or one instance twice),
measuring time-to-first-token to validate KV cache sharing.

Usage:
    python run_benchmark.py [--model MODEL] [--port1 PORT] [--port2 PORT]
                            [--max-tokens N] [--repeat-count N] [--wait-time SEC]
                            [--single-port PORT]  # single instance mode
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
DEFAULT_WAIT_TIME = 3.0


def build_prompt(base: str = BASE_PROMPT, repeat: int = 100) -> str:
    """Build a long repeated prompt for KV cache generation."""
    return base * repeat


async def stream_completion(
    base_url: str, model: str, prompt: str, max_tokens: int
) -> dict:
    """Send a streaming completion request and measure TTFT."""
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
    """Run repeat_count requests to a single instance, return list of results."""
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


def print_box_summary(
    s1_results: list, s2_results: list, wait_time: float, mode: str = "p2p"
) -> None:
    """Print a box-style human-readable summary to stderr."""
    s1_ttft = avg_ttft(s1_results)
    s2_ttft = avg_ttft(s2_results)
    speedup = (
        round(s1_ttft / s2_ttft, 2) if (s1_ttft and s2_ttft and s2_ttft > 0) else None
    )
    cache_hit = speedup is not None and speedup > 1.5

    title = {
        "p2p": "P2P KV Cache Sharing (Maru L3)",
        "hicache_pd": "HiCache PD (Maru L3)",
        "native_pd": "Native PD Disaggregation",
        "single": "Single Instance HiCache",
    }.get(mode, "KV Cache Benchmark")

    print(f"\n{_BLUE}{'=' * 60}{_NC}", file=sys.stderr)
    print(f"{_BLUE}  {title} - Results{_NC}", file=sys.stderr)
    print(f"{_BLUE}{'=' * 60}{_NC}", file=sys.stderr)
    print(
        f"  {_GREEN}Session 1 (store){_NC}:    TTFT = "
        f"{f'{s1_ttft:.1f} ms' if s1_ttft else 'N/A'}",
        file=sys.stderr,
    )
    print(
        f"  {_GREEN}Session 2 (retrieve){_NC}: TTFT = "
        f"{f'{s2_ttft:.1f} ms' if s2_ttft else 'N/A'}",
        file=sys.stderr,
    )
    if speedup:
        print(
            f"  {_CYAN}TTFT Speedup{_NC}:         {speedup:.2f}x",
            file=sys.stderr,
        )
    print(
        f"  {_CYAN}Cache Hit{_NC}:            {'Yes' if cache_hit else 'No'}",
        file=sys.stderr,
    )
    print(f"  Wait between sessions: {wait_time}s", file=sys.stderr)
    print(f"{_BLUE}{'=' * 60}{_NC}\n", file=sys.stderr)


def build_json_summary(s1_results: list, s2_results: list, wait_time: float) -> dict:
    """Build machine-parseable JSON summary."""
    s1_ttft = avg_ttft(s1_results)
    s2_ttft = avg_ttft(s2_results)
    speedup = (
        round(s1_ttft / s2_ttft, 2) if (s1_ttft and s2_ttft and s2_ttft > 0) else None
    )
    cache_hit = speedup is not None and speedup > 1.5

    return {
        "session1_ttft_ms": s1_ttft,
        "session2_ttft_ms": s2_ttft,
        "ttft_speedup": speedup,
        "cache_hit": cache_hit,
        "wait_time_s": wait_time,
    }


async def main():
    parser = argparse.ArgumentParser(
        description="SGLang KV cache benchmark with TTFT measurement"
    )
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument(
        "--port1",
        type=int,
        default=int(os.environ.get("SGLANG_INST1_PORT", 30000)),
        help="Instance 1 port (default: $SGLANG_INST1_PORT or 30000)",
    )
    parser.add_argument(
        "--port2",
        type=int,
        default=int(os.environ.get("SGLANG_INST2_PORT", 30001)),
        help="Instance 2 port (default: $SGLANG_INST2_PORT or 30001)",
    )
    parser.add_argument(
        "--single-port",
        type=int,
        default=None,
        help="Single instance mode: send both sessions to same port",
    )
    parser.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    parser.add_argument(
        "--repeat-count",
        type=int,
        default=DEFAULT_REPEAT_COUNT,
        help="Requests per session (default: 1)",
    )
    parser.add_argument(
        "--wait-time",
        type=float,
        default=DEFAULT_WAIT_TIME,
        help="Seconds between sessions for KV propagation (default: 3.0)",
    )
    parser.add_argument(
        "--mode",
        choices=["p2p", "hicache_pd", "native_pd", "single"],
        default="p2p",
        help="Benchmark mode for display (default: p2p)",
    )
    args = parser.parse_args()

    # Single instance mode: both sessions go to the same port
    if args.single_port:
        args.port1 = args.single_port
        args.port2 = args.single_port

    prompt = build_prompt()

    print(
        f"\nModel: {args.model}, Ports: {args.port1}/{args.port2}, "
        f"MaxTokens: {args.max_tokens}, Repeat: {args.repeat_count}",
        file=sys.stderr,
    )

    # Session 1: store KV cache
    print(
        f"\n[Session 1] Port {args.port1} - Store KV cache",
        file=sys.stderr,
    )
    s1_results = await run_session(
        "session1",
        f"http://localhost:{args.port1}",
        args.model,
        prompt,
        args.max_tokens,
        args.repeat_count,
    )

    # Flush: send a dummy request to inst1 to trigger writing_check() and
    # flush the async GPU→Host→L3 write pipeline before querying inst2.
    print("\n[Flush] Sending dummy request to inst1 to flush L3 write pipeline...", file=sys.stderr)
    await stream_completion(f"http://localhost:{args.port1}", args.model, "hi", 1)

    # Wait for KV cache propagation
    print(
        f"\nWaiting {args.wait_time}s for KV cache propagation...",
        file=sys.stderr,
    )
    await asyncio.sleep(args.wait_time)

    # Session 2: retrieve KV cache
    print(
        f"\n[Session 2] Port {args.port2} - Retrieve KV cache",
        file=sys.stderr,
    )
    s2_results = await run_session(
        "session2",
        f"http://localhost:{args.port2}",
        args.model,
        prompt,
        args.max_tokens,
        args.repeat_count,
    )

    # Print human-readable summary to stderr
    print_box_summary(s1_results, s2_results, args.wait_time, mode=args.mode)

    # Print machine-parseable JSON on stdout
    summary = build_json_summary(s1_results, s2_results, args.wait_time)
    print(json.dumps(summary))

    # Exit with status based on cache hit
    sys.exit(0 if summary["cache_hit"] else 1)


if __name__ == "__main__":
    asyncio.run(main())
