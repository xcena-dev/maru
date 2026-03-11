#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""P2P KV cache sharing test for Maru-vLLM direct integration.

Sends identical prompts to Instance 1 (store) then Instance 2 (retrieve),
measuring TTFT to validate KV cache sharing through CXL.

Usage:
    python run_request.py [--model MODEL] [--port1 PORT] [--port2 PORT]
                          [--max-tokens N] [--repeat-count N] [--wait-time SEC]
"""

import argparse
import asyncio
import json
import os
import sys
import time

BASE_PROMPT = (
    "The KV cache is a critical optimization in modern large language models. "
    "During autoregressive generation, each new token requires attending to all "
    "previous tokens. Without caching, this means recomputing the key and value "
    "projections for every prior token at each generation step, leading to "
    "quadratic computational cost. The KV cache stores these previously computed "
    "key-value pairs, allowing the model to only compute projections for the new "
    "token while reusing cached values for all previous positions. This reduces "
    "the per-step complexity from O(n) to O(1) for the projection computation. "
    "In distributed inference systems, sharing KV caches between instances "
    "enables even greater efficiency by avoiding redundant prefill computation "
    "for common prompt prefixes. Technologies like CXL shared memory make this "
    "possible with near-zero latency data transfer between nodes. "
    "The architecture of a typical KV cache sharing system involves several "
    "components: a metadata server that tracks which KV entries exist and where "
    "they are stored, a shared memory layer that provides the actual data plane "
    "for transferring cache entries, and connector plugins in each inference "
    "engine that intercept KV operations to store and retrieve from the shared "
    "pool. When a new request arrives at any instance, the scheduler first "
    "checks if a matching prefix exists in the shared cache. If found, the "
    "worker loads the cached KV tensors directly into its GPU memory, skipping "
    "the expensive prefill phase entirely. The key challenge is designing an "
    "efficient chunk-based storage scheme where each chunk represents a fixed "
    "number of tokens, and chunk keys encode the full prefix context up to that "
    "point using rolling hashes. This enables partial prefix reuse: even if "
    "only the first N chunks of a long prompt are cached, those chunks can be "
    "loaded while the remaining tokens are computed normally. The system must "
    "also handle concurrent access from multiple instances, cache eviction "
    "policies, and memory pressure management across the shared pool.\n\n"
    "Question: Summarize the three main components of a KV cache sharing "
    "system and explain why chunk-based storage enables partial prefix reuse.\n\n"
    "Answer:"
)
DEFAULT_MODEL = "Qwen/Qwen2.5-0.5B"
DEFAULT_MAX_TOKENS = 64
DEFAULT_REPEAT_COUNT = 1
DEFAULT_WAIT_TIME = 3.0


def build_prompt(base: str = BASE_PROMPT, repeat: int = 1) -> str:
    return base * repeat


async def stream_completion(
    base_url: str, model: str, prompt: str, max_tokens: int
) -> dict:
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
        return {
            "ttft_ms": round(ttft, 2) if ttft else None,
            "total_time_ms": round((end - start) * 1000, 2),
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
        if result["text"]:
            print(f"  [{label}] answer: {result['text']}", file=sys.stderr)
    return results


_B = "\033[0;34m"
_G = "\033[0;32m"
_C = "\033[0;36m"
_NC = "\033[0m"


def avg_ttft(results: list) -> float | None:
    valid = [r["ttft_ms"] for r in results if r["ttft_ms"] is not None]
    return round(sum(valid) / len(valid), 2) if valid else None


def print_summary(s1: list, s2: list, wait: float) -> dict:
    t1, t2 = avg_ttft(s1), avg_ttft(s2)
    speedup = round(t1 / t2, 2) if (t1 and t2 and t2 > 0) else None
    hit = speedup is not None and speedup > 1.5

    print(f"\n{_B}{'=' * 60}{_NC}", file=sys.stderr)
    print(f"{_B}  Maru-vLLM Direct P2P KV Cache Sharing{_NC}", file=sys.stderr)
    print(f"{_B}{'=' * 60}{_NC}", file=sys.stderr)
    print(
        f"  {_G}Instance 1 (store){_NC}:    TTFT = {f'{t1:.1f} ms' if t1 else 'N/A'}",
        file=sys.stderr,
    )
    print(
        f"  {_G}Instance 2 (retrieve){_NC}: TTFT = {f'{t2:.1f} ms' if t2 else 'N/A'}",
        file=sys.stderr,
    )
    if speedup:
        print(f"  {_C}TTFT Speedup{_NC}:         {speedup:.2f}x", file=sys.stderr)
    print(f"  {_C}Cache Hit{_NC}:            {'Yes' if hit else 'No'}", file=sys.stderr)
    print(f"  Wait between sessions: {wait}s", file=sys.stderr)
    print(f"{_B}{'=' * 60}{_NC}\n", file=sys.stderr)

    return {
        "session1_ttft_ms": t1,
        "session2_ttft_ms": t2,
        "ttft_speedup": speedup,
        "cache_hit": hit,
        "wait_time_s": wait,
    }


async def main():
    p = argparse.ArgumentParser(description="Maru-vLLM P2P KV cache test")
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument(
        "--port1", type=int, default=int(os.environ.get("MARU_INST1_PORT", 8000))
    )
    p.add_argument(
        "--port2", type=int, default=int(os.environ.get("MARU_INST2_PORT", 8001))
    )
    p.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    p.add_argument("--repeat-count", type=int, default=DEFAULT_REPEAT_COUNT)
    p.add_argument("--wait-time", type=float, default=DEFAULT_WAIT_TIME)
    args = p.parse_args()

    prompt = build_prompt()
    print(
        f"\nModel: {args.model}, Ports: {args.port1}/{args.port2}, "
        f"MaxTokens: {args.max_tokens}",
        file=sys.stderr,
    )

    print(f"\n[Session 1] Instance 1 (port {args.port1}) - Store KV", file=sys.stderr)
    s1 = await run_session(
        "store",
        f"http://localhost:{args.port1}",
        args.model,
        prompt,
        args.max_tokens,
        args.repeat_count,
    )

    print(f"\nWaiting {args.wait_time}s for CXL propagation...", file=sys.stderr)
    await asyncio.sleep(args.wait_time)

    print(
        f"\n[Session 2] Instance 2 (port {args.port2}) - Retrieve KV", file=sys.stderr
    )
    s2 = await run_session(
        "retrieve",
        f"http://localhost:{args.port2}",
        args.model,
        prompt,
        args.max_tokens,
        args.repeat_count,
    )

    summary = print_summary(s1, s2, args.wait_time)
    print(json.dumps(summary))
    sys.exit(0 if summary["cache_hit"] else 1)


if __name__ == "__main__":
    asyncio.run(main())
