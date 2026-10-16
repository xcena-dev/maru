#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Remote KV sharing test: one engine stores a prompt's KV in the remote pool,
another engine reads it back.

Sends the same prompt to Instance 1 (computes it and stores its KV in the pool)
and then to Instance 2 (loads the KV from the pool). Instance 2 has never seen
the prompt, so every cached token it reports was found in the remote pool.
vLLM counts them when it schedules the request. A load that then fails is
recomputed and logged by the connector ("Maru load failed for req ...;
recomputing", preceded by its cause such as "Maru batch_retrieve failed"); if
it fails from its first chunk, newer vLLM versions report 0 cached tokens. Each run
starts the prompt with a fresh tag, so reruns do not hit earlier runs' KV.

Usage:
    python run_benchmark.py [--model MODEL] [--url1 URL] [--url2 URL]
                            [--prompt-repeat N] [--max-tokens N] [--wait-time SEC]
"""

import argparse
import asyncio
import json
import os
import sys
import time
import uuid

BASE_PROMPT = (
    "The KV cache is a critical optimization in modern large language models. "
    "During autoregressive generation, each new token requires attending to all "
    "previous tokens. Without caching, this means recomputing the key and value "
    "projections for every prior token at each generation step, leading to "
    "quadratic computational cost. The KV cache stores these previously computed "
    "key-value pairs, allowing the model to only compute projections for the new "
    "token while reusing cached values for all previous positions. In distributed "
    "inference systems, sharing KV caches between engines avoids redundant "
    "prefill computation for common prompt prefixes. A remote KV pool keeps "
    "these caches in CXL memory on a separate node, and every engine that can "
    "reach the pool over RDMA reads what any other engine stored. When a new "
    "request arrives, the scheduler first asks the pool which prefix chunks "
    "exist. If they are found, the worker reads them over RDMA into a staging "
    "buffer and copies them into its GPU memory, skipping the prefill of those "
    "tokens. Each chunk holds a fixed number of tokens, and its key encodes the "
    "whole prefix up to that point, so even a partial prefix can be reused. "
)
QUESTION = (
    "\n\nQuestion: Summarize how a remote KV pool lets one engine reuse the "
    "prefill of another.\n\nAnswer:"
)
DEFAULT_MODEL = "Qwen/Qwen2.5-0.5B"
DEFAULT_MAX_TOKENS = 32
DEFAULT_PROMPT_REPEAT = 8
DEFAULT_WAIT_TIME = 3.0


def build_prompt(repeat: int) -> str:
    """Return a prompt that no engine or pool has seen before."""
    tag = f"Run {uuid.uuid4().hex}. "
    return tag + BASE_PROMPT * repeat + QUESTION


async def stream_completion(
    base_url: str, model: str, prompt: str, max_tokens: int
) -> dict:
    """Send one streamed completion and return its TTFT and cached tokens."""
    from openai import AsyncOpenAI

    client = AsyncOpenAI(base_url=f"{base_url}/v1", api_key="dummy")
    start = time.monotonic()
    first_token_time = None
    text_chunks = []
    usage = None
    try:
        stream = await client.completions.create(
            model=model,
            prompt=prompt,
            max_tokens=max_tokens,
            temperature=0.0,
            stream=True,
            stream_options={"include_usage": True},
        )
        async for chunk in stream:
            if chunk.choices and chunk.choices[0].text:
                if first_token_time is None:
                    first_token_time = time.monotonic()
                text_chunks.append(chunk.choices[0].text)
            if chunk.usage is not None:
                usage = chunk.usage
        end = time.monotonic()
        details = getattr(usage, "prompt_tokens_details", None) if usage else None
        return {
            "ttft_ms": round((first_token_time - start) * 1000, 2)
            if first_token_time
            else None,
            "total_time_ms": round((end - start) * 1000, 2),
            "prompt_tokens": usage.prompt_tokens if usage else None,
            "cached_tokens": getattr(details, "cached_tokens", None),
            "text": "".join(text_chunks),
            "status": "ok",
        }
    except Exception as e:  # noqa: BLE001 - report any failure as a result
        return {
            "ttft_ms": None,
            "total_time_ms": round((time.monotonic() - start) * 1000, 2),
            "prompt_tokens": None,
            "cached_tokens": None,
            "text": "",
            "status": f"error: {e}",
        }
    finally:
        await client.close()


_B = "\033[0;34m"
_G = "\033[0;32m"
_C = "\033[0;36m"
_NC = "\033[0m"


def _ms(value: float | None) -> str:
    """Format a duration in milliseconds, or N/A when it was not measured."""
    return f"{value:.1f} ms" if value is not None else "N/A"


def report(label: str, result: dict) -> None:
    """Print one request's result to stderr."""
    print(
        f"  [{label}] TTFT={_ms(result['ttft_ms'])}, "
        f"prompt tokens={result['prompt_tokens']}, "
        f"cached tokens={result['cached_tokens'] or 0}, status={result['status']}",
        file=sys.stderr,
    )
    if result["text"]:
        print(f"  [{label}] answer: {result['text']!r}", file=sys.stderr)


def summarize(store: dict, load: dict, chunk_tokens: int, wait: float) -> dict:
    """Decide whether Instance 2 found Instance 1's KV in the pool; print a summary.

    vLLM reports a prompt's cached tokens when it schedules the request, so a
    hit here means the prefix was found in the pool. A load that fails after
    that is recomputed and shows up in Instance 2's log; one that fails from
    its first chunk can also bring this count to 0.
    """
    cached = load["cached_tokens"] or 0
    hit = store["status"] == "ok" and load["status"] == "ok" and cached >= chunk_tokens
    t1, t2 = store["ttft_ms"], load["ttft_ms"]
    speedup = round(t1 / t2, 2) if (t1 and t2) else None

    print(f"\n{_B}{'=' * 60}{_NC}", file=sys.stderr)
    print(f"{_B}  Maru-vLLM remote KV sharing{_NC}", file=sys.stderr)
    print(f"{_B}{'=' * 60}{_NC}", file=sys.stderr)
    print(f"  {_G}Instance 1 (store){_NC}: TTFT = {_ms(t1)}", file=sys.stderr)
    print(
        f"  {_G}Instance 2 (load){_NC}:  TTFT = {_ms(t2)}, "
        f"{cached} of {load['prompt_tokens']} prompt tokens found in the pool",
        file=sys.stderr,
    )
    if speedup:
        print(f"  {_C}TTFT ratio{_NC}:         {speedup:.2f}x", file=sys.stderr)
    print(f"  {_C}Remote hit{_NC}:         {'Yes' if hit else 'No'}", file=sys.stderr)
    print(f"{_B}{'=' * 60}{_NC}\n", file=sys.stderr)
    return {
        "store_ttft_ms": t1,
        "load_ttft_ms": t2,
        "ttft_ratio": speedup,
        "prompt_tokens": load["prompt_tokens"],
        "cached_tokens_from_pool": cached,
        "remote_hit": hit,
        "wait_time_s": wait,
    }


async def main() -> None:
    p = argparse.ArgumentParser(description="Maru-vLLM remote KV sharing test")
    p.add_argument("--model", default=os.environ.get("MODEL", DEFAULT_MODEL))
    p.add_argument(
        "--url1", default=os.environ.get("MARU_INST1_URL", "http://localhost:8000")
    )
    p.add_argument(
        "--url2", default=os.environ.get("MARU_INST2_URL", "http://localhost:8001")
    )
    p.add_argument("--prompt-repeat", type=int, default=DEFAULT_PROMPT_REPEAT)
    p.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    p.add_argument(
        "--wait-time",
        type=float,
        default=DEFAULT_WAIT_TIME,
        help="seconds between the two requests (async stores finish after the "
        "response)",
    )
    p.add_argument(
        "--chunk-tokens",
        type=int,
        default=int(os.environ.get("MARU_KV_CHUNK_TOKENS", 256)),
    )
    args = p.parse_args()

    prompt = build_prompt(args.prompt_repeat)
    print(
        f"\nModel: {args.model}, Instance 1: {args.url1}, Instance 2: {args.url2}",
        file=sys.stderr,
    )
    print("\n[1] Instance 1 computes the prompt and stores its KV", file=sys.stderr)
    store = await stream_completion(args.url1, args.model, prompt, args.max_tokens)
    report("store", store)

    print(
        f"\nWaiting {args.wait_time}s for the store to reach the pool...",
        file=sys.stderr,
    )
    await asyncio.sleep(args.wait_time)

    print("\n[2] Instance 2 loads the KV from the remote pool", file=sys.stderr)
    load = await stream_completion(args.url2, args.model, prompt, args.max_tokens)
    report("load", load)

    summary = summarize(store, load, args.chunk_tokens, args.wait_time)
    print(json.dumps(summary))
    sys.exit(0 if summary["remote_hit"] else 1)


if __name__ == "__main__":
    asyncio.run(main())
