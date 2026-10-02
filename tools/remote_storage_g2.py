# SPDX-License-Identifier: Apache-2.0
"""Gate G2 driver for the remote storage backend: store a prompt through one vLLM
worker, reuse it from another.

Usage:
  PYTHONPATH=. python tools/remote_storage_g2.py --a http://127.0.0.1:8101 \
      --b http://worker-b:8102 --model Qwen/Qwen2.5-0.5B --words 900 \
      --server-url tcp://127.0.0.1:5575 --out g2.json

Steps (greedy, max_tokens=32), where "A" is the storing worker and "B" the
remote worker given on the command line:
  1. prompt 1 -> A (store)            2. prompt 1 -> B (expect external hit)
  3. prompt 2 -> B (reference, miss)  4. prompt 2 -> A (keys exist, no store)
  5. prompt 2 -> B again (hit)        6. prompt 1 with its first token changed -> B (miss)
After each request the metadata server's registered-key count is read so the
"new_keys" column shows whether the request stored chunks (12 per 900-word
prompt) or reused them (0). Completion text, prompt tokens and latency are
recorded per step.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request


def _post(url: str, payload: dict, timeout: float = 600.0) -> dict:
    req = urllib.request.Request(
        url + "/v1/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = json.loads(resp.read())
    body["_latency_s"] = time.perf_counter() - t0
    return body


def _prompt(seed: int, words: int) -> str:
    # deterministic, low-entropy text so token count is predictable but not trivially cached
    base = [
        "alpha",
        "bravo",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        "juliet",
        "kilo",
        "lima",
        "mike",
        "november",
        "oscar",
        "papa",
    ]
    out = []
    x = seed
    for _ in range(words):
        x = (x * 1103515245 + 12345) & 0x7FFFFFFF
        out.append(base[x % len(base)] + str(x % 97))
    return "Document " + str(seed) + ": " + " ".join(out) + "\nSummary:"


def _key_count(server_url: str) -> int:
    from maru_handler.rpc_client import RpcClient

    c = RpcClient(server_url)
    c.connect()
    try:
        return int(c.get_stats().kv_manager.total_entries)
    finally:
        c.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True)
    ap.add_argument("--b", required=True)
    ap.add_argument("--model", required=True)
    ap.add_argument("--words", type=int, default=900)
    ap.add_argument("--max-tokens", type=int, default=32)
    ap.add_argument("--out", default="g2.json")
    ap.add_argument("--server-url", default="tcp://127.0.0.1:5575")
    ap.add_argument(
        "--seed-base",
        type=int,
        default=1,
        help="document seed for prompt 1; prompt 2 uses seed+1 (use a fresh pair per pool)",
    )
    args = ap.parse_args()

    prompt_1 = _prompt(args.seed_base, args.words)
    prompt_2 = _prompt(args.seed_base + 1, args.words)
    prompt_1_altered = "X" + prompt_1[1:]
    common = {
        "model": args.model,
        "max_tokens": args.max_tokens,
        "temperature": 0,
        "seed": 0,
    }
    steps = [
        ("1 P->A store", args.a, prompt_1),
        ("2 P->B hit", args.b, prompt_1),
        ("3 P'->B miss ref", args.b, prompt_2),
        ("4 P'->A store", args.a, prompt_2),
        ("5 P'->B hit", args.b, prompt_2),
        ("6 P''->B miss", args.b, prompt_1_altered),
    ]
    results = []
    before = _key_count(args.server_url)
    for name, url, prompt in steps:
        r = _post(url, {**common, "prompt": prompt})
        text = r["choices"][0]["text"]
        time.sleep(1.5)
        after = _key_count(args.server_url)
        results.append(
            {
                "step": name,
                "url": url,
                "prompt_tokens": r["usage"]["prompt_tokens"],
                "completion": text,
                "latency_s": round(r["_latency_s"], 4),
                "new_keys": after - before,
            }
        )
        print(
            f"{name:18s} prompt_tokens={r['usage']['prompt_tokens']:5d} latency={r['_latency_s']:.3f}s new_keys={after - before:3d} text={text[:40]!r}"
        )
        before = after
    same_12 = results[0]["completion"] == results[1]["completion"]
    same_35 = results[2]["completion"] == results[4]["completion"]
    summary = {
        "A_vs_B_same_output_P": same_12,
        "B_miss_vs_B_hit_same_output_P2": same_35,
        "steps": results,
    }
    json.dump(summary, open(args.out, "w"), indent=2)
    print(json.dumps({k: v for k, v in summary.items() if k != "steps"}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
