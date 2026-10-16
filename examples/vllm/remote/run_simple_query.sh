#!/bin/bash
# Simple query test for the remote KV pool (vLLM direct).
# Flow: inst1 computes a prompt and stores its KV in the remote pool →
#       inst2 answers the same prompt with KV read from the pool.
#
# Prerequisites:
#   1. The pool node runs remote_pool_server.sh
#   2. Two engines run remote_vllm_launcher.sh (inst1, inst2), on any nodes
#
# Usage:
#   ./run_simple_query.sh [model]

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

MODEL="${1:-${MODEL:-Qwen/Qwen2.5-0.5B}}"
# A fresh tag per run, so the prompt is new to both engines and to the pool.
TAG="Query $(date +%s%N)."

PROMPT="$TAG Explain CXL memory technology in detail. CXL stands for Compute Express Link, a high-speed CPU-to-device and CPU-to-memory interconnect built on the PCI Express physical and electrical interface. It defines three protocols: CXL.io for device discovery and configuration, CXL.cache for device-to-host cache coherency, and CXL.mem for host-managed device memory that the processor accesses with ordinary load and store instructions. CXL Type 3 devices expand memory capacity beyond what is attached to one CPU socket, and CXL 2.0 adds pooling through switches so that several hosts share one pool of CXL-attached memory. Large language model inference keeps a key-value cache for every prompt it has processed; placing that cache in a shared memory pool lets one inference engine reuse the prefill work of another. When the engines run on different servers, a pool node can export its CXL memory over RDMA, and each engine reads and writes the cache directly in the pool node's memory while only small control messages go through the pool node's server process. CXL 3.0 extends these capabilities with fabric-attached memory and larger multi-level switch topologies.

Summarize the key benefits of CXL technology:"

send_query() {
    local url="$1"
    PROMPT="$PROMPT" MODEL="$MODEL" URL="$url" python3 - <<'EOF'
import json
import os
import urllib.request

body = json.dumps({
    "model": os.environ["MODEL"],
    "prompt": os.environ["PROMPT"],
    "max_tokens": 100,
    "temperature": 0.0,
}).encode()
req = urllib.request.Request(
    os.environ["URL"] + "/v1/completions",
    data=body,
    headers={"Content-Type": "application/json"},
)
with urllib.request.urlopen(req, timeout=300) as resp:
    data = json.load(resp)
usage = data.get("usage") or {}
details = usage.get("prompt_tokens_details") or {}  # absent when nothing is cached
print(data["choices"][0]["text"].strip())
print(
    f"\n(prompt tokens: {usage.get('prompt_tokens')}, "
    f"cached tokens: {details.get('cached_tokens') or 0})"
)
EOF
}

echo "=== Prompt ==="
echo "$PROMPT"
echo ""

echo "=== inst1 - compute and store KV ($MARU_INST1_URL) ==="
send_query "$MARU_INST1_URL"
echo ""

# Async stores reach the pool shortly after the response.
sleep 3

echo "=== inst2 - load KV from the remote pool ($MARU_INST2_URL) ==="
send_query "$MARU_INST2_URL"
echo ""
echo "inst2 never saw this prompt, so its cached tokens were found in the remote pool."
echo "A failed load would show 'Maru load failed for req ... recomputing' in inst2's log."
