#!/bin/bash
# P2P query test: inst1 gets TWO queries (to trigger write-through completion),
# then inst2 queries to verify L3 cache hit.
#
# Flow: inst1 query1 (prefill+store) → inst1 query2 (writing_check flushes to L3)
#       → sleep → inst2 query (should hit L3 via batch_exists/batch_get)

cd "$(dirname "${BASH_SOURCE[0]}")"
[ -f "env.sh" ] && source env.sh

PORT1="${SGLANG_INST1_PORT}"
PORT2="${SGLANG_INST2_PORT}"
export MODEL="${MODEL:-Qwen/Qwen2.5-0.5B}"
export PROMPT

PROMPT="Explain CXL memory technology in detail. CXL stands for Compute Express Link, which is a high-speed CPU-to-device and CPU-to-memory interconnect designed to accelerate next-generation data center performance. It enables memory expansion and sharing between host processors and accelerators. CXL builds on the PCI Express (PCIe) physical and electrical interface, adding a set of protocols that allow coherent memory access between CPUs and attached devices. The CXL specification defines three protocols: CXL.io for device discovery and configuration based on PCIe, CXL.cache for device-to-host cache coherency allowing devices to cache host memory with low latency, and CXL.mem for host-managed device memory that enables the host processor to access memory attached to CXL devices using standard load and store instructions. CXL technology is particularly relevant for modern data centers where memory capacity and bandwidth requirements are growing rapidly. Applications such as large language model inference, in-memory databases, and real-time analytics benefit significantly from the ability to expand memory pools beyond what is directly attached to a single CPU socket. CXL Type 3 devices, which are memory expansion devices, allow servers to access additional DRAM or persistent memory through the CXL interface, effectively creating a larger memory pool. This is especially valuable in scenarios where memory capacity is the bottleneck rather than compute power. The CXL 2.0 specification introduced memory pooling and switching capabilities, enabling multiple hosts to share a common pool of CXL-attached memory through a CXL switch. This allows for more efficient memory utilization across a cluster of servers, as memory can be dynamically allocated to the hosts that need it most. CXL 3.0 further extended these capabilities with support for fabric-attached memory, enabling even larger scale memory sharing across multiple levels of switches.\n\nSummarize the key benefits of CXL technology:"

check_health() {
    local name=$1 port=$2
    if ! curl -sf "http://localhost:${port}/health" > /dev/null 2>&1; then
        echo "[ERROR] ${name} (port ${port}) is not reachable. Is it running?"
        return 1
    fi
    return 0
}

send_query() {
    local port=$1
    JSON_PAYLOAD=$(python3 -c "
import json, os
print(json.dumps({
    'model': os.environ['MODEL'],
    'prompt': os.environ['PROMPT'],
    'max_tokens': 200,
    'temperature': 0.0,
    'ignore_eos': True,
}))
")
    curl -sS "http://localhost:${port}/v1/completions" \
        -H "Content-Type: application/json" \
        -d "$JSON_PAYLOAD" 2>&1 \
        | python3 -c "
import sys, json
output = []
for line in sys.stdin:
    line = line.strip()
    if not line or line == 'data: [DONE]':
        continue
    if line.startswith('data: '):
        line = line[6:]
    try:
        data = json.loads(line)
        output.append(data['choices'][0]['text'])
    except (json.JSONDecodeError, KeyError, IndexError):
        pass
print(''.join(output))
"
}

echo "=== P2P Query Test (2-query write-through) ==="
echo "Model: $MODEL"
echo ""

echo "=== Prompt (truncated) ==="
echo "${PROMPT:0:120}..."
echo ""

# Health checks
check_health "Instance 1" "$PORT1" || INST1_OK=false
check_health "Instance 2" "$PORT2" || INST2_OK=false
echo ""

# Step 1: Query inst1 — triggers prefill, starts write-through (GPU→Host async)
echo "--- Instance 1: Query 1 (prefill + write-through start) (port $PORT1) ---"
if [ "${INST1_OK:-}" != false ]; then
    send_query "$PORT1"
else
    echo "(skipped — server not reachable)"
fi
echo ""

sleep 3

# Step 2: Query inst1 again — writing_check() flushes write-through to L3
echo "--- Instance 1: Query 2 (flush write-through to L3) (port $PORT1) ---"
if [ "${INST1_OK:-}" != false ]; then
    send_query "$PORT1"
else
    echo "(skipped — server not reachable)"
fi
echo ""

sleep 3

# Step 3: Query inst2 — should hit L3 via batch_exists/batch_get
echo "--- Instance 2: Query (should hit L3 P2P cache) (port $PORT2) ---"
if [ "${INST2_OK:-}" != false ]; then
    send_query "$PORT2"
else
    echo "(skipped — server not reachable)"
fi
echo ""
