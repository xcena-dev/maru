#!/bin/bash
# P2P KV sharing verification.
#
# Step 1: Query inst1 (compute KV, hit_count=0)
# Step 2: Query inst1 again (radix hit → hit_count=1 → write_through to L3)
# Step 3: Wait for async L3 flush
# Step 4: Query inst2 (should get P2P hit from Maru L3)
#
# Why twice?  HiCache write_through requires hit_count >= write_through_threshold(1).

cd "$(dirname "${BASH_SOURCE[0]}")"
[ -f "env.sh" ] && source env.sh

PORT1="${SGLANG_INST1_PORT}"
PORT2="${SGLANG_INST2_PORT}"
FLUSH_WAIT=${FLUSH_WAIT:-15}
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
    'max_tokens': 100,
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

echo "=== P2P KV Sharing Test ==="
echo "Model: $MODEL"
echo "Instance 1: port=$PORT1, Instance 2: port=$PORT2"
echo ""

echo "=== Prompt (truncated) ==="
echo "${PROMPT:0:120}..."
echo ""

# Health checks
INST1_OK=true
INST2_OK=true
check_health "Instance 1" "$PORT1" || INST1_OK=false
check_health "Instance 2" "$PORT2" || INST2_OK=false
echo ""

# --- Step 1 ---
echo "--- Step 1: inst1 — compute KV (hit_count=0) ---"
if [ "$INST1_OK" = true ]; then
    echo "=== Response ==="
    send_query "$PORT1"
else
    echo "(skipped)"
fi
echo ""

# --- Step 2 ---
echo "--- Step 2: inst1 — cache hit → write_through to L3 ---"
if [ "$INST1_OK" = true ]; then
    echo "=== Response ==="
    send_query "$PORT1"
else
    echo "(skipped)"
fi
echo ""

# --- Step 3 ---
if [ "$INST1_OK" = true ]; then
    echo "--- Step 3: Waiting ${FLUSH_WAIT}s for async L3 write... ---"
    for i in $(seq 1 "$FLUSH_WAIT"); do
        printf "\r  %d/%ds" "$i" "$FLUSH_WAIT"
        sleep 1
    done
    printf "\r  Done.          \n"
    echo ""
fi

# --- Step 4 ---
echo "--- Step 4: inst2 — retrieve KV from Maru (P2P) ---"
if [ "$INST2_OK" = true ]; then
    echo "=== Response ==="
    send_query "$PORT2"
else
    echo "(skipped)"
fi
echo ""

echo "=== Done ==="
echo ""
echo "Verify P2P in logs:"
echo "  grep 'batch_set' inst1.log     # inst1 wrote KV to L3"
echo "  grep 'batch_exists' inst2.log  # inst2 checked L3 for keys"
echo "  grep 'batch_get' inst2.log     # inst2 fetched KV from L3 (P2P hit)"
