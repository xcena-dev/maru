#!/bin/bash
# Single-instance L3 write verification.
# Sends the same prompt to inst1 TWICE to trigger write_through to L3.
#
# Expected flow:
#   1st query: compute KV → L1 (hit_count=0, no L3 write)
#   2nd query: radix cache hit → hit_count=1 → write_through → L1→L2→L3
#
# Success = "batch_set_v1" appears in sglang_server.log after step 2.

cd "$(dirname "${BASH_SOURCE[0]}")"
[ -f "env.sh" ] && source env.sh

PORT1="${SGLANG_PORT}"
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

echo "=== Single Instance L3 Write Test ==="
echo "Model: $MODEL"
echo "Port: $PORT1"
echo ""

echo "=== Prompt (truncated) ==="
echo "${PROMPT:0:120}..."
echo ""

check_health "SGLang server" "$PORT1" || { echo "Aborting."; exit 1; }

# Snapshot inst1.log line count before test
LOG_FILE="sglang_server.log"
LINES_BEFORE=0
if [ -f "$LOG_FILE" ]; then
    LINES_BEFORE=$(wc -l < "$LOG_FILE")
fi
echo ""

# --- Step 1: First query (compute KV, hit_count=0) ---
echo "--- Step 1: Query server — compute KV (hit_count=0, no L3 write expected) ---"
echo "=== Response ==="
send_query "$PORT1"
echo ""

sleep 2

# --- Step 2: Same query again (radix hit → write_through to L3) ---
echo "--- Step 2: Query server again — cache hit → write_through to L3 ---"
echo "=== Response ==="
send_query "$PORT1"
echo ""

# --- Step 3: Wait for async flush ---
echo "--- Step 3: Waiting ${FLUSH_WAIT}s for async L3 write... ---"
for i in $(seq 1 "$FLUSH_WAIT"); do
    printf "\r  %d/%ds" "$i" "$FLUSH_WAIT"
    sleep 1
done
printf "\r  Done.          \n"
echo ""

# --- Step 4: Check logs ---
echo "=== L3 Write Verification ==="
if [ -f "$LOG_FILE" ]; then
    NEW_LOGS=$(tail -n +"$((LINES_BEFORE + 1))" "$LOG_FILE")

    echo "-- batch_set_v1 (L3 write) --"
    SETS=$(echo "$NEW_LOGS" | grep -c "batch_set_v1" || true)
    echo "$NEW_LOGS" | grep "batch_set_v1" || echo "  (none found)"
    echo ""

    echo "-- write_backup / write_through --"
    echo "$NEW_LOGS" | grep -iE "write_backup|write_through|backup_write" || echo "  (none found)"
    echo ""

    if [ "$SETS" -gt 0 ]; then
        echo "SUCCESS: L3 write detected ($SETS batch_set_v1 calls)"
    else
        echo "FAIL: No L3 write detected. Check:"
        echo "  1. write_through_threshold — is it > 1?"
        echo "  2. L2 eviction — did data actually move L1→L2?"
        echo "  3. page_first_layout bug — cache_controller.py:616"
        echo "  4. Try MARU_LOG_LEVEL=DEBUG for more detail"
    fi
else
    echo "WARNING: $LOG_FILE not found. Cannot verify."
fi
