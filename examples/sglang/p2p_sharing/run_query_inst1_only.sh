#!/bin/bash
# inst1에만 1회 쿼리 — write-through가 idle 상태에서 L3까지 flush되는지 확인용

cd "$(dirname "${BASH_SOURCE[0]}")"
[ -f "env.sh" ] && source env.sh

PORT1="${SGLANG_INST1_PORT}"
export MODEL="${MODEL:-Qwen/Qwen2.5-0.5B}"
export PROMPT

PROMPT="Explain CXL memory technology in detail. CXL stands for Compute Express Link, which is a high-speed CPU-to-device and CPU-to-memory interconnect designed to accelerate next-generation data center performance. It enables memory expansion and sharing between host processors and accelerators. CXL builds on the PCI Express (PCIe) physical and electrical interface, adding a set of protocols that allow coherent memory access between CPUs and attached devices. The CXL specification defines three protocols: CXL.io for device discovery and configuration based on PCIe, CXL.cache for device-to-host cache coherency allowing devices to cache host memory with low latency, and CXL.mem for host-managed device memory that enables the host processor to access memory attached to CXL devices using standard load and store instructions. CXL technology is particularly relevant for modern data centers where memory capacity and bandwidth requirements are growing rapidly. Applications such as large language model inference, in-memory databases, and real-time analytics benefit significantly from the ability to expand memory pools beyond what is directly attached to a single CPU socket. CXL Type 3 devices, which are memory expansion devices, allow servers to access additional DRAM or persistent memory through the CXL interface, effectively creating a larger memory pool. This is especially valuable in scenarios where memory capacity is the bottleneck rather than compute power. The CXL 2.0 specification introduced memory pooling and switching capabilities, enabling multiple hosts to share a common pool of CXL-attached memory through a CXL switch. This allows for more efficient memory utilization across a cluster of servers, as memory can be dynamically allocated to the hosts that need it most. CXL 3.0 further extended these capabilities with support for fabric-attached memory, enabling even larger scale memory sharing across multiple levels of switches.\n\nSummarize the key benefits of CXL technology:"

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

echo "=== inst1 only — 1 query ==="
echo "Port: $PORT1"
echo ""

if ! curl -sf "http://localhost:${PORT1}/health" > /dev/null 2>&1; then
    echo "[ERROR] inst1 (port ${PORT1}) not reachable"
    exit 1
fi

echo "--- Sending query ---"
send_query "$PORT1"
echo ""

echo "--- Waiting 5s for write-through flush ---"
sleep 5

echo "--- Check inst1.log for [WT-DEBUG] writing_check after query ---"
echo "  tail -20 inst1.log"
