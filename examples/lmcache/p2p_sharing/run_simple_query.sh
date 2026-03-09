#!/bin/bash
# Simple single query test for Maru P2P KV cache sharing
# Flow: inst1 stores KV cache → inst2 retrieves via Maru P2P

cd "$(dirname "${BASH_SOURCE[0]}")"
[ -f "env.sh" ] && source env.sh

MODEL="Qwen/Qwen2.5-0.5B"
PORT1="${LMCACHE_INST1_PORT:-12010}"
PORT2="${LMCACHE_INST2_PORT:-12011}"

PROMPT="Explain CXL memory technology in detail. CXL stands for Compute Express Link, which is a high-speed CPU-to-device and CPU-to-memory interconnect designed to accelerate next-generation data center performance. It enables memory expansion and sharing between host processors and accelerators. CXL builds on the PCI Express (PCIe) physical and electrical interface, adding a set of protocols that allow coherent memory access between CPUs and attached devices. The CXL specification defines three protocols: CXL.io for device discovery and configuration based on PCIe, CXL.cache for device-to-host cache coherency allowing devices to cache host memory with low latency, and CXL.mem for host-managed device memory that enables the host processor to access memory attached to CXL devices using standard load and store instructions. CXL technology is particularly relevant for modern data centers where memory capacity and bandwidth requirements are growing rapidly. Applications such as large language model inference, in-memory databases, and real-time analytics benefit significantly from the ability to expand memory pools beyond what is directly attached to a single CPU socket. CXL Type 3 devices, which are memory expansion devices, allow servers to access additional DRAM or persistent memory through the CXL interface, effectively creating a larger memory pool. This is especially valuable in scenarios where memory capacity is the bottleneck rather than compute power. The CXL 2.0 specification introduced memory pooling and switching capabilities, enabling multiple hosts to share a common pool of CXL-attached memory through a CXL switch. This allows for more efficient memory utilization across a cluster of servers, as memory can be dynamically allocated to the hosts that need it most. CXL 3.0 further extended these capabilities with support for fabric-attached memory, enabling even larger scale memory sharing across multiple levels of switches.\n\nSummarize the key benefits of CXL technology:"

send_query() {
    local label="$1" port="$2"
    echo "=== ${label} (port ${port}) ==="
    curl -sS "http://localhost:${port}/v1/completions" \
      -H "Content-Type: application/json" \
      -d "{\"model\": \"${MODEL}\", \"prompt\": \"$PROMPT\", \"max_tokens\": 200, \"temperature\": 0.0, \"ignore_eos\": true}" 2>&1 \
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
    echo ""
}

echo "=== Prompt ==="
echo "$PROMPT"
echo ""

# Step 1: Query inst1 to store KV cache
send_query "inst1 - Store KV" "$PORT1"

sleep 1

# Step 2: Query inst2 to retrieve via Maru P2P
send_query "inst2 - Maru P2P Retrieve" "$PORT2"
