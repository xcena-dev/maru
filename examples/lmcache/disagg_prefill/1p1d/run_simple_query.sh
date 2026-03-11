#!/bin/bash
# Simple single query test for Maru disaggregated prefill

cd "$(dirname "${BASH_SOURCE[0]}")"
[ -f "env.sh" ] && source env.sh

PROMPT="Explain CXL memory technology in detail. CXL stands for Compute Express Link, which is a high-speed CPU-to-device and CPU-to-memory interconnect designed to accelerate next-generation data center performance. It enables memory expansion and sharing between host processors and accelerators. CXL builds on the PCI Express (PCIe) physical and electrical interface, adding a set of protocols that allow coherent memory access between CPUs and attached devices. The CXL specification defines three protocols: CXL.io for device discovery and configuration based on PCIe, CXL.cache for device-to-host cache coherency allowing devices to cache host memory with low latency, and CXL.mem for host-managed device memory that enables the host processor to access memory attached to CXL devices using standard load and store instructions. CXL technology is particularly relevant for modern data centers where memory capacity and bandwidth requirements are growing rapidly. Applications such as large language model inference, in-memory databases, and real-time analytics benefit significantly from the ability to expand memory pools beyond what is directly attached to a single CPU socket. CXL Type 3 devices, which are memory expansion devices, allow servers to access additional DRAM or persistent memory through the CXL interface, effectively creating a larger memory pool. This is especially valuable in scenarios where memory capacity is the bottleneck rather than compute power. The CXL 2.0 specification introduced memory pooling and switching capabilities, enabling multiple hosts to share a common pool of CXL-attached memory through a CXL switch. This allows for more efficient memory utilization across a cluster of servers, as memory can be dynamically allocated to the hosts that need it most. CXL 3.0 further extended these capabilities with support for fabric-attached memory, enabling even larger scale memory sharing across multiple levels of switches."
PORT=${LMCACHE_PROXY_EXTERNAL_PORT:-13101}

echo "=== Query (256+ tokens) ==="
echo "$PROMPT"
echo ""
echo "=== Answer ==="

curl -sS "http://localhost:${PORT}/v1/completions" \
  -H "Content-Type: application/json" \
  -d "{\"model\": \"Qwen/Qwen2.5-0.5B\", \"prompt\": \"$PROMPT\", \"max_tokens\": 200, \"temperature\": 0.7}" 2>&1 \
  | grep "^data:" | grep -v "\[DONE\]" \
  | sed 's/^data: //' \
  | python3 -c "import sys,json; print(''.join(json.loads(line)['choices'][0]['text'] for line in sys.stdin if line.strip()))"
