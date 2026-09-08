#!/bin/bash
# Launch one Dynamo component for the Maru single-node example
#
# Usage:
#   ./dynamo_launcher.sh frontend
#   ./dynamo_launcher.sh worker <w0|w1> [model]
#
# The frontend runs in --router-mode direct so the test script can pin a
# request to a specific worker via {"nvext": {"backend_instance_id": N}} —
# that determinism is what lets the example demonstrate cross-instance KV
# sharing (store on one worker, retrieve on the other).
#
# Workers expose NO OpenAI port: requests only enter through the frontend.
# Each worker's DYN_SYSTEM_PORT status server serves /health and /metrics.

set -euo pipefail

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 frontend | worker <w0|w1> [model]"
    exit 1
fi

ROLE="$1"

if [[ $ROLE == "frontend" ]]; then
    echo "=== Dynamo frontend ==="
    echo "  HTTP Port:   $DYN_HTTP_PORT"
    echo "  Discovery:   $DISCOVERY_BACKEND"
    echo "  Router mode: direct"
    echo "======================="
    exec python3 -m dynamo.frontend \
        --discovery-backend "$DISCOVERY_BACKEND" \
        --router-mode direct \
        --http-port "$DYN_HTTP_PORT"
fi

if [[ $ROLE != "worker" || $# -lt 2 ]]; then
    echo "Usage: $0 frontend | worker <w0|w1> [model]"
    exit 1
fi

WORKER="$2"
MODEL="${3:-${MODEL:-Qwen/Qwen2.5-0.5B}}"

if [[ $WORKER == "w0" ]]; then
    DEVICE=$W0_GPU
    SYSTEM_PORT=$DYN_W0_SYSTEM_PORT
elif [[ $WORKER == "w1" ]]; then
    DEVICE=$W1_GPU
    SYSTEM_PORT=$DYN_W1_SYSTEM_PORT
else
    echo "Invalid worker: $WORKER (expected w0 or w1)"
    exit 1
fi

echo "=== Dynamo worker ($WORKER) ==="
echo "  Model:        $MODEL"
echo "  GPU Device:   $DEVICE"
echo "  System Port:  $SYSTEM_PORT (health only — no OpenAI port)"
echo "  Maru Server:  $MARU_SERVER_URL"
echo "  Pool Size:    $MARU_POOL_SIZE"
echo "  Chunk Tokens: $MARU_KV_CHUNK_TOKENS"
echo "==============================="

# Same MaruKVConnector config as the plain vLLM example, plus a per-worker
# maru_instance_id so the two workers register distinct Maru clients.
KV_CONFIG=$(cat <<EOJSON
{
    "kv_connector": "MaruKVConnector",
    "kv_connector_module_path": "maru_vllm",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
        "maru_server_url": "${MARU_SERVER_URL}",
        "maru_pool_size": "${MARU_POOL_SIZE}",
        "maru_kv_chunk_tokens": ${MARU_KV_CHUNK_TOKENS},
        "maru_instance_id": "${WORKER}"
    }
}
EOJSON
)

# --no-enable-prefix-caching keeps Maru as the only cache source, so the
# latency contrast between the two workers is attributable to Maru alone.
DYN_SYSTEM_PORT="$SYSTEM_PORT" CUDA_VISIBLE_DEVICES="$DEVICE" \
exec python3 -m dynamo.vllm \
    --model "$MODEL" \
    --discovery-backend "$DISCOVERY_BACKEND" \
    --gpu-memory-utilization "$GPU_MEM_UTIL" \
    --no-enable-prefix-caching \
    --kv-transfer-config "$KV_CONFIG"
