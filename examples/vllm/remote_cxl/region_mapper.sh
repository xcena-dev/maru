#!/bin/bash
# Start the region mapper on a memory node.
#
# Runs in the foreground. One region mapper runs on every memory node of the
# pool, for every app: it maps the regions Handlers ask for and registers them
# with this node's NIC, so workers on other nodes reach them over RDMA. The
# pool's resource manager must already be running (sudo systemctl start
# maru-resource-manager on the node that runs it).
#
# Usage:
#   ./region_mapper.sh
#
# Example (memory node 10.0.0.2 with NIC mlx5_1):
#   MARU_MEMORY_NODE_HOST=10.0.0.2 MARU_MAPPER_UCX_DEVICE=mlx5_1:1 ./region_mapper.sh

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

# One region mapper per node: the service's lock lives under /run/maru, which
# this script cannot use, so check for the service directly.
if command -v systemctl >/dev/null 2>&1 \
    && systemctl is-active --quiet maru-region-mapper 2>/dev/null; then
    echo "maru-region-mapper.service already serves this node;" \
        "stop it first (sudo systemctl stop maru-region-mapper)." >&2
    exit 1
fi

ARGS=(
    --rm-address "$MARU_RM_ADDRESS"
    --host "$MARU_MEMORY_NODE_HOST"
    --port "$MARU_MAPPER_PORT"
    --lock-file "$MARU_MAPPER_LOCK_FILE"
)
if [[ -n "$MARU_MAPPER_UCX_DEVICE" ]]; then
    ARGS+=(--ucx-device "$MARU_MAPPER_UCX_DEVICE")
    # Keep UCX on that NIC only.
    export UCX_NET_DEVICES=${UCX_NET_DEVICES:-$MARU_MAPPER_UCX_DEVICE}
fi

echo "=== Maru region mapper (memory node) ==="
echo "  Listening on:     $MARU_MEMORY_NODE_HOST:$MARU_MAPPER_PORT"
echo "  Resource manager: $MARU_RM_ADDRESS"
echo "  UCX device:       ${MARU_MAPPER_UCX_DEVICE:-(UCX default)}"
echo "========================================"
echo "Give maru-server this region mapper: --region-mapper ${MARU_MEMORY_NODE_HOST}:${MARU_MAPPER_PORT}"
echo ""

exec maru-region-mapper "${ARGS[@]}"
