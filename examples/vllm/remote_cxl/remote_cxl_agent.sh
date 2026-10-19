#!/bin/bash
# Start the remote CXL agent on a memory node.
#
# Runs in the foreground. One remote CXL agent runs on every memory node of the
# pool, for every app: it maps the regions Handlers ask for and registers them
# with this node's NIC, so workers on other nodes reach them over RDMA. The
# pool's resource manager must already be running (sudo systemctl start
# maru-resource-manager on the node that runs it).
#
# Usage:
#   ./remote_cxl_agent.sh
#
# Example (memory node 10.0.0.2 with NIC mlx5_1):
#   MARU_MEMORY_NODE_HOST=10.0.0.2 MARU_AGENT_UCX_DEVICE=mlx5_1:1 ./remote_cxl_agent.sh

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

# One remote CXL agent per node: the service's lock lives under /run/maru, which
# this script cannot use, so check for the service directly.
if command -v systemctl >/dev/null 2>&1 \
    && systemctl is-active --quiet maru-remote-cxl-agent 2>/dev/null; then
    echo "maru-remote-cxl-agent.service already serves this node;" \
        "stop it first (sudo systemctl stop maru-remote-cxl-agent)." >&2
    exit 1
fi

# Workers and maru-server prove this key on every connection; copy it to
# them (the same file serves every memory node of the pool).
if [[ ! -e "$MARU_REMOTE_CXL_AGENT_KEY_FILE" ]]; then
    mkdir -p "$(dirname "$MARU_REMOTE_CXL_AGENT_KEY_FILE")"
    python -c "import sys; from maru_remote.region_map import write_key; write_key(sys.argv[1])" \
        "$MARU_REMOTE_CXL_AGENT_KEY_FILE"
    echo "Created the remote CXL agent key $MARU_REMOTE_CXL_AGENT_KEY_FILE"
fi

ARGS=(
    --rm-address "$MARU_RM_ADDRESS"
    --host "$MARU_MEMORY_NODE_HOST"
    --port "$MARU_AGENT_PORT"
    --lock-file "$MARU_AGENT_LOCK_FILE"
    --key-file "$MARU_REMOTE_CXL_AGENT_KEY_FILE"
)
if [[ -n "$MARU_AGENT_UCX_DEVICE" ]]; then
    ARGS+=(--ucx-device "$MARU_AGENT_UCX_DEVICE")
    # Keep UCX on that NIC only.
    export UCX_NET_DEVICES=${UCX_NET_DEVICES:-$MARU_AGENT_UCX_DEVICE}
fi

echo "=== Maru remote CXL agent (memory node) ==="
echo "  Listening on:     $MARU_MEMORY_NODE_HOST:$MARU_AGENT_PORT"
echo "  Resource manager: $MARU_RM_ADDRESS"
echo "  UCX device:       ${MARU_AGENT_UCX_DEVICE:-(UCX default)}"
echo "  Key:              $MARU_REMOTE_CXL_AGENT_KEY_FILE"
echo "========================================"
echo "Give maru-server this remote CXL agent: --remote-cxl-agent ${MARU_MEMORY_NODE_HOST}:${MARU_AGENT_PORT}"
echo ""

exec maru-remote-cxl-agent "${ARGS[@]}"
