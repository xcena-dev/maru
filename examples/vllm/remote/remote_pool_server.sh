#!/bin/bash
# Start MaruServer on the pool node with its remote endpoint enabled.
#
# Runs in the foreground. The Maru resource manager must already be running on
# this node, as for a local CXL deployment (./install.sh installs it; start it
# with: sudo systemctl start maru-resource-manager).
#
# Usage:
#   ./remote_pool_server.sh
#
# Example (pool node with its pool on /dev/dax1.0 and NIC mlx5_1):
#   MARU_DAX_PATH=/dev/dax1.0 MARU_POOL_UCX_DEVICE=mlx5_1:1 ./remote_pool_server.sh

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

ARGS=(
    --host 127.0.0.1
    --port "$MARU_SERVER_PORT"
    --remote-bind "tcp://${MARU_REMOTE_BIND_HOST}:${MARU_REMOTE_PORT}"
    --remote-pool-size "$MARU_REMOTE_POOL_SIZE"
    --remote-page-bytes "$MARU_REMOTE_PAGE_BYTES"
    --remote-capacity "$MARU_REMOTE_CAPACITY"
)
if [[ -n "$MARU_DAX_PATH" ]]; then
    ARGS+=(--dax-path "$MARU_DAX_PATH")
fi
if [[ -n "$MARU_POOL_UCX_DEVICE" ]]; then
    ARGS+=(--remote-ucx-device "$MARU_POOL_UCX_DEVICE")
fi

echo "=== Maru remote pool node ==="
echo "  Local RPC:       127.0.0.1:$MARU_SERVER_PORT"
echo "  Remote endpoint: tcp://${MARU_REMOTE_BIND_HOST}:$MARU_REMOTE_PORT"
echo "  DAX device:      ${MARU_DAX_PATH:-(any)}"
echo "  UCX device:      ${MARU_POOL_UCX_DEVICE:-(UCX default)}"
echo "  Pool size:       $MARU_REMOTE_POOL_SIZE (capacity $MARU_REMOTE_CAPACITY)"
echo "  Page size:       $MARU_REMOTE_PAGE_BYTES"
echo "============================="
echo "On each worker node: export MARU_REMOTE_URL=tcp://<this node's address>:${MARU_REMOTE_PORT}"
echo ""

exec maru-server "${ARGS[@]}"
