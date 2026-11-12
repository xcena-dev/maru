#!/bin/bash
# Start maru-server for remote CXL workers.
#
# Runs in the foreground, on the metadata node (it may be a memory node). It
# allocates the workers' regions from the pool's resource manager, as for
# local CXL, and tells the workers which remote CXL agents serve the pool. Once
# a worker leaves, maru-server keeps its regions that still have keys and
# talks to their remote CXL agent itself, so it needs the remote CXL agent key.
#
# Usage:
#   ./maru_server.sh
#
# Example (two memory nodes share the pool on /dev/dax1.0):
#   MARU_SERVER_BIND_HOST=0.0.0.0 MARU_DAX_PATH=/dev/dax1.0 \
#   MARU_REMOTE_CXL_AGENTS=10.0.0.2:11505,10.0.0.3:11505 ./maru_server.sh

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

ARGS=(
    --host "$MARU_SERVER_BIND_HOST"
    --port "$MARU_SERVER_PORT"
    --rm-address "$MARU_RM_ADDRESS"
    --remote-cxl-agent "$MARU_REMOTE_CXL_AGENTS"
    --remote-cxl-agent-key-file "$MARU_REMOTE_CXL_AGENT_KEY_FILE"
)
if [[ -n "$MARU_DAX_PATH" ]]; then
    ARGS+=(--dax-path "$MARU_DAX_PATH")
fi

echo "=== maru-server (metadata node) ==="
echo "  Listening on:     $MARU_SERVER_BIND_HOST:$MARU_SERVER_PORT"
echo "  Resource manager: $MARU_RM_ADDRESS"
echo "  DAX device:       ${MARU_DAX_PATH:-(any)}"
echo "  Remote CXL agents:   $MARU_REMOTE_CXL_AGENTS"
echo "==================================="
echo "On each worker node: export MARU_SERVER_URL=tcp://<this node's address>:${MARU_SERVER_PORT}"
echo ""

exec maru-server "${ARGS[@]}"
