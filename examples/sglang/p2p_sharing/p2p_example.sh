#!/bin/bash
# P2P KV sharing example — manages MaruServer + two SGLang instances.
#
# Usage:
#   bash p2p_example.sh start     # Start all servers
#   bash p2p_example.sh stop      # Stop all servers
#   bash p2p_example.sh restart   # Restart all servers

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.p2p_pids"

PIDS=()

kill_tree() {
    local pid=$1 sig=${2:-TERM}
    for child in $(pgrep -P "$pid" 2>/dev/null); do
        kill_tree "$child" "$sig"
    done
    kill -"$sig" "$pid" 2>/dev/null
}

wait_for_server() {
    local port=$1
    local timeout_seconds=600
    local start_time=$(date +%s)
    local last_report=$start_time

    echo "[$(date +%T)] Waiting for server on port $port..."
    while true; do
        if [ "$port" = "$MARU_SERVER_PORT" ]; then
            if timeout 1 bash -c "echo >/dev/tcp/localhost/$port" 2>/dev/null; then
                echo "[$(date +%T)] Server on port $port is ready"
                return 0
            fi
        else
            if curl -s "localhost:${port}/health" > /dev/null 2>&1; then
                echo "[$(date +%T)] Server on port $port is ready"
                return 0
            fi
        fi

        local now=$(date +%s)
        if (( now - last_report >= 30 )); then
            echo "[$(date +%T)] Still waiting for port $port... ($((now - start_time))s elapsed)"
            last_report=$now
        fi
        if (( now - start_time >= timeout_seconds )); then
            echo "[$(date +%T)] Timeout waiting for server on port $port"
            return 1
        fi
        sleep 1
    done
}

start() {
    echo "=== P2P KV Sharing Example ==="
    echo "Model: $MODEL"
    echo "MaruServer port: $MARU_SERVER_PORT"
    echo "Instance1: port=$SGLANG_INST1_PORT, gpu=$INST1_GPU"
    echo "Instance2: port=$SGLANG_INST2_PORT, gpu=$INST2_GPU"
    echo ""

    # Resolve Maru extra config for command logging
    MARU_EXTRA_CONFIG_INST1=$(MARU_INSTANCE_ID=inst1 MARU_SERVER_PORT=$MARU_SERVER_PORT MARU_POOL_SIZE=$MARU_POOL_SIZE envsubst < "$SCRIPT_DIR/configs/maru-sglang-p2p.json")
    MARU_EXTRA_CONFIG_INST2=$(MARU_INSTANCE_ID=inst2 MARU_SERVER_PORT=$MARU_SERVER_PORT MARU_POOL_SIZE=$MARU_POOL_SIZE envsubst < "$SCRIPT_DIR/configs/maru-sglang-p2p.json")

    # Save resolved commands to file
    CMD_FILE="$SCRIPT_DIR/commands.log"
    cat > "$CMD_FILE" <<CMDS
# P2P KV Sharing — Resolved Commands ($(date '+%Y-%m-%d %H:%M:%S'))
# Run each in a separate terminal.

# 1. Maru Meta Server
python -m maru_server --port $MARU_SERVER_PORT

# 2. SGLang Instance 1 (GPU $INST1_GPU)
CUDA_VISIBLE_DEVICES=$INST1_GPU sglang serve \\
    --model-path $MODEL \\
    --port $SGLANG_INST1_PORT \\
    --mem-fraction-static $GPU_MEM_UTIL \\
    --log-level $SGLANG_LOG_LEVEL \\
    --enable-hierarchical-cache \\
    --hicache-ratio 2.0 \\
    --hicache-write-policy write_through \\
    --hicache-mem-layout page_first_direct \\
    --hicache-storage-backend dynamic \\
    --hicache-storage-backend-extra-config '$MARU_EXTRA_CONFIG_INST1' \\
    --hicache-storage-prefetch-policy wait_complete

# 3. SGLang Instance 2 (GPU $INST2_GPU)
CUDA_VISIBLE_DEVICES=$INST2_GPU sglang serve \\
    --model-path $MODEL \\
    --port $SGLANG_INST2_PORT \\
    --mem-fraction-static $GPU_MEM_UTIL \\
    --log-level $SGLANG_LOG_LEVEL \\
    --enable-hierarchical-cache \\
    --hicache-ratio 2.0 \\
    --hicache-write-policy write_through \\
    --hicache-mem-layout page_first_direct \\
    --hicache-storage-backend dynamic \\
    --hicache-storage-backend-extra-config '$MARU_EXTRA_CONFIG_INST2' \\
    --hicache-storage-prefetch-policy wait_complete
CMDS
    echo "[$(date +%T)] Saved resolved commands to $CMD_FILE"

    # 1. Maru Meta Server
    if timeout 1 bash -c "echo >/dev/tcp/localhost/$MARU_SERVER_PORT" 2>/dev/null; then
        echo "[$(date +%T)] MaruServer already running on port $MARU_SERVER_PORT"
    else
        echo "[$(date +%T)] Launching MaruServer on port $MARU_SERVER_PORT..."
        PYTHONUNBUFFERED=1 python -m maru_server --port "$MARU_SERVER_PORT" \
            > >(tee "$SCRIPT_DIR/maru_server.log") 2>&1 &
        PIDS+=($!)
        sleep 2
        if ! kill -0 "${PIDS[-1]}" 2>/dev/null; then
            echo "[$(date +%T)] ERROR: MaruServer died! Check maru_server.log"
            return 1
        fi
        wait_for_server "$MARU_SERVER_PORT"
    fi

    # 2. SGLang Instance 1 (start first so inst2 can pre-map its region)
    INSTANCE_ID=inst1 PORT=$SGLANG_INST1_PORT GPU_ID=$INST1_GPU \
        bash "$SCRIPT_DIR/sglang_launcher.sh" &
    PIDS+=($!)

    wait_for_server "$SGLANG_INST1_PORT"

    # 3. SGLang Instance 2 (starts after inst1 is ready — sees inst1's shared region)
    INSTANCE_ID=inst2 PORT=$SGLANG_INST2_PORT GPU_ID=$INST2_GPU \
        bash "$SCRIPT_DIR/sglang_launcher.sh" &
    PIDS+=($!)

    # Save PIDs
    printf '%s\n' "${PIDS[@]}" > "$PID_FILE"

    wait_for_server "$SGLANG_INST2_PORT"

    echo ""
    echo "==================================================="
    echo "All servers ready!"
    echo ""
    echo "Quick test:"
    echo "  bash run_simple_query.sh    # Warm inst1 twice and verify inst2 P2P hit"
    echo ""
    echo "Benchmark:"
    echo "  bash run_benchmark.sh       # Measure TTFT speedup"
    echo ""
    echo "Stop:"
    echo "  bash p2p_example.sh stop"
    echo "==================================================="

    # Keep running until Ctrl-C
    trap 'stop; exit 0' INT TERM
    while true; do sleep 1; done
}

stop() {
    echo "Stopping all servers..."

    # Stop PID-tracked processes
    if [ -f "$PID_FILE" ]; then
        while read -r pid; do
            if kill -0 "$pid" 2>/dev/null; then
                kill_tree "$pid" TERM
                echo "  Stopped PID $pid"
            fi
        done < "$PID_FILE"

        sleep 1

        # Force kill survivors
        while read -r pid; do
            if kill -0 "$pid" 2>/dev/null; then
                kill_tree "$pid" 9
                echo "  Force-killed PID $pid"
            fi
        done < "$PID_FILE"

        rm -f "$PID_FILE"
    fi

    # Also kill any MaruServer on our port (handles pre-existing servers)
    local maru_pid
    maru_pid=$(lsof -ti "tcp:$MARU_SERVER_PORT" 2>/dev/null || true)
    if [ -n "$maru_pid" ]; then
        kill $maru_pid 2>/dev/null && echo "  Stopped MaruServer (PID $maru_pid) on port $MARU_SERVER_PORT"
        sleep 1
        kill -9 $maru_pid 2>/dev/null || true
    fi

    echo "All processes stopped."
}

case "${1:-}" in
    start)   start ;;
    stop)    stop ;;
    restart) stop; sleep 2; start ;;
    *)
        echo "Usage: $0 [start|stop|restart]"
        echo ""
        echo "  start    Launch MaruServer + 2 SGLang instances"
        echo "  stop     Stop all managed processes"
        echo "  restart  Stop then start"
        exit 1
        ;;
esac
