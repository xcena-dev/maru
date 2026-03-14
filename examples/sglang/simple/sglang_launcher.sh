#!/bin/bash
# Unified SGLang launcher — switch between baseline / l2 / maru modes.
#
# Usage:
#   bash sglang_launcher.sh [baseline|l2|maru] [--model MODEL]
#
# Modes:
#   baseline  — No HiCache. KV cache only in GPU VRAM (L1).
#   l2        — HiCache L2. KV offload to Host DRAM.
#   maru      — HiCache L3 + Maru. KV shared via CXL memory.

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PIDS=()

# --- Argument parsing ---
usage() {
    echo "Usage: $0 [baseline|l2|maru] [OPTIONS]"
    echo ""
    echo "Modes:"
    echo "  baseline   No HiCache (default)"
    echo "  l2         HiCache L2 (Host DRAM offload)"
    echo "  maru       HiCache L3 + Maru (CXL shared memory)"
    echo ""
    echo "Options:"
    echo "  --model MODEL        HuggingFace model name (default: $MODEL)"
    echo "  --log-level LEVEL    Maru log level: DEBUG, INFO, WARNING, ERROR (default: INFO)"
    echo "  --sglang-log-level   SGLang log level (default: info, caution: debug may hang)"
    echo "  -h, --help           Show this help message"
    exit 0
}

# First arg: mode or --help
case "${1:-}" in
    -h|--help)          usage ;;
    baseline|l2|maru)   MODE="$1" ;;
    "")                 MODE="maru" ;;
    *)                  echo "Unknown mode: $1"; usage ;;
esac
shift 2>/dev/null || true  # consume mode arg
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)     usage ;;
        --model)       MODEL="$2"; shift 2 ;;
        --log-level)        export MARU_LOG_LEVEL="$2"; shift 2 ;;
        --sglang-log-level) SGLANG_LOG_LEVEL="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# --- Utilities ---
kill_tree() {
    local pid=$1 sig=${2:-TERM}
    for child in $(pgrep -P "$pid" 2>/dev/null); do
        kill_tree "$child" "$sig"
    done
    kill -"$sig" "$pid" 2>/dev/null
}

cleanup() {
    echo "Stopping everything..."
    trap - INT TERM EXIT
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill_tree "$pid" TERM
        fi
    done
    sleep 1
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill_tree "$pid" 9
        fi
    done
    echo "All processes stopped."
    exit 0
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

# --- Build SGLang args per mode (populates SGLANG_ARGS array) ---
build_sglang_args() {
    SGLANG_ARGS=(
        --model-path "$MODEL"
        --port "$SGLANG_PORT"
        --mem-fraction-static "$GPU_MEM_UTIL"
        --log-level "$SGLANG_LOG_LEVEL"
    )

    case "$MODE" in
        baseline)
            # No HiCache — just basic SGLang
            ;;
        l2)
            SGLANG_ARGS+=(
                --enable-hierarchical-cache
                --hicache-ratio 2.0
            )
            ;;
        maru)
            MARU_EXTRA_CONFIG="{\"backend_name\":\"maru\",\"module_path\":\"maru_sglang.backend\",\"class_name\":\"MaruHiCacheStorage\",\"maru_server_url\":\"tcp://localhost:${MARU_SERVER_PORT}\",\"maru_pool_size\":\"${MARU_POOL_SIZE:-4G}\"}"
            SGLANG_ARGS+=(
                --enable-hierarchical-cache
                --hicache-ratio 2.0
                --hicache-write-policy write_through
                --hicache-mem-layout page_first_direct
                --hicache-storage-backend dynamic
                --hicache-storage-backend-extra-config "$MARU_EXTRA_CONFIG"
            )
            ;;
        *)
            echo "Unknown mode: $MODE (expected baseline, l2, or maru)"
            exit 1
            ;;
    esac
}

# --- Main ---
main() {
    trap cleanup INT TERM EXIT

    echo "=== SGLang Server (mode: $MODE) ==="
    echo "Model: $MODEL, Port: $SGLANG_PORT, GPU_MEM_UTIL: $GPU_MEM_UTIL"

    # Maru mode: start Maru Meta Server first
    if [[ "$MODE" == "maru" ]]; then
        if timeout 1 bash -c "echo >/dev/tcp/localhost/$MARU_SERVER_PORT" 2>/dev/null; then
            echo "[$(date +%T)] MaruServer already running on port $MARU_SERVER_PORT"
        else
            echo "[$(date +%T)] Launching MaruServer on port $MARU_SERVER_PORT..."
            PYTHONUNBUFFERED=1 python -m maru_server --port "$MARU_SERVER_PORT" \
                > >(tee maru_server.log) 2>&1 &
            PIDS+=($!)
            sleep 2
            if ! kill -0 "${PIDS[-1]}" 2>/dev/null; then
                echo "[$(date +%T)] ERROR: MaruServer died! Check maru_server.log"
                return 1
            fi
            wait_for_server "$MARU_SERVER_PORT"
        fi
    fi

    # Launch SGLang
    LOG_SGLANG="${LOG_SGLANG:-sglang_server.log}"
    build_sglang_args

    # Save resolved commands to file
    CMD_FILE="$SCRIPT_DIR/commands.log"
    {
        echo "# SGLang Single Instance — Resolved Commands ($(date '+%Y-%m-%d %H:%M:%S'))"
        echo "# Mode: $MODE"
        echo ""
        if [[ "$MODE" == "maru" ]]; then
            echo "# 1. Maru Meta Server"
            echo "python -m maru_server --port $MARU_SERVER_PORT"
            echo ""
            echo "# 2. SGLang Server"
        else
            echo "# 1. SGLang Server"
        fi
        local i=0
        local cmd="sglang serve"
        while (( i < ${#SGLANG_ARGS[@]} )); do
            if [[ "${SGLANG_ARGS[$i]}" == --* ]] && (( i + 1 < ${#SGLANG_ARGS[@]} )) && [[ "${SGLANG_ARGS[$((i+1))]}" != --* ]]; then
                cmd+=" \\\\\n    ${SGLANG_ARGS[$i]} ${SGLANG_ARGS[$((i+1))]}"
                (( i += 2 ))
            else
                cmd+=" \\\\\n    ${SGLANG_ARGS[$i]}"
                (( i += 1 ))
            fi
        done
        echo -e "$cmd"
        echo ""
    } > "$CMD_FILE"
    echo "[$(date +%T)] Saved resolved commands to $CMD_FILE"

    sglang serve "${SGLANG_ARGS[@]}" \
        > >(tee "$LOG_SGLANG") 2>&1 &
    PIDS+=($!)

    wait_for_server "$SGLANG_PORT"

    echo "==================================================="
    echo "Server is up: http://localhost:$SGLANG_PORT"
    echo "Mode: $MODE"
    echo "Log: $LOG_SGLANG"
    echo ""
    echo "Quick test:"
    echo "  bash run_simple_query.sh"
    echo ""
    echo "Press Ctrl-C to stop."
    echo "==================================================="

    while true; do sleep 1; done
}

main
