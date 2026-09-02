#!/bin/bash
# Single-instance example for the Maru direct vLLM connector (MaruKVConnector,
# no LMCache). Launches maru-server + one vLLM instance (prefix cache OFF, so
# Maru is the only KV cache source), runs the cold->warm benchmark, and exits.

set -euo pipefail

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

echo "Warning: Maru direct vLLM connector (MaruKVConnector) is experimental and subject to change."

# Load common environment variables
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

PIDS=()

# Switch to the directory of the current script
cd "$(dirname "${BASH_SOURCE[0]}")"

ensure_python_library_installed() {
    echo "Checking if $1 is installed..."
    python3 -c "import $1" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "$1 is not installed. Please install it via uv pip install $1."
        exit 1
    else
        echo "$1 is installed."
    fi
}

kill_tree() {
    local pid=$1 sig=${2:-TERM}
    for child in $(pgrep -P "$pid" 2>/dev/null); do
        kill_tree "$child" "$sig"
    done
    kill -"$sig" "$pid" 2>/dev/null
}

cleanup() {
    local status=$?
    echo "Stopping everything..."
    trap - INT TERM USR1 EXIT

    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Killing process tree of $pid"
            kill_tree "$pid" TERM
        fi
    done

    sleep 2

    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Force killing process tree of $pid"
            kill_tree "$pid" 9
        fi
    done

    echo "All processes stopped."
    exit "$status"
}

wait_for_server() {
  local port=$1
  local timeout_seconds=1200
  local start_time=$(date +%s)
  local last_report=$start_time

  echo "[$(date +%T)] Waiting for server on port $port (timeout: ${timeout_seconds}s)..."

  while true; do
    # MaruServer (ZeroMQ)
    if [ "$port" = "$MARU_SERVER_PORT" ]; then
      if timeout 1 bash -c "echo >/dev/tcp/localhost/$port" 2>/dev/null; then
        echo "[$(date +%T)] MaruServer is ready on port $port"
        return 0
      fi
    # vLLM server
    else
      if curl -s "localhost:${port}/v1/completions" > /dev/null; then
        echo "[$(date +%T)] Server on port $port is ready"
        return 0
      fi
    fi

    local now=$(date +%s)
    if (( now - last_report >= 30 )); then
      local elapsed=$(( now - start_time ))
      echo "[$(date +%T)] Still waiting for port $port... (${elapsed}s elapsed)"
      if [ -f "$LOG_INST" ]; then
        echo "  [inst last log] $(tail -1 "$LOG_INST" 2>/dev/null)"
      fi
      last_report=$now
    fi

    if (( now - start_time >= timeout_seconds )); then
      echo "[$(date +%T)] Timeout waiting for server on port $port (${timeout_seconds}s)"
      if [ -f "$LOG_INST" ]; then
        echo "--- inst log (last 20 lines) ---"
        tail -20 "$LOG_INST" 2>/dev/null
      fi
      return 1
    fi

    sleep 1
  done
}


main() {
    echo "Using Maru direct vLLM connector (single instance)..."

    ensure_python_library_installed maru
    ensure_python_library_installed vllm

    trap cleanup INT TERM USR1 EXIT

    # Launch MaruServer
    if timeout 1 bash -c "echo >/dev/tcp/localhost/$MARU_SERVER_PORT" 2>/dev/null; then
        echo "[$(date +%T)] MaruServer already running on port $MARU_SERVER_PORT, skipping launch..."
    else
        echo "[$(date +%T)] Launching MaruServer on port $MARU_SERVER_PORT..."
        PYTHONUNBUFFERED=1 python3 -m maru_server --port $MARU_SERVER_PORT --log-level "${_LOG_LEVEL:-INFO}" \
            > >(tee "${LOG_DIR:-.}/maru_server.log") 2>&1 &
        maru_server_pid=$!
        PIDS+=($maru_server_pid)
        echo "[$(date +%T)] MaruServer PID: $maru_server_pid (log: ${LOG_DIR:-.}/maru_server.log)"
        sleep 2
        if ! kill -0 $maru_server_pid 2>/dev/null; then
            echo "[$(date +%T)] ERROR: MaruServer process died! Log:"
            cat "${LOG_DIR:-.}/maru_server.log" 2>/dev/null || true
            return 1
        fi
        wait_for_server $MARU_SERVER_PORT
        echo "[$(date +%T)] MaruServer ready."
    fi

    # Log file name
    LOG_INST="${LOG_INST:-single.log}"

    echo "[$(date +%T)] Launching vLLM instance (MODEL=$MODEL, GPU_MEM_UTIL=$GPU_MEM_UTIL)..."
    echo "Please check $LOG_INST for logs."

    # Launch single vLLM instance (GPU 0, prefix cache OFF — see single_vllm_launcher.sh)
    bash single_vllm_launcher.sh ${_MODEL:+"$_MODEL"} \
        > >(tee "$LOG_INST") 2>&1 &
    inst_pid=$!
    PIDS+=($inst_pid)

    wait_for_server $MARU_INST_PORT

    echo "==================================================="
    echo "Server is up. Running cold -> warm verification..."
    echo "  Port: $MARU_INST_PORT"
    echo "==================================================="

    bash run_benchmark.sh --model "$MODEL"
}

# --- Help ---
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Launch a single vLLM instance with the Maru direct connector (no LMCache),"
    echo "run a cold-to-warm cache-hit benchmark, then clean up."
    echo ""
    echo "Options:"
    echo "  --model MODEL          HuggingFace model name (default: Qwen/Qwen2.5-0.5B)"
    echo "  --log-level LEVEL      Log level: DEBUG, INFO, WARNING, ERROR"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Environment variables (from env.sh):"
    echo "  MARU_INST_PORT         Instance port (default: PORT_BASE + 20)"
    echo "  MARU_SERVER_PORT       MaruServer port (default: 10000 + UID)"
    echo "  GPU_MEM_UTIL           GPU memory utilization (default: 0.1)"
    exit 0
}

# --- Argument parsing ---
_LOG_LEVEL=""
_MODEL=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)   usage ;;
        --log-level) _LOG_LEVEL="$2"; shift 2 ;;
        --model)     _MODEL="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

if [[ -n "$_LOG_LEVEL" ]]; then
    export VLLM_LOG_LEVEL="$_LOG_LEVEL"
fi

# Resolve model for the launch banner (launcher applies its own default too).
MODEL="${_MODEL:-Qwen/Qwen2.5-0.5B}"

main
