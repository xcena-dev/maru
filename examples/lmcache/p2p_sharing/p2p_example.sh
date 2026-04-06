#!/bin/bash

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

echo "Warning: LMCache KV cache sharing support for vLLM v1 is experimental and subject to change."

# Load common environment variables
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

PIDS=()

# Switch to the directory of the current script
cd "$(dirname "${BASH_SOURCE[0]}")"

check_hf_token() {
    if [ -z "$HF_TOKEN" ]; then
        echo "HF_TOKEN is not set. Please set it to your Hugging Face token."
        exit 1
    fi
    if [[ "$HF_TOKEN" != hf_* ]]; then
        echo "HF_TOKEN is not a valid Hugging Face token. Please set it to your Hugging Face token."
        exit 1
    fi
    echo "HF_TOKEN is set and valid."
}

check_num_gpus() {
    num_gpus=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)
    if [ "$num_gpus" -lt 2 ]; then
        echo "You need at least 2 GPUs to run KV cache sharing."
        exit 1
    else
        echo "Found $num_gpus GPUs."
    fi
}

ensure_python_library_installed() {
    echo "Checking if $1 is installed..."
    python3 -c "import $1" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "$1 is not installed. Please install it via pip install $1."
        exit 1
    else
        echo "$1 is installed."
    fi
}

kill_tree() {
    # Recursively kill a process and all its descendants
    local pid=$1 sig=${2:-TERM}
    for child in $(pgrep -P "$pid" 2>/dev/null); do
        kill_tree "$child" "$sig"
    done
    kill -"$sig" "$pid" 2>/dev/null
}

cleanup() {
    echo "Stopping everything…"
    trap - INT TERM USR1 EXIT   # prevent re-entrancy

    # Graceful: recursively kill all tracked process trees
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Killing process tree of $pid"
            kill_tree "$pid" TERM
        fi
    done

    # Wait a moment for graceful shutdown
    sleep 2

    # Force kill any survivors
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Force killing process tree of $pid"
            kill_tree "$pid" 9
        fi
    done

    echo "All processes stopped."
    exit 0
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
      if [ "$port" = "$LMCACHE_INST1_PORT" ] && [ -f "${LOG_INST1}" ]; then
        echo "  [inst1 last log] $(tail -1 "${LOG_INST1}" 2>/dev/null)"
      elif [ "$port" = "$LMCACHE_INST2_PORT" ] && [ -f "${LOG_INST2}" ]; then
        echo "  [inst2 last log] $(tail -1 "${LOG_INST2}" 2>/dev/null)"
      fi
      last_report=$now
    fi

    if (( now - start_time >= timeout_seconds )); then
      echo "[$(date +%T)] Timeout waiting for server on port $port (${timeout_seconds}s)"
      if [ "$port" = "$LMCACHE_INST1_PORT" ] && [ -f "${LOG_INST1}" ]; then
        echo "--- inst1 log (last 20 lines) ---"
        tail -20 "${LOG_INST1}" 2>/dev/null
      elif [ "$port" = "$LMCACHE_INST2_PORT" ] && [ -f "${LOG_INST2}" ]; then
        echo "--- inst2 log (last 20 lines) ---"
        tail -20 "${LOG_INST2}" 2>/dev/null
      fi
      return 1
    fi

    sleep 1
  done
}


main() {
    echo "Using Maru shared storage backend..."

    # check_hf_token
    check_num_gpus
    ensure_python_library_installed lmcache
    ensure_python_library_installed pandas
    ensure_python_library_installed datasets
    ensure_python_library_installed vllm

    trap cleanup INT TERM USR1 EXIT

    # Launch MaruServer
    if timeout 1 bash -c "echo >/dev/tcp/localhost/$MARU_SERVER_PORT" 2>/dev/null; then
        echo "[$(date +%T)] MaruServer already running on port $MARU_SERVER_PORT, skipping launch..."
    else
        echo "[$(date +%T)] Launching MaruServer on port $MARU_SERVER_PORT..."
        echo "[$(date +%T)] maru package: $(python3 -c 'import maru; print(maru.__file__)' 2>&1)"
        PYTHONUNBUFFERED=1 python3 -m maru_server --port $MARU_SERVER_PORT --rm-address "${MARU_RM_ADDRESS:-127.0.0.1:9850}" --log-level "${_LOG_LEVEL:-INFO}" \
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

    # Log file names (injectable via env vars)
    LOG_INST1="${LOG_INST1:-inst1.log}"
    LOG_INST2="${LOG_INST2:-inst2.log}"

    echo "[$(date +%T)] Launching vLLM instances (MODEL=$MODEL, GPU_MEM_UTIL=$GPU_MEM_UTIL)..."
    echo "Please check $LOG_INST1 and $LOG_INST2 for logs."

    # Launch Instance 1 (GPU 0) and wait for it to be ready
    bash p2p_vllm_launcher.sh inst1 ${_MODEL:+"$_MODEL"} \
        > >(tee "$LOG_INST1")  2>&1 &
    inst1_pid=$!
    PIDS+=($inst1_pid)
    wait_for_server $LMCACHE_INST1_PORT

    # Launch Instance 2 (GPU 1) after Instance 1 is ready
    bash p2p_vllm_launcher.sh inst2 ${_MODEL:+"$_MODEL"} \
        > >(tee "$LOG_INST2") 2>&1 &
    inst2_pid=$!
    PIDS+=($inst2_pid)
    wait_for_server $LMCACHE_INST2_PORT

    echo "==================================================="
    echo "All servers are up. You can send request now..."
    echo "Press Ctrl-C to terminate all instances."

    # Keep the script running until interrupted
    echo "Script is running. Waiting for termination signal..."
    echo "==================================================="

    while true; do
        sleep 1
    done
}

# --- Help ---
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Launch two vLLM instances with Maru shared storage KV cache sharing."
    echo "Servers stay running until Ctrl-C."
    echo ""
    echo "Options:"
    echo "  --model MODEL          HuggingFace model name (default: Qwen/Qwen2.5-0.5B)"
    echo "  --log-level LEVEL      Log level: DEBUG, INFO, WARNING, ERROR"
    echo "                         (default: VLLM=DEBUG, LMCACHE=ERROR from env.sh)"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                     # Default model"
    echo "  $0 --model Qwen/Qwen2.5-7B"
    echo "  $0 --log-level WARNING"
    echo ""
    echo "Environment variables (from env.sh):"
    echo "  LMCACHE_PORT_BASE      Port base (default: 9000 + UID)"
    echo "  LMCACHE_INST1_PORT     Instance 1 port (default: PORT_BASE + 10)"
    echo "  LMCACHE_INST2_PORT     Instance 2 port (default: PORT_BASE + 11)"
    echo "  GPU_MEM_UTIL           GPU memory utilization (default: 0.9)"
    echo "  LOG_INST1              Instance 1 log file (default: inst1.log)"
    echo "  LOG_INST2              Instance 2 log file (default: inst2.log)"
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

# Apply log level (override env.sh defaults if specified)
if [[ -n "$_LOG_LEVEL" ]]; then
    export VLLM_LOG_LEVEL="$_LOG_LEVEL"
    export LMCACHE_LOG_LEVEL="$_LOG_LEVEL"
fi

main
