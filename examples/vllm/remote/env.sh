#!/bin/bash
# Environment variables for the remote KV example: vLLM engines that keep KV
# in a CXL pool on another node (MaruHandler storage_backend="remote").

export VLLM_LOG_LEVEL=${VLLM_LOG_LEVEL:-INFO}
export GPU_MEM_UTIL=${GPU_MEM_UTIL:-0.1}
# Also sizes each worker's staging buffer (one prompt of this length).
export MAX_MODEL_LEN=${MAX_MODEL_LEN:-8192}

# Port configuration (user ID based to avoid conflicts on shared machines).
# User IDs can differ between nodes, so a worker on another node must be given
# the pool node's MARU_REMOTE_URL explicitly (remote_pool_server.sh prints it).
export MARU_PORT_BASE=${MARU_PORT_BASE:-$((12000 + $(id -u)))}
export MARU_INST1_PORT=${MARU_INST1_PORT:-$((MARU_PORT_BASE + 10))}
export MARU_INST2_PORT=${MARU_INST2_PORT:-$((MARU_PORT_BASE + 11))}

# ── Pool node ─────────────────────────────────────────────────────────
# MaruServer's local RPC port (clients on the pool node itself).
export MARU_SERVER_PORT=${MARU_SERVER_PORT:-$((10000 + $(id -u)))}
# Remote endpoint that workers on other nodes connect to.
export MARU_REMOTE_PORT=${MARU_REMOTE_PORT:-$((11000 + $(id -u)))}
# Interface the remote endpoint listens on (127.0.0.1 keeps it on this host).
export MARU_REMOTE_BIND_HOST=${MARU_REMOTE_BIND_HOST:-0.0.0.0}
# Address workers use to reach the pool node (its RDMA-capable network).
export MARU_POOL_HOST=${MARU_POOL_HOST:-127.0.0.1}
# Remote endpoint as the workers see it. Set it directly on worker nodes.
export MARU_REMOTE_URL=${MARU_REMOTE_URL:-tcp://${MARU_POOL_HOST}:${MARU_REMOTE_PORT}}
# DAX device of the pool (empty: any pool the resource manager offers).
export MARU_DAX_PATH=${MARU_DAX_PATH:-}
# RDMA NIC the pool registers its regions on, e.g. mlx5_1:1 (empty: UCX default).
export MARU_POOL_UCX_DEVICE=${MARU_POOL_UCX_DEVICE:-}
# Size of each remote region. With the capacity equal to it, the whole pool is
# mapped and registered at start-up and never grows while serving.
export MARU_REMOTE_POOL_SIZE=${MARU_REMOTE_POOL_SIZE:-8G}
export MARU_REMOTE_CAPACITY=${MARU_REMOTE_CAPACITY:-$MARU_REMOTE_POOL_SIZE}
# One page holds one KV object: all layers of one chunk,
#   2 x layers x kv_heads x head_dim x dtype_bytes x chunk_tokens.
# 256-token chunks: Qwen2.5-0.5B 3 MiB (4M fits), Llama-3.1-8B 32 MiB (32M).
export MARU_REMOTE_PAGE_BYTES=${MARU_REMOTE_PAGE_BYTES:-4M}

# ── Worker nodes ──────────────────────────────────────────────────────
export MARU_KV_CHUNK_TOKENS=${MARU_KV_CHUNK_TOKENS:-256}
# RDMA NIC each worker uses, e.g. mlx5_0:1 (empty: UCX default).
export MARU_WORKER_UCX_DEVICE=${MARU_WORKER_UCX_DEVICE:-}
# Engines share KV only when this name (plus model shape) matches.
export MARU_CACHE_NAMESPACE=${MARU_CACHE_NAMESPACE:-maru-remote-example}
export MARU_INST1_GPU=${MARU_INST1_GPU:-0}
export MARU_INST2_GPU=${MARU_INST2_GPU:-1}
# Where the query scripts reach each engine (set when they run on other nodes).
export MARU_INST1_URL=${MARU_INST1_URL:-http://localhost:${MARU_INST1_PORT}}
export MARU_INST2_URL=${MARU_INST2_URL:-http://localhost:${MARU_INST2_PORT}}

# ── FlashInfer sampler workaround (Blackwell sm_120) ──────────────────
# On Blackwell sm_120 (e.g. RTX PRO 6000 Blackwell) the current FlashInfer
# build crashes vLLM's EngineCore init in the sampler path
# ("RuntimeError: FlashInfer requires GPUs with sm75 or higher" — misleading).
# Disabling the FlashInfer sampler avoids it. Auto-applied on that arch only;
# export VLLM_USE_FLASHINFER_SAMPLER yourself to override.
if [[ -z "${VLLM_USE_FLASHINFER_SAMPLER:-}" ]]; then
    _cc=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null | head -1 | tr -d ' ') || true
    if [[ "${_cc:-}" == 12.* ]]; then
        export VLLM_USE_FLASHINFER_SAMPLER=0
        echo "[env.sh] Blackwell sm_${_cc} detected → VLLM_USE_FLASHINFER_SAMPLER=0 (FlashInfer sampler workaround)"
    fi
    unset _cc
fi
