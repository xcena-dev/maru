#!/bin/bash
# Environment variables for the remote CXL KV example: vLLM engines on nodes without
# CXL keep KV in the CXL pool of memory nodes and move it over RDMA
# (MaruHandler storage_backend="remote_cxl").

export VLLM_LOG_LEVEL=${VLLM_LOG_LEVEL:-INFO}
export GPU_MEM_UTIL=${GPU_MEM_UTIL:-0.1}
export MAX_MODEL_LEN=${MAX_MODEL_LEN:-8192}

# Port configuration (user ID based to avoid conflicts on shared machines).
# User IDs can differ between nodes, so workers on other nodes must be given
# the metadata node's MARU_SERVER_URL explicitly (maru_server.sh prints it).
export MARU_PORT_BASE=${MARU_PORT_BASE:-$((12000 + $(id -u)))}
export MARU_INST1_PORT=${MARU_INST1_PORT:-$((MARU_PORT_BASE + 20))}
export MARU_INST2_PORT=${MARU_INST2_PORT:-$((MARU_PORT_BASE + 21))}

# ── Memory node (each node attached to the pool's CXL memory) ─────────
# The resource manager of the pool (one per pool, as for local CXL).
export MARU_RM_ADDRESS=${MARU_RM_ADDRESS:-127.0.0.1:9850}
# This memory node's address on the RDMA network. Workers reach its remote CXL
# agent there, so do not use 127.0.0.1 when workers run on other nodes.
export MARU_MEMORY_NODE_HOST=${MARU_MEMORY_NODE_HOST:-127.0.0.1}
export MARU_AGENT_PORT=${MARU_AGENT_PORT:-$((10500 + $(id -u)))}
# RDMA NIC the remote CXL agent registers regions with, e.g. mlx5_1:1
# (empty: UCX default).
export MARU_AGENT_UCX_DEVICE=${MARU_AGENT_UCX_DEVICE:-}
# One remote CXL agent runs per node; this lock file enforces it.
export MARU_AGENT_LOCK_FILE=${MARU_AGENT_LOCK_FILE:-/tmp/maru-remote-cxl-agent.lock}
# Key the remote CXL agents require from workers and maru-server. The remote CXL
# agent service keeps it in /etc/maru/remote-cxl-agent.key (root only); give
# every user that runs a worker or maru-server its own copy:
#   sudo install -D -m 0600 -o "$USER" /etc/maru/remote-cxl-agent.key \
#       ~/.maru/remote-cxl-agent.key
# remote_cxl_agent.sh creates this file when it runs the remote CXL agent itself.
export MARU_REMOTE_CXL_AGENT_KEY_FILE=${MARU_REMOTE_CXL_AGENT_KEY_FILE:-$HOME/.maru/remote-cxl-agent.key}

# ── Metadata node (maru-server, one per app and pool) ─────────────────
export MARU_SERVER_BIND_HOST=${MARU_SERVER_BIND_HOST:-127.0.0.1}
export MARU_SERVER_PORT=${MARU_SERVER_PORT:-$((10000 + $(id -u)))}
# DAX device of the pool (empty: any pool the resource manager offers).
export MARU_DAX_PATH=${MARU_DAX_PATH:-}
# Remote CXL agents of the pool, host:port, comma separated: one per memory node.
export MARU_REMOTE_CXL_AGENTS=${MARU_REMOTE_CXL_AGENTS:-${MARU_MEMORY_NODE_HOST}:${MARU_AGENT_PORT}}
# maru-server as the workers see it. Set it directly on worker nodes.
export MARU_SERVER_HOST=${MARU_SERVER_HOST:-127.0.0.1}
export MARU_SERVER_URL=${MARU_SERVER_URL:-tcp://${MARU_SERVER_HOST}:${MARU_SERVER_PORT}}

# ── Worker nodes ──────────────────────────────────────────────────────
# Region size. A worker gets one region when it opens its session and one more
# each time its regions are full (maru_pool_size, as for the cxl backend).
export MARU_POOL_SIZE=${MARU_POOL_SIZE:-4G}
export MARU_KV_CHUNK_TOKENS=${MARU_KV_CHUNK_TOKENS:-256}
# RDMA NIC each worker uses, e.g. mlx5_0:1 (empty: UCX default).
export MARU_WORKER_UCX_DEVICE=${MARU_WORKER_UCX_DEVICE:-}
# Engines share KV only when this name (plus model shape) matches.
export MARU_CACHE_NAMESPACE=${MARU_CACHE_NAMESPACE:-maru-remote-cxl-example}
export MARU_INST1_GPU=${MARU_INST1_GPU:-0}
export MARU_INST2_GPU=${MARU_INST2_GPU:-1}
# NUMA node to run each engine on (empty: no binding), e.g. the GPU's socket.
export MARU_NUMA_NODE=${MARU_NUMA_NODE:-}
# Where the query scripts reach each engine (set when they run on other nodes).
export MARU_INST1_URL=${MARU_INST1_URL:-http://localhost:${MARU_INST1_PORT}}
export MARU_INST2_URL=${MARU_INST2_URL:-http://localhost:${MARU_INST2_PORT}}

# ── FlashInfer sampler workaround (Blackwell sm_120) ──────────────────
# On Blackwell sm_120 (e.g. RTX PRO 6000 Blackwell) the current FlashInfer
# build crashes vLLM's EngineCore init in the sampler path
# ("RuntimeError: FlashInfer requires GPUs with sm75 or higher" - misleading).
# Disabling the FlashInfer sampler avoids it. Auto-applied on that arch only;
# export VLLM_USE_FLASHINFER_SAMPLER yourself to override.
if [[ -z "${VLLM_USE_FLASHINFER_SAMPLER:-}" ]]; then
    _cc=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null | head -1 | tr -d ' ') || true
    if [[ "${_cc:-}" == 12.* ]]; then
        export VLLM_USE_FLASHINFER_SAMPLER=0
        echo "[env.sh] Blackwell sm_${_cc} detected -> VLLM_USE_FLASHINFER_SAMPLER=0 (FlashInfer sampler workaround)"
    fi
    unset _cc
fi
