#!/bin/bash
# Environment variables for the single-instance Maru-vLLM direct example

export VLLM_LOG_LEVEL=${VLLM_LOG_LEVEL:-DEBUG}
export GPU_MEM_UTIL=${GPU_MEM_UTIL:-0.1}

# Port configuration (user ID based to avoid conflicts on shared machines).
# Uses base+20 to stay clear of the p2p example ports (base+10 / base+11).
export MARU_PORT_BASE=${MARU_PORT_BASE:-$((12000 + $(id -u)))}
export MARU_INST_PORT=${MARU_INST_PORT:-$((MARU_PORT_BASE + 20))}

# Maru Server
export MARU_SERVER_PORT=${MARU_SERVER_PORT:-$((10000 + $(id -u)))}
export MARU_SERVER_URL="tcp://localhost:${MARU_SERVER_PORT}"

# Maru KV connector settings
export MARU_POOL_SIZE=${MARU_POOL_SIZE:-"4G"}
export MARU_KV_CHUNK_TOKENS=${MARU_KV_CHUNK_TOKENS:-256}

# ── FlashInfer sampler workaround (Blackwell sm_120) ──────────────────
# On Blackwell sm_120 (e.g. RTX PRO 6000 Blackwell) the current FlashInfer
# build crashes vLLM's EngineCore init in the sampler path
# ("RuntimeError: FlashInfer requires GPUs with sm75 or higher" — misleading).
# Disabling the FlashInfer sampler avoids it. Auto-applied on that arch only;
# export VLLM_USE_FLASHINFER_SAMPLER yourself to override. Harmless elsewhere
# (falls back to the native sampler). Broaden the match if other archs hit it.
if [[ -z "${VLLM_USE_FLASHINFER_SAMPLER:-}" ]]; then
    _cc=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null | head -1 | tr -d ' ') || true
    if [[ "${_cc:-}" == 12.* ]]; then
        export VLLM_USE_FLASHINFER_SAMPLER=0
        echo "[env.sh] Blackwell sm_${_cc} detected → VLLM_USE_FLASHINFER_SAMPLER=0 (FlashInfer sampler workaround)"
    fi
    unset _cc
fi
