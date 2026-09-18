#!/bin/bash
# Environment variables for the Maru x Dynamo single-node example

# Frontend HTTP port + worker system (health) ports.
# User ID based to avoid conflicts on shared machines.
export DYN_HTTP_PORT=${DYN_HTTP_PORT:-$((14000 + $(id -u)))}
export DYN_W0_SYSTEM_PORT=${DYN_W0_SYSTEM_PORT:-$((DYN_HTTP_PORT + 81))}
export DYN_W1_SYSTEM_PORT=${DYN_W1_SYSTEM_PORT:-$((DYN_HTTP_PORT + 82))}

# Discovery backend: "file" (zero dependencies, single node) or "etcd"
# (needs a reachable etcd, e.g. `docker run -d -p 2379:2379 quay.io/coreos/etcd:v3.5.21 \
#   etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379`).
# The file backend's mtime-lease + inotify implementation can intermittently
# drop or miss a worker's registration (see README troubleshooting); etcd
# has real leases and lossless watches, so prefer it when reliability
# matters more than zero setup.
export DISCOVERY_BACKEND=${DISCOVERY_BACKEND:-file}
export ETCD_ENDPOINTS=${ETCD_ENDPOINTS:-http://localhost:2379}

# File-backed discovery directory shared by the frontend and workers
# (single node only, DISCOVERY_BACKEND=file). Wiped by
# single_node_example.sh before the frontend starts so stale registrations
# from dead runs can't linger.
export DYN_FILE_KV=${DYN_FILE_KV:-/tmp/dynamo_kv_$(id -u)}

# Maru Server
export MARU_SERVER_PORT=${MARU_SERVER_PORT:-$((10000 + $(id -u)))}
export MARU_SERVER_URL="tcp://localhost:${MARU_SERVER_PORT}"

# Maru KV connector settings
export MARU_POOL_SIZE=${MARU_POOL_SIZE:-"4G"}
export MARU_KV_CHUNK_TOKENS=${MARU_KV_CHUNK_TOKENS:-256}

# GPU assignment per worker. Point both at the same index to run the whole
# example on a single GPU (the default 0.3 utilization leaves room for two
# small-model workers on one device).
export W0_GPU=${W0_GPU:-0}
export W1_GPU=${W1_GPU:-1}
export GPU_MEM_UTIL=${GPU_MEM_UTIL:-0.3}

# Required on Blackwell (sm_120) GPUs; harmless elsewhere.
export VLLM_USE_FLASHINFER_SAMPLER=${VLLM_USE_FLASHINFER_SAMPLER:-0}
