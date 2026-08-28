# Disaggregated Prefill Examples for LMCache with vLLM v1

This directory contains examples demonstrating how to run LMCache with disaggregated prefill using Maru shared storage. Disaggregated prefill allows you to separate the prefill (prompt processing) and decode (token generation) phases of LLM inference across different GPU instances, enabling better resource utilization and scalability.

## Overview

Disaggregated prefill architecture separates the compute-intensive prefill phase from the memory-intensive decode phase:

- **Prefill servers**: Handle prompt processing and KV cache generation
- **Decode server**: Handles token generation using cached KV states
- **Proxy server**: Coordinates requests between prefill and decode servers

This architecture provides several benefits:
- Better GPU utilization by matching workload characteristics to hardware
- Improved scalability by independently scaling prefill and decode capacity
- Reduced latency through parallel processing
- Cost optimization by using different instance types for different phases

## Available Examples

### 1p1d - Single Prefill, Single Decode
Directory: [`1p1d/`](./1p1d/)

**Requirements**: At least 2 GPUs

This is the simplest configuration to get started with disaggregated prefill.

## Prerequisites

Before running any example, ensure you have:

- [LMCache](https://github.com/LMCache/LMCache) installed: `uv pip install lmcache`
- [Maru](https://github.com/xcena-dev/maru) installed: `uv pip install -e .`
- Sufficient GPU resources (see individual example requirements)

## Quick Start

1. Navigate to the example directory:
   ```bash
   cd 1p1d/
   ```

2. Follow the specific README instructions in that directory

## Architecture Components

Each example includes:

- **Main script**: `disagg_example_*.sh` - Main entry point to run the example
- **Launcher script**: `disagg_vllm_launcher.sh` - Launches vLLM servers
- **Proxy server**: `disagg_proxy_server.py` - FastAPI server coordinating requests
- **Configuration files**: YAML configs for prefill and decode servers

## Troubleshooting

- **GPU Memory Issues**: Ensure you have sufficient VRAM for the model on each GPU
- **Port Conflicts**: Check that the configured ports are available (see `env.sh`)
- **Dependencies**: Ensure both LMCache and Maru are properly installed

For detailed troubleshooting, check the log files generated in each example directory.

## Further Reading

- [LMCache Documentation](https://github.com/LMCache/LMCache)
- [vLLM Documentation](https://docs.vllm.ai/)
