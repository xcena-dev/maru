# Phase 1: Simple SGLang Examples

단일 SGLang 인스턴스로 기본 동작 및 HiCache를 검증한다.
하나의 통합 launcher에서 모드를 선택하여 실행.

## Usage

```bash
# 모드 선택: baseline / l2 / maru
bash sglang_launcher.sh baseline     # HiCache 없음
bash sglang_launcher.sh l2           # HiCache L2 (Host DRAM)
bash sglang_launcher.sh maru         # HiCache L3 + Maru

# 모델 변경
bash sglang_launcher.sh l2 --model Qwen/Qwen2.5-7B

# 서버가 뜬 후 (별도 터미널):
bash run_simple_query.sh             # 간단 쿼리 테스트
bash run_benchmark.sh                # TTFT 벤치마크
```

## Modes

| Mode | HiCache | L3 Backend | Maru 필요 | 설명 |
|------|---------|------------|-----------|------|
| `baseline` | Off | - | No | KV cache는 GPU VRAM(L1)에만 존재 |
| `l2` | On | - | No | GPU evict → Host DRAM 보관 |
| `maru` | On | Maru CXL | **Yes** | GPU → Host DRAM → Maru CXL 공유 메모리 |

## Files

| File | 설명 |
|------|------|
| `sglang_launcher.sh` | 통합 launcher (baseline/l2/maru) |
| `run_simple_query.sh` | curl 기반 간단 쿼리 테스트 |
| `run_benchmark.sh` | TTFT 벤치마크 (동일 prompt 2회 전송) |
| `env.sh` | 포트, 모델, GPU 설정 |

## Logs

- SGLang 서버: `sglang_server.log` (CWD)
- Maru 서버 (maru 모드): `maru_server.log` (CWD)
