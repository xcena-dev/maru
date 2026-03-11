# Maru KV Connector for vLLM

vLLM에서 Maru CXL 공유 메모리를 통해 KV 캐시를 직접 공유하는 커넥터.
LMCache 없이 vLLM ↔ Maru를 직접 연결합니다.

## 아키텍처

```
기존 (LMCache 경유):
  vLLM → LMCacheConnector → LMCache Engine → StorageManager → MaruConnector → MaruHandler → CXL

신규 (직접 연결):
  vLLM → MaruKVConnector → MaruHandler → CXL
```

## 사전 요구사항

1. **Maru 설치** (maru-server, maru-resourced 포함)
2. **vLLM v0.14+** (KVConnectorBase_V1 지원)
3. **CXL DAX 디바이스** 및 `maru-resourced` 데몬 실행 중

## 빠른 시작

### 1. Maru 설치

```bash
cd /home/jhlee/workspace/maru
pip install -e .
```

### 2. Maru Resource Manager 실행 확인

```bash
# systemd 서비스 확인
sudo systemctl status maru-resourced
```

### 3. Maru Server 실행

```bash
maru-server
# 기본 tcp://0.0.0.0:5555 에서 리스닝
```

### 4. vLLM 실행

**방법 A: 동적 로딩 (vLLM 코드 수정 없음)**

```bash
vllm serve <model> \
    --kv-transfer-config '{
        "kv_connector": "MaruKVConnector",
        "kv_connector_module_path": "maru_vllm",
        "kv_role": "kv_both",
        "kv_connector_extra_config": {
            "maru_server_url": "tcp://localhost:5555",
            "maru_pool_size": "4G"
        }
    }'
```

**방법 B: factory 등록 (vLLM `feat/maru-kv-connector` 브랜치 사용 시)**

```bash
vllm serve <model> \
    --kv-transfer-config '{
        "kv_connector": "MaruKVConnector",
        "kv_role": "kv_both",
        "kv_connector_extra_config": {
            "maru_server_url": "tcp://localhost:5555",
            "maru_pool_size": "4G"
        }
    }'
```

### 5. 두 번째 vLLM 인스턴스 (같은 노드)

같은 설정으로 실행하면 첫 번째 인스턴스가 저장한 KV 캐시를 CXL에서 바로 읽습니다.

```bash
# 다른 포트에서 두 번째 인스턴스
vllm serve <model> \
    --port 8001 \
    --kv-transfer-config '{
        "kv_connector": "MaruKVConnector",
        "kv_connector_module_path": "maru_vllm",
        "kv_role": "kv_both",
        "kv_connector_extra_config": {
            "maru_server_url": "tcp://localhost:5555",
            "maru_pool_size": "4G"
        }
    }'
```

## 설정 옵션

`kv_connector_extra_config`에서 설정:

| 키 | 타입 | 기본값 | 설명 |
|----|------|--------|------|
| `maru_server_url` | str | `tcp://localhost:5555` | MaruServer 주소 |
| `maru_pool_size` | str/int | `1G` | CXL 메모리 풀 크기. `4G`, `500M` 등 |
| `maru_chunk_size` | str/int | `4M` | Maru 페이지 크기 (CXL 할당 단위) |
| `maru_instance_id` | str | auto | 인스턴스 고유 ID (기본: UUID 자동 생성) |
| `maru_eager_map` | bool | `true` | 연결 시 다른 인스턴스의 CXL 영역 미리 매핑 |
| `maru_kv_chunk_tokens` | int | `256` | KV 캐시 chunk 단위 (토큰 수) |

### maru_kv_chunk_tokens

KV 캐시를 몇 토큰 단위로 나눠서 저장할지 결정합니다.

- **작을수록** (64, 128): prefix 재사용 granularity 높음, maru key 수 많음
- **클수록** (512, 1024): key 수 적음, 하지만 재사용 granularity 낮음
- **기본 256**: 대부분의 경우 적절한 균형
- **자동 정렬**: vLLM block_size의 배수로 자동 조정됨

### maru_pool_size

인스턴스당 할당할 CXL 메모리 크기입니다.

필요 용량 계산:
```
pool_size ≈ num_layers × kv_head_dim × 2(K+V) × max_cached_tokens × dtype_bytes
```

예시 (Llama 7B, fp16):
```
32 layers × 128 head_dim × 32 heads × 2(K+V) × 4096 tokens × 2 bytes
= 약 2GB
```

## KV 캐시 공유 동작

### Chunk 기반 저장

토큰이 chunk 단위(기본 256)로 나뉘어 저장됩니다:

```
Prompt: [tok0..tok255 | tok256..tok511 | tok512..tok767 | tok768..tok900]
         chunk 0        chunk 1          chunk 2          (incomplete, 미저장)
```

각 chunk의 key = `kv_{hash(tok0..끝)}_L{layer}` (rolling prefix hash)

### 부분 Prefix 재사용

```
인스턴스 A:
  요청: "The quick brown fox jumps over the lazy dog. Once upon a time..."
  → chunk 0, 1, 2 저장

인스턴스 B:
  요청: "The quick brown fox jumps over the lazy dog. In a galaxy far away..."
  → chunk 0, 1 히트 (공통 prefix), chunk 2 미스
  → chunk 0, 1은 CXL에서 로드, 나머지만 compute
```

### 데이터 흐름

```
[저장 경로]
  vLLM forward pass
    → save_kv_layer() 호출 (매 레이어)
      → GPU KV 텐서에서 chunk 슬롯 추출
        → .cpu().contiguous().numpy().tobytes()
          → MaruHandler.store(key, data=memoryview)
            → CXL mmap write (zero-copy on CXL side)

[로드 경로]
  Scheduler: batch_exists()로 chunk별 히트 확인
    → 매칭된 chunk 수 전달
  Worker: start_load_kv() 호출
    → MaruHandler.retrieve(key) per chunk per layer
      → CXL mmap read (zero-copy)
        → torch.frombuffer() → .cuda()
          → GPU KV 버퍼에 inject
```

## 문제 해결

### MaruServer 연결 실패

```
ERROR: Failed to connect to MaruServer at tcp://localhost:5555
```
→ `maru-server`가 실행 중인지 확인

### CXL 메모리 부족

```
ERROR: Cannot allocate page for key ...
```
→ `maru_pool_size`를 늘리거나, maru-server 재시작

### chunk_tokens 정렬 경고

```
WARNING: maru_kv_chunk_tokens 300 not aligned to block_size 16, adjusted to 288
```
→ 정상 동작. block_size 배수로 자동 조정됨

## LMCache 경유 방식과 비교

| 항목 | LMCache 경유 | 직접 연결 (이 커넥터) |
|------|-------------|---------------------|
| 의존성 | vLLM + LMCache + maru | vLLM + maru |
| 중간 레이어 | LMCache Engine, StorageManager, RemoteBackend | 없음 |
| 직렬화 | LMCache MemoryObj 변환 | torch tensor ↔ bytes 직접 |
| Prefix matching | LMCache CacheEngineKey 해싱 | vLLM 토큰 prefix 해싱 |
| 설정 | LMCACHE_CONFIG_FILE YAML | kv_connector_extra_config JSON |
