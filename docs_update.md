# 문서 리뷰 통합 분석 — Multi-Node 반영 관점

> 원본: `examples/docs_review.md` (1차 고수준 + 2차 코드 기반 + 3차 재검증, 총 3회 리뷰)
> 대상 문서: `quick_start.md`, `installation.md`, `maru_resource_manager.md`
> 정리 기준: 중복 제거, 우선순위별 분류, 파일별 수정 매핑

3회 리뷰에서 약 절반이 동일 이슈의 반복. 중복 제거 후 **19개 고유 이슈**로 정리.

---

## CRITICAL (4건) — 문서 정확성/사용성을 심각하게 해치는 문제

### C1. Multi-Node CXL 물리적 토폴로지 전제조건 미설명

- **대상**: `quick_start.md:145-157`, `installation.md:15`
- **문제**: "All nodes must have direct access to the CXL memory pool"만 명시. 물리적으로 어떻게 가능한지(CXL switch, Type 3 device), device path 동일성 요구(`/dev/dax0.0`이 모든 노드에서 같은 물리 메모리), offset 정합성 가정이 어디에도 없음
- **코드 근거**: `DaxMapper`는 로컬 `/dev/dax*`를 `open()+mmap()`, `AccessType`은 항상 `LOCAL`
- **수정안**: Prerequisites 섹션에 CXL 하드웨어 요구사항 추가 (CXL fabric/switch, 동일 device path, udev rule 등)

### C2. Multi-Node 다이어그램이 3-tier 연결을 숨김

- **대상**: `quick_start.md:147-157`
- **문제**: Handler→Server 연결 1개만 표시. 실제로는 3개 독립 연결 필요:
  - Handler→Server (ZMQ :5555) — 메타데이터 RPC
  - Handler→RM (TCP :9850) — mmap용 device path 조회
  - Server→RM (TCP :9850) — alloc/free 라이프사이클
- **코드 근거**: `handler.py:147-153`에서 handshake 후 DaxMapper가 RM에 별도 TCP 연결
- **수정안**: 3-tier 토폴로지 다이어그램으로 교체, 두 포트(5555, 9850) 모두 방화벽 개방 필요 명시

### C3. FS_DAX 설명이 프로젝트 현실과 불일치

- **대상**: `maru_resource_manager.md` 전반 (§1 다이어그램, 첫 문단, §3 IPC 테이블, §4 Memory Management)
- **문제**: "DEV_DAX and FS_DAX 두 가지를 지원"이라고 설명하지만, 프로젝트 현실은 DEV_DAX only (FS_DAX는 별도 프로젝트)
- **수정안**: FS_DAX 관련 설명 제거 또는 "(experimental, reserved for future use)" 마킹

### C4. Design doc 설정 테이블에 multi-node 핵심 옵션 누락

- **대상**: `maru_resource_manager.md:146-152` (§8 Configuration)
- **문제**: 코드에 7개 CLI 옵션이 있는데 5개만 나열. 누락:
  - `--grace-period, -g` (default 30초) — remote client disconnect 시 allocation 유지 시간
  - `--max-clients, -m` (default 256) — 동시 연결 수 제한
- **코드 근거**: `server_config.h:16-17`
- **수정안**: 설정 테이블에 두 옵션 추가

---

## HIGH (5건) — 사용성을 의미있게 저해

### H1. rm_address 전파 메커니즘 미문서화

- **대상**: `maru_resource_manager.md:176-188` (§8 RM address propagation), `quick_start.md:143-200`
- **문제**: MaruServer→handshake→MaruHandler→DaxMapper 체인이 핵심인데, 현재 "MaruServer에 --rm-address 설정" 수준의 설명만 있음
- **치명적 함정**: `maru-server --rm-address 127.0.0.1:9850` → handshake로 원격 Handler에 전달 → 원격 Handler가 자기 loopback에 접속 → 실패
- **수정안**: 전파 흐름 다이어그램 + "반드시 외부 접근 가능한 IP 사용" 경고. Quick Start multi-node 예시에 `--rm-address <node-a-ip>:9850` 추가

### H2. Installation의 multi-node 설명 부정확/부족

- **대상**: `installation.md:15`, `installation.md:58-62`
- **문제**:
  - "connect to them over TCP" — MaruServer? RM? 둘 다? 모호함
  - `--no-rm` 설치 시: 어떤 노드가 "client"인지 기준 없음, client node에도 CXL device 필요하다는 점 미언급, client node verification 방법 없음
- **수정안**: 연결 대상 구체화("MaruServer와 Resource Manager 모두에 연결"), client node 요구사항(CXL device 접근 필수) 명시

### H3. Quick Start Multi-Node 예시 불완전

- **대상**: `quick_start.md:159-200`
- **문제**:
  - `maru-server --host 0.0.0.0`만 표시, `--rm-address` 외부 IP 설정 누락
  - systemd `ExecStart` 바이너리 경로 하드코딩 (`/usr/local/bin/`)
  - `MaruConfig`에 `rm_address` fallback 필드 미표시
- **수정안**:
  ```bash
  maru-server --host 0.0.0.0 --rm-address <node-a-ip>:9850
  ```
  MaruConfig에 `rm_address` 필드 예시 추가

### H4. Security "Authentication" 표현이 오해 유발 + 방화벽 가이드 없음

- **대상**: `maru_resource_manager.md:130-138` (§7)
- **문제**:
  - `client_id`는 self-reported (`platform.node()+os.getpid()`) → "owner verification"이지 "authentication"이 아님
  - multi-node 배포 시 어떤 포트(9850, 5555)를 어떤 노드 간에 열어야 하는지 없음
- **수정안**: "Authentication" → "Integrity & ownership tracking"으로 수정, self-reported client_id의 한계 명시, 방화벽 포트 가이드 추가

### H5. Remote Client Reaping 운영 정보 부족

- **대상**: `maru_resource_manager.md:118-124` (§6)
- **문제**: 메커니즘만 설명, 운영 지식 누락:
  - grace period 설정 방법 (`--grace-period`, 문서에 없음)
  - reconnect 시 grace period 취소 (`clientReconnected()`) 미설명
  - local vs remote 판별 기준 (hostname 비교) 미설명
  - TCP keepalive ~25초 + grace 30초 = 총 ~55초 후 회수 미설명
- **수정안**: §6에 운영 관점 정보 추가 (설정, reconnect, 타이밍)

---

## MODERATE (6건) — 이해도를 떨어뜨리는 문제

### M1. Two-Protocol Architecture가 암묵적

- RM IPC (TCP binary, :9850)와 MaruServer RPC (ZMQ MessagePack, :5555)가 완전히 다른 프로토콜인데, design doc은 RM IPC만 설명
- 사용자가 "TCP"라는 단어를 보고 두 프로토콜 혼동 가능

### M2. rm.conf 기록 내용이 "resolved configuration"과 불일치

- **대상**: `maru_resource_manager.md:144`
- **코드**: host, port, log_level 3개만 기록. num_workers, grace_period, max_clients 미기록

### M3. pool_size 의미 혼동

- **대상**: `quick_start.md:53`
- `pool_size`는 CXL pool 전체 크기가 아니라 Handler의 초기 allocation 요청 크기. 주석 "100MB"만으로는 오해 가능

### M4. SIGHUP device rescan 미문서화

- SIGHUP → `rescanDevices()` 트리거. CXL device 핫플러그 시 유용하지만 signal, 사용법, 시점 모두 미설명

### M5. Cross-Instance _premap_shared_regions 동작 미설명

- consumer가 producer 메모리를 읽을 수 있는 핵심 메커니즘(`list_allocations` → pre-mmap)인데 Quick Start에서 설명 없음

### M6. Health check / Readiness probe 미문서화

- 예제 스크립트는 `nc -z`, `curl /health`로 확인하지만, 공식 문서에서 서비스 준비 확인 방법 미안내
- Multi-node 서비스 시작 순서 가이드 없음

---

## MINOR (4건) — 개선하면 좋은 사항

### S1. Troubleshooting 섹션 부재

- 흔한 multi-node 오류 안내 없음: RM loopback binding, `/dev/dax` 권한, 방화벽 차단, `--rm-address` loopback 전파 등

### S2. Idempotency cache 미문서화

- `kMaxCacheEntries=1024`, `kCacheTtlSec=60`으로 네트워크 재시도 시 중복 allocation 방지

### S3. TCP Keepalive 파라미터 미문서화

- idle 10초, interval 5초, count 3 → dead client 감지 ~25초

### S4. Design doc 섹션 번호 건너뜀

- §1 → §3으로 점프, §2 없음

---

## 파일별 수정 매핑

| 파일 | 수정 항목 |
|------|-----------|
| **quick_start.md** | C1(CXL 전제조건), C2(3-tier 다이어그램), H1(--rm-address 예시), H3(multi-node 예시 보강), M3(pool_size 설명) |
| **installation.md** | C1(Prerequisites에 CXL 요구사항), H2(multi-node 설명 정확화, --no-rm 요구사항) |
| **maru_resource_manager.md** | C3(FS_DAX 정리), C4(설정 테이블 보강), H4(Security 표현), H5(Reaper 운영 정보), M1(Two-Protocol), M2(rm.conf), M4(SIGHUP), S4(섹션 번호) |
