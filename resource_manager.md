#### Before: 싱글 노드 (UDS)

```mermaid
flowchart TB
    subgraph NodeA["Node A (싱글 노드)"]

        subgraph RM["maru-resource-manager"]
            UdsSrv["UdsServer<br/>UDS IPC"]
            RH["RequestHandler"]
            PM["PoolManager"]
            Reaper["Reaper"]
            UdsSrv --> RH --> PM
        end

        subgraph MS["MaruServer"]
            AllocMgr["AllocationManager"]
            KVMgr["KVManager"]
            RpcSrv["RpcServer (ZMQ)"]
            ShmC_S["MaruShmClient"]
            AllocMgr --> ShmC_S
        end

        subgraph MH1["MaruHandler (vLLM)"]
            ZMQ1["ZMQ Client"]
            DaxMap1["DaxMapper"]
            ShmC_H1["MaruShmClient"]
            DaxMap1 --> ShmC_H1
        end

        subgraph Storage["CXL Memory"]
            D1["/dev/dax0.0<br/>(DEV_DAX)"]
            D2["/mnt/pmem0/<br/>(FS_DAX)"]
        end

        ShmC_S -- "UDS: alloc/free" --> UdsSrv
        ShmC_H1 -- "UDS: get_fd → fd via SCM_RIGHTS" --> UdsSrv
        ZMQ1 -- "ZMQ: KV metadata" --> RpcSrv
        ShmC_H1 -. "fd → mmap() 직접 접근" .-> Storage
        PM -.-> Storage
    end

    style RM fill:#e1f0ff,stroke:#4a90d9,color:#000
    style MS fill:#fff3e0,stroke:#f5a623,color:#000
    style MH1 fill:#e8f5e9,stroke:#66bb6a,color:#000
    style Storage fill:#f3e5f5,stroke:#ab47bc,color:#000
```

#### After: 멀티 노드 (TCP)

```mermaid
flowchart TB
    subgraph NodeA["Node A — Master"]

        subgraph RM["maru-resource-manager"]
            TcpSrv["TcpServer<br/>TCP :9850"]
            RH["RequestHandler"]
            PM["PoolManager"]
            Reaper["Reaper<br/>1s 주기"]
            TcpSrv --> RH --> PM
            Reaper -- "reapExpired()<br/>로컬: kill(pid,0)<br/>원격: grace period" --> PM
        end

        subgraph MS["MaruServer"]
            AllocMgr["AllocationManager"]
            KVMgr["KVManager"]
            RpcSrv["RpcServer (ZMQ)"]
            ShmC_S["MaruShmClient"]
            AllocMgr --> ShmC_S
        end

        subgraph MH_local["MaruHandler (vLLM — local)"]
            ZMQ_L["ZMQ Client"]
            DaxMap_L["DaxMapper"]
            ShmC_L["MaruShmClient"]
            DaxMap_L --> ShmC_L
        end

        subgraph Storage["CXL Memory"]
            D1["/dev/dax0.0<br/>(DEV_DAX)"]
            D2["/mnt/pmem0/<br/>(FS_DAX)"]
        end

        ShmC_S -- "TCP: alloc/free" --> TcpSrv
        ShmC_L -- "TCP: get_access → path string" --> TcpSrv
        ZMQ_L -- "ZMQ: KV metadata" --> RpcSrv
        ShmC_L -. "open(path) → mmap() 직접 접근" .-> Storage
        PM -.-> Storage
    end

    subgraph NodeB["Node B — Remote Client"]

        subgraph MH_remote["MaruHandler (vLLM — remote)"]
            ZMQ_R["ZMQ Client"]
            DaxMap_R["DaxMapper"]
            ShmC_R["MaruShmClient"]
            DaxMap_R --> ShmC_R
        end
    end

    ShmC_R -- "TCP: alloc / get_access" --> TcpSrv
    ZMQ_R -- "ZMQ: KV metadata" --> RpcSrv
    ShmC_R -. "원격 메모리 접근<br/>(CXL fabric / RDMA — 향후)" .-> Storage
    TcpSrv -. "연결 끊김 감지 →<br/>clientDisconnected()" .-> Reaper

    style RM fill:#e1f0ff,stroke:#4a90d9,color:#000
    style MS fill:#fff3e0,stroke:#f5a623,color:#000
    style MH_local fill:#e8f5e9,stroke:#66bb6a,color:#000
    style MH_remote fill:#e8f5e9,stroke:#66bb6a,color:#000
    style Storage fill:#f3e5f5,stroke:#ab47bc,color:#000
    style NodeA fill:#fafafa,stroke:#999,color:#000
    style NodeB fill:#fafafa,stroke:#999,color:#000
```

## 13. 상세 내부 아키텍처

### 13.1 C++ Resource Manager 내부 구조

```mermaid
flowchart TB
    subgraph main["main.cpp"]
        direction TB
        SIG["Signal Handler<br/>SIGTERM→gStop=1<br/>SIGHUP→gRescan=1"]
        LOOP["Main Loop<br/>sleep(1s), check gStop/gRescan"]
    end

    subgraph TcpSrv["TcpServer"]
        direction TB
        EL["eventLoop thread<br/>(epoll_wait)"]
        LFD["listenFd_<br/>EPOLLIN"]
        IDLE["Idle client fds<br/>EPOLLIN | EPOLLONESHOT"]
        TQ["taskQueue_<br/>(mutex + condvar)"]
        W1["Worker 1"]
        W2["Worker 2"]
        WN["Worker N"]

        EL -- "accept()" --> LFD
        EL -- "data ready" --> IDLE
        IDLE -- "dispatch fd" --> TQ
        LFD -- "new conn →<br/>epoll_ctl ADD" --> IDLE
        TQ --> W1 & W2 & WN
        W1 -- "handleOneRequest()<br/>→ epoll_ctl MOD<br/>(re-arm)" --> IDLE
    end

    subgraph RH["RequestHandler"]
        direction TB
        HA["handleAlloc()"]
        HF["handleFree()"]
        HGA["handleGetAccess()"]
        HS["handleStats()"]
    end

    subgraph PM["PoolManager"]
        direction TB
        MU["mu_ (mutex)<br/>전체 상태 직렬화"]
        ALLOC["alloc()"]
        VAF["verifyAndFree()"]
        VAGP["verifyAndGetPath()"]
        STATS["getStats()"]
        POOLS["pools_<br/>vector&lt;PoolState&gt;"]
        ALLOCS["allocations_<br/>map&lt;regionId, Allocation&gt;"]
        WAL["WalStore<br/>crash recovery"]
        META["MetadataStore<br/>pool 메타데이터"]

        MU --> ALLOC & VAF & VAGP & STATS
        ALLOC --> POOLS & ALLOCS & WAL
        VAF --> ALLOCS & POOLS & WAL
        VAGP --> ALLOCS & POOLS
        STATS --> POOLS
    end

    subgraph Reaper["Reaper"]
        direction TB
        RT["1s 주기 스레드"]
        RE["reapExpired()"]
        RT --> RE
    end

    subgraph Storage["CXL/DAX Devices"]
        D1["/dev/dax0.0"]
        D2["/dev/dax1.0"]
        D3["/mnt/pmem0/"]
    end

    W1 & W2 & WN --> RH
    HA --> ALLOC
    HF --> VAF
    HGA --> VAGP
    HS --> STATS
    RE --> PM
    PM --> Storage

    style TcpSrv fill:#e1f0ff,stroke:#4a90d9,color:#000
    style RH fill:#fff3e0,stroke:#f5a623,color:#000
    style PM fill:#e8f5e9,stroke:#66bb6a,color:#000
    style Reaper fill:#fce4ec,stroke:#ef5350,color:#000
    style Storage fill:#f3e5f5,stroke:#ab47bc,color:#000
```

### 13.2 요청 처리 흐름 (Alloc 예시)

```mermaid
sequenceDiagram
    participant C as MaruShmClient<br/>(Python)
    participant EL as eventLoop<br/>(epoll)
    participant TQ as taskQueue
    participant W as Worker Thread
    participant RH as RequestHandler
    participant PM as PoolManager

    C->>EL: TCP connect
    EL->>EL: accept() → epoll_ctl ADD<br/>(EPOLLIN | EPOLLONESHOT)

    Note over EL: client idle 상태 —<br/>worker 점유 없음

    C->>EL: send ALLOC_REQ
    EL->>EL: epoll_wait → fd ready
    EL->>TQ: push fd (auto-disarm)
    TQ->>W: pop fd

    W->>W: readFull(header + payload)
    W->>RH: handleAlloc(req, ctx)
    RH->>PM: alloc(size, clientId, ...)
    PM->>PM: mu_.lock()
    PM->>PM: allocateFromPool()
    PM->>PM: computeAuthToken()
    PM->>PM: wal_->appendAlloc()
    PM->>PM: mu_.unlock()
    PM-->>RH: handle, devicePath
    RH-->>W: AllocResult

    W->>C: writeFull(ALLOC_RESP + path)
    W->>EL: epoll_ctl MOD<br/>(re-arm EPOLLONESHOT)

    Note over EL: client 다시 idle —<br/>worker 해제됨
```

### 13.3 TOCTOU 방지: verifyAndFree 단일 lock

```mermaid
sequenceDiagram
    participant W as Worker
    participant PM as PoolManager
    participant R as Reaper

    Note over W,R: Before (TOCTOU 취약)
    W->>PM: verifyAuthToken(handle)
    PM->>PM: mu_.lock() → 검증 OK → mu_.unlock()
    R->>PM: reapExpired() → mu_.lock() → free(handle) → mu_.unlock()
    W->>PM: free(handle)
    PM->>PM: mu_.lock() → ENOENT! → mu_.unlock()

    Note over W,R: After (atomic)
    W->>PM: verifyAndFree(handle, clientId)
    PM->>PM: mu_.lock()
    PM->>PM: 토큰 검증 → ownership 검증 → free 실행
    PM->>PM: mu_.unlock()
    Note over PM: Reaper는 lock 대기 —<br/>끼어들 수 없음
```

### 13.4 epoll 기반 연결 생명주기

```mermaid
stateDiagram-v2
    [*] --> Accepted: accept()
    Accepted --> IdleInEpoll: epoll_ctl ADD<br/>(ONESHOT)

    IdleInEpoll --> Dispatched: data ready →<br/>push to taskQueue
    note right of IdleInEpoll: worker 점유 없음<br/>epoll이 모니터링

    Dispatched --> Processing: worker가 pop
    Processing --> IdleInEpoll: 요청 성공 →<br/>epoll_ctl MOD (re-arm)
    Processing --> Closed: EOF / 에러 →<br/>removeClient()

    IdleInEpoll --> Closed: EPOLLHUP /  서버 shutdown

    Closed --> [*]: close(fd)

    note right of Processing: worker는 요청 1개만<br/>처리 후 즉시 반환
```

### 13.5 Reaper: 클라이언트 사망 감지 및 자원 회수

Reaper는 1초 주기로 `reapExpired()`를 호출하여, 죽은 클라이언트의 allocation을 자동 회수합니다.

#### 로컬 클라이언트 vs 원격 클라이언트

| | 로컬 클라이언트 | 원격 클라이언트 |
|--|--|--|
| client_id 예시 | `host-a:2000` (같은 호스트) | `host-b:3000` (다른 호스트) |
| 사망 감지 방식 | `kill(pid, 0)` + PID start time 비교 | TCP 연결 끊김 + grace period |
| PID 재사용 방지 | `/proc/pid/stat` start time 비교 | 해당 없음 |

#### 동작 흐름

```mermaid
flowchart TB
    subgraph Reaper["Reaper (1s 주기)"]
        direction TB
        SCAN["allocations_ 순회"]
        LOCAL{"isLocalClient?"}
        KILL["kill(pid, 0)"]
        DEAD_L["프로세스 없음<br/>(ESRCH)"]
        PID_REUSE["PID start time<br/>불일치"]
        DISC{"disconnectedClients_<br/>에 존재?"}
        GRACE{"grace period<br/>(30s) 만료?"}
        REAP["doFreeAllocation()"]
        SKIP["skip"]

        SCAN --> LOCAL
        LOCAL -- "Yes" --> KILL
        LOCAL -- "No" --> DISC
        KILL -- "프로세스 죽음" --> DEAD_L --> REAP
        KILL -- "PID 재사용" --> PID_REUSE --> REAP
        KILL -- "살아있음" --> SKIP
        DISC -- "Yes" --> GRACE
        DISC -- "No" --> SKIP
        GRACE -- "만료" --> REAP
        GRACE -- "대기 중" --> SKIP
    end

    style Reaper fill:#fce4ec,stroke:#ef5350,color:#000
```

#### 원격 클라이언트 연결 끊김 처리 (TCP disconnect + grace period)

```mermaid
sequenceDiagram
    participant C as 원격 Client<br/>(host-b)
    participant TCP as TcpServer
    participant PM as PoolManager
    participant R as Reaper

    C->>TCP: TCP connect
    C->>TCP: ALLOC_REQ (client_id="host-b:3000")
    TCP->>TCP: trackClientId(fd, "host-b:3000")
    TCP->>PM: clientReconnected("host-b:3000")
    Note over PM: grace period 타이머 취소<br/>(있었다면)

    Note over C: 클라이언트 크래시

    TCP->>TCP: epoll: EOF / EPOLLHUP
    TCP->>TCP: removeClient(fd)
    TCP->>PM: clientDisconnected("host-b:3000")
    Note over PM: disconnectedClients_에<br/>타임스탬프 기록

    loop 매 1초
        R->>PM: reapExpired()
        PM->>PM: grace period 확인<br/>(30초 미만 → skip)
    end

    Note over PM: 30초 경과, 재접속 없음

    R->>PM: reapExpired()
    PM->>PM: grace period 만료 →<br/>doFreeAllocation()
    Note over PM: "host-b:3000"의<br/>모든 allocation 회수
```

#### Reaper 입장에서의 "client"

일반적인 LMCache 배포에서 Resource Manager에 직접 alloc/free를 요청하는 주체는 **Maru Server 프로세스**입니다. LMCache의 prefiller/decoder는 Maru Server의 고객이지, Resource Manager의 client가 아닙니다.

```
Prefiller (PID 1000) → Maru Server (PID 2000) → Resource Manager
                              ↑
                      MaruShmClient 소유
                      client_id = "host-a:2000"
```

- Prefiller가 죽어도 → Reaper는 모름 (Maru Server가 관리)
- **Maru Server가 죽으면** → Reaper가 PID 2000의 모든 allocation 회수
- **원격 Maru Server가 끊기면** → TCP disconnect 감지 → grace period 후 회수