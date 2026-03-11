# Resource Manager Server 전환 — 상세 구현 가이드

`resource_manager_server.md`의 설계를 기반으로 한 파일별, 라인별 구현 가이드.

---

## Phase 1: systemd 제거 + 경로 정리

> 순수 삭제 작업. 기존 동작 로직을 변경하지 않으면서 systemd 의존성을 제거한다.

### 1-1. `maru_resource_manager/systemd/` 디렉토리 전체 삭제

```bash
rm -rf maru_resource_manager/systemd/
```

삭제 대상:
- `maru_resource_manager/systemd/maru-resourced.service` — systemd unit 파일
- `maru_resource_manager/systemd/99-maru-resourced.rules` — udev hotplug rules

### 1-2. `maru_resource_manager/CMakeLists.txt` — systemd 관련 코드 제거

**삭제할 것:**

1. `option(MARU_INSTALL_SYSTEMD ...)` 옵션 정의
2. `if(MARU_INSTALL_SYSTEMD)` 블록 전체:
   - `configure_file()` (서비스 파일 템플릿 치환)
   - `install(FILES ... /etc/systemd/system/)` (서비스 파일 설치)
   - `install(FILES ... /etc/udev/rules.d/)` (udev rules 설치)
   - `install(CODE ...)` 블록 (systemctl daemon-reload, enable, restart, udevadm 등)

**변경할 것:**

바이너리 이름을 `maru_resourced` → `maru-resource-manager`로 변경:

```cmake
# Before
set_target_properties(maru_resourced PROPERTIES OUTPUT_NAME "maru_resourced")

# After
set_target_properties(maru_resource_manager PROPERTIES OUTPUT_NAME "maru-resource-manager")
```

**최종 CMakeLists.txt 형태:**

```cmake
cmake_minimum_required(VERSION 3.16)
project(maru_resource_manager LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

find_package(OpenSSL REQUIRED)

add_executable(maru_resource_manager
    src/main.cpp
    src/pool_manager.cpp
    src/uds_server.cpp
    src/reaper.cpp
    src/metadata.cpp
    src/wal.cpp
    src/util.cpp
    src/log.cpp
)

target_include_directories(maru_resource_manager PRIVATE include src)
target_link_libraries(maru_resource_manager PRIVATE pthread OpenSSL::Crypto)

set_target_properties(maru_resource_manager PROPERTIES OUTPUT_NAME "maru-resource-manager")

install(TARGETS maru_resource_manager RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
```

### 1-3. `maru_common/resource_manager_installer.py` — systemd 로직 제거

**삭제할 것:**

- `_SERVICE_NAME`, `_SERVICE_FILE`, `_UDEV_RULES_FILE` 상수
- `--no-systemd` CLI 옵션
- `-DMARU_INSTALL_SYSTEMD=OFF` cmake 플래그 분기
- `_do_uninstall()` 내 systemd 관련 코드:
  - `systemctl stop/disable` 호출
  - service 파일, udev rules 파일 삭제
  - `systemctl daemon-reload`, `udevadm control --reload-rules` 호출

**남는 것:**

```python
def main():
    parser = argparse.ArgumentParser(description="Install Maru Resource Manager")
    parser.add_argument("--prefix", default="/usr/local", help="Install prefix")
    parser.add_argument("--clean", action="store_true", help="Clean build directory before building")
    parser.add_argument("--uninstall", action="store_true", help="Uninstall binary")
    args = parser.parse_args()

    if args.uninstall:
        _do_uninstall(args.prefix)
    else:
        _do_install(args.prefix, args.clean)

def _do_install(prefix, clean):
    # cmake configure + build + install (systemd 없이)
    build_dir = _get_build_dir()
    if clean and build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(exist_ok=True)
    subprocess.run(["cmake", str(_get_source_dir()), f"-DCMAKE_INSTALL_PREFIX={prefix}"], cwd=build_dir, check=True)
    subprocess.run(["cmake", "--build", str(build_dir), "-j"], check=True)
    subprocess.run(["cmake", "--install", str(build_dir)], check=True)

def _do_uninstall(prefix):
    # 바이너리만 삭제
    binary = Path(prefix) / "bin" / "maru-resource-manager"
    if binary.exists():
        binary.unlink()
        print(f"Removed {binary}")
```

### 1-4. `maru_shm/constants.py` — 기본 경로 업데이트

```python
# Before
DEFAULT_SOCKET_PATH = os.environ.get(
    "MARU_SOCKET_PATH", "/run/maru-resourced/maru-resourced.sock"
)
DEFAULT_STATE_DIR = os.environ.get("MARU_STATE_DIR", "/var/lib/maru-resourced")

# After
DEFAULT_SOCKET_PATH = "/tmp/maru-resourced/maru-resourced.sock"
DEFAULT_STATE_DIR = "/var/lib/maru-resourced"
```

환경변수 오버라이드 제거. 클라이언트가 다른 경로를 쓰려면 `MaruShmClient(socket_path="...")` 사용.

### 1-5. `maru_resource_manager/src/util.cpp` — 기본 경로 변경

```cpp
// Before (line 58)
std::string defaultSocketPath() {
  const char* env = std::getenv("MARU_SOCKET_PATH");
  if (env) return std::string(env);
  return std::string("/run/maru-resourced/maru-resourced.sock");
}

// After
std::string defaultSocketPath() {
  return std::string("/tmp/maru-resourced/maru-resourced.sock");
}

// Before (line 66)
std::string defaultStateDir() {
  const char* env = std::getenv("MARU_STATE_DIR");
  if (env) return std::string(env);
  return std::string("/var/lib/maru-resourced");
}

// After
std::string defaultStateDir() {
  return std::string("/var/lib/maru-resourced");
}
```

환경변수 분기 제거. Phase 3에서 이 함수들 자체를 삭제하고 CLI 인자로 대체.

### 1-6. `maru_resource_manager/tools/maru_test_client.cpp` — 경로 변경

```cpp
// Before (line 40)
static const char *kDefaultSocketPath = "/run/maru-resourced/maru-resourced.sock";

// After
static const char *kDefaultSocketPath = "/tmp/maru-resourced/maru-resourced.sock";
```

`resolveSocketPath()`의 환경변수 분기도 제거:
```cpp
// Before
static std::string resolveSocketPath() {
    if (!g_socketPath.empty()) return g_socketPath;
    const char* env = std::getenv("MARU_SOCKET_PATH");
    if (env) return std::string(env);
    return std::string(kDefaultSocketPath);
}

// After
static std::string resolveSocketPath() {
    if (!g_socketPath.empty()) return g_socketPath;
    return std::string(kDefaultSocketPath);
}
```

### Phase 1 완료 확인

- [ ] `maru_resource_manager/systemd/` 디렉토리 삭제됨
- [ ] `CMakeLists.txt`에서 systemd 코드 전부 제거됨
- [ ] `resource_manager_installer.py`에서 systemd 로직 제거됨
- [ ] C++/Python 모든 기본 경로가 `/tmp/maru-resourced/`로 통일됨
- [ ] 환경변수 오버라이드(`MARU_SOCKET_PATH`, `MARU_STATE_DIR`) 전부 제거됨
- [ ] cmake 빌드 성공, 바이너리 이름 `maru-resource-manager`
- [ ] 수동 실행(`./maru-resource-manager`)으로 기존과 동일하게 동작

---

## Phase 2: 코드 레이어 분리 (멀티 노드 대비)

> 기존 바이너리 프로토콜을 유지하면서, UdsServer의 비즈니스 로직을 `RequestHandler`로 분리한다.
> 새 의존성 없이 코드 구조만 개선. 향후 ZMQ/TCP transport 추가 시 `RequestHandler`를 그대로 재사용.
>
> **왜 protobuf를 쓰지 않는가**: 메시지 타입 5개, 고정 크기 구조체 — protobuf의 이점(스키마 진화, 코드 생성)
> 대비 의존성 추가 + 빌드 복잡도가 과함. 멀티 노드를 실제 구현할 때 도입해도 늦지 않음.

### 2-1. 현재 구조 분석

현재 `UdsServer::handleClient()`가 모든 것을 담당:

```
UdsServer::handleClient(clientFd):
  ┌─ Transport ──────────────────────────────┐
  │ readFullWithCred() → 소켓 I/O            │
  │ getsockopt(SO_PEERCRED) → 인증           │
  ├─ Protocol ───────────────────────────────┤
  │ MsgHeader 파싱 (magic, version, type)    │
  │ payload 읽기, 구조체 캐스팅              │
  ├─ Business Logic ─────────────────────────┤
  │ switch (type):                           │
  │   ALLOC_REQ → pm_.alloc() → 응답 생성    │
  │   FREE_REQ  → pm_.free() → 응답 생성     │
  │   ...                                    │
  ├─ Transport ──────────────────────────────┤
  │ sendAllocRespWithFd() (SCM_RIGHTS)       │
  │ writeFull() (plain response)             │
  └──────────────────────────────────────────┘
```

### 2-2. 목표 구조: Transport / Handler 분리

```
UdsServer (Transport)              RequestHandler (Business Logic)
┌─────────────────────────┐        ┌──────────────────────────────────┐
│ accept, read, write     │        │ handleAlloc(req, cred) → resp    │
│ SCM_RIGHTS fd passing   │───────►│ handleFree(req, cred) → resp     │
│ SO_PEERCRED 인증        │        │ handleGetFd(req, cred) → resp    │
│ MsgHeader 파싱          │◄───────│ handleStats() → resp             │
│ 응답 전송               │        │                                  │
└─────────────────────────┘        │ PoolManager& 참조만 보유         │
                                   │ Transport에 의존하지 않음        │
                                   └──────────────────────────────────┘
```

향후 멀티 노드:
```
UdsServer (로컬)  ──►│
                     │ RequestHandler (동일한 코드)
ZmqServer (원격)  ──►│
```

### 2-3. `RequestHandler` 클래스 생성 (`src/request_handler.h/cpp`)

`handleClient()` 내부의 비즈니스 로직을 추출한다.

```cpp
// request_handler.h
#pragma once
#include "pool_manager.h"
#include "include/ipc.h"
#include "include/types.h"
#include <sys/types.h>

struct RequestContext {
    pid_t pid;
    uid_t uid;
};

struct AllocResult {
    AllocResp resp;
    int daxFd = -1;     // >= 0이면 SCM_RIGHTS로 전달
};

struct GetFdResult {
    GetFdResp resp;
    int daxFd = -1;
};

class RequestHandler {
public:
    explicit RequestHandler(PoolManager& pm);

    AllocResult handleAlloc(const AllocReq& req, const RequestContext& ctx);
    FreeResp    handleFree(const FreeReq& req, const RequestContext& ctx);
    GetFdResult handleGetFd(const GetFdReq& req, const RequestContext& ctx);
    StatsResult handleStats();
    ErrorResp   makeError(int32_t status, const std::string& msg);

private:
    PoolManager& pm_;
};
```

```cpp
// request_handler.cpp
#include "request_handler.h"

RequestHandler::RequestHandler(PoolManager& pm) : pm_(pm) {}

AllocResult RequestHandler::handleAlloc(const AllocReq& req, const RequestContext& ctx) {
    AllocResult result;
    // 기존 handleClient()의 ALLOC_REQ 분기 로직을 여기로 이동
    // pm_.alloc(), authToken 생성, daxFd 설정 등
    // ...
    return result;
}

FreeResp RequestHandler::handleFree(const FreeReq& req, const RequestContext& ctx) {
    FreeResp resp;
    // 기존 handleClient()의 FREE_REQ 분기 로직을 여기로 이동
    // 소유권 검증 (ctx.uid == 0 → root, 아니면 ctx.pid == owner)
    // pm_.free()
    // ...
    return resp;
}

GetFdResult RequestHandler::handleGetFd(const GetFdReq& req, const RequestContext& ctx) {
    GetFdResult result;
    // 기존 handleClient()의 GET_FD_REQ 분기 로직을 여기로 이동
    // authToken 검증, daxFd 조회
    // ...
    return result;
}

StatsResult RequestHandler::handleStats() {
    // 기존 STATS_REQ 분기 로직을 여기로 이동
    // pm_.getPoolStats()
    // ...
}
```

### 2-4. `UdsServer` 리팩토링 — Transport 역할만

```cpp
// uds_server.h (변경 후)
class UdsServer {
public:
    UdsServer(PoolManager& pm, const std::string& socketPath);  // Phase 3에서 socketPath 추가
    void start();
    void stop();

private:
    void acceptLoop();
    void handleClient(int clientFd);

    // Transport 헬퍼
    void sendResponseWithFd(int fd, const void* hdr, size_t hdrSize,
                            const void* payload, size_t payloadSize, int daxFd);
    void sendResponse(int fd, const void* hdr, size_t hdrSize,
                      const void* payload, size_t payloadSize);

    PoolManager& pm_;
    RequestHandler handler_;    // 새로 추가
    std::string socketPath_;
    int listenFd_ = -1;
    std::atomic<bool> running_{false};
    std::atomic<int> activeClients_{0};
};
```

```cpp
// uds_server.cpp — handleClient() 리팩토링
void UdsServer::handleClient(int cfd) {
    // --- Transport: 읽기 ---
    MsgHeader hdr;
    ucred cred;
    readFullWithCred(cfd, &hdr, sizeof(hdr), &cred);
    // magic/version 검증...

    uint8_t payload[1024];
    readFull(cfd, payload, hdr.payloadLen);

    RequestContext ctx{.pid = cred.pid, .uid = cred.uid};

    // --- Dispatch to Handler ---
    switch (hdr.type) {
    case MsgType::ALLOC_REQ: {
        auto& req = *reinterpret_cast<AllocReq*>(payload);
        auto result = handler_.handleAlloc(req, ctx);
        MsgHeader respHdr{/* ALLOC_RESP */};
        sendResponseWithFd(cfd, &respHdr, sizeof(respHdr),
                           &result.resp, sizeof(result.resp), result.daxFd);
        break;
    }
    case MsgType::FREE_REQ: {
        auto& req = *reinterpret_cast<FreeReq*>(payload);
        auto resp = handler_.handleFree(req, ctx);
        MsgHeader respHdr{/* FREE_RESP */};
        sendResponse(cfd, &respHdr, sizeof(respHdr), &resp, sizeof(resp));
        break;
    }
    case MsgType::GET_FD_REQ: {
        auto& req = *reinterpret_cast<GetFdReq*>(payload);
        auto result = handler_.handleGetFd(req, ctx);
        MsgHeader respHdr{/* GET_FD_RESP */};
        sendResponseWithFd(cfd, &respHdr, sizeof(respHdr),
                           &result.resp, sizeof(result.resp), result.daxFd);
        break;
    }
    case MsgType::STATS_REQ: {
        auto result = handler_.handleStats();
        // stats 응답 전송...
        break;
    }
    default: {
        auto err = handler_.makeError(-1, "unknown message type");
        // 에러 응답 전송...
        break;
    }
    }

    close(cfd);
}
```

### 2-5. `ipc.h` — accessType 필드 추가 (향후 확장 대비)

기존 바이너리 프로토콜에 `accessType` 필드를 예약한다. 현재는 항상 `LOCAL(0)`.

```cpp
// include/ipc.h

enum class AccessType : uint32_t {
    LOCAL  = 0,   // fd via SCM_RIGHTS + mmap (현재)
    REMOTE = 1,   // 향후: RDMA 등 원격 메모리 접근
};

// AllocResp — 기존 reserved/pad 필드를 accessType으로 활용
struct AllocResp {
    int32_t status;
    uint32_t accessType;    // Before: _pad, After: AccessType (0=LOCAL)
    Handle handle;
    uint64_t requestedSize;
};
static_assert(sizeof(AllocResp) == 48);  // 크기 변경 없음 (pad → accessType)

// GetFdResp — 기존 4바이트에 accessType 추가할 공간이 없으므로
// 현재는 LOCAL 고정, 멀티 노드 시 프로토콜 버전업과 함께 확장
```

**Python 쪽도 동일하게:**

```python
# maru_shm/ipc.py — AllocResp
# Before
_ALLOC_RESP_FORMAT = "=iIQQQQQ"  # status, _pad, regionId, offset, length, authToken, requestedSize

# After (바이너리 레이아웃 동일, 필드 이름만 변경)
_ALLOC_RESP_FORMAT = "=iIQQQQQ"  # status, accessType, regionId, offset, length, authToken, requestedSize
# accessType: 0=LOCAL (현재), 1=REMOTE (향후)
```

바이너리 레이아웃이 동일하므로 **하위 호환성 유지** — 기존 클라이언트도 그대로 동작.

### 2-6. 파일 변경 요약

```
새로 생성:
  maru_resource_manager/src/request_handler.h     ← 비즈니스 로직
  maru_resource_manager/src/request_handler.cpp

변경:
  maru_resource_manager/src/uds_server.h          ← RequestHandler 멤버 추가
  maru_resource_manager/src/uds_server.cpp        ← handleClient()에서 비즈니스 로직 분리
  maru_resource_manager/include/ipc.h             ← AccessType enum 추가, AllocResp._pad → accessType
  maru_resource_manager/CMakeLists.txt            ← request_handler.cpp 추가
  maru_shm/ipc.py                                 ← AllocResp._pad → accessType (이름만 변경)

변경 없음:
  maru_shm/client.py                              ← 바이너리 호환, 코드 변경 불필요
  maru_shm/uds_helpers.py                         ← 변경 없음
  maru_resource_manager/include/types.h           ← 변경 없음
```

### Phase 2 완료 확인

- [ ] `request_handler.h/cpp` 생성, PoolManager에만 의존 (소켓 코드 없음)
- [ ] `UdsServer::handleClient()` — transport만 담당, handler에 위임
- [ ] `AllocResp._pad` → `accessType` (바이너리 호환 유지)
- [ ] `AccessType` enum 추가 (LOCAL=0, REMOTE=1)
- [ ] CMakeLists.txt에 `request_handler.cpp` 추가
- [ ] 빌드 성공, 기존 테스트 통과 (alloc → mmap → free → stats)
- [ ] `RequestHandler`가 소켓/transport 코드에 의존하지 않음을 확인

---

## Phase 3: 서버 구조 전환

> Phase 2에서 재구성된 코드 위에 서버 기능을 추가한다.

### 3-1. `main.cpp` — `ServerConfig` + CLI 파싱

```cpp
#include <getopt.h>

struct ServerConfig {
    std::string socketPath = "/tmp/maru-resourced/maru-resourced.sock";
    std::string stateDir   = "/var/lib/maru-resourced";
    LogLevel logLevel      = LogLevel::Info;
    int idleTimeout        = 60;  // 초, 0=비활성화
};

static LogLevel parseLogLevel(const char* s) {
    if (strcmp(s, "debug") == 0) return LogLevel::Debug;
    if (strcmp(s, "info") == 0)  return LogLevel::Info;
    if (strcmp(s, "warn") == 0)  return LogLevel::Warn;
    if (strcmp(s, "error") == 0) return LogLevel::Error;
    fprintf(stderr, "unknown log level: %s (using info)\n", s);
    return LogLevel::Info;
}

static void printUsage(const char* prog) {
    fprintf(stderr,
        "Usage: %s [OPTIONS]\n\n"
        "Options:\n"
        "  -s, --socket-path PATH    UDS socket path (default: /tmp/maru-resourced/maru-resourced.sock)\n"
        "  -d, --state-dir PATH      State directory (default: /var/lib/maru-resourced)\n"
        "  -l, --log-level LEVEL     Log level: debug, info, warn, error (default: info)\n"
        "  -t, --idle-timeout SECS   Auto-shutdown after N seconds idle (default: 60, 0=disable)\n"
        "  -h, --help                Show this help\n"
        "  -v, --version             Show version\n",
        prog);
}

static ServerConfig parseArgs(int argc, char* argv[]) {
    ServerConfig cfg;
    static struct option longOpts[] = {
        {"socket-path",  required_argument, nullptr, 's'},
        {"state-dir",    required_argument, nullptr, 'd'},
        {"log-level",    required_argument, nullptr, 'l'},
        {"idle-timeout", required_argument, nullptr, 't'},
        {"help",         no_argument,       nullptr, 'h'},
        {"version",      no_argument,       nullptr, 'v'},
        {nullptr, 0, nullptr, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "s:d:l:t:hv", longOpts, nullptr)) != -1) {
        switch (opt) {
        case 's': cfg.socketPath = optarg; break;
        case 'd': cfg.stateDir = optarg; break;
        case 'l': cfg.logLevel = parseLogLevel(optarg); break;
        case 't': cfg.idleTimeout = std::atoi(optarg); break;
        case 'h': printUsage(argv[0]); exit(0);
        case 'v': printf("maru-resource-manager version %s\n", VERSION); exit(0);
        default:  printUsage(argv[0]); exit(1);
        }
    }
    return cfg;
}
```

### 3-2. `main.cpp` — 새 main() 구현

```cpp
int main(int argc, char* argv[]) {
    ServerConfig cfg = parseArgs(argc, argv);
    setLogLevel(cfg.logLevel);

    // 디렉토리 보장
    ensureDirExists(parentDir(cfg.socketPath));
    ensureDirExists(cfg.stateDir);

    // 시작 배너
    logf(Info, "maru-resource-manager starting");
    logf(Info, "  socket    : %s", cfg.socketPath.c_str());
    logf(Info, "  state dir : %s", cfg.stateDir.c_str());
    logf(Info, "  log level : %s", logLevelStr(cfg.logLevel));
    logf(Info, "  idle timeout: %ds%s", cfg.idleTimeout,
         cfg.idleTimeout == 0 ? " (disabled)" : "");

    // 시그널 핸들러
    std::signal(SIGINT,  [](int) { gStop = 1; });
    std::signal(SIGTERM, [](int) { gStop = 1; });
    std::signal(SIGHUP,  [](int) { gRescan = 1; });

    // 초기화 — 설정을 명시적으로 주입
    PoolManager pm(cfg.stateDir);
    if (!pm.loadPools()) {
        logf(Warn, "no CXL/DAX devices found — starting with empty pool");
    }

    UdsServer server(pm, cfg.socketPath);
    server.start();

    Reaper reaper(pm);
    reaper.start();

    logf(Info, "ready — listening on %s", cfg.socketPath.c_str());

    // 메인 루프 (idle timeout 포함)
    int idleSeconds = 0;
    while (!gStop) {
        if (gRescan) { pm.rescanDevices(); gRescan = 0; }

        // idle timeout 체크
        if (cfg.idleTimeout > 0) {
            if (pm.allocationCount() == 0) {
                idleSeconds++;
            } else {
                idleSeconds = 0;
            }
            if (idleSeconds >= cfg.idleTimeout) {
                logf(Info, "no active allocations for %ds, shutting down", cfg.idleTimeout);
                break;
            }
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // graceful shutdown
    logf(Info, "shutting down...");
    reaper.stop();
    server.stop();
    logf(Info, "shutdown complete");
    return 0;
}
```

### 3-3. `pool_manager.h/cpp` — 생성자에 `stateDir` 인자 추가

```cpp
// Before (pool_manager.h)
class PoolManager {
public:
    PoolManager();
    ...
};

// After
class PoolManager {
public:
    explicit PoolManager(const std::string& stateDir);
    uint32_t allocationCount() const;  // 새로 추가
    ...
private:
    std::string stateDir_;
};
```

```cpp
// Before (pool_manager.cpp, lines 258-262)
PoolManager::PoolManager() {
    metadata_ = std::make_unique<MetadataStore>(defaultStateDir());
    wal_ = std::make_unique<WalStore>(defaultStateDir());
}

// After
PoolManager::PoolManager(const std::string& stateDir)
    : stateDir_(stateDir) {
    metadata_ = std::make_unique<MetadataStore>(stateDir);
    wal_ = std::make_unique<WalStore>(stateDir);
}

uint32_t PoolManager::allocationCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<uint32_t>(allocations_.size());
}
```

### 3-4. `uds_server.h/cpp` — 생성자에 `socketPath` 인자 추가

```cpp
// Before (uds_server.h)
class UdsServer {
public:
    explicit UdsServer(PoolManager& pm);
    ...
};

// After
class UdsServer {
public:
    UdsServer(PoolManager& pm, const std::string& socketPath);
    ...
private:
    std::string socketPath_;
};
```

```cpp
// Before (uds_server.cpp, start() 내부)
void UdsServer::start() {
    // ...
    std::string path = defaultSocketPath();
    // bind to path...
}

// After
UdsServer::UdsServer(PoolManager& pm, const std::string& socketPath)
    : pm_(pm), socketPath_(socketPath) {}

void UdsServer::start() {
    // 소켓 디렉토리 자동 생성
    ensureDirExists(parentDir(socketPath_));
    // initSecret with stateDir from pm
    initSecret(pm_.stateDir(), pm_.hasExistingAllocations());
    // bind to socketPath_...
}
```

### 3-5. `util.h/cpp` — 함수 추가/삭제

**추가:**

```cpp
// util.h
bool ensureDirExists(const std::string& path);
std::string parentDir(const std::string& path);

// util.cpp
std::string parentDir(const std::string& path) {
    auto pos = path.rfind('/');
    if (pos == std::string::npos || pos == 0) return "/";
    return path.substr(0, pos);
}

bool ensureDirExists(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) == 0) return S_ISDIR(st.st_mode);
    ensureDirExists(parentDir(path));
    if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
        logf(Error, "failed to create directory %s: %s", path.c_str(), strerror(errno));
        return false;
    }
    return true;
}
```

**삭제:**

```cpp
// 삭제: defaultSocketPath(), defaultStateDir()
// Phase 1에서 경로만 변경했던 이 함수들을 이제 완전 삭제
```

**변경:**

```cpp
// Before
int initSecret(bool hasExistingAllocations);
// 내부에서 defaultStateDir() 호출

// After
int initSecret(const std::string& stateDir, bool hasExistingAllocations);
// stateDir을 인자로 받음
```

### 3-6. `log.h/cpp` — `parseLogLevel()`, `logLevelStr()` 추가

```cpp
// log.h에 추가
LogLevel parseLogLevel(const std::string& s);
const char* logLevelStr(LogLevel level);

// log.cpp에 추가
LogLevel parseLogLevel(const std::string& s) {
    if (s == "debug") return LogLevel::Debug;
    if (s == "info")  return LogLevel::Info;
    if (s == "warn")  return LogLevel::Warn;
    if (s == "error") return LogLevel::Error;
    fprintf(stderr, "unknown log level: %s (using info)\n", s.c_str());
    return LogLevel::Info;
}

const char* logLevelStr(LogLevel level) {
    switch (level) {
    case LogLevel::Debug: return "debug";
    case LogLevel::Info:  return "info";
    case LogLevel::Warn:  return "warn";
    case LogLevel::Error: return "error";
    default: return "unknown";
    }
}
```

### 3-7. `MaruServer` 측 — 자동 시작 + 크래시 복구

`maru_shm/client.py`에 `_ensure_resource_manager()` 추가:

```python
import fcntl
import subprocess
import time
from pathlib import Path

class MaruShmClient:
    def __init__(self, socket_path: str | None = None):
        self._socket_path = socket_path or DEFAULT_SOCKET_PATH
        self._fd_cache: dict[int, int] = {}
        self._mmap_cache: dict[int, mmap_module.mmap] = {}
        self._lock = threading.Lock()

    def _ensure_resource_manager(self) -> None:
        """Resource manager가 없으면 자동으로 시작한다."""
        # 먼저 빠르게 확인
        if self._try_connect():
            return

        lock_path = Path(self._socket_path).parent / "rm.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)

        lock_fd = open(lock_path, "w")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

            # flock 획득 후 다시 확인 (다른 프로세스가 이미 시작했을 수 있음)
            if self._try_connect():
                return

            # resource manager 백그라운드 시작
            logger.info("Starting maru-resource-manager (socket: %s)", self._socket_path)
            subprocess.Popen(
                ["maru-resource-manager", "--socket-path", self._socket_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            # 소켓이 나타날 때까지 대기
            for _ in range(50):  # 최대 5초, 100ms 간격
                time.sleep(0.1)
                if self._try_connect():
                    logger.info("maru-resource-manager is ready")
                    return

            raise RuntimeError(
                f"maru-resource-manager failed to start within 5s (socket: {self._socket_path})"
            )
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()

    def _try_connect(self) -> bool:
        """소켓에 연결 가능한지 테스트 (즉시 닫음)."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(self._socket_path)
            sock.close()
            return True
        except OSError:
            sock.close()
            return False

    def _connect(self) -> socket.socket:
        """UDS 연결. 실패 시 resource manager 재시작 후 재시도."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(self._socket_path)
        except OSError:
            sock.close()
            self._ensure_resource_manager()
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self._socket_path)
        return sock
```

`maru_server/allocation_manager.py` 변경:

```python
# Before
class AllocationManager:
    def __init__(self):
        self._client = MaruShmClient()

# After
class AllocationManager:
    def __init__(self):
        self._client = MaruShmClient()
        self._client._ensure_resource_manager()  # eager: 시작 시 바로 확인
```

### Phase 3 완료 확인

- [ ] `main.cpp` — `ServerConfig` + `getopt_long`, 시작 배너, idle timeout
- [ ] `PoolManager` — 생성자에 `stateDir` 인자, `allocationCount()` 메서드
- [ ] `UdsServer` — 생성자에 `socketPath` 인자, 소켓 디렉토리 자동 생성
- [ ] `util.cpp` — `ensureDirExists()`, `parentDir()` 추가, `defaultSocketPath()`/`defaultStateDir()` 삭제
- [ ] `util.cpp` — `initSecret()`이 `stateDir`을 인자로 받음
- [ ] `log.cpp` — `parseLogLevel()`, `logLevelStr()` 추가
- [ ] `MaruShmClient` — `_ensure_resource_manager()`, `_connect()` 크래시 복구
- [ ] `AllocationManager` — 초기화 시 eager resource manager 확인
- [ ] `maru-resource-manager --help` 동작
- [ ] `maru-server` 시작 시 resource manager 자동 시작 확인
- [ ] resource manager 수동 kill 후 다음 alloc 호출 시 자동 재시작 확인
- [ ] idle timeout 동작 확인 (할당 0개 → 60초 후 자동 종료)

---

## 전체 완료 확인

- [ ] Phase 1: systemd 제거, 경로 통일, 환경변수 제거
- [ ] Phase 2: protobuf 프로토콜, LocalAccess/RemoteAccess oneof
- [ ] Phase 3: CLI server, 자동 시작/종료, 크래시 복구
- [ ] 기존 테스트 전부 통과
- [ ] `install.sh` 정상 동작 (systemd 없이)
- [ ] fork에 push → xcena-dev/maru로 PR 생성
