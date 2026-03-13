# marufs Instance Authentication and Authorization System Design

> **Status**: Design proposal. Not yet implemented. Current Hybrid mode uses `perm_set_default(PERM_ALL)` — all processes have full access to all regions.

- marufs kernel module supports per-region permission enforcement via `perm_grant` and `perm_set_default` ioctl
- The inter-instance permission delegation mechanism (who grants what to whom) is the subject of this design document
- Design an authentication and authorization system based on pre-provisioned X.509 certificates

---

## 1. Background

### Current State (Hybrid Mode)

In the current Hybrid mode (RPC server + marufs VFS backend):

- **Server-side**: `AllocationManager` creates regions via `MarufsClient.alloc()`, which calls `perm_set_default(PERM_ALL)` — granting all permissions to every process by default
- **Client-side**: `DaxMapper` opens and mmaps region files — no authentication required
- **No access control enforcement**: Any process that can reach the marufs mount point can open and mmap any region

This is acceptable for single-tenant deployments but insufficient for multi-tenant or security-sensitive environments.

### Security Requirements

KV cache contains user prompts and model response state. Sharing without authentication risks data leakage and cache poisoning.

- **Authentication**: Verify that the requesting instance is a legitimate vLLM instance in the cluster
- **Authorization**: Grant only the necessary permissions to authenticated instances (read-only instances do not need write permissions)
- **Least privilege**: Set region default permissions to minimum, grant individually after authentication (current `PERM_ALL` default needs improvement)
- **Certificate-based identity**: X.509 certificate-based mutual authentication — industry standard (K8s, Service Mesh, gRPC)

### Threat Model

| Threat | Mitigation |
|--------|------------|
| **Unauthenticated access** — external process hijacks KV cache region | Individual grant after authentication |
| **Identity spoofing** — impersonating another instance | X.509 chain verification + kernel `(node_id, pid, birth_time)` dual verification |
| **Privilege escalation** — read-only instance overwrites cache | Kernel enforces only `perm_grant`-ed permissions |
| **Certificate compromise** — cert/key leaked externally | Expiration limits + revocation (CRL/OCSP) + kernel process verification |
| **Man-in-the-middle** — eavesdropping/tampering auth traffic (Option A) | mTLS encrypted channel + mutual certificate verification |
| **Process impersonation** — privilege hijacking on the same node | Kernel `pid + birth_time` identification + automatic GC on termination |

### Out of Scope

- Certificate issuance/renewal infrastructure (delegated to existing PKI infrastructure)
- Certificate/key file protection (delegated to existing OS security mechanisms)
- CXL hardware physical security

---

## 2. Overview

### Goals

- Authentication and authorization for CXL region access between vLLM instances
- Support two authentication models:
  - **Option A (P2P)**: Direct mTLS between instances — owner decides permissions based on its own policy
  - **Option B (marufs-mediated)**: marufs acts as proxy authenticator + automatically grants permissions according to pre-defined policy

### Integration with Current Architecture

In Hybrid mode, the authentication flow would be added between the RPC alloc response and the client-side mmap:

```mermaid
sequenceDiagram
    participant C as LMCache Client
    participant S as MaruServer
    participant Auth as Auth Layer
    participant K as marufs (kernel)

    C->>S: request_alloc() → {handle, mount_path}
    Note over C: Client knows region exists

    C->>Auth: Authenticate + request access
    Auth->>K: perm_grant(fd, node_id, pid, perms)

    C->>K: open(region_file) + mmap()
    Note over K: Kernel checks permissions on open/mmap
```

The key change from current behavior: `perm_set_default(PERM_ALL)` would be replaced with `perm_set_default(PERM_READ)` or even no default permissions, requiring explicit grants after authentication.

### Architecture

#### Option A: P2P mTLS

```mermaid
graph LR
    A["Instance A (owner)<br/>cert + key"] <-->|mTLS direct auth| B["Instance B<br/>cert + key"]
```

#### Option B: marufs-mediated

```mermaid
graph TB
    A["Instance A (owner)<br/>cert + key"] -->|submit cert| M["marufs<br/>cert verification + policy<br/>→ proxy perm_grant"]
    B["Instance B<br/>cert + key"] -->|submit cert| M
```

### Trust Model

| Layer | Role |
|-------|------|
| **Instance cert** | Identity included in SAN. Proves identity during authentication. Chain verification prevents forgery |
| **marufs kernel** | Process identification via `(node_id, pid, birth_time)` + permission enforcement |

Certificates prove the process's identity, and the kernel enforces access permissions.

### Authentication Model Comparison

| | Option A: P2P | Option B: marufs-mediated |
|---|---|---|
| **Auth entity** | Each instance (owner) | marufs (filesystem) |
| **Policy location** | Inside owner code | marufs config file (pre-defined) |
| **Auth Server** | 1 per owner | Not required (marufs acts as proxy) |
| **perm_grant caller** | Owner process | marufs (kernel) |
| **Pros** | Owner has fine-grained control | Operationally simple, centralized policy management |
| **Cons** | Each instance needs Auth Server implementation | Policy changes require marufs config update |

---

## 3. Authentication Flows

### Option A: P2P mTLS

Instance B connects directly to Instance A (owner) via mTLS, authenticates, and obtains permissions. Permission mapping is determined by the owner according to its own policy and is not specified in this document.

```mermaid
sequenceDiagram
    participant Deploy as Deployment
    participant A as Instance A (owner)
    participant B as Instance B

    rect rgb(240, 240, 255)
    Note over Deploy: At deployment time
    Deploy->>A: Issue + distribute certificate (cert/key)
    Deploy->>B: Issue + distribute certificate (cert/key)
    end

    rect rgb(240, 255, 240)
    Note over A: Instance A startup
    A->>A: Load certificate
    A->>A: Create region + perm_set_default(least privilege)
    A->>A: Register metadata (endpoint, identity)
    A->>A: Start Auth Server (mTLS listen)
    end

    rect rgb(255, 245, 230)
    Note over B: Instance B startup + authentication
    B->>B: Load certificate
    B->>A: Query peer metadata → discover endpoint
    B->>A: mTLS connection (mutual cert chain verification)
    A->>A: Verify Instance B cert → confirm identity
    B->>A: ACCESS_REQUEST {node_id, pid, regions}
    A->>A: Determine perms according to owner policy
    A->>A: perm_grant(fd, node_id, pid, perms)
    A-->>B: ACCESS_GRANTED
    end

    rect rgb(255, 240, 240)
    Note over B: Data access
    B->>B: open(region_file) + mmap()
    B->>B: Zero-copy CXL memory access (kernel enforces permissions)
    end
```

### Option B: marufs-mediated

marufs acts as a proxy authenticator. Instances do not need to implement an Auth Server — they simply register with marufs, and the kernel automatically grants permissions according to pre-defined policy.

**Pre-defined Policy:**

```yaml
# /etc/maru/policy.yaml
policy:
  # identity → accessible region patterns + permissions
  instance-a:
    - pattern: "maru_*"
      perms: [READ, WRITE, ADMIN, IOCTL]
  instance-b:
    - pattern: "maru_*"
      perms: [READ, WRITE, IOCTL]
```

```mermaid
sequenceDiagram
    participant Deploy as Deployment
    participant A as Instance A (owner)
    participant M as marufs (kernel)
    participant B as Instance B

    rect rgb(240, 240, 255)
    Note over Deploy: At deployment time
    Deploy->>A: Issue + distribute certificate (cert/key)
    Deploy->>B: Issue + distribute certificate (cert/key)
    Deploy->>M: Deploy policy.yaml
    end

    rect rgb(240, 255, 240)
    Note over A: Instance A startup
    A->>A: Load certificate
    A->>M: Create region + perm_set_default(least privilege)
    A->>M: Register instance (including cert)
    end

    rect rgb(255, 245, 230)
    Note over B: Instance B startup + authentication
    B->>B: Load certificate
    B->>M: Register instance (including cert)
    M->>M: Cert chain verification + identity extraction
    B->>M: Request region access
    M->>M: Policy matching (identity + region pattern)
    M->>M: Proxy perm_grant execution
    M-->>B: Permission granted
    end

    rect rgb(255, 240, 240)
    Note over B: Data access
    B->>B: open(region_file) + mmap()
    B->>B: Zero-copy CXL memory access (kernel enforces permissions)
    end
```

**Advantages:**
- No Auth Server implementation required — automatic permission grant upon registration
- Centralized policy management — cluster-wide consistency guaranteed
- Even if the owner is offline, permissions can be granted to existing regions based on policy
