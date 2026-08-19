# Vendored kernel sources

`csrc/` holds paged-KV placement kernels copied from
[LMCache](https://github.com/LMCache/LMCache). Both projects are Apache-2.0, and
the SPDX headers on every copied file are retained.

## Why these live here

Maru's vLLM connector called `lmcache.c_ops` for its default KV copy path. That
made LMCache a runtime requirement of a connector whose whole point is to reach
Maru without LMCache, and the requirement was never declared: `pyproject.toml`
listed only `pyzmq`, `msgpack` and `dacite`, and a missing LMCache degraded to a
slower per-layer path behind one log line. Copying the kernels in removes the
requirement and makes the copy path Maru's own to change.

Design note and measured expectations:
`_vault/_design/maru_vllm_direct/20260819_lmcache-kernel-dependency-removal.md`.

## Source revision

| | |
|---|---|
| Upstream | `github.com/LMCache/LMCache`, `csrc/` |
| Revision | `43a3318e1c4ee471fd69316ba47a2aee3916c124` (2026-07-21) |
| License | Apache-2.0 |
| Modifications | None. Every file below is byte-identical to that revision. |

The revision is **deliberately not upstream's latest.** It is the revision that
the LMCache build Maru has been measured against was compiled from, so a
vendored-vs-`lmcache.c_ops` comparison isolates the packaging change. Upstream
has since moved these files on in four commits — `#4220` and `#4351` add engine
KV formats, `#4200` optimises a retrieve path, and `#4431` refactors
`EngineKVFormat` onto a `KVFormatSpec` and so changes the API surface. Picking
those up is a separate step that needs its own on-device comparison.

## Files

| File | Role |
|---|---|
| `csrc/engine_kv_format.h` | Paged-cache axis orders the kernels index with |
| `csrc/kv_transfer_types.h` | Transfer direction |
| `csrc/mem_kernels.cuh` / `.cu` | Token-granular placement, all layers or one |
| `csrc/mp_mem_kernels.cuh` / `.cu` | Block-granular placement |
| `csrc/pybind.cpp` | **Maru's own.** Binds the three entry points Maru calls |

`mem_kernels.cu` also defines four entry points Maru never calls (two SGLang
variants and two the upstream header marks deprecated), and `mp_mem_kernels.cu`
defines a transfer-plan executor Maru never calls. They are compiled and left
unbound. Deleting them would shrink the build, but it would also mean the files
are no longer byte-identical to a known upstream revision, which is what lets a
refresh be a plain copy and a diff. The build cost is paid once per install.

## Refreshing

1. Copy the files listed above, minus `pybind.cpp`, from the target revision.
2. Re-read `pybind.cpp` against the new headers: a signature or enum change
   upstream shows up here as a compile error, which is the intent.
3. Update the revision table above, including what moved and why.
4. Re-run the connector unit tests, then compare against the previous revision
   on device before adopting. A refresh changes kernel behaviour and cannot be
   validated by unit tests alone.
