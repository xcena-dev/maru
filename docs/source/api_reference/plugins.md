# Handler Plugins

Maru core is **vendor-neutral**. Hardware- or vendor-specific behaviour — for
example a CXL device's prefetch/pin hints — does not live in the Maru tree.
Instead it ships as a **separate package** that registers a
`maru.handler_plugins` [entry point](https://packaging.python.org/en/latest/specifications/entry-points/).
`MaruHandler` discovers those packages when it is constructed and calls them at
a few well-defined lifecycle seams. Maru never imports a plugin directly.

The model deliberately follows [vLLM's plugin system](https://blog.vllm.ai/2025/05/12/hardware-plugin.html)
(soft-fail) rather than PyTorch's device-backend autoload (hard-fail): a Maru
plugin is an *optional optimization*. A plugin that fails to load — or whose
hook raises — is **logged and skipped, never propagated**, so a missing or
broken plugin can never break KV-cache operation.

## Registering a plugin

In the plugin package's `pyproject.toml`:

```toml
[project.entry-points."maru.handler_plugins"]
my_plugin = "my_pkg.plugin:MyPlugin"
```

The entry-point value is a zero-argument callable (a class or factory) that
returns an object implementing any subset of `MaruHandlerPlugin`.

## Selecting plugins at runtime

```bash
MARU_PLUGINS=my_plugin,other   # load only these names
                               # unset / empty → load all discovered plugins
```

## The `MaruHandlerPlugin` interface

Every hook is **optional** — a plugin implements only the ones it needs, and
`MaruHandler` skips any that are absent. All hooks are best-effort: an
exception raised inside one is logged and swallowed.

| Hook | When it runs | Purpose |
|------|--------------|---------|
| `on_init(handler)` | end of `MaruHandler.__init__` (not yet connected — `mapper` is `None`) | cheap setup only |
| `on_batch_retrieve(handler, keys, batch_resp)` | end of `batch_retrieve`, after regions are mapped | issue hints against live mapped memory; runs on the hot path |
| `on_close(handler)` | during `close`, while regions are still mapped and RPC is live | release resources tied to mapped memory |
| `contribute_stats() -> dict \| None` | during `get_stats` | returns a dict merged under `stats["plugins"][<PluginClassName>]` |

In `on_batch_retrieve`, `keys[i]` corresponds to `batch_resp.entries[i]`. A
found entry exposes a `handle` (region/offset) whose region is already mapped,
so a plugin can act on real memory addresses.

## Stable accessor surface

```{admonition} Contract — do not break without deprecation
:class: warning
The following `MaruHandler` methods, together with the four hook signatures
above, are the **public, stable plugin API**. Plugins live in separate
packages on independent release cycles, so renaming, removing, or changing the
behaviour of any of these is a breaking change for every plugin in the field
and must go through a deprecation cycle. `tests/unit/test_plugin_loader.py`
contains a contract test that fails CI if this surface drifts.
```

Everything a plugin needs about a *batch* arrives through the
`on_batch_retrieve` arguments. These two methods cover the region-mapping state
that is not in those arguments:

| Method | Returns |
|--------|---------|
| `MaruHandler.is_region_mapped(region_id: int) -> bool` | whether the region is currently mmap'd |
| `MaruHandler.get_region_dax_path(region_id: int) -> str \| None` | the DAX device path for a mapped region |

```{note}
`on_batch_retrieve` fires for every *found* entry whose region is mapped —
and **both** owned regions (this handler's own allocations) and shared regions
(mmap'd from another instance) are registered in the mapper. So a handler that
stores and then retrieves its own keys will invoke the hook on those owned
regions too; the region-mapping accessors do not distinguish owned from shared.
A plugin that should act only on shared regions must filter them itself — the
public `get_owned_region_ids()` lists the owned ones (note it is outside the
minimal stable accessor contract above).
```

## Writing a plugin

A hardware/vendor plugin typically implements `on_batch_retrieve` to issue
device hints (prefetch/pin) against the mapped regions of a batch, and
`on_close` to release any device resources before regions are unmapped. See
[`maru_handler/plugin.py`](https://github.com/xcena-dev/maru/blob/main/maru_handler/plugin.py)
for the full interface definition.
