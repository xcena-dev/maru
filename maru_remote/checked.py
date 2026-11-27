# SPDX-License-Identifier: Apache-2.0
"""UCX settings that make registration revocation a memory fence."""

from collections.abc import MutableMapping

_CHECKED = frozenset(
    {"rc", "rc_v", "rc_x", "rc_verbs", "rc_mlx5", "dc", "dc_x", "dc_mlx5", "self"}
)
_AUX = frozenset({"ud", "ud_v", "ud_x", "ud_verbs", "ud_mlx5"})
_DEFAULT = "rc,dc,cuda_copy"


class UncheckedTransportError(RuntimeError):
    """The transport does not guarantee registration revocation."""


def require_checked_transport(environ: MutableMapping[str, str]) -> str:
    """Validate the transport and disable registration caching before NIXL starts."""
    cache = environ.get("UCX_RCACHE_ENABLE", "").strip().lower()
    if not cache:
        environ["UCX_RCACHE_ENABLE"] = "n"
    elif cache not in ("n", "no", "0"):
        raise UncheckedTransportError("UCX registration cache must be off")
    value = environ.get("UCX_TLS", "").strip()
    if not value:
        environ["UCX_TLS"] = _DEFAULT
        return _DEFAULT
    items = [item.strip() for item in value.split(",") if item.strip()]
    for item in items:
        name, _, flag = item.partition(":")
        if item == "cuda_copy" or name in _CHECKED or (name in _AUX and flag == "aux"):
            continue
        raise UncheckedTransportError("UCX_TLS must use key-checked RC/DC transports")
    if "cuda_copy" in items and not any(item in _CHECKED - {"self"} for item in items):
        raise UncheckedTransportError("cuda_copy needs an RC/DC transport")
    return value
