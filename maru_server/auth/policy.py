"""Policy table for role-based authorization.

Loads a YAML config mapping roles to permission bitmasks.
Used by the auth gRPC server to determine what permissions
to grant via perm_grant after mTLS authentication.

Example policy.yaml:
    roles:
      prefill:
        perms: [READ, WRITE, DELETE, ADMIN, IOCTL]
      decode:
        perms: [READ]
      server:
        perms: [GRANT]
      admin:
        perms: [READ, WRITE, DELETE, ADMIN, IOCTL, GRANT]
"""

import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# Permission flag constants (must match marufs_uapi.h)
PERM_FLAGS = {
    "READ": 0x0001,
    "WRITE": 0x0002,
    "DELETE": 0x0004,
    "ADMIN": 0x0008,
    "IOCTL": 0x0010,
    "GRANT": 0x0020,
}


class PolicyTable:
    """Role → permission bitmask mapping."""

    def __init__(self, policy_path: str | Path):
        self._roles: dict[str, int] = {}
        self._load(Path(policy_path))

    def _load(self, path: Path) -> None:
        with open(path) as f:
            data = yaml.safe_load(f)

        roles = data.get("roles", {})
        for role_name, role_def in roles.items():
            perm_names = role_def.get("perms", [])
            bitmask = 0
            for name in perm_names:
                flag = PERM_FLAGS.get(name.upper())
                if flag is None:
                    logger.warning(
                        "Unknown permission %r in role %r, skipping",
                        name,
                        role_name,
                    )
                    continue
                bitmask |= flag
            self._roles[role_name] = bitmask

        logger.info(
            "Loaded policy: %d roles from %s",
            len(self._roles),
            path,
        )

    def lookup(self, role: str) -> int | None:
        """Return permission bitmask for a role, or None if role not found."""
        return self._roles.get(role)

    def has_role(self, role: str) -> bool:
        return role in self._roles

    @property
    def roles(self) -> list[str]:
        return list(self._roles.keys())
