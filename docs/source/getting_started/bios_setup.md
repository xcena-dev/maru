# System Setup

Maru requires CXL memory exposed as **DEV_DAX** (`/dev/dax*`). Follow your
server vendor's CXL setup instructions on each host.

If you use **InfiniteMemory**, first follow its
[BIOS configuration guide](https://xcena-dev.github.io/InfiniteMemory_docs/getting-started.html#bios-configuration).

## Intel (GNR)

For multi-node KV sharing, apply this setting on each participating GNR host:

| Setting | Value |
|---------|-------|
| `Allocating Write Flows` | `Non-Allocating` |

Save the settings and reboot. The menu location depends on your server vendor
and BIOS version.

## AMD (Turin)

Complete your platform's CXL setup, then verify DEV_DAX access below. For
InfiniteMemory, use the BIOS configuration guide linked above.

## Verify after reboot

On every participating host, inspect the CXL/DAX devices:

```bash
# Install the inspection utilities if needed.
sudo apt-get install -y cxl daxctl
sudo cxl list -R -D
sudo daxctl list
ls -l /dev/dax*
```

Confirm that the selected memory is in `devdax` mode and the account running
Maru has read/write access to its local device. If it appears as `system-ram`,
follow your platform's instructions to provision it as DEV_DAX before continuing.

For multi-node sharing, also configure the CXL fabric to expose the same memory
to every host.

BIOS settings alone do not guarantee cross-host cache coherence. See
{doc}`../design_doc/consistency_and_safety` for the data-visibility requirements.

Once device access is verified, continue to {doc}`installation`.
