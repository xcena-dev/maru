# System Setup

Maru requires CXL memory exposed as **DEV_DAX** (`/dev/dax*`) on every host
attached to the CXL memory; with the `remote_cxl` backend, these are the memory
nodes. Follow your server vendor's CXL setup instructions on each of these hosts.

## Intel (GNR)

For multi-host KV sharing on Intel Granite Rapids (GNR), configure the BIOS as follows:

- If a **DDIO** control is available, use it to **disable DDIO**.
- Otherwise, set **`Allocating Write Flows`** to **`Non-Allocating`**.

Option names and menu locations vary by server vendor and BIOS version.
If neither option is available, consult your server vendor's documentation.

Apply the setting on every participating host, save the configuration, and reboot.

A `remote_cxl` pool that several memory nodes share is multi-host sharing too:
those memory nodes are the hosts that share the CXL memory, so apply the same
setting on each of them. Worker nodes without CXL need none of the settings on
this page.

## AMD (Turin)

Complete your platform's CXL setup, then verify DEV_DAX access below.

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
