# System Setup

CXL stores require memory exposed as **DEV_DAX** (`/dev/dax*`) on each host mapping the memory. Follow the platform vendor's instructions on CXL-attached workers and on memory nodes using `local_cxl`. GPU workers using remote storage and nodes using only DRAM need none of the CXL settings on this page.

## Intel (GNR)

For multi-host KV sharing on Intel Granite Rapids (GNR), configure the BIOS as follows:

- If a **DDIO** control is available, use it to **disable DDIO**.
- Otherwise, set **`Allocating Write Flows`** to **`Non-Allocating`**.

Option names and menu locations vary by server vendor and BIOS version. If neither option is available, consult your server vendor's documentation.

Apply the setting on every participating host, save the configuration, and reboot.

Apply cross-host visibility settings wherever several hosts directly access the same physical CXL memory. Each memory node owns its regions; the remote store does not map one region through multiple memory nodes.

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

Confirm that the selected memory is in `devdax` mode and the account running Maru has read/write access to its local device. If it appears as `system-ram`, follow your platform's instructions to provision it as DEV_DAX before continuing.

For multi-node sharing, also configure the CXL fabric to expose the same memory to every host.

BIOS settings alone do not guarantee cross-host cache coherence. See {doc}`../design_doc/consistency_and_safety` for the data-visibility requirements.

Once device access is verified, continue to {doc}`installation`.
