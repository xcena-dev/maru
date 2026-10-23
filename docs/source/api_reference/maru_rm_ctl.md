# Resource Manager Admin (`maru-rm-ctl`)

`maru-rm-ctl` controls a running Resource Manager (RM): it shows which DAX devices are served and why others are not, hands devices over to other owners, and releases allocations that the RM cannot reclaim on its own. It is installed with the RM.

It talks to the RM over the admin socket `<state-dir>/admin.sock` (default `/var/lib/maru-resourced/admin.sock`), never over the RM's TCP port. Only root or the RM's own user may connect, so run it with `sudo` when the RM runs as a system service.

```bash
sudo maru-rm-ctl [--state-dir DIR | --socket PATH] COMMAND [ARGS]
```

Devices are named `daxN.M` or `/dev/daxN.M`. The RM serves device `daxN.M` as pool `N`.

## Commands

| Command | What it does |
|---|---|
| `status` | Served pools, devices that are not served and why, excluded devices, checkpoint state |
| `allocations [daxN.M]` | Allocations of every pool, served or not, and whether each owner is still running |
| `exclude daxN.M` | Stop using a device for good, e.g. to hand it to another owner. Refused while any allocation on it is live |
| `include daxN.M` | Judge an excluded or refused device again, against every other device as at startup, and serve it if it passes |
| `include daxN.M --reinit` | Start the device over with a new UUID, discarding whatever it holds (also over a ROOF filesystem or another header). Refused while an allocation on it may still be in use |
| `include daxN.M --adopt` | Accept the Maru header of an excluded device that has no identity record yet (excluded by an older RM) |
| `include daxN.M --restore-header` | Write the recorded UUID back to a device whose header alone was cleared, when the rest of its identity still matches |
| `discard daxN.M [--confirm [--force]]` | Release every allocation of the device's pool without opening the device. Without `--confirm` it only lists them |
| `release REGION --confirm [--force]` | Release one allocation, e.g. of a remote owner that died while the RM was down |
| `wipe daxN.M --confirm` | Zero all of an excluded device, header included. Waits until done, which can take minutes; Ctrl-C stops it and leaves the device partly zeroed |
| `drop-holds NODE [--confirm]` | List the regions the remote CXL agent of memory node NODE holds. A freed region stays out of use while held; `--confirm` drops the holds once that node's agent is gone for good |

`discard` and `release` refuse while a local owner process is still running. `--force` releases anyway, for a PID that is running but is not the owner, such as a PID reused after a reboot. Before either command, stop every process that maps the regions on every host: the RM cannot see remote owners.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Done |
| 1 | Refused by the RM; the reason is printed |
| 2 | The RM is not reachable |
| 3 | Sent, but no reply in time. The command may still complete; check `status` |
| 4 | Done, but not on disk yet: a power loss could undo it. Run the same command again until it exits 0 |

## Reading `status`

```text
pool=0 device=/dev/dax0.0 state=active total=1000725282816 free=1000723185664 allocations=1
device=/dev/dax1.0 pool=1 state=refused live=2 owners_alive=1 reason=device header lost while 1 allocation(s) may still be in use
device=/dev/dax2.0 pool=2 state=skipped live=0 owners_alive=0 reason=ROOF filesystem on device, not registering it
device=/dev/dax3.0 state=excluded
checkpoints=enabled
```

| Line | Meaning |
|---|---|
| `state=active` | Served |
| `state=refused` | Not served and not written. `live` allocations are recorded on its pool, and `owners_alive` of them may still be in use. The RM judges the device again every 10 s and on `SIGHUP` |
| `state=skipped` | Not served: formatted by ROOF, it could not be read, or the pool records in the state directory could not be listed |
| `state=busy` | An operation on the device has not finished, e.g. a read or write that timed out |
| `state=excluded` | Excluded with `exclude`; never opened. `write_pending=yes` means an earlier write to it timed out and may still land: do not hand it over yet |
| `excluded_list=not-durable` | The excluded list is not on disk yet; `exclude` and `wipe` refuse until it is |

## Procedures

### Hand a device to another owner

1. Stop the processes that use the device on every host.
2. `sudo maru-rm-ctl exclude daxN.M`. Hand the device over only after it exits 0. It refuses with the live allocations listed while any remain.
3. Optionally `sudo maru-rm-ctl wipe daxN.M --confirm`, so that the new owner does not see Maru data.

To take it back later: `sudo maru-rm-ctl include daxN.M --reinit`.

### A device is refused

`status` gives the reason. The common ones:

| Reason | What to do |
|---|---|
| Header lost, or layout changed (e.g. the device was resized), while allocations may still be in use | Stop the owners listed by `allocations daxN.M` on every host. A local owner that exits is noticed by the next rescan, which starts the device over. For remote owners, run `discard daxN.M --confirm` once they are stopped |
| Header UUID differs from the recorded UUID | If an `include --reinit` was interrupted, run it again. If the reason names another pool, device numbering changed: fix the numbering instead |
| Header cleared, identity tags still match | `include daxN.M --restore-header` |

After `discard`, the device is judged again at the next rescan (within 10 s), or at once with `include daxN.M`.

### Downgrade to an earlier RM

An earlier RM does not read the pool records this version writes. Stop all clients, check that `sudo maru-rm-ctl allocations` prints nothing, stop the RM, move `<state-dir>/pool_*.meta` aside, then start the earlier version.

See the [Resource Manager design](../design_doc/maru_resource_manager.md), section 4, for how devices are judged.
