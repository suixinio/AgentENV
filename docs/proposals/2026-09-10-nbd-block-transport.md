# NBD as the block transport, with the layer format unchanged

Status: in progress. Written 2026-09-10 as the answer to the third open item of
`2026-09-09-ublk-transport-resilience.md`. It replaces the kernel-facing half
of the device path and nothing else: overlaybd's LSMT layers, the `image.json`
contract, the daemon protocol, the warm pool and the node-side
`UblkDeviceManager` API all stay as they are.

## What changes and what does not

Today a device is `OverlaybdTarget` (reads and writes against an opened
`ImageFile`, `swap_state` to change the image under a live device) driven by
`UVMUblkDev` (per-queue io_uring workers talking to `/dev/ublkcN`). The target
is transport-agnostic in everything but its trait signature; the transport is
the 1,800 lines of `ctrl.rs`, `dev.rs`, `queue.rs`, `io_buffer.rs` and
`ublk_caps.rs` under `storage/ublk`.

The new crate `storage/nbd` (`uvm-nbd`) provides the same shape over the
kernel nbd driver:

- `NbdTarget`, the trait `OverlaybdTarget` implements, with no ublk types in
  its signature.
- `NbdDevice::start(target, opts)`: one `socketpair` per connection, one worker
  thread per connection (tokio current-thread runtime, `LocalSet`, its own
  `AsyncIoRing`, the same layout as a ublk queue worker), then one generic
  netlink `NBD_CMD_CONNECT` carrying every kernel-side socket. The kernel
  allocates the index and answers with it; `/dev/nbd<index>` is the device.
- `stop` is `NBD_CMD_DISCONNECT`; `update_size` is `NBD_CMD_RECONFIGURE` with
  `NBD_ATTR_SIZE_BYTES`.

Everything downstream is unchanged: Firecracker still receives a block device
path for `path_on_host`, the memory snapshot device is still a read-only block
device handed to the `File` memory backend (the Firecracker fork's `lseek`
size patch covers nbd exactly as it covers ublk), and `RestackSnapshot` is
still `swap_state` inside the daemon with no kernel involvement.

## Why netlink and not the ioctl path

The ioctl path (`NBD_SET_SOCK` + `NBD_DO_IT`) is what pandastack uses: one
socket, one blocking thread, read-only. It cannot carry more than one
connection, cannot resize a live device and cannot reattach a socket to a
device whose server died. Netlink gives all three:

- Multiple sockets per device: each becomes a hardware queue, so the
  parallelism of the ublk per-queue workers is kept.
- `NBD_CMD_RECONFIGURE`: live resize (replaces `UBLK_F_UPDATE_SIZE`) and
  socket replacement, which is the `USER_RECOVERY` equivalent the resilience
  note asked for, available on the 6.1 kernel the build machine runs.
- `NBD_ATTR_TIMEOUT` and `NBD_ATTR_DEAD_CONN_TIMEOUT`: the kernel bounds a
  stalled request instead of wedging the guest forever, which is the property
  ublk lacks today.

The cost is one extra copy in each direction through the socket, against
ublk's registered buffers. The memory snapshot device, whose reads are 4 KiB
page faults, is where this will show first; it is also the device the
resilience note wanted to keep on ublk longest. The decision on that device is
made on benchmark numbers, not now.

## Semantics that must be decided per device kind

| | writable rootfs and extra drives | memory snapshot device |
|---|---|---|
| server flags | `SEND_FLUSH`, `SEND_FUA`, `SEND_TRIM` when the image supports discard | `READ_ONLY` |
| flush | forwarded to `ImageFile::sync`, as `0f98414` intended | never sent |
| timeout | must exceed the overlaybd download gate budget, or a slow first fetch turns into a guest `EIO` | same |
| connections | default 4 | default 4, revisit with benchmarks |

Answering flush in the daemon keeps guest `fsync` durability. e2b's choice of
connecting without `SEND_FLUSH` is the other option and is a configuration
flag, not a code change.

## Phases

1. `storage/nbd` crate with its own test suite against real `/dev/nbdN` on
   the build machine, plus the overlaybd target adapter. No consumer changes.
   Running today.
2. Daemon integration. `storage/ublk-daemon` gains a `BlockDevice` that is
   either the ublk or the nbd device, chosen by `--transport ublk|nbd`
   (config `[ublk].transport`, default `ublk`). Device creation, pool refill,
   `UpdateSize`, `Delete` and shutdown dispatch on it; the protocol, the pool
   and `RestackSnapshot` do not change. The ublk control ring becomes optional.
   The 37 daemon tests run under both transports where the kernel allows.
3. Node and host setup. `aenv-node --setup-host` loads `nbd` and installs the
   `/dev/nbd*` udev rule when the transport is nbd; the daemon needs
   `CAP_SYS_ADMIN` for netlink, which the DaemonSet already grants and
   `scripts/run-with-capabilities.sh` must add for bare-metal installs.
   `docker-setup.sh` stops requiring `ublk_drv` in that mode.
4. Cluster acceptance on pve-mf with writable drives on nbd and the memory
   device on ublk, then the benchmark that decides the memory device, then the
   docs (`architecture.md`, `configuration/reference.md`,
   `sandbox-testing.md`) and the removal of the three `ADD_DEV` workarounds in
   the workflows.

## Not in scope

Renaming `[ublk]`, `UblkDeviceManager` and the daemon binary. The names stay
until the ublk transport is actually removed; renaming while both exist buys
nothing.
