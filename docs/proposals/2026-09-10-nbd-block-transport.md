# NBD as the block transport, with the layer format unchanged

Status: phases 1 to 3 merged on `feat/nbd-block-transport`; phase 4 accepted on
pve-mf on 2026-09-10 (both nodes on `transport = "nbd"`, 17 e2e suites
169 pass / 9 skip / 0 fail, zero daemon warnings), with the memory-device
benchmark and the CI runner change still open. Written 2026-09-10 as the answer
to the third open item of `2026-09-09-ublk-transport-resilience.md`. It replaces the kernel-facing half
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
| timeout | above the overlaybd registry request timeout with room for retries; after it the kernel drops the connection, the device rebuilds it and the kernel retries the request there | same |
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
4. Cluster acceptance on pve-mf. The transport is one daemon-wide switch, so
   the memory device went to nbd together with the writable drives; the
   e2e suites, including the snapshot and template ones, passed on it.
   Switching is one DaemonSet patch (`AENV_UBLK_TRANSPORT=nbd` plus the image)
   after `modprobe nbd` on the hosts; every sandbox on the node dies with the
   node restart, as with any other node roll. The docs are updated.

## What the kernel does, measured on 6.1

The device suite established these and the code is shaped around them:

- A request that outlives `io_timeout` takes its connection down. Without a
  replacement the device answers `EIO` for good; with `dead_conn_timeout` set
  the kernel holds requests that window long while a replacement lands, and
  retries the timed-out request on it. `NbdDevice` therefore supervises its
  connections: a dead one is rebuilt over the same target and put back with
  `NBD_CMD_RECONFIGURE`, one slot at a time after a 500 ms settle, five
  attempts over about 11 s. A read held across a 9 s stall finishes with its
  bytes and no `EIO`.
- `NBD_CMD_RECONFIGURE` reports success and drops the surplus when offered
  more sockets than the kernel has marked dead, which is why the settle and
  the one-at-a-time rule exist.
- `dead_conn_timeout` is a reconnect window, not a teardown timer: an
  abandoned device stays configured at its index, which is what lets a new
  daemon reattach after a crash (`NbdDevice::reattach`); a request in flight
  at the crash is re-sent to the replacement after one `io_timeout`.
- `NBD_CFLAG_DESTROY_ON_DISCONNECT` removes the gendisk and its node for good,
  so it is off by default; indices above `nbds_max` are allocated on demand
  and their nodes persist.
- The supervisor is a task on the caller's runtime; a current-thread runtime
  that blocks on the device starves it. The daemon and the node run
  multi-threaded runtimes.

Thirty-two devices under 256 writers read back byte for byte; `O_DSYNC`,
`fdatasync` and `BLKDISCARD` reach the target as FUA, flush and discard.

## Measured on the build machine (2026-09-10)

fio through `uvm-nbd expose-mem` (an in-memory target, 4 connections, queue
depth 64) against a loop device over tmpfs, both `direct=1`, `libaio`, 12 s per
profile, on a 16-vCPU build host with kernel 6.1. The loop numbers are the
in-kernel ceiling; the gap is the cost of the socket hop and the daemon.

| profile | nbd | loop over tmpfs |
|---|---|---|
| randread 4k, qd32 x 4 jobs | 127k IOPS, 1.0 ms avg | 269k IOPS, 0.47 ms avg |
| randwrite 4k, qd32 x 4 jobs | 41k IOPS, 3.1 ms avg | 240k IOPS, 0.53 ms avg |
| read 1M, qd8 | 1.4 GiB/s | 3.8 GiB/s |
| randread 4k, qd1 | 14k IOPS, 61 us avg | 41k IOPS, 18 us avg |

Reads sit at about half the kernel ceiling; writes at a sixth, because every
write payload crosses the socket and lands in a per-request buffer before the
target sees it. Both are far above what an overlaybd image behind the target
delivers from a registry or a local layer file, so the transport is not the
bottleneck for a sandbox; the write path is the first thing to optimise if it
ever becomes one (a buffer pool, then more connections).

The Firecracker integration suites also ran on this machine under
`AENV_UBLK_TRANSPORT=nbd`: the 14 cases that do not need a snapshot catalog
passed, the 7 that do failed exactly as they do under ublk on a node-only
harness.

## Still open

- The benchmark that compares the memory device under both transports.
  `crates/benchmarks/benches/ublk_overlaybd_benchmark.rs` drives ublk
  in-process and needs an nbd twin; until it exists the memory device stays
  on whichever transport the daemon runs, and pve-mf runs nbd.
- The three workflows that pin `ubuntu-22.04` because of the ublk `ADD_DEV`
  crash. `nbd-tests.yml` already runs on 24.04; moving the integration suite
  there means running it under `AENV_UBLK_TRANSPORT=nbd`, which changes what
  that suite covers and has not been exercised on a runner.
- Under ublk, the `UpdateSize` RPC grows the kernel device without moving the
  target's own bound, so a read past the old end answers `EINVAL`. The nbd
  path sets the bound first. No production caller sends `UpdateSize` today.
- `queue_depth` and `destroy_on_disconnect` are `NbdOptions` fields without
  configuration keys.
- A daemon crash still needs an operator: the device survives at its index
  and `NbdDevice::reattach` is there, but nothing calls it yet. A restarted
  daemon adopting the devices its predecessor left is the next step.

## Not in scope

Renaming `[ublk]`, `UblkDeviceManager` and the daemon binary. The names stay
until the ublk transport is actually removed; renaming while both exist buys
nothing.
