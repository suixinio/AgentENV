# The ublk transport: what it costs, and what is left to decide

Status: open question, no decision yet. Written 2026-09-09 after the acceptance of
`refactor/api-half-cleanup` found that dev commit `0f98414` broke every writable
drive on real kernels while its unit test stayed green.

## What happened

`0f98414` made writable Firecracker drives `Writeback` and had the ublk device
declare a volatile cache, so a guest `fsync` reaches `OverlaybdTarget`'s flush
handler and the upper layer's `sync()`. The kernel describes a flush with
`start_sector = -1` and `nr_sectors = 0`; `request_meta` shifted the -1 into an
offset past the device and answered `EINVAL`. ext4 could not write its
superblock at mount, and no sandbox with a writable user image booted. The unit
test built the descriptor by hand with `start_sector = 0`. The worker
integration suite caught it (0 of 19 VMs booted); nothing that runs on the
build machine could, because the build machine's kernel has no ublk. Fixed in
`d89d811`.

## How the two reference implementations avoid the class

- **pandastack-ai** (`agent/internal/nbdstream/server_linux.go`) chose the
  in-kernel NBD driver over ublk and says why: ublk's `ADD_DEV` kernel-panicked
  across its production GCP 6.17 kernel line, with both the upstream ublksrv C
  daemon and a pure-Go daemon; NBD is in-tree and a crashed server cannot panic
  the host, the worst case being a blocked reader recovered by the kernel
  `io_timeout`. It exposes only a read-only streamed rootfs through NBD, routes
  writes through dm-snapshot, and answers `NBD_CMD_FLUSH` / `TRIM` as no-ops.
- **e2b-infra** (`packages/orchestrator/pkg/sandbox/nbd/path_direct.go`)
  connects its NBD device without `NBD_FLAG_SEND_FLUSH`, so the block layer
  completes flushes itself and no `NBD_CMD_FLUSH` reaches userspace; durability
  is a `fsync` plus `BLKFLSBUF` on the device descriptor at pause, export and
  teardown, counted in a metric. Its backend is a plain `ReadAt`/`WriteAt`
  interface over chunked object-storage cache, with no io_uring.

Both keep flush semantics out of userspace. AgentENV does the opposite on
purpose: a guest `fsync` lands in the upper layer, so data written before a
pause is durable, not batched to the next boundary.

## Why AgentENV is on ublk

- The I/O path: io_uring submission, one worker thread per queue, and the
  kernel 6.8+ zero-copy buffer registration (`UBLK_F_AUTO_BUF_REG`).
- Memory snapshots are ublk devices: on resume a read-only device built from
  the stacked memory layers is handed to Firecracker as a `File` memory backend
  and shared by refcount across sandboxes of one snapshot
  (`docs/src/internals/architecture.md`). Both reference implementations restore
  memory through userfaultfd and have no block-device equivalent of this.

## What it costs

- Kernel 6.8+ with `CONFIG_BLK_DEV_UBLK`; Debian 12's 6.1 cannot run the
  integration suite, so the only real-kernel gate is a cluster node.
- A larger and younger kernel surface than NBD; pandastack's panic is on a
  6.17 line this project has not run, and pve-mf's 6.8 has not shown one.
- Protocol handling that is ours to get right: flush descriptor shape, buffer
  registration, warm-pool state swaps. `0f98414` is one instance.
- No daemon-crash survivability today: `uvm-ublk-daemon` owns every device in
  one process and does not set `UBLK_F_USER_RECOVERY`, so a daemon crash is an
  I/O error on every sandbox on the node. This is the property pandastack calls
  "no-wedge" and designed for.

## To decide

1. Whether to enable `UBLK_F_USER_RECOVERY` (kernel 6.1+; the queue is
   quiesced until a new daemon reattaches) and what the node does during the
   gap, versus accepting the blast radius as it is.
2. Whether the integration suite, or a subset that boots one VM with a writable
   drive, becomes a required gate on a machine with ublk before any change under
   `storage/ublk` or to drive cache configuration merges.
3. Whether the memory-snapshot device, which is read-only and never flushed,
   should be the only ublk consumer, with writable drives moving to a transport
   that keeps flush in the kernel; this trades the guest-`fsync` durability
   above for the resilience the references chose.

Not to decide now: leaving ublk altogether. The memory-snapshot path is the
reason it exists.
