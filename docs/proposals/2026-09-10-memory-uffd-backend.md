# A userfaultfd backend for memory snapshot restore

Status: accepted 2026-09-10, not started. Written after reading pandastack's
`agent/internal/uffd` (about 1k lines of Go, missing-page serving only) and
e2b's `pkg/sandbox/uffd` (about 3.5k lines, write-protect dirty tracking and
copy-on-write export) together with the public e2b Firecracker branch
`firecracker-v1.14-direct-mem`. Companion to
`2026-09-10-nbd-block-transport.md`, whose open item "a transport per device
kind" this proposal closes by taking the memory device off the block path.

## What the block-device path costs today

On resume the stacked overlaybd memory layers become a read-only block device
(ublk or nbd) that Firecracker mmaps as a `File` memory backend. Every guest
page fault on a not-yet-resident page is one 4 KiB read through the block
layer and the transport: 18 us on ublk, 38 us on nbd, measured on one host
(`2026-09-10-nbd-block-transport.md`). Fault concurrency and prefetch are the
kernel's, not ours.

On pause the dirty set comes from `GET /vm/dirty-memory-ranges`, which uses
`mincore` unless `[memory_snapshot].track_dirty_pages` turns on KVM dirty
logging (experimental, disabled for PVM). `mincore` reports resident pages,
not written pages: a page read from a parent layer is copied into the new
layer as if the guest had written it, so layers grow with the working set.

Both halves depend on three of the six patches our two Firecracker builds
carry (`dirty-memory-ranges`, `guest-memory-regions`, the `lseek` size fix
for block devices).

## The design

`[memory_snapshot].backend = "block" | "uffd"`, default `block`, switchable
per node. Under `uffd` no memory device exists; the daemon serves page faults
itself.

### Step 1: missing-page serving, no Firecracker change

The handler lives in `uvm-ublk-daemon`, which already holds the opened memory
image per snapshot by refcount, runs io_uring workers on a multi-threaded
runtime, has `CAP_SYS_ADMIN`, and is the crash domain every VM on the node
shares anyway. A new request `ServeMemoryUffd { image_config, socket_path }`
makes the daemon listen on the socket Firecracker will connect to; the reply
carries nothing the node needs beyond success.

- The node calls `load_snapshot_uffd` with `resume_vm = false`, exactly like
  the file path, so MMDS and the rate limiter are patched before `resume`.
  The handler must be listening before `/snapshot/load`: the load itself
  faults while restoring device and vCPU state.
- Firecracker performs `UFFDIO_API` and `UFFDIO_REGISTER`; the handler
  receives the fd and the region mappings over the socket.
- A fault resolves to `ImageFile::read_at_into_with_ctx` on the stacked image,
  then `UFFDIO_COPY`. A page the LSMT index shows as a hole is served with
  `UFFDIO_ZEROPAGE` and no read. `EEXIST` is benign (a racing worker or the
  prefaulter won).
- N worker threads per VM, a bounded queue, a cancel pipe for teardown.
- Prefetch: the first resume of a snapshot records the pages it faulted into
  `mem_prefetch.json` next to `mem_image.json`; later resumes replay it in
  the background. Both reference implementations measured this as the main
  cold-start win.
- Pause is unchanged in mechanism but must run with KVM dirty logging on:
  every UFFD-installed page is anonymous, so `mincore` would report the whole
  faulted set as dirty. Under PVM that logging is unavailable, so step 1 on
  PVM validates resume only.

### Step 2: exact dirty pages from one 50-line patch

Port commit `e9febb1` of the public e2b branch into both of our builds:
register regions with `MISSING | WRITE_PROTECT` and request
`UFFD_FEATURE_WP_ASYNC`. The handler installs pages with
`UFFDIO_COPY | MODE_WP`; the kernel clears the protection bit in place on the
first guest write. At pause the daemon reads bit 57 of
`/proc/<firecracker pid>/pagemap` for the resident pages and gets exactly the
written set, on KVM and PVM alike, with no KVM dirty log.

After this step `dirty-memory-ranges` and `guest-memory-regions` retire from
our patch set (mappings arrive in the handshake) and the `lseek` fix is
unused by the memory path. The block path keeps needing them until it is
removed.

Kernel floor: `WP_ASYNC` is 6.7. The pve-mf workers run 6.8; the 6.1 build
machine can run step 1 only, so the step 2 suites are gated on the feature
probe, not on a version string.

Status (2026-09-11): implemented, with two departures from the sketch above.
The node, not the daemon, reads the pagemap: it already holds the VMM's pid
for `process_vm_readv`, and the daemon only has to report the handshake
regions and whether the registration carried write protection
(`QueryMemoryUffd`). And the Firecracker patch (`1.15.1-patch-v3`) does not
require `WP_ASYNC`: a kernel that refuses the feature gets synchronous write
protection instead, where the handler answers each first write by
unprotecting the page, and a kernel without userfaultfd write protection at
all (before 5.7) gets the missing-page registration only. Under both
protected modes the written set is the same pagemap readout, so the 6.1
build machine runs the whole path, only with one event per first write. The
handler detects the registration by unprotecting one absent page, so no
configuration ties the daemon to a Firecracker build; the node's
`[memory_snapshot.uffd].write_protect` (default on) says whether a build
without the patch is an error at resume. `dirty-memory-ranges` and
`guest-memory-regions` stay in the patch set for the block path and for
`write_protect = false`.

### Not in this proposal

e2b's `use_memfd` (Firecracker keeps guest memory on a memfd and hands the fd
to the handler, so pause copies dirty pages from a mapping instead of
`process_vm_readv`) and the copy-on-write export window behind its in-place
checkpoint (needs the balloon free-page-reporting pause API, which exists
only in e2b's private repository). Neither changes what our pause model
stores.

## What is given up and what must be checked

- Page-cache sharing. Today sandboxes resumed from one snapshot share one
  read-only device and its clean pages exist once in the page cache. UFFD
  installs private anonymous pages per VM, so the RSS of a warm pool or a
  fork fan-out grows per sandbox. e2b accepts this. The way back is a shared
  memfd mapped `MAP_PRIVATE` per VM with minor-fault serving
  (`UFFDIO_CONTINUE`, shmem since 5.14), which is a new Firecracker patch and
  a later proposal.
- UFFD under PVM is unverified. The step 1 prototype runs on a PVM node
  before anything else.
- A sandbox with a balloon needs `UFFD_FEATURE_EVENT_REMOVE` and a `Removed`
  page state, or a page the guest gave back is refilled from the image on
  its next fault.
- Three numbers decide whether step 1 ships: resume-to-envd-ready latency,
  single-fault latency, and RSS of N sandboxes from one snapshot, each
  against the ublk and nbd memory devices on the same host.

## Firecracker source under our control

The binaries the manifest pins come from `kvcache-ai/firecracker` (branch
`v1.15.1-patch`, release `aenv-deps`) and `kvcache-ai/firecracker-next`
(tag `v1.17.0-next.1`, PVM). On 2026-09-10 both were copied under `suixinio`
so the step 2 patch and any later one land on branches we own:

- `suixinio/firecracker` is a GitHub fork of `firecracker-microvm/firecracker`
  (the kvcache-ai repository is not one, so it has no upstream tags and no
  compare view) carrying the kvcache-ai branches `v1.15.1-patch`,
  `v1.15.1-patch-nestedvirt`, `v1.16.1-patch`, the upstream tags `v1.15.1`,
  `v1.16.1`, `v1.17.0`, and both releases with their assets re-uploaded
  byte for byte. Default branch `v1.15.1-patch`, as before.
- Tag `v1.15.1-patch-v1` marks commit `90288c39` (the pinned binary: Cargo
  version `1.15.1-patch-v1` at the head of the branch when `aenv-deps` was
  published, 2026-07-30); kvcache-ai never tagged it.
- kvcache-ai's `v1.16.1-patch` branch had been reset to plain upstream
  `v1.16.1` after PR #19 (`release: v1.16.1-patch-v1`, 15 commits, 38 files)
  was merged, so the series existed only in `refs/pull/19/head`. It is
  restored as `v1.16.1-patch` and tagged `v1.16.1-patch-v1` on the suixinio
  fork. No binary was ever published for it. That series carries `direct`,
  `guest-memory-regions`, `dirty-memory-ranges` and `pre-fault-memory`; the
  `mem_file_path` and `lseek` patches of the 1.15.1 line are not separate
  commits there and need checking before that line is adopted.
- `suixinio/firecracker-next` is a plain mirror (GitHub allows one fork per
  network per account, and `firecracker-next` is in the Firecracker network
  through `loopholelabs/firecracker`) with the release re-created.

`config/deps_manifest.toml` points at the suixinio repositories for both
Firecracker builds and the KVM guest kernel; the five assets it names were
checked byte for byte against the kvcache-ai originals. The e2b public branch
is checked out at `/home/debian/e2b-firecracker` for cherry-picking; the
kvcache-ai clone with every recovered ref is at `/home/debian/fc-kvcache`.
