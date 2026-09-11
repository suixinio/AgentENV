# Guest memory on 2 MiB hugepages

Status: accepted 2026-09-11, implemented behind `[memory_snapshot].huge_pages`
(default off). Written after reading pandastack's `agent/internal/firecracker/hugepages.go`
and e2b's template build config (`HugePages`), both of which ship it.

## What it buys and what it does not

A resume through the `uffd` backend takes one page fault per missing page.
On 2 MiB hugetlbfs pages that is 512 times fewer faults than on 4 KiB pages,
and each fault is one 2 MiB read from the memory image instead of one 4 KiB
read. That is the benefit both reference implementations cite and the one
this proposal is after.

The other benefit of hugepages, fewer TLB misses and cheaper EPT walks, does
not arrive yet: with `track_dirty_pages = true`, which the `uffd` backend
requires today, KVM maps the guest at 4 KiB granularity regardless of the
host backing (Firecracker's `docs/hugepages.md`). It arrives with the second
step of `2026-09-10-memory-uffd-backend.md`, where dirty pages come from
userfaultfd write protection and KVM dirty logging is switched off.

## Constraints that shape the design

Firecracker fixes `huge_pages` at boot in the machine config; a snapshot of a
hugepage VM restores as a hugepage VM, and only through a `Uffd` memory
backend (`GuestMemoryFromFileError::HugetlbfsSnapshot` for a file backend).
So:

- The setting is per node for fresh boots (`[memory_snapshot].huge_pages`)
  and per snapshot afterwards: `SnapshotPublishMetadata` and
  `CommittedSnapshot` carry `huge_pages`, set from the sandbox record, which
  takes it from the node config at create and from the snapshot at resume.
  Template builds take the node's setting for a fresh build and the base
  snapshot's for a derived one. This is pandastack's marker file and e2b's
  build config in the catalog instead of on disk.
- `huge_pages = true` requires `backend = "uffd"` (config validation), and a
  hugepage snapshot arriving at a node on the block backend is refused at
  resume with a message that says why.
- The host must be able to hand out 2 MiB pages. Firecracker maps guest
  memory `MAP_NORESERVE` and takes pages from the pool on demand; a fault that
  finds none is a `SIGBUS` in the VMM. `--setup-host` sets
  `vm.nr_overcommit_hugepages` to cover all of RAM, which lets the kernel
  assemble pages on demand. pandastack's notes are explicit that on-demand
  assembly fails once free memory is fragmented and only a boot-time
  reservation (`vm.nr_hugepages`) is immune; sizing that reservation is a
  capacity decision left to the operator, and the startup check warns when
  it is absent. A node resuming a hugepage snapshot runs the same check.
- The balloon reports free pages at 4 KiB, so it cannot return hugepage
  backing to the host; `REMOVE` events still arrive and are handled, they
  just do not shrink RSS.

## Handler

The handler already served whatever page size the handshake named. Two things
change: the install slots scale with the page size (`max_inflight` is sized
for 4 KiB pages; 2 MiB pages get one sixteenth of the slots with a floor of
eight, so the bytes in flight and the buffer pool stay near the configured
figure), and the hugetlb path has a test that maps a real 2 MiB region and
skips on a host with no pool.

## Not in this proposal

- e2b's `MADV_COLLAPSE` of envd's heap before pause, which lowers the fault
  count further by making the guest's own memory contiguous.
- A per-template or per-sandbox switch. The recorded flag makes one possible
  later without a catalog change; today every fresh boot on a node follows
  the node.
