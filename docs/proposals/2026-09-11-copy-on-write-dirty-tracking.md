# Copy-on-write dirty tracking for the block memory backend

Status: proposed 2026-09-11, step one implemented and measured on the build
machine. Written after reading how pandastack and e2b each choose a memory
model (`agent/internal/firecracker/hugepages.go`, `pkg/service/service_info.go`)
and measuring both models on one host. Revisits the pause half of
`2026-09-10-memory-uffd-backend.md`.

## What this changes

That proposal names two costs of the block path. The second one —

> `mincore` reports resident pages, not written pages: a page read from a
> parent layer is copied into the new layer as if the guest had written it,
> so layers grow with the working set.

— is not a property of block devices. It is a property of asking `mincore`.
A `MAP_PRIVATE` file mapping already distinguishes the two states in the
VMM's own page tables, and `/proc/<pid>/pagemap` already exposes the bit:

- An untouched page still points at the image's page cache: `PM_FILE` set.
- The first guest write copies it to an anonymous page: `PM_FILE` clear.

So `resident && !PM_FILE` is the written set, with no KVM dirty log, no
userfaultfd, and no Firecracker patch. This is the same page-table fact
pandastack relies on when it keeps a template on 4 KiB pages specifically to
stay "CoW-shareable via mem_file_path MAP_PRIVATE".

`DirtySource` in `storage/uffd/src/pagemap.rs` now names the two ways a
written page is marked, and `guest_regions_backed_by` in `proc_maps.rs`
recovers the region list from `/proc/<pid>/maps` — Firecracker maps the image
`MAP_PRIVATE` once per architectural region with the image offset accumulating
across them (`vstate/memory.rs::create`), and we run no jailer, so the map
lines carry what a uffd handshake would carry.

## The predicate is exact, and more exact than the dirty log

Probed directly on a private mapping of a file and of a loop block device,
100 pages per group:

| group | present + PM_FILE | present, anonymous | absent |
|---|---|---|---|
| read only | 100 | 0 | 0 |
| written | 0 | 100 | 0 |
| never touched | 0 | 0 | 100 |
| written, then `MADV_DONTNEED` | 0 | 0 | 50 |

The last row is the balloon's shape: after the hole the page reads back as the
image's bytes, so the write is gone and leaving it out of the layer is right.

Cross-checked in a live VM against `GET /vm/dirty-memory-ranges` with
`track_dirty_pages = true`, over two successive incremental pauses:

```
only_pagemap=0  only_reference=91  absent=66  file=25  anon=0  swap=0
only_pagemap=0  only_reference=97  absent=74  file=23  anon=0  swap=0
```

`anon = 0` is the completeness criterion, and the code states it: a page the
pause leaves out whose private copy still holds a guest write would be data
loss, and there were none. The 91-97 pages the KVM dirty log reports and this
pause does not are all pages whose current contents *are* the image's — either
discarded by the balloon or never diverged. The dirty log remembers that a
write once happened; the pause needs to know whether the page still differs.
The predicate is the more accurate of the two, and it drops the balloon's
retracted writes for free.

## How much the predicate saves, measured in the same pause

A stashed baseline run tells you little here, because the layer size is not
logged on the old path and a whole-test wall clock is mostly noise. Instead
`DirtySource::Resident` names the third predicate -- every faulted page,
written or not, which is exactly what `mincore` reports -- so one pause
computes both and logs them side by side as `bytes` and `resident_bytes`.
The extra pagemap scan costs well under a millisecond against a pause of
hundreds, and it keeps the saving visible on every pause rather than only in
this document.

Over the four incremental pauses the `fc` suite performs:

| written | what `mincore` would have copied | ratio |
|---|---|---|
| 10.5 MiB | 41.9 MiB | 4.0x |
| 10.3 MiB | 41.5 MiB | 4.0x |
| 9.2 MiB | 40.6 MiB | 4.4x |
| 8.1 MiB | 41.9 MiB | 5.2x |

The ratio is not the interesting number; the shape of the two columns is. The
`mincore` column is nearly constant because it reports the resident set, which
depends on the VM rather than on what the guest did, while the written column
tracks the workload. So the ratio grows with guest memory and with idleness,
and these VMs are small and short-lived. A 512 MiB sandbox that has been
running a while has a resident set far above 40 MiB and a written set that
does not grow with it.

## What the two memory models cost

Four processes each touching the same 512 MiB, one host:

| | Cached | AnonPages | MemAvailable | sum(Pss) |
|---|---|---|---|---|
| `MAP_PRIVATE` file | +526 MiB | 0 | **+12 MiB** | **512 MiB** |
| anonymous install | +536 MiB | +2107 MiB | **-2086 MiB** | 2560 MiB |

`MemAvailable` is what `memory_used_bytes` reports (`observability/host.rs`
computes `MemTotal - MemAvailable`), so the same working set reads as nothing
under one model and as 2 GiB under the other. Note also that `sum(Rss)` under
the file model is 2050 MiB for 512 MiB of real memory: RSS counts a shared
page once per mapper, so it is the wrong unit for comparing the two.

Successive VMs touching that same 512 MiB, started one after another:

| | VM 1 | VM 2 | VM 3 | VM 4 |
|---|---|---|---|---|
| `MAP_PRIVATE` file | 6.64 us/page | **0.31** | 0.32 | 0.32 |
| anonymous install | 8.96 us/page | 1.91 | 0.82 | 0.83 |

The second VM onward faults minor against the page cache. The anonymous column
is a floor, not the uffd path: it is an in-process `memcpy` with no VM exit and
no handler round trip, where the real handler measures 21 us/page single
threaded and 6 us at four threads. Against 0.31 us the gap in a fan-out is one
to two orders of magnitude.

Reading the pagemap costs 0.80 ms for 512 MiB and 2.48 ms for 2 GiB, against a
pause of 200-700 ms.

## What this leaves for the uffd backend

Hugepage snapshots, which Firecracker restores only through a userfaultfd
backend, and a host whose memory image is not local. Both are cases where
sharing is unavailable anyway, which is exactly the division pandastack draws.
The uffd path keeps its write-protect dirty tracking; the two now differ only
in `DirtySource`.

The node-level `[memory_snapshot].backend` should become a default that a
snapshot's own properties can override, rather than a constant that makes a
hugepage snapshot unschedulable on a block node (`config.rs::from_runnable_snapshot`
refuses it today). That is the next step and is not in this change.

## Not done

- PVM. The predicate is host memory management with no KVM involvement, so
  there is no reason for it to differ, and nothing has been run there.
- A guest-memory content check across a pause/resume chain. The integration
  suite verifies a disk marker, not memory, so `anon = 0` is currently the
  strongest statement available and it is a statement about page state rather
  than about bytes.
- The cross-check runs only when `track_dirty_pages` is set, which now buys
  nothing else. It should probably become a first-class verification mode
  rather than a side effect of an experimental flag.
