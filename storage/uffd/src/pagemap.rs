//! The written pages of a running VM, read from `/proc/<pid>/pagemap`.
//!
//! Two memory models mark a written page differently, so the caller picks
//! the bit to read with [`DirtySource`]. Neither needs a KVM dirty log.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::handshake::GuestRegionUffdMapping;

const PM_PRESENT: u64 = 1 << 63;
const PM_SWAP: u64 = 1 << 62;
const PM_UFFD_WP: u64 = 1 << 57;
const PM_FILE: u64 = 1 << 61;
/// pagemap describes 4 KiB pages whatever backs them; a huge page appears as
/// its subpages with the same flags.
const PAGEMAP_PAGE: u64 = 4096;
const ENTRIES_PER_READ: usize = 8192;

/// How a written page is told apart from one the guest never touched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirtySource {
    /// Guest memory served over userfaultfd: the handler installs every page
    /// write-protected and the first guest write clears the bit, so a
    /// resident page without it was written.
    UffdWriteProtect,
    /// Guest memory mapped `MAP_PRIVATE` from the memory image: an untouched
    /// page still points at the image's page cache (`PM_FILE`), and the first
    /// write copies it to an anonymous page, so a resident page without
    /// `PM_FILE` was written. A page swapped out is anonymous by definition
    /// here -- a clean file page is dropped rather than swapped -- so it
    /// counts as written.
    PrivateFileCow,
    /// Every resident page, written or not. This is what Firecracker's
    /// `mincore` readout reports; it is not a dirty set, and a pause uses it
    /// only to record how much that readout would have copied.
    Resident,
}

impl DirtySource {
    fn is_written(self, entry: u64) -> bool {
        if entry & (PM_PRESENT | PM_SWAP) == 0 {
            return false;
        }
        match self {
            Self::UffdWriteProtect => entry & PM_UFFD_WP == 0,
            Self::PrivateFileCow => entry & PM_FILE == 0,
            Self::Resident => true,
        }
    }
}

/// A written run of guest memory, as an offset into the memory image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirtyRange {
    pub host_addr: u64,
    pub image_offset: u64,
    pub len: u64,
}

/// The pages of `regions` that `source` reports as written by the guest,
/// coalesced into runs ordered by image offset.
pub fn dirty_ranges(
    pid: u32,
    regions: &[GuestRegionUffdMapping],
    source: DirtySource,
) -> Result<Vec<DirtyRange>> {
    let path = format!("/proc/{pid}/pagemap");
    let file = File::open(&path).with_context(|| format!("open {path}"))?;
    dirty_ranges_in(&file, regions, source).with_context(|| format!("read {path}"))
}

fn dirty_ranges_in(
    pagemap: &File,
    regions: &[GuestRegionUffdMapping],
    source: DirtySource,
) -> Result<Vec<DirtyRange>> {
    let mut regions: Vec<&GuestRegionUffdMapping> = regions.iter().collect();
    regions.sort_by_key(|m| m.offset);
    let mut out: Vec<DirtyRange> = Vec::new();
    let mut buf = vec![0u8; ENTRIES_PER_READ * 8];
    for region in regions {
        if region.base_host_virt_addr % PAGEMAP_PAGE != 0 || region.size % PAGEMAP_PAGE != 0 {
            bail!(
                "region at {:#x} (+{}) is not page aligned",
                region.base_host_virt_addr,
                region.size
            );
        }
        let first_page = region.base_host_virt_addr / PAGEMAP_PAGE;
        let pages = region.size / PAGEMAP_PAGE;
        let mut done = 0u64;
        while done < pages {
            let batch = (pages - done).min(ENTRIES_PER_READ as u64) as usize;
            let bytes = &mut buf[..batch * 8];
            pagemap
                .read_exact_at(bytes, (first_page + done) * 8)
                .with_context(|| {
                    format!(
                        "pagemap entries for {:#x}",
                        (first_page + done) * PAGEMAP_PAGE
                    )
                })?;
            for (i, entry) in bytes.chunks_exact(8).enumerate() {
                let entry = u64::from_ne_bytes(entry.try_into().expect("8 bytes"));
                if !source.is_written(entry) {
                    continue;
                }
                let page = done + i as u64;
                let host_addr = region.base_host_virt_addr + page * PAGEMAP_PAGE;
                let image_offset = region.offset + page * PAGEMAP_PAGE;
                match out.last_mut() {
                    Some(last)
                        if last.image_offset + last.len == image_offset
                            && last.host_addr + last.len == host_addr =>
                    {
                        last.len += PAGEMAP_PAGE;
                    }
                    _ => out.push(DirtyRange {
                        host_addr,
                        image_offset,
                        len: PAGEMAP_PAGE,
                    }),
                }
            }
            done += batch as u64;
        }
    }
    Ok(out)
}

/// Whether this kernel reports the uffd-wp bit at all: it is a fixed
/// property of `pagemap`, but a build without `CONFIG_PTE_MARKER_UFFD_WP`
/// never sets it, so a caller wanting to trust an empty dirty set can look.
pub fn pagemap_readable(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}/pagemap")).exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn mapping(base: u64, size: u64, offset: u64) -> GuestRegionUffdMapping {
        GuestRegionUffdMapping {
            base_host_virt_addr: base,
            size,
            offset,
            page_size: 4096,
            page_size_kib: 4096,
            guest_phys_addr: 0,
        }
    }

    // A file standing in for pagemap: entry i describes page i.
    fn fake_pagemap(entries: &[u64]) -> File {
        let mut file = tempfile::tempfile().unwrap();
        for entry in entries {
            file.write_all(&entry.to_ne_bytes()).unwrap();
        }
        file
    }

    #[test]
    fn present_pages_without_the_wp_bit_are_dirty_and_runs_coalesce() {
        let entries = [
            PM_PRESENT | PM_UFFD_WP, // clean
            PM_PRESENT,              // dirty
            PM_PRESENT,              // dirty, same run
            0,                       // never faulted
            PM_SWAP,                 // written, then swapped out
            PM_PRESENT | PM_UFFD_WP, // clean
        ];
        let file = fake_pagemap(&entries);
        let ranges = dirty_ranges_in(
            &file,
            &[mapping(0, 6 * 4096, 1 << 20)],
            DirtySource::UffdWriteProtect,
        )
        .unwrap();
        assert_eq!(
            ranges,
            vec![
                DirtyRange {
                    host_addr: 4096,
                    image_offset: (1 << 20) + 4096,
                    len: 2 * 4096
                },
                DirtyRange {
                    host_addr: 4 * 4096,
                    image_offset: (1 << 20) + 4 * 4096,
                    len: 4096
                },
            ]
        );
    }

    #[test]
    fn private_file_pages_are_clean_until_copy_on_write_makes_them_anonymous() {
        let entries = [
            PM_PRESENT | PM_FILE, // read only, still the image's page
            PM_PRESENT,           // written, copied to an anonymous page
            PM_PRESENT,           // written, same run
            0,                    // never faulted
            PM_SWAP,              // written, then swapped out
            PM_PRESENT | PM_FILE, // read only
        ];
        let file = fake_pagemap(&entries);
        let ranges = dirty_ranges_in(
            &file,
            &[mapping(0, 6 * 4096, 0)],
            DirtySource::PrivateFileCow,
        )
        .unwrap();
        assert_eq!(
            ranges,
            vec![
                DirtyRange {
                    host_addr: 4096,
                    image_offset: 4096,
                    len: 2 * 4096
                },
                DirtyRange {
                    host_addr: 4 * 4096,
                    image_offset: 4 * 4096,
                    len: 4096
                },
            ]
        );
    }

    #[test]
    fn residency_is_every_faulted_page_whatever_its_bits_say() {
        let file = fake_pagemap(&[
            PM_PRESENT | PM_FILE,
            PM_PRESENT,
            0,
            PM_SWAP,
            PM_PRESENT | PM_UFFD_WP,
        ]);
        let ranges =
            dirty_ranges_in(&file, &[mapping(0, 5 * 4096, 0)], DirtySource::Resident).unwrap();
        let bytes: u64 = ranges.iter().map(|r| r.len).sum();
        assert_eq!(bytes, 4 * 4096, "every page but the never-faulted one");
    }

    #[test]
    fn the_two_sources_read_different_bits_of_the_same_entry() {
        // A page carrying both bits: written under CoW, clean under uffd-wp.
        let file = fake_pagemap(&[PM_PRESENT | PM_UFFD_WP]);
        let region = [mapping(0, 4096, 0)];
        assert!(
            dirty_ranges_in(&file, &region, DirtySource::UffdWriteProtect)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            dirty_ranges_in(&file, &region, DirtySource::PrivateFileCow)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn regions_are_walked_in_image_order_and_never_merged_across_a_gap() {
        // Two regions whose host order is the reverse of their image order,
        // adjacent in the image but not in host memory.
        let entries = [PM_PRESENT, PM_PRESENT, PM_PRESENT, PM_PRESENT];
        let file = fake_pagemap(&entries);
        let regions = [
            mapping(2 * 4096, 2 * 4096, 0),
            mapping(0, 2 * 4096, 2 * 4096),
        ];
        let ranges = dirty_ranges_in(&file, &regions, DirtySource::UffdWriteProtect).unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!((ranges[0].image_offset, ranges[0].len), (0, 2 * 4096));
        assert_eq!(
            (ranges[1].image_offset, ranges[1].len),
            (2 * 4096, 2 * 4096)
        );
    }

    #[test]
    fn an_unaligned_region_is_refused() {
        let file = fake_pagemap(&[PM_PRESENT]);
        assert!(dirty_ranges_in(
            &file,
            &[mapping(100, 4096, 0)],
            DirtySource::UffdWriteProtect
        )
        .is_err());
    }
}
