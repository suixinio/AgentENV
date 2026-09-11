//! The written pages of a VM served with write protection, read from
//! `/proc/<pid>/pagemap`: a page the handler installed carries the uffd-wp
//! bit, and the first guest write clears it (in place under `WP_ASYNC`, by
//! the handler's unprotect under synchronous write protection). A page that
//! is present without the bit is therefore a page the guest wrote.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::handshake::GuestRegionUffdMapping;

const PM_PRESENT: u64 = 1 << 63;
const PM_SWAP: u64 = 1 << 62;
const PM_UFFD_WP: u64 = 1 << 57;
/// pagemap describes 4 KiB pages whatever backs them; a huge page appears as
/// its subpages with the same flags.
const PAGEMAP_PAGE: u64 = 4096;
const ENTRIES_PER_READ: usize = 8192;

/// A written run of guest memory, as an offset into the memory image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirtyRange {
    pub host_addr: u64,
    pub image_offset: u64,
    pub len: u64,
}

/// The pages of `regions` that are present in `pid` without the uffd-wp
/// bit, coalesced into runs ordered by image offset.
pub fn dirty_ranges(pid: u32, regions: &[GuestRegionUffdMapping]) -> Result<Vec<DirtyRange>> {
    let path = format!("/proc/{pid}/pagemap");
    let file = File::open(&path).with_context(|| format!("open {path}"))?;
    dirty_ranges_in(&file, regions).with_context(|| format!("read {path}"))
}

fn dirty_ranges_in(pagemap: &File, regions: &[GuestRegionUffdMapping]) -> Result<Vec<DirtyRange>> {
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
                let resident = entry & (PM_PRESENT | PM_SWAP) != 0;
                if !resident || entry & PM_UFFD_WP != 0 {
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
        let ranges = dirty_ranges_in(&file, &[mapping(0, 6 * 4096, 1 << 20)]).unwrap();
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
    fn regions_are_walked_in_image_order_and_never_merged_across_a_gap() {
        // Two regions whose host order is the reverse of their image order,
        // adjacent in the image but not in host memory.
        let entries = [PM_PRESENT, PM_PRESENT, PM_PRESENT, PM_PRESENT];
        let file = fake_pagemap(&entries);
        let regions = [
            mapping(2 * 4096, 2 * 4096, 0),
            mapping(0, 2 * 4096, 2 * 4096),
        ];
        let ranges = dirty_ranges_in(&file, &regions).unwrap();
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
        assert!(dirty_ranges_in(&file, &[mapping(100, 4096, 0)]).is_err());
    }
}
