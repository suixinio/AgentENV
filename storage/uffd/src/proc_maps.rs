//! Guest memory regions read from `/proc/<pid>/maps`.
//!
//! Firecracker's file-backed memory backend maps the memory image
//! `MAP_PRIVATE` in one mapping per architectural region, with the image
//! offset accumulating across them, so the map lines carry the same region
//! list a userfaultfd handshake would -- without a handshake.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use crate::handshake::GuestRegionUffdMapping;

/// The private read-write mappings of `backing` in `pid`, ordered by image
/// offset. Empty when the process maps the path nowhere.
pub fn guest_regions_backed_by(pid: u32, backing: &Path) -> Result<Vec<GuestRegionUffdMapping>> {
    let path = format!("/proc/{pid}/maps");
    let content = fs::read_to_string(&path).with_context(|| format!("read {path}"))?;
    parse_regions(&content, backing)
}

fn parse_regions(maps: &str, backing: &Path) -> Result<Vec<GuestRegionUffdMapping>> {
    let backing = backing.to_string_lossy();
    let mut out = Vec::new();
    for line in maps.lines() {
        let mut fields = line.split_whitespace();
        let (Some(range), Some(perms), Some(offset), Some(_dev), Some(_inode), Some(pathname)) = (
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
        ) else {
            continue;
        };
        if pathname != backing {
            continue;
        }
        // Guest memory is PROT_READ | PROT_WRITE and MAP_PRIVATE; any other
        // mapping of the same path is a different use of the descriptor.
        if !matches!(perms.as_bytes(), [b'r', b'w', _, b'p', ..]) {
            continue;
        }
        let (start, end) = range
            .split_once('-')
            .with_context(|| format!("address range in {line:?}"))?;
        let start =
            u64::from_str_radix(start, 16).with_context(|| format!("start address in {line:?}"))?;
        let end =
            u64::from_str_radix(end, 16).with_context(|| format!("end address in {line:?}"))?;
        let offset =
            u64::from_str_radix(offset, 16).with_context(|| format!("file offset in {line:?}"))?;
        out.push(GuestRegionUffdMapping {
            base_host_virt_addr: start,
            size: end.saturating_sub(start),
            offset,
            page_size: 4096,
            page_size_kib: 4,
            guest_phys_addr: 0,
        });
    }
    out.sort_by_key(|r| r.offset);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAPS: &str = "\
55a0c0000000-55a0c0021000 r--p 00000000 fd:01 1179651                    /usr/bin/firecracker
7f1000000000-7f10c0000000 rw-p 00000000 00:2f 471                        /dev/nbd0
7f1100000000-7f1140000000 rw-p c0000000 00:2f 471                        /dev/nbd0
7f1200000000-7f1200021000 r--p 00000000 00:2f 471                        /dev/nbd0
7f1300000000-7f1340000000 rw-s 00000000 00:2f 472                        /dev/nbd1
7ffd10000000-7ffd10021000 rw-p 00000000 00:00 0                          [stack]
";

    #[test]
    fn picks_private_read_write_mappings_of_the_backing_path() {
        let regions = parse_regions(MAPS, Path::new("/dev/nbd0")).unwrap();
        assert_eq!(regions.len(), 2);
        assert_eq!(regions[0].base_host_virt_addr, 0x7f1000000000);
        assert_eq!(regions[0].size, 0xc000_0000);
        assert_eq!(regions[0].offset, 0);
        assert_eq!(regions[1].base_host_virt_addr, 0x7f1100000000);
        assert_eq!(regions[1].size, 0x4000_0000);
        assert_eq!(regions[1].offset, 0xc000_0000);
    }

    #[test]
    fn skips_read_only_and_shared_mappings() {
        assert!(parse_regions(MAPS, Path::new("/dev/nbd1"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn unmapped_path_yields_no_regions() {
        assert!(parse_regions(MAPS, Path::new("/dev/nbd9"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn regions_come_back_ordered_by_image_offset() {
        let reversed = "\
7f1100000000-7f1140000000 rw-p c0000000 00:2f 471                        /dev/nbd0
7f1000000000-7f10c0000000 rw-p 00000000 00:2f 471                        /dev/nbd0
";
        let regions = parse_regions(reversed, Path::new("/dev/nbd0")).unwrap();
        assert_eq!(regions[0].offset, 0);
        assert_eq!(regions[1].offset, 0xc000_0000);
    }
}
