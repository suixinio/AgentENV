use std::collections::HashSet;
use std::path::PathBuf;

use crate::sandbox::{validate_drive_id, validate_mount_path, validate_sub_path, ExtraDrive};
use agentenv_http_server::models;

pub const MIB: u64 = 1024 * 1024;

struct PendingAttachedDrive {
    drive_id: String,
    read_only: bool,
    mount_path: PathBuf,
    sub_path: Option<PathBuf>,
    virtual_size: Option<u64>,
    image: String,
}

/// Validates and deduplicates attached drive declarations, without resolving
/// any of their source images.
///
/// # 🔴 Validation without registry access, on purpose
///
/// Everything here is pure input validation — drive id shape, mount path
/// shape, sub-path shape, uniqueness, `diskSizeMB`'s bounds — and needs no
/// registry access. `sandboxes_cold_post` cannot resolve an image (no
/// `regctl` on the deciding half), but it can and must still run these checks
/// before it ever asks a node to: a caller sending a malformed drive should
/// get a 400 from the machine it talked to, not a registry round trip on
/// another machine followed by a refusal that says nothing about the request.
/// See [`unresolved_attached_drives`], this function's caller.
fn validate_attached_drives(
    drives: &[models::AttachedDrive],
) -> Result<Vec<PendingAttachedDrive>, models::Error> {
    let mut pending = Vec::with_capacity(drives.len());
    let mut drive_ids = HashSet::new();
    let mut mount_paths = HashSet::new();

    for drive in drives {
        let drive_id = drive.drive_id.trim();
        validate_drive_id(drive_id).map_err(bad_request)?;
        if !drive_ids.insert(drive_id.to_string()) {
            return Err(models::Error::new(
                400,
                format!("duplicate attached drive driveID: {drive_id}"),
            ));
        }

        let mount_path = drive
            .mount_path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| ExtraDrive::default_mount_path(drive_id));
        validate_mount_path(&mount_path).map_err(bad_request)?;
        let sub_path = drive
            .sub_path
            .as_deref()
            .map(validate_sub_path)
            .transpose()
            .map_err(bad_request)?;
        if !mount_paths.insert(mount_path.clone()) {
            return Err(models::Error::new(
                400,
                format!(
                    "duplicate attached drive mountPath: {}",
                    mount_path.display()
                ),
            ));
        }
        // In cold launches, ExtraDrive::virtual_size carries the optional API
        // target size. The source image's actual/base size is resolved by the
        // ublk daemon because prepare_extra_drives(Fresh) passes known=None.
        let virtual_size = virtual_size_from_disk_size_mb(drive.disk_size_mb)?;

        let image = drive.source.image.trim().to_string();
        if image.is_empty() {
            return Err(models::Error::new(
                400,
                format!(
                    "attached drive '{}' source requires exactly one image",
                    drive_id
                ),
            ));
        }

        pending.push(PendingAttachedDrive {
            drive_id: drive_id.to_string(),
            read_only: drive.read_only.unwrap_or(true),
            mount_path,
            sub_path,
            virtual_size,
            image,
        });
    }

    Ok(pending)
}

/// Validates every attached drive declaration and leaves its image reference
/// unresolved, for the node that will build the sandbox to resolve instead.
///
/// 🔴 The only shape left. A `resolve_attached_drives` sibling used to sit
/// here, resolving each drive's image through a `RootfsImageResolver` for the
/// half that ran the sandbox in-process; `sandboxes_cold_post` chose between
/// the two on `ApiImpl::runs_sandbox_runtime`. That arm is deleted — the
/// deciding half never took it, and the running half answers this route with
/// 404 (`crate::api::role_gate`) and creates over gRPC, where
/// `NodeSandboxService::create` does its own resolution.
pub fn unresolved_attached_drives(
    drives: &[models::AttachedDrive],
) -> Result<Vec<crate::sandbox::UnresolvedAttachedDrive>, models::Error> {
    let pending = validate_attached_drives(drives)?;
    Ok(pending
        .into_iter()
        .map(|drive| crate::sandbox::UnresolvedAttachedDrive {
            image_ref: drive.image,
            drive_id: drive.drive_id,
            mount_path: drive.mount_path,
            sub_path: drive.sub_path,
            read_only: drive.read_only,
            virtual_size: drive.virtual_size,
        })
        .collect())
}

fn bad_request(err: anyhow::Error) -> models::Error {
    models::Error::new(400, err.to_string())
}

pub fn virtual_size_from_disk_size_mb(
    disk_size_mb: Option<u32>,
) -> Result<Option<u64>, models::Error> {
    let Some(disk_size_mb) = disk_size_mb else {
        return Ok(None);
    };
    if disk_size_mb < 1024 || !disk_size_mb.is_multiple_of(1024) {
        return Err(models::Error::new(
            400,
            "attached drive diskSizeMB must be at least 1024 and divisible by 1024".to_string(),
        ));
    }
    u64::from(disk_size_mb)
        .checked_mul(MIB)
        .map(Some)
        .ok_or_else(|| {
            models::Error::new(400, "attached drive diskSizeMB overflows bytes".to_string())
        })
}

/// The validation every attached-drive declaration goes through.
///
/// 🔴 These moved here from `crates/aenv-node/src/tests/`, where they lived
/// because they drove a real `ImageResolver` over a fake `regctl` through
/// `resolve_attached_drives`. That function is deleted with the cold-create
/// arm that called it; what it and [`unresolved_attached_drives`] shared —
/// [`validate_attached_drives`], the whole of what these tests ever asserted —
/// is reached through the surviving one, and needs nothing from the running
/// half.
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn drive(
        drive_id: &str,
        source: models::AttachedDriveSource,
        read_only: Option<bool>,
        mount_path: Option<&str>,
    ) -> models::AttachedDrive {
        drive_with_sub_path(drive_id, source, read_only, mount_path, None)
    }

    fn drive_with_sub_path(
        drive_id: &str,
        source: models::AttachedDriveSource,
        read_only: Option<bool>,
        mount_path: Option<&str>,
        sub_path: Option<&str>,
    ) -> models::AttachedDrive {
        drive_with_sub_path_and_disk_size(drive_id, source, read_only, mount_path, sub_path, None)
    }

    fn drive_with_sub_path_and_disk_size(
        drive_id: &str,
        source: models::AttachedDriveSource,
        read_only: Option<bool>,
        mount_path: Option<&str>,
        sub_path: Option<&str>,
        disk_size_mb: Option<u32>,
    ) -> models::AttachedDrive {
        models::AttachedDrive {
            drive_id: drive_id.to_string(),
            read_only,
            mount_path: mount_path.map(ToString::to_string),
            sub_path: sub_path.map(ToString::to_string),
            disk_size_mb,
            source,
        }
    }

    fn source_image(image_ref: &str) -> models::AttachedDriveSource {
        models::AttachedDriveSource::new(image_ref.to_string())
    }

    #[test]
    fn rejects_missing_source() {
        let err = unresolved_attached_drives(&[drive(
            "data",
            models::AttachedDriveSource::new("".to_string()),
            None,
            None,
        )])
        .expect_err("blank image should fail");

        assert!(err.message.contains("requires exactly one"));
    }

    #[test]
    fn rejects_duplicates_and_invalid_mount_paths() {
        let duplicate_id = unresolved_attached_drives(&[
            drive("data", source_image("img"), None, None),
            drive("data", source_image("img"), None, Some("/mnt/other")),
        ])
        .expect_err("duplicate id should fail");
        assert!(duplicate_id
            .message
            .contains("duplicate attached drive driveID"));

        let duplicate_mount = unresolved_attached_drives(&[
            drive("data", source_image("img"), None, Some("/mnt/shared")),
            drive("logs", source_image("img"), None, Some("/mnt/shared")),
        ])
        .expect_err("duplicate mount should fail");
        assert!(duplicate_mount
            .message
            .contains("duplicate attached drive mountPath"));

        // mount_path validation fires before source validation.
        let invalid_mount = unresolved_attached_drives(&[drive(
            "data",
            source_image("img"),
            None,
            Some("/proc/data"),
        )])
        .expect_err("reserved mount path should fail");
        assert!(invalid_mount.message.contains("reserved path"));
    }

    #[test]
    fn sub_path_validation() {
        // Valid values pass through unchanged.
        assert_eq!(
            validate_sub_path("workspace/data").expect("valid sub_path"),
            PathBuf::from("workspace/data"),
        );
    }

    #[test]
    fn rejects_invalid_sub_paths() {
        // Strict mode: empty / whitespace-padded values are *not* normalised
        // into "absent"; they are rejected with 400 unchanged.
        let cases: &[(Option<&str>, &str)] = &[
            (Some(""), "subPath must not be empty"),
            (Some("   "), "whitespace"),
            (Some(" workspace/data "), "whitespace"),
            (Some("/workspace/data"), "relative"),
            (Some("workspace/../etc"), "'..'"),
            // ':' must be rejected: it is the cmdline separator in
            // `agentenv_drives=vd<letter>:<mountPath>[:<subPath>]`.
            (Some("workspace:data"), "colons"),
        ];

        for (input, needle) in cases {
            let err = unresolved_attached_drives(&[drive_with_sub_path(
                "data",
                source_image("img"),
                None,
                None,
                *input,
            )])
            .unwrap_err();
            assert_eq!(err.code, 400, "sub_path {input:?}");
            assert!(
                err.message.contains(needle),
                "sub_path {input:?}: expected message to contain {needle:?}, got {:?}",
                err.message,
            );
        }
    }

    #[test]
    fn disk_size_mb_validation() {
        assert_eq!(
            virtual_size_from_disk_size_mb(None).expect("omitted size should pass"),
            None,
        );
        assert_eq!(
            virtual_size_from_disk_size_mb(Some(2048)).expect("valid size should pass"),
            Some(2048 * MIB),
        );

        for disk_size_mb in [0, 512, 1536] {
            let err = virtual_size_from_disk_size_mb(Some(disk_size_mb))
                .expect_err("invalid disk size should fail");
            assert_eq!(err.code, 400);
            assert!(err.message.contains("diskSizeMB"));
        }
    }

    /// 🔴 `diskSizeMB` is checked in the same first pass as every other field,
    /// before anything downstream is asked to resolve the drive's image.
    #[test]
    fn rejects_invalid_disk_size_mb_with_the_rest_of_validation() {
        for disk_size_mb in [0, 512, 1536] {
            let err = unresolved_attached_drives(&[drive_with_sub_path_and_disk_size(
                "data",
                source_image("img"),
                None,
                None,
                None,
                Some(disk_size_mb),
            )])
            .expect_err("invalid disk size should fail");
            assert_eq!(err.code, 400);
            assert!(err.message.contains("diskSizeMB"));
        }
    }
}
