use std::collections::HashSet;
use std::path::PathBuf;

use crate::image::RootfsImageResolver;
use crate::sandbox::{validate_drive_id, validate_mount_path, validate_sub_path, ExtraDrive};
use agentenv_http_server::models;

pub const MIB: u64 = 1024 * 1024;

/// Resolved attached drive with its ExtraDrive definition and optional raw source image config.
#[derive(Debug)]
pub struct ResolvedAttachedDrive {
    pub drive: ExtraDrive,
    /// Raw image config JSON from the source image.
    pub raw_config: Option<serde_json::Value>,
}

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
/// # 🔴 Deliberately split out of [`resolve_attached_drives`]
///
/// Everything here is pure input validation — drive id shape, mount path
/// shape, sub-path shape, uniqueness, `diskSizeMB`'s bounds — and needs no
/// registry access. `sandboxes_cold_post` on `aenv-api` cannot resolve an
/// image (no `regctl`), but it can and must still run these same checks
/// before it ever asks a node to: a caller sending a malformed drive should
/// get a 400 from the machine it talked to, not a registry round trip on
/// another machine followed by a refusal that says nothing about the request.
/// See [`unresolved_attached_drives`], this function's other caller.
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

/// Resolves attached drive declarations into `ResolvedAttachedDrive` values ready for sandbox launch.
pub async fn resolve_attached_drives(
    drives: &[models::AttachedDrive],
    image_resolver: &dyn RootfsImageResolver,
) -> Result<Vec<ResolvedAttachedDrive>, models::Error> {
    let pending = validate_attached_drives(drives)?;

    let resolved_images = futures::future::try_join_all(pending.iter().map(|drive| async move {
        image_resolver
            .resolve(&drive.image)
            .await
            .map(|resolved| (resolved.overlaybd_config_path, resolved.raw_config))
            .map_err(|err| {
                models::Error::new(
                    if err.is_user_error() { 400 } else { 500 },
                    format!(
                        "resolve attached drive '{}' image '{}': {err:#}",
                        drive.drive_id, drive.image
                    ),
                )
            })
    }))
    .await?;

    let mut resolved = Vec::with_capacity(pending.len());
    for (drive, (image_config_path, raw_config)) in pending.into_iter().zip(resolved_images) {
        let mut extra_drive = ExtraDrive::try_new_overlaybd_with_mount_path(
            drive.drive_id,
            image_config_path,
            drive.read_only,
            drive.mount_path,
            drive.sub_path,
        )
        .map_err(bad_request)?;
        if let Some(virtual_size) = drive.virtual_size {
            extra_drive = extra_drive
                .try_with_virtual_size(virtual_size)
                .map_err(bad_request)?;
        }
        resolved.push(ResolvedAttachedDrive {
            drive: extra_drive,
            raw_config,
        });
    }

    Ok(resolved)
}

/// The `aenv-api` counterpart of [`resolve_attached_drives`]: validates the
/// same way, but leaves every drive's image reference unresolved for the node
/// that will build the sandbox to resolve instead.
///
/// 🔴 Used only when `ApiImpl::runs_sandbox_runtime` is false — see the branch
/// it guards in `sandboxes_cold_post`. `aenv-node`, which can resolve images
/// itself, always takes `resolve_attached_drives`, unchanged.
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
