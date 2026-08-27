//! Resolving the attached drives a create request names.
//!
//! 🔴 In `aenv-node`: every fixture here builds a real `ImageResolver` over a
//! fake `regctl`, which is this crate's half. The code under test —
//! `aenv_core::api::impls::attached_drives` — is the deciding half's, and is
//! reached through its public path.

use std::path::PathBuf;

use agentenv_http_server::models;

use aenv_core::api::impls::attached_drives::MIB;
use aenv_core::api::impls::attached_drives::*;

use crate::cfg::AppConfig;
use tempfile::TempDir;

fn test_resolver(temp: &TempDir) -> crate::image::ImageResolver {
    let config = AppConfig {
        deps_path: temp.path().join("deps"),
        ..AppConfig::default()
    };
    crate::image::ImageResolver::new(&config)
}

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

#[tokio::test]
async fn rejects_missing_source() {
    let temp = TempDir::new().expect("tempdir");
    let resolver = test_resolver(&temp);

    let err = resolve_attached_drives(
        &[drive(
            "data",
            models::AttachedDriveSource::new("".to_string()),
            None,
            None,
        )],
        &resolver,
    )
    .await
    .expect_err("blank image should fail");

    assert!(err.message.contains("requires exactly one"));
}

#[tokio::test]
async fn rejects_duplicates_and_invalid_mount_paths() {
    let temp = TempDir::new().expect("tempdir");
    let resolver = test_resolver(&temp);

    // duplicate_id and duplicate_mount checks fire in the first-pass loop,
    // before image resolution, so a placeholder image ref is sufficient.
    let duplicate_id = resolve_attached_drives(
        &[
            drive("data", source_image("img"), None, None),
            drive("data", source_image("img"), None, Some("/mnt/other")),
        ],
        &resolver,
    )
    .await
    .expect_err("duplicate id should fail");
    assert!(duplicate_id
        .message
        .contains("duplicate attached drive driveID"));

    let duplicate_mount = resolve_attached_drives(
        &[
            drive("data", source_image("img"), None, Some("/mnt/shared")),
            drive("logs", source_image("img"), None, Some("/mnt/shared")),
        ],
        &resolver,
    )
    .await
    .expect_err("duplicate mount should fail");
    assert!(duplicate_mount
        .message
        .contains("duplicate attached drive mountPath"));

    // mount_path validation fires before source validation.
    let invalid_mount = resolve_attached_drives(
        &[drive("data", source_image("img"), None, Some("/proc/data"))],
        &resolver,
    )
    .await
    .expect_err("reserved mount path should fail");
    assert!(invalid_mount.message.contains("reserved path"));
}

#[test]
fn sub_path_validation() {
    use crate::sandbox::validate_sub_path;
    // Valid values pass through unchanged.
    assert_eq!(
        validate_sub_path("workspace/data").expect("valid sub_path"),
        PathBuf::from("workspace/data"),
    );
}

#[tokio::test]
async fn rejects_invalid_sub_paths() {
    let temp = TempDir::new().expect("tempdir");
    let resolver = test_resolver(&temp);

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
        let err = resolve_attached_drives(
            &[drive_with_sub_path(
                "data",
                source_image("img"),
                None,
                None,
                *input,
            )],
            &resolver,
        )
        .await
        .expect_err(&format!("sub_path {input:?} should fail"));
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

#[tokio::test]
async fn rejects_invalid_disk_size_mb_before_image_resolution() {
    let temp = TempDir::new().expect("tempdir");
    let resolver = test_resolver(&temp);

    for disk_size_mb in [0, 512, 1536] {
        let err = resolve_attached_drives(
            &[drive_with_sub_path_and_disk_size(
                "data",
                source_image("img"),
                None,
                None,
                None,
                Some(disk_size_mb),
            )],
            &resolver,
        )
        .await
        .expect_err("invalid disk size should fail before image resolution");
        assert_eq!(err.code, 400);
        assert!(err.message.contains("diskSizeMB"));
    }
}
