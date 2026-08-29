pub mod catalog_write;
pub mod common;
pub mod oss;
pub mod posixfs;

use std::sync::Arc;

use anyhow::{Context, Result};

use crate::cfg::{AppConfig, SnapshotImageStoragePolicy, SnapshotRepositoryBackendKind};
use crate::snapshot::repository::interfaces::SnapshotCatalog;
use crate::snapshot::repository::interfaces::SnapshotRuntimeResolver;
use crate::snapshot::repository::SnapshotRepository;
pub use catalog_write::{CatalogRefusal, CatalogWrite};
use posixfs::posixfs_artifacts_only_repository;

/// Whether this process holds a snapshot catalog at all.
///
/// 🔴 A parameter and not a compile-time constant because
/// [`build_snapshot_backend`] is in the crate both binaries link. Each of them
/// passes one literal: `aenv-api` [`AsConfigured`](CentralCatalogUse::AsConfigured),
/// `aenv-node` [`Never`](CentralCatalogUse::Never).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CentralCatalogUse {
    /// `aenv-api`: the `PostgresSnapshotCatalog` over the shared `[pg]` pool.
    ///
    /// 🔴 Required, not optional. PostgreSQL is the only snapshot catalog there
    /// is — object storage held one until Stage B's cutover and holds byte
    /// artifacts alone now — so an api replica assembled without `[pg]` has no
    /// catalog and can answer no snapshot request. [`build_snapshot_backend`]
    /// refuses to assemble under this arm rather than starting a process whose
    /// first snapshot call is the one that discovers it.
    AsConfigured,
    /// `aenv-node`: none, ever.
    ///
    /// 🔴 A node queries no catalog at all. Both of its request-time reads
    /// (`create`'s `Source::Snapshot` arm, `build_template`'s
    /// `Base::BaseSnapshotRef` arm) are pre-resolved by api and sent down with
    /// the request, and the row for a snapshot a node captures is written by
    /// api's `commit_staged` — a node only ever `stage`s the bytes.
    ///
    /// The repository this half assembles therefore carries
    /// [`NoSnapshotCatalog`][crate::snapshot::repository::no_catalog::NoSnapshotCatalog],
    /// which refuses. It cannot hold the real one: `[pg]` is the deciding
    /// half's alone, and `aenv-node` does not link `sqlx`
    /// (`make check-crate-boundaries`).
    Never,
}

/// Everything the snapshot layer needs from storage, assembled.
pub struct AssembledSnapshotBackend {
    pub repository: Arc<SnapshotRepository>,
    /// 🔴 `None` in the half that runs no sandbox runtime — `aenv-api`.
    /// Resolving a snapshot is not a lookup: it downloads
    /// `vm_state.bin` onto this machine's disk, materializes the memory and
    /// rootfs overlaybd `image.json` files, and leases all of it in this
    /// process's local artifact cache. An api replica boots nothing, so it
    /// builds none of that; see [`build_catalog_only_storage`].
    pub runtime_resolver: Option<Arc<dyn SnapshotRuntimeResolver>>,
}

/// Puts the configured catalog in front of the byte half the caller built.
///
/// 🔴 The composition is here rather than inside a backend because it is not a
/// property of any backend: the rows live in PostgreSQL and the bytes live in
/// object storage or on a POSIX filesystem, and neither half knows about the
/// other. That is what the catalog/artifact trait split bought.
pub fn build_snapshot_backend(
    // 🔴 Built by the caller — see [`RoleStorage`]. Which byte half exists at
    // all is the calling binary's decision, and now its crate's.
    storage: RoleStorage,
    // 🔴 `None` in `aenv-node` always — that binary holds no `[pg]` pool (see
    // `crates/aenv-node/src/bin/aenv-node.rs::refuse_configured_pg_dsn`, and
    // the dependency graph it is a belt over). In `aenv-api` this is `None`
    // only when `[pg].dsn` is unset, which is a misconfiguration this refuses
    // to start under.
    //
    // 🔴 An already-built catalog rather than the `sqlx::PgPool` it comes from:
    // constructing it is the deciding half's business, and this function is
    // shared. Build one with
    // [`pg_snapshot_catalog`][postgres::pg_snapshot_catalog].
    pg: Option<Arc<dyn SnapshotCatalog>>,
    central: CentralCatalogUse,
) -> Result<AssembledSnapshotBackend> {
    let (repository, runtime_resolver) = storage;

    if central == CentralCatalogUse::Never {
        // The repository already carries `NoSnapshotCatalog` — see
        // [`CentralCatalogUse::Never`].
        return Ok(AssembledSnapshotBackend {
            repository,
            runtime_resolver,
        });
    }

    let catalog = pg.context(
        "no snapshot catalog is configured: [pg].dsn is unset and PostgreSQL is the only \
         snapshot catalog there is. Object storage held one until the Stage B cutover and \
         holds byte artifacts alone now, so starting without [pg] would leave every snapshot \
         and template request with nowhere to read or write a row. Set [pg].dsn for this half \
         — it is TOML-file-only, with no environment binding (confique cannot descend into \
         AppConfig::pg's Option), so supply it through the file AENV_CONFIG_PATH names or an \
         AENV_CONFIG_OVERLAY_PATH overlay, the way deploy/k8s/base's pg-dsn.toml and \
         deploy/docker-compose.yml's /tmp/agentenv-pg/pg-dsn.toml both do",
    )?;

    let node_id = crate::identity::local_node_id();
    tracing::info!(
        target: "agentenv",
        node_id = %node_id,
        "snapshot catalog is served solely by PostgreSQL; object storage holds no catalog rows"
    );
    Ok(AssembledSnapshotBackend {
        repository: Arc::new(SnapshotRepository::on_node(
            catalog,
            repository.artifacts(),
            node_id,
        )),
        runtime_resolver,
    })
}

/// The two storage halves an assembly hands [`build_snapshot_backend`]: the
/// durable repository, and a runtime resolver only for a process that has
/// somewhere to run a sandbox.
///
/// 🔴 Built by the caller, not here. `aenv-node` builds both halves
/// ([`build_node_storage`][storage::build_node_storage]); `aenv-api` builds
/// the first and passes `None` for the second
/// ([`build_catalog_only_storage`]), because resolving a snapshot is not a
/// lookup — it downloads `vm_state.bin` onto local disk, materializes the
/// memory and rootfs overlaybd `image.json` files and leases all of it in a
/// node-local artifact cache. That machinery, and the overlaybd layer store
/// it drags in, is not linked into the api binary at all.
///
/// 🔴 The `SnapshotRepository` in here carries no usable catalog in either
/// half — both are assembled over
/// [`NoSnapshotCatalog`][crate::snapshot::repository::no_catalog::NoSnapshotCatalog].
/// [`build_snapshot_backend`] is what puts PostgreSQL in front of the api
/// half's byte store; the node half keeps the refusal.
pub type RoleStorage = (
    Arc<SnapshotRepository>,
    Option<Arc<dyn SnapshotRuntimeResolver>>,
);

/// The durable byte half — byte *lifecycle*, with nothing that turns bytes into
/// something a VM can mmap, and no catalog.
///
/// Both backends already had the seam: POSIX's is
/// [`posixfs_artifacts_only_repository`], and the OSS one is
/// [`oss_durable_parts`][oss::oss_durable_parts]. What each of them *doesn't*
/// build is the resolver, which is the only consumer of the overlaybd layer
/// store, the shared artifact cache, and the runtime cache root.
///
/// 🔴 Delete stays on this side on purpose, and that is why this arm still
/// gets a real artifact store rather than a stub. A snapshot's origin node can
/// be gone — hard death, or simply rolled — and a delete that had to be
/// dispatched there would leave the row removed, the bytes orphaned, and
/// nobody holding a record of either.
pub fn build_catalog_only_storage(config: &AppConfig) -> Result<RoleStorage> {
    Ok((build_artifacts_only_repository(config)?, None))
}

fn build_artifacts_only_repository(config: &AppConfig) -> Result<Arc<SnapshotRepository>> {
    match config.snapshot.repository_backend {
        SnapshotRepositoryBackendKind::PosixFs => {
            let root = config
                .backend
                .posix_fs
                .as_ref()
                .context("backend.posix_fs config is required when repository_backend = posix_fs")?
                .snapshot_store
                .join("repository");
            Ok(Arc::new(posixfs_artifacts_only_repository(&root)))
        }
        SnapshotRepositoryBackendKind::Oss => {
            let oss_config = config
                .backend
                .oss
                .as_ref()
                .context("backend.oss config is required when repository_backend = oss")?;
            Ok(
                oss::oss_durable_parts(oss_config, snapshot_image_storage_policy(config))?
                    .into_repository(),
            )
        }
    }
}

/// Whether committed snapshot rootfs/drive deltas are published back to the
/// source registry or kept as object-storage managed layers.
pub fn snapshot_image_storage_policy(config: &AppConfig) -> SnapshotImageStoragePolicy {
    if config.snapshot.image_publish.enabled {
        SnapshotImageStoragePolicy::SourceRegistry
    } else {
        SnapshotImageStoragePolicy::ObjectStorage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::mock::MockSnapshotCatalog;
    use crate::snapshot::repository::interfaces::SnapshotArtifactStore;
    use crate::snapshot::types::SnapshotId;

    fn storage() -> RoleStorage {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let root = dir.keep();
        (
            Arc::new(posixfs_artifacts_only_repository(&root)),
            None::<Arc<dyn SnapshotRuntimeResolver>>,
        )
    }

    /// 🔴 The startup error the removal of the object-storage catalog made
    /// necessary. Before it, an api replica with no `[pg]` assembled happily
    /// over an object-storage catalog; there is no such catalog any more, so
    /// the same configuration now has *nothing* behind it, and the failure has
    /// to be at assembly rather than at the first snapshot request.
    #[test]
    fn the_deciding_half_refuses_to_assemble_without_postgresql() {
        let Err(error) = build_snapshot_backend(storage(), None, CentralCatalogUse::AsConfigured)
        else {
            panic!("an api half with no [pg] holds no catalog and must not start");
        };
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("[pg]"),
            "the error must name the setting an operator has to add: {rendered}"
        );
    }

    /// 🔴 And it must not name an environment variable that does not exist.
    ///
    /// This message said "Configure [pg] (or AENV_PG_DSN)" for as long as the
    /// refusal has existed, and nothing reads that name: `AppConfig::pg` is an
    /// `Option<PgConfig>`, and confique reaches a field from the environment
    /// only through `#[config(nested)]`, which may not be optional — so no
    /// field under `[pg]` can carry an `env =` binding at all (see
    /// `src/cfg.rs`'s own note). An operator who followed it would export the
    /// variable, restart, and get the identical error back.
    ///
    /// The positive half is what makes the negative one actionable, and it is
    /// deliberately the same three facts `build_pg_pool`'s message carries
    /// (`crates/aenv-api/src/bin/aenv-api.rs`): the setting is `[pg].dsn`, it
    /// is TOML-file-only, and it arrives through `AENV_CONFIG_PATH` or an
    /// `AENV_CONFIG_OVERLAY_PATH` overlay.
    #[test]
    fn the_refusal_names_no_environment_variable_that_does_not_exist() {
        let Err(error) = build_snapshot_backend(storage(), None, CentralCatalogUse::AsConfigured)
        else {
            panic!("an api half with no [pg] holds no catalog and must not start");
        };
        let rendered = format!("{error:#}");
        assert!(
            !rendered.contains("AENV_PG_DSN"),
            "there is no such environment variable: {rendered}"
        );
        assert!(rendered.contains("[pg].dsn"), "{rendered}");
        assert!(rendered.contains("TOML-file-only"), "{rendered}");
        assert!(rendered.contains("AENV_CONFIG_OVERLAY_PATH"), "{rendered}");
        assert!(rendered.contains("pg-dsn.toml"), "{rendered}");
    }

    /// The other direction: a catalog is handed over, and it is the one the
    /// repository then reads through.
    ///
    /// 🔴 Asserted on `MockSnapshotCatalog::get_calls`, not on the answer: what
    /// this test is for is *which* catalog the assembly wired in, and a
    /// returned value would be the same whether the read reached the handed-over
    /// catalog or the `NoSnapshotCatalog` the byte half arrives carrying. The
    /// counter can only move if the former happened.
    #[tokio::test]
    async fn the_deciding_half_reads_the_catalog_it_was_handed() {
        let catalog = Arc::new(MockSnapshotCatalog::default());
        assert_eq!(catalog.get_calls(), 0);

        let assembled = build_snapshot_backend(
            storage(),
            Some(Arc::clone(&catalog) as Arc<dyn SnapshotCatalog>),
            CentralCatalogUse::AsConfigured,
        )
        .expect("an api half with a catalog assembles");

        assert!(assembled.runtime_resolver.is_none());
        let _ = assembled
            .repository
            .get(&SnapshotId::generate().to_string())
            .await;
        assert_eq!(
            catalog.get_calls(),
            1,
            "the assembled repository read through some other catalog than the one it was handed"
        );
    }

    /// 🔴 The node half assembles with no catalog and is not refused for it —
    /// and what it holds refuses rather than reporting absence. Absence is what
    /// callers act on by deleting artifacts and refusing resumes; that is the
    /// failure the object-storage catalog left behind on this half after the
    /// cutover, and the reason it is an error now.
    #[tokio::test]
    async fn the_running_half_assembles_with_a_catalog_that_refuses() {
        let assembled = build_snapshot_backend(storage(), None, CentralCatalogUse::Never)
            .expect("a node half assembles without a catalog");

        let error = assembled
            .repository
            .get("anything")
            .await
            .expect_err("a node must not answer a catalog read");
        assert!(
            matches!(
                error,
                crate::snapshot::repository::RepositoryError::Unsupported { .. }
            ),
            "a node's catalog read must refuse, never report absence: {error}"
        );

        // The byte half is real on this arm: delete still has to work where
        // the bytes are.
        let _: Arc<dyn SnapshotArtifactStore> = assembled.repository.artifacts();
    }
}
