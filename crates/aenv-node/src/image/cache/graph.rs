use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use overlaybd::config::{
    lexically_normalize_path, load_image_config as load_overlaybd_image_config,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::warn;

use crate::image::commit_index::{self, CommitIndex};

const IMAGE_CONFIG_SUFFIX: &str = "-image.json";

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ImageCacheHoldOwner {
    namespace: String,
    key: String,
}

impl ImageCacheHoldOwner {
    pub fn new(namespace: impl Into<String>, key: impl Into<String>) -> Result<Self> {
        Ok(Self {
            namespace: non_empty("image cache hold namespace", namespace.into())?,
            key: non_empty("image cache hold key", key.into())?,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl fmt::Display for ImageCacheHoldOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.key)
    }
}

pub fn non_empty(field: &str, value: String) -> Result<String> {
    if value.trim().is_empty() {
        bail!("{field} is empty");
    }
    Ok(value)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheOwnedLocalFileLower {
    pub digest: String,
    pub file: PathBuf,
    pub size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheOwnedImageConfigFacts {
    pub local_file_lowers: Vec<CacheOwnedLocalFileLower>,
    pub upper_file: Option<PathBuf>,
}

/// The image cache's reference graph, rebuilt from the cache directories
/// rather than stored.
///
/// Config references and hard-commit facts come from `configs/` and
/// `indexes/`; holds exist only for the lifetime of the process that took
/// them, which is why a deleting pass needs [`ReclaimAuthority`].
#[derive(Clone, Debug, Default)]
pub struct ImageCacheMetadataStore {
    state: Arc<RwLock<CacheGraph>>,
}

#[derive(Debug, Default)]
struct CacheGraph {
    config_refs: BTreeMap<ImageCacheConfigId, BTreeSet<HardCommitId>>,
    hard_commits: BTreeMap<HardCommitId, HardCommitObjectRecord>,
    holds: BTreeMap<ImageCacheHoldOwner, BTreeSet<HardCommitId>>,
    last_used: BTreeMap<ImageCacheConfigId, u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ImageCacheConfigId(String);

impl ImageCacheConfigId {
    pub fn from_config_path(path: &Path) -> Result<Self> {
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .with_context(|| {
                format!(
                    "image cache config path has no file name: {}",
                    path.display()
                )
            })?;
        Self::from_filename(filename)
    }

    fn from_filename(filename: impl Into<String>) -> Result<Self> {
        let filename = filename.into();
        if !is_regular_config_filename(&filename) {
            bail!(
                "image cache config '{}' is not a source image config",
                filename
            );
        }
        Ok(Self(filename))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ImageCacheConfigId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HardCommitId(String);

impl HardCommitId {
    pub fn new(digest: impl Into<String>) -> Result<Self> {
        let digest = digest.into();
        if digest.trim().is_empty() {
            bail!("hard commit digest is empty");
        }
        Ok(Self(digest))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HardCommitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HardCommitObjectRecord {
    pub digest: HardCommitId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CapacityEvictionPlan {
    pub candidates: Vec<CapacityEvictionCandidate>,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapacityEvictionCandidate {
    pub config_id: ImageCacheConfigId,
    pub last_used: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedHardCommitRef {
    digest: HardCommitId,
    file: PathBuf,
    size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedConfig {
    hard_refs: Vec<ParsedHardCommitRef>,
    /// Seeds LRU recency for a config this process has not resolved yet.
    modified_secs: u64,
}

/// Proof that the startup reconcile ran, which a deleting pass needs because
/// holds do not survive the process that took them.
///
/// [`ImageCacheMetadataStore::grant_reclaim_authority`] is the only source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReclaimAuthority(());

impl ImageCacheMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that the startup reconcile ran, unblocking deleting passes.
    pub async fn grant_reclaim_authority(&self) -> ReclaimAuthority {
        ReclaimAuthority(())
    }

    pub async fn record_hard_commit_object(
        &self,
        digest: HardCommitId,
        file: Option<PathBuf>,
        size: Option<u64>,
    ) -> Result<()> {
        self.state.write().await.hard_commits.insert(
            digest.clone(),
            HardCommitObjectRecord { digest, file, size },
        );
        Ok(())
    }

    pub async fn record_config_refs_from_config_path(&self, config_path: &Path) -> Result<()> {
        let config_id = ImageCacheConfigId::from_config_path(config_path)?;
        let hard_refs = load_cache_owned_hard_commit_refs(config_path)?;
        let mut state = self.state.write().await;
        state.apply_config(&config_id, &hard_refs);
        // This path runs on every resolve (cache hit via record_source_image
        // config root and miss via publish), so it doubles as the LRU "touch".
        // `rebuild_from_configs` does not call it, so reconciles never reset it.
        if hard_refs.is_empty() {
            state.last_used.remove(&config_id);
        } else {
            state.last_used.insert(config_id, unix_now_secs());
        }
        Ok(())
    }

    /// Cache-owned `file=` lowers inside the commit store; remote-recoverable
    /// `dir=` layers and runtime-owned local files are intentionally excluded.
    pub fn commit_store_hard_commit_digests_from_config_path(
        config_path: &Path,
        commit_store: &Path,
    ) -> Result<Vec<HardCommitId>> {
        Ok(
            load_commit_store_owned_hard_commit_refs(config_path, commit_store)?
                .into_iter()
                .map(|reference| reference.digest)
                .collect(),
        )
    }

    pub async fn remove_hard_commit_object(&self, digest: &HardCommitId) -> Result<()> {
        self.state.write().await.hard_commits.remove(digest);
        Ok(())
    }

    pub async fn get_hard_commit_object(
        &self,
        digest: &HardCommitId,
    ) -> Result<Option<HardCommitObjectRecord>> {
        Ok(self.state.read().await.hard_commits.get(digest).cloned())
    }

    pub async fn list_hard_commit_objects(&self) -> Result<Vec<HardCommitObjectRecord>> {
        Ok(self
            .state
            .read()
            .await
            .hard_commits
            .values()
            .cloned()
            .collect())
    }

    pub async fn create_or_replace_hold(
        &self,
        owner: &ImageCacheHoldOwner,
        refs: &BTreeSet<HardCommitId>,
    ) -> Result<()> {
        self.state
            .write()
            .await
            .holds
            .insert(owner.clone(), refs.clone());
        Ok(())
    }

    pub async fn release_hold(&self, owner: &ImageCacheHoldOwner) -> Result<()> {
        self.state.write().await.holds.remove(owner);
        Ok(())
    }

    #[cfg(test)]
    pub async fn list_hold_owners_in_namespaces(
        &self,
        namespaces: &[&str],
    ) -> Result<Vec<ImageCacheHoldOwner>> {
        Ok(self
            .state
            .read()
            .await
            .holds
            .keys()
            .filter(|owner| namespaces.contains(&owner.namespace()))
            .cloned()
            .collect())
    }

    /// Startup cleanup for transient namespaces. Do not pass durable namespaces.
    pub async fn release_holds_in_namespaces(
        &self,
        namespaces: &[&str],
    ) -> Result<Vec<ImageCacheHoldOwner>> {
        let mut state = self.state.write().await;
        let owners: Vec<ImageCacheHoldOwner> = state
            .holds
            .keys()
            .filter(|owner| namespaces.contains(&owner.namespace()))
            .cloned()
            .collect();
        for owner in &owners {
            state.holds.remove(owner);
        }
        Ok(owners)
    }

    pub async fn hard_commit_hold_referrers(
        &self,
        digest: &HardCommitId,
    ) -> Result<Vec<ImageCacheHoldOwner>> {
        Ok(self
            .state
            .read()
            .await
            .holds
            .iter()
            .filter(|(_, refs)| refs.contains(digest))
            .map(|(owner, _)| owner.clone())
            .collect())
    }

    pub async fn hard_commit_config_referrer_map(
        &self,
    ) -> Result<BTreeMap<HardCommitId, Vec<ImageCacheConfigId>>> {
        let mut referrers = BTreeMap::<HardCommitId, Vec<ImageCacheConfigId>>::new();
        for (config_id, refs) in &self.state.read().await.config_refs {
            for digest in refs {
                referrers
                    .entry(digest.clone())
                    .or_default()
                    .push(config_id.clone());
            }
        }
        Ok(referrers)
    }

    pub async fn hard_commit_config_referrers(
        &self,
        digest: &HardCommitId,
    ) -> Result<Vec<ImageCacheConfigId>> {
        Ok(self
            .state
            .read()
            .await
            .config_refs
            .iter()
            .filter(|(_, refs)| refs.contains(digest))
            .map(|(config_id, _)| config_id.clone())
            .collect())
    }

    pub async fn remove_config_refs(&self, config_id: &ImageCacheConfigId) -> Result<()> {
        let mut state = self.state.write().await;
        state.config_refs.remove(config_id);
        state.last_used.remove(config_id);
        Ok(())
    }

    /// Rebuilds the config-derived half of the graph from the cache directories.
    ///
    /// Configs no longer on disk lose their references; hard-commit facts are
    /// only added, so a commit this process recorded but has not yet published
    /// a config for stays accounted for. Holds are untouched, and recency this
    /// process already observed wins over the on-disk seed.
    pub async fn rebuild_from_configs(
        &self,
        configs_dir: &Path,
        index_dir: &Path,
        commit_store: &Path,
    ) -> Result<()> {
        let parsed = parse_configs_dir(configs_dir).await.with_context(|| {
            format!(
                "rebuild image cache metadata from {}",
                configs_dir.display()
            )
        })?;
        let indexed = scan_indexed_hard_commits(index_dir, commit_store).await?;
        let mut config_refs = BTreeMap::new();
        let mut last_used = BTreeMap::new();

        let mut state = self.state.write().await;
        state.hard_commits.extend(indexed);
        for (config_id, config) in parsed {
            for reference in &config.hard_refs {
                state.hard_commits.insert(
                    reference.digest.clone(),
                    HardCommitObjectRecord {
                        digest: reference.digest.clone(),
                        file: Some(reference.file.clone()),
                        size: Some(reference.size),
                    },
                );
            }
            let refs = config
                .hard_refs
                .iter()
                .map(|reference| reference.digest.clone())
                .collect::<BTreeSet<_>>();
            if refs.is_empty() {
                continue;
            }
            let recency = state
                .last_used
                .get(&config_id)
                .copied()
                .unwrap_or(config.modified_secs);
            last_used.insert(config_id.clone(), recency);
            config_refs.insert(config_id, refs);
        }

        state.config_refs = config_refs;
        state.last_used = last_used;
        Ok(())
    }

    pub async fn config_last_used(&self, config_id: &ImageCacheConfigId) -> Result<Option<u64>> {
        Ok(self.state.read().await.last_used.get(config_id).copied())
    }

    /// LRU source-config eviction plan. The freed estimate ignores holds, so any
    /// shortfall is retried next pass by hard-commit GC.
    pub async fn plan_capacity_eviction(
        &self,
        high_watermark_bytes: u64,
        low_watermark_bytes: u64,
        evictable_before: u64,
    ) -> Result<CapacityEvictionPlan> {
        let state = self.state.read().await;
        let config_refs = state.config_refs.clone();
        let sizes: BTreeMap<HardCommitId, u64> = state
            .hard_commits
            .values()
            .map(|record| (record.digest.clone(), record.size.unwrap_or(0)))
            .collect();
        let last_used = state.last_used.clone();
        drop(state);
        let total_bytes: u64 = sizes.values().copied().sum();
        if total_bytes <= high_watermark_bytes {
            return Ok(CapacityEvictionPlan {
                total_bytes,
                ..Default::default()
            });
        }

        let mut referrers: BTreeMap<HardCommitId, BTreeSet<ImageCacheConfigId>> = BTreeMap::new();
        for (config_id, commits) in &config_refs {
            for commit in commits {
                referrers
                    .entry(commit.clone())
                    .or_default()
                    .insert(config_id.clone());
            }
        }

        let mut evictable: Vec<(u64, ImageCacheConfigId)> = config_refs
            .keys()
            .filter_map(|config_id| {
                last_used
                    .get(config_id)
                    .filter(|&&used| used <= evictable_before)
                    .map(|&used| (used, config_id.clone()))
            })
            .collect();
        evictable.sort();

        let mut evicting = BTreeSet::new();
        let mut covered: BTreeSet<HardCommitId> = BTreeSet::new();
        let mut estimated_freed_bytes = 0u64;
        let mut candidates = Vec::new();
        for (last_used, config_id) in evictable {
            if total_bytes.saturating_sub(estimated_freed_bytes) <= low_watermark_bytes {
                break;
            }
            evicting.insert(config_id.clone());
            if let Some(commits) = config_refs.get(&config_id) {
                for commit in commits {
                    if covered.contains(commit) {
                        continue;
                    }
                    if referrers
                        .get(commit)
                        .is_some_and(|refs| refs.is_subset(&evicting))
                    {
                        covered.insert(commit.clone());
                        estimated_freed_bytes += sizes.get(commit).copied().unwrap_or(0);
                    }
                }
            }
            candidates.push(CapacityEvictionCandidate {
                config_id,
                last_used,
            });
        }

        Ok(CapacityEvictionPlan {
            candidates,
            total_bytes,
        })
    }
}

impl CacheGraph {
    fn apply_config(&mut self, config_id: &ImageCacheConfigId, refs: &[ParsedHardCommitRef]) {
        for reference in refs {
            self.hard_commits.insert(
                reference.digest.clone(),
                HardCommitObjectRecord {
                    digest: reference.digest.clone(),
                    file: Some(reference.file.clone()),
                    size: Some(reference.size),
                },
            );
        }
        let digests = refs
            .iter()
            .map(|reference| reference.digest.clone())
            .collect::<BTreeSet<_>>();
        if digests.is_empty() {
            self.config_refs.remove(config_id);
        } else {
            self.config_refs.insert(config_id.clone(), digests);
        }
    }
}

/// Hard-commit facts for every commit this node's conversion indexes name,
/// keeping commits that no published config references yet accountable.
async fn scan_indexed_hard_commits(
    index_dir: &Path,
    commit_store: &Path,
) -> Result<BTreeMap<HardCommitId, HardCommitObjectRecord>> {
    let mut found = BTreeMap::new();
    let mut digest_dirs = match tokio::fs::read_dir(index_dir).await {
        Ok(dirs) => dirs,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(found),
        Err(err) => {
            return Err(err).with_context(|| format!("read index dir {}", index_dir.display()))
        }
    };

    while let Some(digest_dir) = digest_dirs
        .next_entry()
        .await
        .with_context(|| format!("scan index dir {}", index_dir.display()))?
    {
        if !digest_dir.file_type().await.is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let mut index_files = tokio::fs::read_dir(digest_dir.path())
            .await
            .with_context(|| format!("read index dir {}", digest_dir.path().display()))?;
        while let Some(index_file) = index_files
            .next_entry()
            .await
            .with_context(|| format!("scan index dir {}", digest_dir.path().display()))?
        {
            let index = match CommitIndex::read(&index_file.path()).await {
                Ok(Some(index)) => index,
                Ok(None) => continue,
                Err(error) => {
                    warn!(
                        index = %index_file.path().display(),
                        error = %error,
                        "skipping unreadable conversion index while rebuilding image cache metadata"
                    );
                    continue;
                }
            };
            let Ok(digest) = HardCommitId::new(index.commit_digest.clone()) else {
                continue;
            };
            let file = commit_index::commit_file(commit_store, &index.commit_digest);
            if !tokio::fs::metadata(&file)
                .await
                .is_ok_and(|metadata| metadata.is_file())
            {
                continue;
            }
            found.insert(
                digest.clone(),
                HardCommitObjectRecord {
                    digest,
                    file: Some(file),
                    size: Some(index.size),
                },
            );
        }
    }
    Ok(found)
}

pub fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

pub fn load_cache_owned_image_config_facts(
    image_config_path: &Path,
) -> Result<CacheOwnedImageConfigFacts> {
    let image_config = load_overlaybd_image_config(image_config_path)
        .with_context(|| format!("load image config {}", image_config_path.display()))?;

    let mut local_file_lowers = Vec::new();
    for (idx, lower) in image_config.lowers.into_iter().enumerate() {
        if lower.file.is_empty() {
            continue;
        }
        if lower.digest.is_empty() {
            bail!(
                "image config {} lower {idx} has local file but no digest",
                image_config_path.display()
            );
        }
        if lower.size == 0 {
            bail!(
                "image config {} lower {idx} has local file but no size",
                image_config_path.display()
            );
        }
        local_file_lowers.push(CacheOwnedLocalFileLower {
            digest: lower.digest,
            file: PathBuf::from(lower.file),
            size: lower.size,
        });
    }

    let upper_file =
        (!image_config.upper.data.is_empty()).then(|| PathBuf::from(image_config.upper.data));

    Ok(CacheOwnedImageConfigFacts {
        local_file_lowers,
        upper_file,
    })
}

pub fn stable_path_identity(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    let absolute = lexically_normalize_path(&absolute);
    canonicalize_nearest_existing_parent(&absolute)
}

/// True when `path` is `parent` or lives under it, compared on stable identities
/// (lexical normalization + nearest-existing-parent canonicalization) so it is
/// robust to `..`, symlinks, and not-yet-existing leaves.
pub fn path_is_inside(path: &Path, parent: &Path) -> bool {
    stable_path_identity(path).starts_with(stable_path_identity(parent))
}

fn canonicalize_nearest_existing_parent(path: &Path) -> PathBuf {
    let mut suffix = Vec::<OsString>::new();
    let mut cursor = path;
    loop {
        if let Ok(mut base) = std::fs::canonicalize(cursor) {
            for component in suffix.iter().rev() {
                base.push(component);
            }
            return base;
        }
        if let Some(name) = cursor.file_name() {
            suffix.push(name.to_os_string());
        }
        let Some(parent) = cursor.parent().filter(|parent| *parent != cursor) else {
            return path.to_path_buf();
        };
        cursor = parent;
    }
}

async fn parse_configs_dir(
    configs_dir: &Path,
) -> Result<BTreeMap<ImageCacheConfigId, ParsedConfig>> {
    let mut paths = Vec::new();
    let mut entries = match tokio::fs::read_dir(configs_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(err) => {
            return Err(err).with_context(|| format!("read configs dir {}", configs_dir.display()))
        }
    };

    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("read configs dir {}", configs_dir.display()))?
    {
        let path = entry.path();
        let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !is_regular_config_filename(filename) {
            continue;
        }
        let metadata = entry
            .metadata()
            .await
            .with_context(|| format!("stat image config {}", path.display()))?;
        if metadata.is_file() {
            paths.push((path, modified_unix_secs(&metadata)));
        }
    }
    paths.sort();

    let mut parsed = BTreeMap::new();
    for (path, modified_secs) in paths {
        let config_id = ImageCacheConfigId::from_config_path(&path)?;
        let hard_refs = load_cache_owned_hard_commit_refs(&path)?;
        parsed.insert(
            config_id,
            ParsedConfig {
                hard_refs,
                modified_secs,
            },
        );
    }
    Ok(parsed)
}

fn modified_unix_secs(metadata: &std::fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_else(unix_now_secs)
}

fn load_cache_owned_hard_commit_refs(image_config_path: &Path) -> Result<Vec<ParsedHardCommitRef>> {
    let facts = load_cache_owned_image_config_facts(image_config_path)?;
    let mut refs = Vec::new();
    let mut seen = BTreeSet::new();

    // In cache-owned image configs, `file=` lowers are hard commits.
    // `dir=`+repoBlobUrl lowers are remote-recoverable and are never strongly
    // pinned, so they are not tracked as hard commits here.
    for lower in facts.local_file_lowers {
        let digest = HardCommitId::new(lower.digest)?;
        if seen.insert(digest.clone()) {
            refs.push(ParsedHardCommitRef {
                digest,
                file: lower.file,
                size: lower.size,
            });
        }
    }
    Ok(refs)
}

fn load_commit_store_owned_hard_commit_refs(
    image_config_path: &Path,
    commit_store: &Path,
) -> Result<Vec<ParsedHardCommitRef>> {
    let image_config = load_overlaybd_image_config(image_config_path)
        .with_context(|| format!("load image config {}", image_config_path.display()))?;
    let mut refs = Vec::new();
    let mut seen = BTreeSet::new();

    for (idx, lower) in image_config.lowers.into_iter().enumerate() {
        if lower.file.is_empty() {
            continue;
        }
        let file = PathBuf::from(lower.file);
        if !path_is_inside(&file, commit_store) {
            continue;
        }
        if lower.digest.is_empty() {
            bail!(
                "image config {} lower {idx} has image-cache commit-store file but no digest",
                image_config_path.display()
            );
        }
        if lower.size == 0 {
            bail!(
                "image config {} lower {idx} has image-cache commit-store file but no size",
                image_config_path.display()
            );
        }

        let digest = HardCommitId::new(lower.digest)?;
        if seen.insert(digest.clone()) {
            refs.push(ParsedHardCommitRef {
                digest,
                file,
                size: lower.size,
            });
        }
    }

    Ok(refs)
}

fn is_regular_config_filename(filename: &str) -> bool {
    filename.ends_with(IMAGE_CONFIG_SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn test_store() -> ImageCacheMetadataStore {
        ImageCacheMetadataStore::new()
    }

    async fn rebuild(store: &ImageCacheMetadataStore, temp: &TempDir) -> Result<()> {
        store
            .rebuild_from_configs(
                &temp.path().join("configs"),
                &temp.path().join("indexes"),
                &temp.path().join("commits"),
            )
            .await
    }

    fn config_id(name: &str) -> ImageCacheConfigId {
        ImageCacheConfigId::from_filename(name).expect("config id")
    }

    fn hard(digest: &str) -> HardCommitId {
        HardCommitId::new(digest).expect("hard commit id")
    }

    fn hold_owner(namespace: &str, key: &str) -> ImageCacheHoldOwner {
        ImageCacheHoldOwner::new(namespace, key).expect("hold owner")
    }

    fn refs_of(digests: impl IntoIterator<Item = HardCommitId>) -> BTreeSet<HardCommitId> {
        digests.into_iter().collect()
    }

    fn write_config(path: &Path, lowers: serde_json::Value, repo_blob_url: &str) {
        std::fs::create_dir_all(path.parent().expect("config parent")).expect("create configs");
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&json!({
                "repoBlobUrl": repo_blob_url,
                "lowers": lowers,
                "upper": {},
                "resultFile": ""
            }))
            .expect("serialize config"),
        )
        .expect("write config");
    }

    async fn record_config(
        store: &ImageCacheMetadataStore,
        path: &Path,
        lowers: serde_json::Value,
    ) {
        write_config(path, lowers, "");
        store
            .record_config_refs_from_config_path(path)
            .await
            .expect("record config refs");
    }

    #[tokio::test]
    async fn hold_ref_lifecycle_replaces_stale_refs_then_clears_on_release() {
        let store = test_store();
        let owner = hold_owner("test", "owner-1");
        let digest1 = hard("sha256:one");
        let digest2 = hard("sha256:two");
        let digest3 = hard("sha256:three");

        // Replacing {digest1, digest2} with {digest2, digest3} drops digest1's
        // reverse ref and keeps digest2/digest3 pointing at the owner.
        store
            .create_or_replace_hold(&owner, &refs_of([digest1.clone(), digest2.clone()]))
            .await
            .expect("record initial hold");
        store
            .create_or_replace_hold(&owner, &refs_of([digest2.clone(), digest3.clone()]))
            .await
            .expect("replace hold");

        assert!(store
            .hard_commit_hold_referrers(&digest1)
            .await
            .expect("digest1 referrers")
            .is_empty());
        assert_eq!(
            store
                .hard_commit_hold_referrers(&digest2)
                .await
                .expect("digest2 referrers"),
            vec![owner.clone()]
        );
        assert_eq!(
            store
                .hard_commit_hold_referrers(&digest3)
                .await
                .expect("digest3 referrers"),
            vec![owner.clone()]
        );

        // Releasing clears the hold record and both forward/reverse ref indexes.
        store.release_hold(&owner).await.expect("release hold");

        assert!(store
            .list_hold_owners_in_namespaces(&["test"])
            .await
            .expect("hold owners")
            .is_empty());
        for digest in [&digest2, &digest3] {
            assert!(store
                .hard_commit_hold_referrers(digest)
                .await
                .expect("digest referrers")
                .is_empty());
        }
    }

    #[tokio::test]
    async fn rebuild_from_configs_records_only_hard_commit_refs() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store();
        let configs = temp.path().join("configs");
        let config = configs.join("mixed-image.json");

        write_config(
            &config,
            json!([
                {
                    "file": "../commits/sha256-hard/overlaybd.commit",
                    "digest": "sha256:hard",
                    "size": 11
                },
                {
                    "dir": "../commits/sha256-soft",
                    "digest": "sha256:soft",
                    "size": 22
                },
                {
                    "digest": "sha256:remote-block",
                    "size": 33
                },
                {
                    "dir": "../indexes/base",
                    "gzipIndex": "../indexes/base.index",
                    "digest": "sha256:index",
                    "size": 44
                }
            ]),
            "https://registry.example/v2/repo/blobs",
        );

        rebuild(&store, &temp).await.expect("rebuild metadata");

        let config = config_id("mixed-image.json");
        assert_eq!(
            store
                .list_hard_commit_objects()
                .await
                .expect("hard objects")
                .into_iter()
                .map(|record| record.digest)
                .collect::<Vec<_>>(),
            vec![hard("sha256:hard")]
        );
        let mut referrers = store
            .hard_commit_config_referrer_map()
            .await
            .expect("hard config referrers");
        assert_eq!(
            referrers.remove(&hard("sha256:hard")).unwrap_or_default(),
            vec![config]
        );
    }

    #[tokio::test]
    async fn rebuild_from_configs_fails_closed_on_malformed_config() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store();
        let configs = temp.path().join("configs");
        let existing_config = config_id("existing-image.json");
        let existing_digest = hard("sha256:existing");

        record_config(
            &store,
            &temp.path().join("seed/existing-image.json"),
            json!([{
                "file": "../commits/sha256-existing/overlaybd.commit",
                "digest": "sha256:existing",
                "size": 10
            }]),
        )
        .await;
        write_config(
            &configs.join("bad-image.json"),
            json!([{
                "file": "../commits/missing-digest/overlaybd.commit",
                "size": 10
            }]),
            "",
        );

        let err = rebuild(&store, &temp)
            .await
            .expect_err("malformed config should fail rebuild");

        assert!(
            // The "no digest" cause is a source in the error chain; the outer
            // `to_string()` only carries the "rebuild ..." context, so match the
            // full alternate-formatted chain.
            format!("{err:#}").contains("local file but no digest"),
            "unexpected error: {err:#}"
        );
        let mut referrers = store
            .hard_commit_config_referrer_map()
            .await
            .expect("existing referrers");
        assert_eq!(
            referrers.remove(&existing_digest).unwrap_or_default(),
            vec![existing_config]
        );
    }

    #[tokio::test]
    async fn plan_capacity_eviction_frees_shared_commit_only_when_all_referrers_evicted() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store();
        let a = config_id("a-image.json");
        let b = config_id("b-image.json");
        // Both reference the shared base; a also has an exclusive layer.
        record_config(
            &store,
            &temp.path().join(a.as_str()),
            json!([
                {
                    "file": "../commits/sha256-shared/overlaybd.commit",
                    "digest": "sha256:shared",
                    "size": 500
                },
                {
                    "file": "../commits/sha256-a-only/overlaybd.commit",
                    "digest": "sha256:a-only",
                    "size": 50
                }
            ]),
        )
        .await;
        record_config(
            &store,
            &temp.path().join(b.as_str()),
            json!([{
                "file": "../commits/sha256-shared/overlaybd.commit",
                "digest": "sha256:shared",
                "size": 500
            }]),
        )
        .await;

        // High watermark 0 forces eviction; low watermark 0 cleans everything
        // eligible. Evicting `a` first frees only its exclusive 50 (shared still
        // held by b); evicting `b` then frees shared.
        let plan = store
            .plan_capacity_eviction(0, 0, u64::MAX)
            .await
            .expect("plan");
        assert_eq!(plan.total_bytes, 550);
        assert_eq!(
            plan.candidates
                .iter()
                .map(|candidate| candidate.config_id.clone())
                .collect::<Vec<_>>(),
            vec![a, b]
        );
    }
}
