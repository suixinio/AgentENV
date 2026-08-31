//! The registry a snapshot's disk image was published to, parsed out of a
//! `repoBlobUrl`.

use url::Url;

use crate::snapshot::repository::{RepositoryError, RepositoryResult};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceRegistryRepository {
    pub registry: String,
    pub repository: String,
    pub repo_blob_url: String,
}

impl SourceRegistryRepository {
    pub fn parse(repo_blob_url: &str) -> RepositoryResult<Self> {
        let url = Url::parse(repo_blob_url).map_err(|e| RepositoryError::Unsupported {
            feature: format!("invalid ACR repoBlobUrl '{repo_blob_url}': {e}"),
        })?;
        // Cross-crate fake-registry tests permit loopback HTTP under `test-support`.
        let test_loopback_http = cfg!(any(test, feature = "test-support"))
            && url.scheme() == "http"
            && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "::1"));
        if url.scheme() != "https" && !test_loopback_http {
            return Err(RepositoryError::Unsupported {
                feature: format!("ACR repoBlobUrl must use https: {repo_blob_url}"),
            });
        }
        let host = url.host_str().ok_or_else(|| RepositoryError::Unsupported {
            feature: format!("ACR repoBlobUrl is missing registry host: {repo_blob_url}"),
        })?;
        // Strip the default HTTPS :443 so equivalent source URLs normalize to
        // one registry; keep every other explicit port intact.
        let registry = match url.port() {
            Some(443) if url.scheme() == "https" => host.to_string(),
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };

        let segments: Vec<&str> = url
            .path_segments()
            .map(|segments| segments.collect())
            .unwrap_or_default();
        if segments.len() < 3
            || segments.first() != Some(&"v2")
            || segments.last() != Some(&"blobs")
        {
            return Err(RepositoryError::Unsupported {
                feature: format!(
                    "ACR repoBlobUrl must have shape https://<registry>/v2/<repo>/blobs: {repo_blob_url}"
                ),
            });
        }
        let repository = segments[1..segments.len() - 1].join("/");
        if repository.is_empty() {
            return Err(RepositoryError::Unsupported {
                feature: format!("ACR repoBlobUrl repository is empty: {repo_blob_url}"),
            });
        }
        let repo_blob_url = format!(
            "{}://{}{}",
            url.scheme(),
            registry,
            url.path().trim_end_matches('/')
        );

        Ok(Self {
            registry,
            repository,
            repo_blob_url,
        })
    }

    pub fn image_ref(&self, tag: &str) -> String {
        format!("{}/{}:{tag}", self.registry, self.repository)
    }

    fn registry_api_url(&self) -> String {
        let scheme = self
            .repo_blob_url
            .split_once("://")
            .map(|(scheme, _)| scheme)
            .unwrap_or("https");
        format!("{scheme}://{}", self.registry)
    }

    pub fn upload_url(&self) -> String {
        format!(
            "{}/v2/{}/blobs/uploads/",
            self.registry_api_url(),
            self.repository
        )
    }

    pub fn manifest_url(&self, tag: &str) -> String {
        format!(
            "{}/v2/{}/manifests/{tag}",
            self.registry_api_url(),
            self.repository
        )
    }
}
