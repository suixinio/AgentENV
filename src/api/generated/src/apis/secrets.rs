use async_trait::async_trait;
use axum::extract::*;
use axum_extra::extract::CookieJar;
use bytes::Bytes;
use headers::Host;
use http::Method;
use serde::{Deserialize, Serialize};

use crate::{models, types::*};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SecretsGetResponse {
    /// Successfully listed the secrets
    Status200_SuccessfullyListedTheSecrets {
        body: Vec<models::Secret>,
        x_next_token: Option<String>,
    },
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
    /// No secrets store is configured
    Status503_NoSecretsStoreIsConfigured(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SecretsPostResponse {
    /// Successfully created the secret
    Status201_SuccessfullyCreatedTheSecret(models::Secret),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Conflict
    Status409_Conflict(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
    /// No secrets store is configured
    Status503_NoSecretsStoreIsConfigured(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SecretsSecretIdDeleteResponse {
    /// Successfully deleted the secret
    Status204_SuccessfullyDeletedTheSecret,
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
    /// No secrets store is configured
    Status503_NoSecretsStoreIsConfigured(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SecretsSecretIdGetResponse {
    /// Successfully retrieved the secret
    Status200_SuccessfullyRetrievedTheSecret(models::Secret),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
    /// No secrets store is configured
    Status503_NoSecretsStoreIsConfigured(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SecretsSecretIdPostResponse {
    /// Successfully updated the secret
    Status200_SuccessfullyUpdatedTheSecret(models::Secret),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
    /// No secrets store is configured
    Status503_NoSecretsStoreIsConfigured(models::Error),
}

/// Secrets
#[async_trait]
#[allow(clippy::ptr_arg)]
pub trait Secrets<E: std::fmt::Debug + Send + Sync + 'static = ()>: super::ErrorHandler<E> {
    type Claims;

    /// List secrets.
    ///
    /// SecretsGet - GET /secrets
    async fn secrets_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        query_params: &models::SecretsGetQueryParams,
    ) -> Result<SecretsGetResponse, E>;

    /// Create a secret.
    ///
    /// SecretsPost - POST /secrets
    async fn secrets_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        body: &models::NewSecret,
    ) -> Result<SecretsPostResponse, E>;

    /// Delete a secret.
    ///
    /// SecretsSecretIdDelete - DELETE /secrets/{secretID}
    async fn secrets_secret_id_delete(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SecretsSecretIdDeletePathParams,
    ) -> Result<SecretsSecretIdDeleteResponse, E>;

    /// Get a secret.
    ///
    /// SecretsSecretIdGet - GET /secrets/{secretID}
    async fn secrets_secret_id_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SecretsSecretIdGetPathParams,
    ) -> Result<SecretsSecretIdGetResponse, E>;

    /// Update a secret.
    ///
    /// SecretsSecretIdPost - POST /secrets/{secretID}
    async fn secrets_secret_id_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::SecretsSecretIdPostPathParams,
        body: &models::SecretUpdate,
    ) -> Result<SecretsSecretIdPostResponse, E>;
}
