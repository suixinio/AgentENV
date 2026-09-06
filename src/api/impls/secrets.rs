use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::http::Method;
use axum_extra::extract::CookieJar;
use headers::Host;
use tracing::warn;

use super::ApiImpl;
use crate::secrets::{
    is_secret_id, SecretMetadata, SecretRef, SecretString, SecretValue, SecretsError,
    SecretsService,
};
use agentenv_http_server::apis::secrets::{
    Secrets, SecretsGetResponse, SecretsPostResponse, SecretsSecretIdDeleteResponse,
    SecretsSecretIdGetResponse, SecretsSecretIdPostResponse,
};
use agentenv_http_server::models;

const NO_STORE: &str = "no secrets store is configured on this deployment";
const DEFAULT_PAGE: usize = 100;
const MAX_PAGE: usize = 100;

impl ApiImpl {
    fn secrets_or_unavailable(&self) -> Result<Arc<SecretsService>, models::Error> {
        self.secrets().ok_or_else(|| Self::error(503, NO_STORE))
    }
}

fn secret_model(secret: SecretRef) -> models::Secret {
    models::Secret {
        secret_id: secret.secret_id,
        name: secret.name,
        current_version: secret.current_version,
        metadata: secret.metadata.into_iter().collect(),
        created_at: chrono::DateTime::<chrono::Utc>::from(secret.created_at),
        updated_at: chrono::DateTime::<chrono::Utc>::from(secret.updated_at),
    }
}

/// Exactly one of the two shapes a write may carry. `fields` is the only
/// form a handler that speaks a protocol to the upstream can use; `value` is
/// what a header substitution takes.
fn value_from(
    value: Option<&zeroize::Zeroizing<String>>,
    fields: Option<&HashMap<String, zeroize::Zeroizing<String>>>,
) -> Result<SecretValue, models::Error> {
    match (value, fields) {
        (Some(value), None) => Ok(SecretValue::Opaque(SecretString::new(value.to_string()))),
        (None, Some(fields)) => Ok(SecretValue::Fields(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), SecretString::new(value.to_string())))
                .collect(),
        )),
        (Some(_), Some(_)) => Err(ApiImpl::error(
            400,
            "value and fields are mutually exclusive; give exactly one",
        )),
        (None, None) => Err(ApiImpl::error(400, "one of value or fields is required")),
    }
}

fn metadata_from(model: Option<&HashMap<String, String>>) -> SecretMetadata {
    model
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}

/// Client errors keep their message; store errors are reported without
/// detail so nothing about the value or the store's response leaks.
fn client_error(err: &SecretsError) -> Option<models::Error> {
    match err {
        SecretsError::InvalidName
        | SecretsError::ReservedName
        | SecretsError::InvalidMetadata(_)
        | SecretsError::EmptyValue
        | SecretsError::EmptyFields
        | SecretsError::ShapeChanged { .. } => Some(ApiImpl::error(400, err.to_string())),
        SecretsError::NotFound => Some(ApiImpl::error(404, "secret not found")),
        SecretsError::AlreadyExists(_) => Some(ApiImpl::error(409, err.to_string())),
        SecretsError::Unavailable(_) => None,
    }
}

fn store_error(operation: &str, err: &SecretsError) -> models::Error {
    warn!(operation, error = %format_args!("{err:#}"), "secrets store operation failed");
    ApiImpl::error(500, format!("secrets store failed to {operation}"))
}

#[async_trait]
impl Secrets<()> for ApiImpl {
    type Claims = super::Claims;

    async fn secrets_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::SecretsGetQueryParams,
    ) -> Result<SecretsGetResponse, ()> {
        let service = match self.secrets_or_unavailable() {
            Ok(service) => service,
            Err(err) => {
                return Ok(SecretsGetResponse::Status503_NoSecretsStoreIsConfigured(
                    err,
                ))
            }
        };
        let limit = query_params
            .limit
            .map(|limit| (limit as usize).clamp(1, MAX_PAGE))
            .unwrap_or(DEFAULT_PAGE);
        // The cursor is a secret id this API handed out; anything else would
        // sort before every id and silently restart the listing.
        if let Some(token) = query_params.next_token.as_deref() {
            if !is_secret_id(token) {
                return Ok(SecretsGetResponse::Status400_BadRequest(Self::error(
                    400,
                    "nextToken is not a cursor this API issued",
                )));
            }
        }
        match service
            .list(query_params.next_token.as_deref(), limit)
            .await
        {
            Ok((page, next)) => Ok(SecretsGetResponse::Status200_SuccessfullyListedTheSecrets {
                body: page.into_iter().map(secret_model).collect(),
                x_next_token: next,
            }),
            Err(err) => Ok(SecretsGetResponse::Status500_ServerError(store_error(
                "list secrets",
                &err,
            ))),
        }
    }

    async fn secrets_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::NewSecret,
    ) -> Result<SecretsPostResponse, ()> {
        let service = match self.secrets_or_unavailable() {
            Ok(service) => service,
            Err(err) => {
                return Ok(SecretsPostResponse::Status503_NoSecretsStoreIsConfigured(
                    err,
                ))
            }
        };
        let value = match value_from(body.value.as_ref(), body.fields.as_ref()) {
            Ok(value) => value,
            Err(err) => return Ok(SecretsPostResponse::Status400_BadRequest(err)),
        };
        match service
            .create(
                &body.name,
                value,
                metadata_from(body.metadata.as_ref()),
                body.allowed_hosts.clone().unwrap_or_default(),
            )
            .await
        {
            Ok(secret) => Ok(SecretsPostResponse::Status201_SuccessfullyCreatedTheSecret(
                secret_model(secret),
            )),
            Err(err) => Ok(match client_error(&err) {
                Some(client) if client.code == 409 => {
                    SecretsPostResponse::Status409_Conflict(client)
                }
                Some(client) => SecretsPostResponse::Status400_BadRequest(client),
                None => SecretsPostResponse::Status500_ServerError(store_error(
                    "create the secret",
                    &err,
                )),
            }),
        }
    }

    async fn secrets_secret_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SecretsSecretIdDeletePathParams,
    ) -> Result<SecretsSecretIdDeleteResponse, ()> {
        let service = match self.secrets_or_unavailable() {
            Ok(service) => service,
            Err(err) => {
                return Ok(SecretsSecretIdDeleteResponse::Status503_NoSecretsStoreIsConfigured(err))
            }
        };
        match service.delete(&path_params.secret_id).await {
            Ok(()) => Ok(SecretsSecretIdDeleteResponse::Status204_SuccessfullyDeletedTheSecret),
            Err(SecretsError::NotFound) => Ok(SecretsSecretIdDeleteResponse::Status404_NotFound(
                Self::error(404, "secret not found"),
            )),
            Err(err) => Ok(SecretsSecretIdDeleteResponse::Status500_ServerError(
                store_error("delete the secret", &err),
            )),
        }
    }

    async fn secrets_secret_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SecretsSecretIdGetPathParams,
    ) -> Result<SecretsSecretIdGetResponse, ()> {
        let service = match self.secrets_or_unavailable() {
            Ok(service) => service,
            Err(err) => {
                return Ok(SecretsSecretIdGetResponse::Status503_NoSecretsStoreIsConfigured(err))
            }
        };
        match service.get(&path_params.secret_id).await {
            Ok(secret) => Ok(
                SecretsSecretIdGetResponse::Status200_SuccessfullyRetrievedTheSecret(secret_model(
                    secret,
                )),
            ),
            Err(SecretsError::NotFound) => Ok(SecretsSecretIdGetResponse::Status404_NotFound(
                Self::error(404, "secret not found"),
            )),
            Err(err) => Ok(SecretsSecretIdGetResponse::Status500_ServerError(
                store_error("read the secret", &err),
            )),
        }
    }

    async fn secrets_secret_id_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SecretsSecretIdPostPathParams,
        body: &models::SecretUpdate,
    ) -> Result<SecretsSecretIdPostResponse, ()> {
        let service = match self.secrets_or_unavailable() {
            Ok(service) => service,
            Err(err) => {
                return Ok(SecretsSecretIdPostResponse::Status503_NoSecretsStoreIsConfigured(err))
            }
        };
        let value = match value_from(body.value.as_ref(), body.fields.as_ref()) {
            Ok(value) => value,
            Err(err) => return Ok(SecretsSecretIdPostResponse::Status400_BadRequest(err)),
        };
        let metadata = body.metadata.as_ref().map(|m| metadata_from(Some(m)));
        match service
            .update(
                &path_params.secret_id,
                value,
                metadata,
                body.allowed_hosts.clone().unwrap_or_default(),
            )
            .await
        {
            Ok(secret) => Ok(
                SecretsSecretIdPostResponse::Status200_SuccessfullyUpdatedTheSecret(secret_model(
                    secret,
                )),
            ),
            Err(err) => Ok(match client_error(&err) {
                Some(client) if client.code == 404 => {
                    SecretsSecretIdPostResponse::Status404_NotFound(client)
                }
                Some(client) => SecretsSecretIdPostResponse::Status400_BadRequest(client),
                None => SecretsSecretIdPostResponse::Status500_ServerError(store_error(
                    "update the secret",
                    &err,
                )),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use agentenv_http_server::models;

    use super::*;

    fn zeroizing(value: &str) -> zeroize::Zeroizing<String> {
        zeroize::Zeroizing::new(value.to_string())
    }

    #[test]
    fn a_secret_value_is_never_printed_in_either_shape() {
        let mut created = models::NewSecret::new("gh".to_string());
        created.value = Some(zeroizing("sk-live-123"));
        assert_eq!(format!("{created:?}"), "NewSecret([redacted])");
        assert!(!created.to_string().contains("sk-live-123"));

        let mut updated = models::SecretUpdate::new();
        updated.fields = Some(
            [("password".to_string(), zeroizing("sk-live-456"))]
                .into_iter()
                .collect(),
        );
        assert_eq!(format!("{updated:?}"), "SecretUpdate([redacted])");
        assert!(!updated.to_string().contains("sk-live-456"));
    }

    #[test]
    fn a_write_carries_exactly_one_of_value_and_fields() {
        let value = zeroizing("sk");
        let fields: HashMap<String, zeroize::Zeroizing<String>> =
            [("password".to_string(), zeroizing("pw"))]
                .into_iter()
                .collect();

        assert!(matches!(
            value_from(Some(&value), None),
            Ok(SecretValue::Opaque(_))
        ));
        assert!(matches!(
            value_from(None, Some(&fields)),
            Ok(SecretValue::Fields(_))
        ));
        for refused in [
            value_from(Some(&value), Some(&fields)),
            value_from(None, None),
        ] {
            assert_eq!(refused.err().map(|err| err.code), Some(400));
        }
    }
}
