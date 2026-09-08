mod nodes;
mod registry;
mod snapshots;

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use agentenv_http_server::apis::admin::*;
use agentenv_http_server::models;

use super::ApiImpl;

#[async_trait]
impl Admin<()> for ApiImpl {
    type Claims = super::Claims;

    async fn nodes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::NodesGetQueryParams,
    ) -> Result<NodesGetResponse, ()> {
        self.list_nodes(query_params).await
    }

    async fn nodes_node_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::NodesNodeIdGetPathParams,
        query_params: &models::NodesNodeIdGetQueryParams,
    ) -> Result<NodesNodeIdGetResponse, ()> {
        self.get_node(path_params, query_params).await
    }

    async fn nodes_node_id_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::NodesNodeIdPostPathParams,
        query_params: &models::NodesNodeIdPostQueryParams,
        body: &models::NodeStatusChange,
    ) -> Result<NodesNodeIdPostResponse, ()> {
        self.set_node_status(path_params, query_params, body).await
    }

    async fn registry_sandboxes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::RegistrySandboxesGetQueryParams,
    ) -> Result<RegistrySandboxesGetResponse, ()> {
        self.list_registry_sandboxes(query_params).await
    }

    async fn snapshots_snapshot_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SnapshotsSnapshotIdDeletePathParams,
    ) -> Result<SnapshotsSnapshotIdDeleteResponse, ()> {
        self.delete_snapshot(path_params).await
    }
}
