// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The endpoint management API, served on the admin listener only.
//!
//! The operations here are the ones a TOML edit plus a restart is the wrong
//! tool for: revoking a compromised endpoint is time-critical, and a platform
//! provisioning a webhook URL per tenant is inherently programmatic.
//!
//! Disabled unless `management_token` is set, and never reachable from the
//! public listener. Mutations become durable on the next successful poll, so
//! a response here means "accepted", not "written"; `GET /admin/endpoints`
//! reports `submitted` per endpoint for callers that need the difference.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rand::RngExt;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::auth::{secrets_match, strip_bearer};
use crate::routes::{Endpoint, EndpointOrigin, EndpointState};
use crate::server::{ServerState, error_response, refresh_routes};
use crate::types::{EndpointId, unix_now_seconds};
use crate::{CONNECTOR_NAME, EndpointAuthType, SharedState};

/// Bytes of entropy behind a generated endpoint id. The URL is the bearer
/// token for a secret-path endpoint, so it carries the whole secret.
const ENDPOINT_ID_BYTES: usize = 16;

pub(crate) fn router(state: Arc<ServerState>) -> Router<Arc<ServerState>> {
    if state.management_token.is_none() {
        info!(
            "Dynamic endpoint management is disabled for the {CONNECTOR_NAME} listener on {}: no management_token is configured",
            state.listen_addr
        );
        return Router::new();
    }
    info!(
        "Enabled dynamic endpoint management for the {CONNECTOR_NAME} listener on {}",
        state.listen_addr
    );
    Router::new()
        .route(
            "/admin/endpoints",
            post(register_endpoint).get(list_endpoints),
        )
        .route(
            "/admin/endpoints/{endpoint_id}",
            get(get_endpoint)
                .patch(rotate_secret)
                .delete(revoke_endpoint),
        )
}

async fn register_endpoint(
    State(state): State<Arc<ServerState>>,
    request_headers: HeaderMap,
    body: Result<Json<RegisterRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Some(response) = denied(&state, &request_headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(rejection) => return error_response(rejection.status(), "invalid request body"),
    };

    let Some(instance) = state.instance(&request.instance) else {
        return error_response(StatusCode::NOT_FOUND, "unknown instance");
    };
    if request.auth_type != EndpointAuthType::None && !is_usable(&request.auth_secret) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "a non-empty auth_secret is required",
        );
    }
    if request
        .expires_at
        .is_some_and(|expires_at| expires_at <= unix_now_seconds())
    {
        return error_response(StatusCode::BAD_REQUEST, "expires_at is already past");
    }

    let endpoint_id = generate_endpoint_id();
    let endpoint = Endpoint {
        endpoint_id: endpoint_id.clone(),
        auth_type: request.auth_type,
        auth_secret: request.auth_secret,
        hmac_header: request.hmac_header,
        hmac_prefix: request.hmac_prefix,
        expires_at: request.expires_at,
        origin: EndpointOrigin::Dynamic,
        state: EndpointState::Active,
        submitted: false,
    };

    if !instance
        .mutate_registry(|registry| registry.insert(endpoint))
        .await
    {
        // 128 bits of entropy makes this unreachable in practice; refusing to
        // overwrite is what keeps it from silently retargeting live traffic.
        warn!(
            "Generated a colliding endpoint id for {CONNECTOR_NAME} connector ID: {instance_id}, refusing the registration",
            instance_id = instance.id
        );
        return error_response(StatusCode::CONFLICT, "endpoint id collision");
    }
    if let Some(failure) = republish(&state).await {
        // Undo the insert. Left in place it would be persisted on the next
        // flush and come back live after a restart, despite the caller having
        // been told the registration failed.
        if !instance
            .mutate_registry(|registry| registry.remove(endpoint_id.as_str()))
            .await
        {
            error!(
                "Failed to roll back endpoint {} on {CONNECTOR_NAME} connector ID: {}; it may be persisted and served after a restart despite this registration failing",
                endpoint_id.log_prefix(),
                instance.id
            );
        }
        return failure;
    }
    if !still_joined(&state, &instance) {
        // The instance closed while we were mutating it. Its registry is no
        // longer polled or projected, so the URL we would hand back is dead.
        // No rollback: that registry is already unreachable, and the poll task
        // that could have persisted it is stopped.
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "instance is closing");
    }

    // Logged by prefix only: the full id is the credential for a secret-path
    // endpoint, and process logs reach a far wider audience than the state
    // directory the README scopes secrets to.
    info!(
        "Registered endpoint {} for {CONNECTOR_NAME} connector ID: {}",
        endpoint_id.log_prefix(),
        instance.id
    );
    (
        StatusCode::CREATED,
        Json(RegisteredEndpoint {
            path: format!("/e/{endpoint_id}"),
            endpoint_id,
        }),
    )
        .into_response()
}

/// Replaces an endpoint's secret without changing its URL.
///
/// Deliberate: a webhook sender configures the URL once, so rotating the
/// shared secret must not force it to be reconfigured.
async fn rotate_secret(
    State(state): State<Arc<ServerState>>,
    Path(endpoint_id): Path<String>,
    request_headers: HeaderMap,
    body: Result<Json<RotateRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Some(response) = denied(&state, &request_headers) {
        return response;
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(rejection) => return error_response(rejection.status(), "invalid request body"),
    };
    if request.auth_secret.expose_secret().is_empty() {
        // An empty HMAC key validates any signature the holder of the URL can
        // compute, so rotating to one silently removes the second factor.
        return error_response(StatusCode::BAD_REQUEST, "auth_secret must not be empty");
    }
    let Some(instance) = owner_of(&state, &endpoint_id) else {
        return error_response(StatusCode::NOT_FOUND, "not found");
    };
    // Restoring prefers TOML over any still-active persisted entry, so a
    // rotated static secret would silently revert on the next restart and the
    // operator would believe a leaked secret had been replaced.
    if instance
        .registry()
        .endpoint(&endpoint_id)
        .is_some_and(|endpoint| endpoint.origin == EndpointOrigin::Static)
    {
        return error_response(
            StatusCode::CONFLICT,
            "a static endpoint's secret lives in TOML; edit auth_secret there and restart the instance",
        );
    }

    let rotated = instance
        .mutate_registry(|registry| {
            let Some(endpoint) = registry.endpoint_mut(&endpoint_id) else {
                return false;
            };
            if !endpoint.is_active() {
                return false;
            }
            endpoint.auth_secret = Some(request.auth_secret.clone());
            endpoint.submitted = false;
            true
        })
        .await;
    if !rotated {
        return error_response(StatusCode::NOT_FOUND, "not found");
    }
    if let Some(failure) = republish_or_close(&state).await {
        return failure;
    }
    if !still_joined(&state, &instance) {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "instance is closing");
    }

    info!(
        "Rotated the secret for endpoint {} on {CONNECTOR_NAME} connector ID: {}",
        EndpointId::log_prefix_of(&endpoint_id),
        instance.id
    );
    summary_response(&instance, &endpoint_id)
}

async fn revoke_endpoint(
    State(state): State<Arc<ServerState>>,
    Path(endpoint_id): Path<String>,
    request_headers: HeaderMap,
    body: Option<Json<RevokeRequest>>,
) -> Response {
    if let Some(response) = denied(&state, &request_headers) {
        return response;
    }
    let reason = body
        .and_then(|Json(request)| request.reason)
        .unwrap_or_else(|| "unspecified".to_string());
    let Some(instance) = owner_of(&state, &endpoint_id) else {
        return error_response(StatusCode::NOT_FOUND, "not found");
    };

    let revoked = instance
        .mutate_registry(|registry| registry.revoke(&endpoint_id, reason, unix_now_seconds()))
        .await;
    if !revoked {
        return error_response(StatusCode::NOT_FOUND, "not found");
    }
    if let Some(failure) = republish_or_close(&state).await {
        return failure;
    }
    // Most important here of the three: a revocation reported 204 but landed
    // on a departed instance is never persisted, so the compromised endpoint
    // comes back after the restart the operator is probably about to do.
    if !still_joined(&state, &instance) {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "instance is closing");
    }

    info!(
        "Revoked endpoint {} on {CONNECTOR_NAME} connector ID: {}",
        EndpointId::log_prefix_of(&endpoint_id),
        instance.id
    );
    StatusCode::NO_CONTENT.into_response()
}

async fn list_endpoints(
    State(state): State<Arc<ServerState>>,
    request_headers: HeaderMap,
) -> Response {
    if let Some(response) = denied(&state, &request_headers) {
        return response;
    }
    let endpoints: Vec<EndpointSummary> = state
        .instances()
        .iter()
        .flat_map(|instance| {
            instance
                .registry()
                .endpoints()
                .map(|endpoint| EndpointSummary::new(instance, endpoint))
                .collect::<Vec<_>>()
        })
        .collect();
    Json(endpoints).into_response()
}

async fn get_endpoint(
    State(state): State<Arc<ServerState>>,
    Path(endpoint_id): Path<String>,
    request_headers: HeaderMap,
) -> Response {
    if let Some(response) = denied(&state, &request_headers) {
        return response;
    }
    let Some(instance) = owner_of(&state, &endpoint_id) else {
        return error_response(StatusCode::NOT_FOUND, "not found");
    };
    summary_response(&instance, &endpoint_id)
}

/// Guards every management call with the shared token, answering with the
/// rejection to send when the caller may not proceed.
///
/// The token is required rather than optional here: the router is only
/// mounted when one is configured, so reaching a handler without one would
/// mean the listener is serving an endpoint it never meant to expose.
fn denied(state: &ServerState, request_headers: &HeaderMap) -> Option<Response> {
    let Some(expected) = &state.management_token else {
        return Some(error_response(StatusCode::NOT_FOUND, "not found"));
    };
    let presented = request_headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(strip_bearer)
        .map(SecretString::from);
    match presented {
        Some(presented) if secrets_match(&presented, expected) => None,
        _ => Some(error_response(StatusCode::UNAUTHORIZED, "unauthorized")),
    }
}

fn is_usable(secret: &Option<SecretString>) -> bool {
    secret
        .as_ref()
        .is_some_and(|secret| !secret.expose_secret().is_empty())
}

fn owner_of(state: &ServerState, endpoint_id: &str) -> Option<Arc<SharedState>> {
    state
        .instances()
        .into_iter()
        .find(|instance| instance.registry().endpoint(endpoint_id).is_some())
}

/// Whether the instance a handler resolved earlier is still the one joined
/// under that name. A handler awaits between resolving and mutating, and an
/// instance can close in between - the mutation would then land on a registry
/// nobody polls and the caller would be told it succeeded.
fn still_joined(state: &ServerState, instance: &Arc<SharedState>) -> bool {
    state
        .instance(&instance.instance_name)
        .is_some_and(|current| Arc::ptr_eq(&current, instance))
}

/// Projects the mutated registry into the shared route table.
///
/// A failure here leaves the registry ahead of the routes, so it answers 500
/// rather than pretending the endpoint is live.
async fn republish(state: &ServerState) -> Option<Response> {
    let Err(error) = refresh_routes(&state.listen_addr).await else {
        return None;
    };
    warn!(
        "Failed to republish {CONNECTOR_NAME} routes on {}. {error}",
        state.listen_addr
    );
    Some(error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "route update failed",
    ))
}

/// Republish for a mutation that takes access AWAY. If the table cannot be
/// rebuilt, the old one is still serving the endpoint the operator just
/// revoked or re-keyed, so serve nothing rather than a credential they
/// believe is dead.
async fn republish_or_close(state: &ServerState) -> Option<Response> {
    let failure = republish(state).await?;
    state.serve_nothing();
    Some(failure)
}

fn summary_response(instance: &Arc<SharedState>, endpoint_id: &str) -> Response {
    let registry = instance.registry();
    match registry.endpoint(endpoint_id) {
        Some(endpoint) => Json(EndpointSummary::new(instance, endpoint)).into_response(),
        None => error_response(StatusCode::NOT_FOUND, "not found"),
    }
}

fn generate_endpoint_id() -> EndpointId {
    let bytes: [u8; ENDPOINT_ID_BYTES] = rand::rng().random();
    hex::encode(bytes)
        .parse()
        .expect("hex of 16 bytes is 32 lowercase hex characters")
}

// Unlike the plugin config, these never carry the runtime's flat env
// overrides, so a rejected unknown field is a caller typo and nothing else.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterRequest {
    instance: String,
    #[serde(default)]
    auth_type: EndpointAuthType,
    #[serde(default)]
    auth_secret: Option<SecretString>,
    #[serde(default = "crate::default_hmac_header")]
    hmac_header: String,
    #[serde(default = "crate::default_hmac_prefix")]
    hmac_prefix: String,
    #[serde(default)]
    expires_at: Option<u64>,
}

// Unlike the plugin config, these never carry the runtime's flat env
// overrides, so a rejected unknown field is a caller typo and nothing else.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RotateRequest {
    auth_secret: SecretString,
}

// Unlike the plugin config, these never carry the runtime's flat env
// overrides, so a rejected unknown field is a caller typo and nothing else.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RevokeRequest {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct RegisteredEndpoint {
    endpoint_id: EndpointId,
    path: String,
}

/// The operator-facing view of an endpoint. Carries no secret, which is why
/// it exists rather than serializing [`Endpoint`] directly.
#[derive(Debug, Serialize)]
struct EndpointSummary {
    endpoint_id: EndpointId,
    instance: String,
    state: &'static str,
    origin: &'static str,
    auth_type: EndpointAuthType,
    expires_at: Option<u64>,
    /// False until the batch carrying this endpoint has been handed to the
    /// runtime. Not `persisted`: the plugin gets no acknowledgement that the
    /// runtime's write landed, so this is submission, not durability.
    submitted: bool,
    revoked_at: Option<u64>,
    revoked_reason: Option<String>,
}

impl EndpointSummary {
    fn new(instance: &Arc<SharedState>, endpoint: &Endpoint) -> Self {
        let (state, revoked_at, revoked_reason) = match &endpoint.state {
            EndpointState::Active => ("active", None, None),
            EndpointState::Revoked { reason, revoked_at } => {
                ("revoked", Some(*revoked_at), Some(reason.clone()))
            }
        };
        EndpointSummary {
            endpoint_id: endpoint.endpoint_id.clone(),
            instance: instance.instance_name.clone(),
            state,
            origin: match endpoint.origin {
                EndpointOrigin::Static => "static",
                EndpointOrigin::Dynamic => "dynamic",
            },
            auth_type: endpoint.auth_type,
            expires_at: endpoint.expires_at,
            submitted: endpoint.submitted,
            revoked_at,
            revoked_reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HttpSource;
    use crate::test_support::{ENDPOINT_ONE, client, free_port};
    use iggy_connector_sdk::Source;
    use serde_json::{Value, json};

    const TOKEN: &str = "mgmt-secret";

    struct Fixture {
        source: HttpSource,
        public: String,
        admin: String,
    }

    impl Fixture {
        async fn start(management_token: Option<&str>) -> Self {
            let mut config = crate::test_support::config(Some("github"), &[ENDPOINT_ONE]);
            config.listen_addr = format!("127.0.0.1:{}", free_port());
            config.admin_listen_addr = format!("127.0.0.1:{}", free_port());
            config.instance_name = Some("http_github".to_string());
            config.management_token = management_token.map(SecretString::from);
            let public = format!("http://{}", config.listen_addr);
            let admin = format!("http://{}", config.admin_listen_addr);

            let mut source = HttpSource::new(1, config, None);
            source.open().await.expect("open must succeed");
            Fixture {
                source,
                public,
                admin,
            }
        }

        async fn register(&self, body: Value) -> reqwest::Response {
            client()
                .post(format!("{}/admin/endpoints", self.admin))
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .json(&body)
                .send()
                .await
                .expect("the request must reach the admin listener")
        }

        async fn close(mut self) {
            self.source.close().await.expect("close must succeed");
        }
    }

    async fn post_signed(url: &str, secret: &str, body: &'static str) -> reqwest::Response {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
        let signature = format!(
            "sha256={}",
            hex::encode(ring::hmac::sign(&key, body.as_bytes()).as_ref())
        );
        client()
            .post(url)
            .header(crate::DEFAULT_HMAC_HEADER, signature)
            .body(body)
            .send()
            .await
            .expect("the request must reach the listener")
    }

    fn hmac_endpoint() -> Value {
        json!({
            "instance": "http_github",
            "auth_type": "hmac-sha256",
            "auth_secret": "whsec_dynamic",
        })
    }

    #[tokio::test]
    async fn given_no_management_token_when_endpoints_called_should_answer_not_found() {
        let fixture = Fixture::start(None).await;

        let response = client()
            .get(format!("{}/admin/endpoints", fixture.admin))
            .send()
            .await
            .expect("the request must reach the admin listener");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(
            response.text().await.expect("a body").is_empty(),
            "an unconfigured management API must not exist, not merely refuse: axum's \
             fallback has no body, whereas a handler answering 404 would render one"
        );
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_missing_token_when_endpoints_called_should_answer_unauthorized() {
        let fixture = Fixture::start(Some(TOKEN)).await;

        let response = client()
            .get(format!("{}/admin/endpoints", fixture.admin))
            .send()
            .await
            .expect("the request must reach the admin listener");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_wrong_token_when_endpoints_called_should_answer_unauthorized() {
        let fixture = Fixture::start(Some(TOKEN)).await;

        let response = client()
            .get(format!("{}/admin/endpoints", fixture.admin))
            .header(header::AUTHORIZATION, "Bearer not-the-token")
            .send()
            .await
            .expect("the request must reach the admin listener");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_wrong_token_when_each_route_called_should_answer_unauthorized() {
        // Every route, not just the one route a single test happens to reach.
        // Each handler carries its own guard, so a check dropped from any of
        // the four mutating ones would still leave the suite green.
        let fixture = Fixture::start(Some(TOKEN)).await;
        let endpoints = format!("{}/admin/endpoints", fixture.admin);
        let one = format!("{endpoints}/{ENDPOINT_ONE}");
        let http = client();
        let cases = vec![
            (
                "POST /admin/endpoints",
                http.post(&endpoints)
                    .json(&json!({"instance": "http_github"})),
            ),
            ("GET /admin/endpoints", http.get(&endpoints)),
            ("GET /admin/endpoints/{endpoint_id}", http.get(&one)),
            (
                "PATCH /admin/endpoints/{endpoint_id}",
                http.patch(&one)
                    .json(&json!({"auth_secret": "whsec_rotated"})),
            ),
            ("DELETE /admin/endpoints/{endpoint_id}", http.delete(&one)),
        ];

        for (route, request) in cases {
            let response = request
                .header(header::AUTHORIZATION, "Bearer not-the-token")
                .send()
                .await
                .expect("the request must reach the admin listener");

            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{route} must refuse a wrong token before it does anything else"
            );
        }
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_no_bound_listener_when_republished_should_fail_closed() {
        // A mutation that cannot reproject the route table must answer 500
        // rather than report success. For a revoke or a rotate the old table is
        // still honouring the credential the operator believes is now dead, so
        // the take-access-away variant drops the routes on its way out.
        let mut config = crate::test_support::config(Some("github"), &[ENDPOINT_ONE]);
        config.listen_addr = format!("127.0.0.1:{}", free_port());
        let state = Arc::new(ServerState::new(&config));

        let failure = republish(&state)
            .await
            .expect("an unbound address cannot be reprojected");
        let fail_closed = republish_or_close(&state)
            .await
            .expect("and the take-access-away variant must report it too");

        assert_eq!(failure.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(fail_closed.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn given_valid_request_when_endpoint_registered_should_generate_a_secret_path() {
        let fixture = Fixture::start(Some(TOKEN)).await;

        let response = fixture.register(hmac_endpoint()).await;

        assert_eq!(response.status(), StatusCode::CREATED);
        let body: Value = response.json().await.expect("the response must be JSON");
        let endpoint_id = body["endpoint_id"].as_str().expect("an id is returned");
        assert_eq!(endpoint_id.len(), EndpointId::LENGTH);
        assert!(endpoint_id.parse::<EndpointId>().is_ok());
        assert_eq!(body["path"], json!(format!("/e/{endpoint_id}")));
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_registered_endpoint_when_revoked_should_stop_accepting_webhooks() {
        let fixture = Fixture::start(Some(TOKEN)).await;
        let created: Value = fixture
            .register(json!({"instance": "http_github"}))
            .await
            .json()
            .await
            .expect("the response must be JSON");
        let endpoint_id = created["endpoint_id"]
            .as_str()
            .expect("an id is returned")
            .to_string();
        let webhook = format!("{}/e/{endpoint_id}", fixture.public);

        let accepted = client()
            .post(&webhook)
            .body("{}")
            .send()
            .await
            .expect("the request must reach the listener");
        let revoked = client()
            .delete(format!("{}/admin/endpoints/{endpoint_id}", fixture.admin))
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .json(&json!({"reason": "compromised"}))
            .send()
            .await
            .expect("the request must reach the admin listener");
        let after = client()
            .post(&webhook)
            .body("{}")
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(accepted.status(), StatusCode::OK);
        assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            after.status(),
            StatusCode::NOT_FOUND,
            "a revocation must take effect without a restart"
        );
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_registered_endpoint_when_secret_rotated_should_keep_the_same_path() {
        let fixture = Fixture::start(Some(TOKEN)).await;
        let created: Value = fixture
            .register(hmac_endpoint())
            .await
            .json()
            .await
            .expect("the response must be JSON");
        let endpoint_id = created["endpoint_id"]
            .as_str()
            .expect("an id is returned")
            .to_string();

        let rotated = client()
            .patch(format!("{}/admin/endpoints/{endpoint_id}", fixture.admin))
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .json(&json!({"auth_secret": "whsec_rotated"}))
            .send()
            .await
            .expect("the request must reach the admin listener");

        assert_eq!(rotated.status(), StatusCode::OK);
        let body: Value = rotated.json().await.expect("the response must be JSON");
        assert_eq!(
            body["endpoint_id"],
            json!(endpoint_id),
            "a sender configures the URL once, so rotation must not move it"
        );

        let webhook = format!("{}/e/{endpoint_id}", fixture.public);
        let with_new = post_signed(&webhook, "whsec_rotated", "{}").await;
        let with_old = post_signed(&webhook, "whsec_dynamic", "{}").await;
        assert_eq!(
            with_new.status(),
            StatusCode::OK,
            "the rotated secret must be the one that now validates"
        );
        assert_eq!(
            with_old.status(),
            StatusCode::UNAUTHORIZED,
            "and the replaced secret must stop working"
        );
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_static_endpoint_when_secret_rotated_should_refuse() {
        let fixture = Fixture::start(Some(TOKEN)).await;

        let rotated = client()
            .patch(format!("{}/admin/endpoints/{ENDPOINT_ONE}", fixture.admin))
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .json(&json!({"auth_secret": "whsec_rotated"}))
            .send()
            .await
            .expect("the request must reach the admin listener");

        assert_eq!(
            rotated.status(),
            StatusCode::CONFLICT,
            "restore prefers TOML for an active static endpoint, so a rotation here would \
             silently revert on the next restart and leave a leaked secret in service"
        );
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_empty_secret_when_rotated_should_refuse() {
        let fixture = Fixture::start(Some(TOKEN)).await;
        let created: Value = fixture
            .register(hmac_endpoint())
            .await
            .json()
            .await
            .expect("the response must be JSON");
        let endpoint_id = created["endpoint_id"].as_str().expect("an id").to_string();

        let rotated = client()
            .patch(format!("{}/admin/endpoints/{endpoint_id}", fixture.admin))
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .json(&json!({"auth_secret": ""}))
            .send()
            .await
            .expect("the request must reach the admin listener");

        assert_eq!(
            rotated.status(),
            StatusCode::BAD_REQUEST,
            "an empty HMAC key validates any signature the URL holder can compute"
        );
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_revoked_endpoint_when_secret_rotated_should_answer_not_found() {
        let fixture = Fixture::start(Some(TOKEN)).await;
        let created: Value = fixture
            .register(hmac_endpoint())
            .await
            .json()
            .await
            .expect("the response must be JSON");
        let endpoint_id = created["endpoint_id"]
            .as_str()
            .expect("an id is returned")
            .to_string();
        client()
            .delete(format!("{}/admin/endpoints/{endpoint_id}", fixture.admin))
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .send()
            .await
            .expect("the request must reach the admin listener");

        let rotated = client()
            .patch(format!("{}/admin/endpoints/{endpoint_id}", fixture.admin))
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .json(&json!({"auth_secret": "whsec_rotated"}))
            .send()
            .await
            .expect("the request must reach the admin listener");

        assert_eq!(
            rotated.status(),
            StatusCode::NOT_FOUND,
            "a tombstone must not be revivable through rotation"
        );
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_auth_type_without_secret_when_registered_should_reject() {
        let fixture = Fixture::start(Some(TOKEN)).await;

        let response = fixture
            .register(json!({"instance": "http_github", "auth_type": "hmac-sha256"}))
            .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_past_expiry_when_registered_should_reject() {
        let fixture = Fixture::start(Some(TOKEN)).await;

        let response = fixture
            .register(json!({"instance": "http_github", "expires_at": 1}))
            .await;

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "registering an endpoint that is already Gone is an operator error"
        );
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_unknown_instance_when_registered_should_answer_not_found() {
        let fixture = Fixture::start(Some(TOKEN)).await;

        let response = fixture.register(json!({"instance": "http_stripe"})).await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_untyped_body_when_registered_should_reject() {
        let fixture = Fixture::start(Some(TOKEN)).await;

        let response = fixture
            .register(json!({"instance": "http_github", "auth_type": "totally-made-up"}))
            .await;

        assert!(
            response.status().is_client_error(),
            "an unknown auth_type must be rejected at parse, not coerced into a default"
        );
        fixture.close().await;
    }

    #[tokio::test]
    async fn given_listed_endpoints_when_read_should_never_return_secrets() {
        let fixture = Fixture::start(Some(TOKEN)).await;
        fixture.register(hmac_endpoint()).await;

        let response = client()
            .get(format!("{}/admin/endpoints", fixture.admin))
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .send()
            .await
            .expect("the request must reach the admin listener");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .text()
            .await
            .expect("the response must have a body");
        assert!(!body.contains("whsec_dynamic"));
        assert!(!body.contains("whsec_static"));
        assert!(body.contains("\"origin\":\"dynamic\""));
        assert!(body.contains("\"origin\":\"static\""));
        assert!(
            body.contains("\"submitted\":false"),
            "a caller must be able to tell an accepted mutation from a durable one"
        );
        fixture.close().await;
    }
}
