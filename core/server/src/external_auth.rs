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

//! External authentication callout.
//!
//! When enabled, the server POSTs credential and connection metadata to an
//! external HTTP service during login. The service decides whether to grant
//! access (with inline permissions or by mapping to an existing Iggy user)
//! or deny it. This module owns the request/response types, the HTTP
//! callout, and the session-scoped permission carrier.
//!
//! Known limitation: there is no per-client rate limit on callout attempts.
//! Every failed built-in login triggers a callout, so a brute-force attack
//! can amplify traffic to the external service. The shard reactor's
//! single-threaded model bounds binary-transport concurrency, but the HTTP
//! transport is concurrent. Operators should rate-limit at the network
//! layer or inside the external auth service itself.

use std::fmt;

use configs::external_auth::ExternalAuthConfig;
use iggy_common::Permissions;
use serde::{Deserialize, Serialize};
use tracing::warn;

const MAX_RESPONSE_BODY_BYTES: usize = 1_048_576;

/// Reject credentials longer than this before serializing them into the
/// callout body. A multi-MB PAT passes the built-in hash check (mismatch)
/// and would otherwise be forwarded verbatim to the external service.
const MAX_CREDENTIAL_BYTES: usize = 8_192;

thread_local! {
    static HTTP_CLIENT: cyper::Client =
        cyper::Client::builder()
            .redirect(cyper::redirect::Policy::none())
            .build()
            .expect("failed to build cyper HTTP client for external auth");
}

fn get_http_client() -> cyper::Client {
    HTTP_CLIENT.with(cyper::Client::clone)
}

/// Credential metadata sent to the external auth service.
///
/// For `PersonalAccessToken` logins, `username` is empty (PATs are not
/// associated with a username on the wire). The auth service should use
/// the `credential` value to identify the caller.
///
/// Manual `Debug` redacts the `credential` field so passwords and tokens
/// never appear in log output.
#[derive(Serialize)]
pub struct ExternalAuthRequest {
    pub credential_type: CredentialType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential: Option<String>,
    pub username: String,
    pub transport: String,
    pub client_address: String,
}

impl fmt::Debug for ExternalAuthRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalAuthRequest")
            .field("credential_type", &self.credential_type)
            .field(
                "credential",
                &self.credential.as_ref().map(|_| "[REDACTED]"),
            )
            .field("username", &self.username)
            .field("transport", &self.transport)
            .field("client_address", &self.client_address)
            .finish()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CredentialType {
    Password,
    PersonalAccessToken,
}

/// JSON response from the external auth service.
#[derive(Debug, Deserialize)]
struct ExternalAuthResponse {
    decision: DecisionTag,
    user_id: Option<u32>,
    principal: Option<String>,
    permissions: Option<Permissions>,
    expires_at: Option<u64>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DecisionTag {
    IggyUser,
    InlineGrant,
    Deny,
}

/// Parsed decision from the external auth service.
#[derive(Debug)]
pub enum ExternalAuthDecision {
    IggyUser {
        user_id: u32,
    },
    InlineGrant {
        principal: String,
        permissions: Permissions,
        expires_at: u64,
    },
    Deny {
        reason: String,
    },
}

/// Callout failure (network, timeout, bad response, or rejected request).
#[derive(Debug)]
pub enum ExternalAuthError {
    HttpError(String),
    Timeout,
    BadResponse(String),
    /// The request was rejected before the callout was made (e.g. credential
    /// too large). Distinguished from `BadResponse` so callers/logs can tell
    /// which side was at fault.
    BadRequest(String),
}

impl fmt::Display for ExternalAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HttpError(msg) => write!(f, "external auth HTTP error: {msg}"),
            Self::Timeout => write!(f, "external auth callout timed out"),
            Self::BadResponse(msg) => write!(f, "external auth bad response: {msg}"),
            Self::BadRequest(msg) => write!(f, "external auth bad request: {msg}"),
        }
    }
}

impl std::error::Error for ExternalAuthError {}

/// # Errors
///
/// Returns [`ServerError::InvalidExternalAuthConfig`](crate::server_error::ServerError::InvalidExternalAuthConfig)
/// when the URL is empty or uses an unsupported scheme.
pub fn validate_config(
    config: &ExternalAuthConfig,
) -> Result<(), crate::server_error::ServerError> {
    if !config.enabled {
        return Ok(());
    }
    if config.url.is_empty() {
        return Err(
            crate::server_error::ServerError::InvalidExternalAuthConfig {
                reason: "external_auth.url must be set when external_auth.enabled = true"
                    .to_owned(),
            },
        );
    }
    if !config.url.starts_with("http://") && !config.url.starts_with("https://") {
        return Err(
            crate::server_error::ServerError::InvalidExternalAuthConfig {
                reason: format!(
                    "external_auth.url must start with http:// or https://, got: {}",
                    config.url
                ),
            },
        );
    }
    // Catch bare schemes ("http://") with no host at boot instead of on
    // first callout. The scheme prefix check above already guarantees the
    // split will succeed.
    let after_scheme = config.url.split_once("://").map_or("", |(_, rest)| rest);
    if after_scheme.is_empty() || after_scheme.starts_with('/') {
        return Err(
            crate::server_error::ServerError::InvalidExternalAuthConfig {
                reason: format!("external_auth.url has no host: {}", config.url),
            },
        );
    }
    if config.timeout.get_duration().is_zero() {
        return Err(
            crate::server_error::ServerError::InvalidExternalAuthConfig {
                reason: "external_auth.timeout must be greater than zero".to_owned(),
            },
        );
    }
    // The callout runs inline on the single-threaded shard reactor; a very
    // large timeout blocks the entire shard for its duration. Warn above 30 s.
    if config.timeout.get_duration().as_secs() > 30 {
        tracing::warn!(
            timeout = %config.timeout,
            "external_auth.timeout is unusually large (> 30 s); \
             the callout blocks the shard reactor for its full duration"
        );
    }
    if config.user_id == 0 {
        return Err(
            crate::server_error::ServerError::InvalidExternalAuthConfig {
                reason: "external_auth.user_id must not be 0 (reserved for root)".to_owned(),
            },
        );
    }
    // The slab allocator assigns user IDs sequentially from 0 (root).
    // A low user_id will collide with a real user once enough are created,
    // silently restricting that user to data-plane ops. The default
    // (u32::MAX) avoids this; warn if the operator picked a low value.
    if config.user_id < 1_000_000 {
        tracing::warn!(
            user_id = config.user_id,
            "external_auth.user_id is low; it may collide with a future Iggy user ID. \
             Consider using a large value (the default is u32::MAX)."
        );
    }
    Ok(())
}

pub fn warn_insecure_url(config: &ExternalAuthConfig) {
    if config.enabled && config.url.starts_with("http://") {
        tracing::warn!(
            url = config.url,
            "external auth URL uses plain HTTP; credentials will be sent in cleartext"
        );
    }
}

/// Session-scoped permissions from an external auth inline grant.
/// Carried on the connection/session, never persisted. The permissions
/// are `Arc`-wrapped so dispatch-time lookups share rather than clone
/// the full `BTreeMap` tree on every request.
#[derive(Debug, Clone)]
pub struct SessionPermissions {
    pub principal: String,
    pub permissions: std::sync::Arc<Permissions>,
    pub expires_at: u64,
}

/// Call the external auth service and parse the response.
///
/// # Errors
///
/// Returns [`ExternalAuthError`] on network/timeout/parse failure.
/// Fail-closed: every error variant denies the login.
pub async fn callout_external_auth(
    config: &ExternalAuthConfig,
    request: ExternalAuthRequest,
) -> Result<ExternalAuthDecision, ExternalAuthError> {
    use futures::StreamExt;

    if let Some(ref cred) = request.credential
        && cred.len() > MAX_CREDENTIAL_BYTES
    {
        return Err(ExternalAuthError::BadRequest(format!(
            "credential too large ({} bytes, limit {MAX_CREDENTIAL_BYTES})",
            cred.len()
        )));
    }

    let client = get_http_client();
    let timeout = config.timeout.get_duration();

    let body = serde_json::to_vec(&request)
        .map_err(|e| ExternalAuthError::BadResponse(format!("failed to serialize request: {e}")))?;

    // Single timeout wrapping the entire round-trip (connect + headers +
    // body read) so a slow-drip body cannot extend the window to 2x.
    let round_trip = async {
        let request_builder = client
            .post(&config.url)
            .map_err(|e| ExternalAuthError::HttpError(format!("failed to build request: {e}")))?
            .header("content-type", "application/json")
            .map_err(|e| ExternalAuthError::HttpError(format!("failed to set header: {e}")))?
            .body(body);

        let response = request_builder
            .send()
            .await
            .map_err(|e| ExternalAuthError::HttpError(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            return Err(ExternalAuthError::HttpError(format!(
                "non-success status: {status}"
            )));
        }

        if let Some(len) = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok())
            && len > MAX_RESPONSE_BODY_BYTES
        {
            return Err(ExternalAuthError::BadResponse(
                "response body too large".to_owned(),
            ));
        }

        // Stream the body with a running cap so a length-less response cannot
        // OOM the server. Matches the forward.rs pattern.
        let mut buf = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|e| ExternalAuthError::HttpError(format!("body read: {e}")))?;
            if buf.len() + chunk.len() > MAX_RESPONSE_BODY_BYTES {
                return Err(ExternalAuthError::BadResponse(
                    "response body too large".to_owned(),
                ));
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    };
    let bytes = compio::time::timeout(timeout, round_trip)
        .await
        .map_err(|_| ExternalAuthError::Timeout)??;

    let resp: ExternalAuthResponse = serde_json::from_slice(&bytes)
        .map_err(|e| ExternalAuthError::BadResponse(format!("invalid JSON: {e}")))?;

    parse_decision(resp)
}

/// Map a deserialized response to a decision. Extracted so tests can
/// exercise the mapping without an HTTP round-trip.
fn parse_decision(resp: ExternalAuthResponse) -> Result<ExternalAuthDecision, ExternalAuthError> {
    match resp.decision {
        DecisionTag::IggyUser => {
            let user_id = resp.user_id.ok_or_else(|| {
                ExternalAuthError::BadResponse("iggy_user decision missing user_id".to_owned())
            })?;
            Ok(ExternalAuthDecision::IggyUser { user_id })
        }
        DecisionTag::InlineGrant => {
            let principal = resp.principal.ok_or_else(|| {
                ExternalAuthError::BadResponse("inline_grant decision missing principal".to_owned())
            })?;
            let permissions = resp.permissions.ok_or_else(|| {
                ExternalAuthError::BadResponse(
                    "inline_grant decision missing permissions".to_owned(),
                )
            })?;
            // When the external auth service omits expires_at, the grant
            // logically never expires. On the HTTP path, generate_capped
            // caps the JWT at the server's configured token expiry. On the
            // binary transport, the raw u64::MAX rides on the connection
            // and expires_at is only checked against the bus clock at
            // dispatch time.
            let expires_at = resp.expires_at.unwrap_or(u64::MAX);
            Ok(ExternalAuthDecision::InlineGrant {
                principal,
                permissions,
                expires_at,
            })
        }
        DecisionTag::Deny => {
            let reason = resp
                .reason
                .unwrap_or_else(|| "denied by external auth".to_owned());
            Ok(ExternalAuthDecision::Deny { reason })
        }
    }
}

/// Try external auth and map the result to a decision the login flow
/// can act on. Fail-closed: a callout failure (timeout, network error,
/// bad response) denies the login.
///
/// # Errors
///
/// Returns [`ExternalAuthError`] when the callout itself failed.
pub async fn try_external_auth(
    config: &ExternalAuthConfig,
    request: ExternalAuthRequest,
) -> Result<ExternalAuthDecision, ExternalAuthError> {
    match callout_external_auth(config, request).await {
        Ok(decision) => Ok(decision),
        Err(error) => {
            warn!(error = %error, "external auth callout failed");
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deserialize JSON and run the same decision-mapping logic that
    /// `callout_external_auth` uses after receiving the HTTP body.
    fn parse_response(json: &str) -> Result<ExternalAuthDecision, ExternalAuthError> {
        let resp: ExternalAuthResponse = serde_json::from_str(json)
            .map_err(|e| ExternalAuthError::BadResponse(format!("invalid JSON: {e}")))?;
        parse_decision(resp)
    }

    // ── request serialization (full schema) ─────────────────────────

    #[test]
    fn request_password_round_trip() {
        let req = ExternalAuthRequest {
            credential_type: CredentialType::Password,
            credential: Some("s3cret".to_owned()),
            username: "alice".to_owned(),
            transport: "tcp".to_owned(),
            client_address: "10.0.0.1:5000".to_owned(),
        };
        let val: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(val["credential_type"], "password");
        assert_eq!(val["credential"], "s3cret");
        assert_eq!(val["username"], "alice");
        assert_eq!(val["transport"], "tcp");
        assert_eq!(val["client_address"], "10.0.0.1:5000");
        assert_eq!(val.as_object().unwrap().len(), 5);
    }

    #[test]
    fn request_pat_omits_credential_when_none() {
        let req = ExternalAuthRequest {
            credential_type: CredentialType::PersonalAccessToken,
            credential: None,
            username: String::new(),
            transport: "http".to_owned(),
            client_address: "10.0.0.1:443".to_owned(),
        };
        let val: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(val["credential_type"], "personal_access_token");
        assert!(
            val.get("credential").is_none(),
            "credential must be omitted"
        );
        assert_eq!(val["username"], "");
        assert_eq!(val.as_object().unwrap().len(), 4);
    }

    // ── response deserialization: iggy_user ──────────────────────────

    #[test]
    fn response_iggy_user_complete() {
        let json = r#"{"decision": "iggy_user", "user_id": 42}"#;
        match parse_response(json).unwrap() {
            ExternalAuthDecision::IggyUser { user_id } => assert_eq!(user_id, 42),
            other => panic!("expected IggyUser, got {other:?}"),
        }
    }

    #[test]
    fn response_iggy_user_missing_user_id_should_error() {
        let json = r#"{"decision": "iggy_user"}"#;
        let err = parse_response(json).unwrap_err();
        assert!(err.to_string().contains("missing user_id"));
    }

    // ── response deserialization: inline_grant ───────────────────────

    #[test]
    fn response_inline_grant_complete() {
        let json = r#"{
            "decision": "inline_grant",
            "principal": "device-1234",
            "permissions": {
                "global": {
                    "read_streams": true,
                    "read_topics": true,
                    "poll_messages": true,
                    "send_messages": true
                },
                "streams": {
                    "1": {
                        "poll_messages": true,
                        "send_messages": true,
                        "topics": {
                            "0": { "poll_messages": true }
                        }
                    }
                }
            },
            "expires_at": 1700000000
        }"#;
        match parse_response(json).unwrap() {
            ExternalAuthDecision::InlineGrant {
                principal,
                permissions,
                expires_at,
            } => {
                assert_eq!(principal, "device-1234");
                assert_eq!(expires_at, 1_700_000_000);
                // global
                assert!(permissions.global.poll_messages);
                assert!(permissions.global.send_messages);
                assert!(permissions.global.read_streams);
                assert!(!permissions.global.manage_servers);
                // per-stream
                let stream = permissions.streams.as_ref().unwrap().get(&1).unwrap();
                assert!(stream.poll_messages);
                assert!(stream.send_messages);
                assert!(!stream.manage_stream);
                // per-topic inside stream
                let topic = stream.topics.as_ref().unwrap().get(&0).unwrap();
                assert!(topic.poll_messages);
                assert!(!topic.send_messages);
            }
            other => panic!("expected InlineGrant, got {other:?}"),
        }
    }

    #[test]
    fn response_inline_grant_minimal_permissions_default_all_false() {
        let json = r#"{
            "decision": "inline_grant",
            "principal": "dev-1",
            "permissions": {}
        }"#;
        match parse_response(json).unwrap() {
            ExternalAuthDecision::InlineGrant {
                permissions,
                expires_at,
                ..
            } => {
                assert!(!permissions.global.send_messages);
                assert!(!permissions.global.poll_messages);
                assert!(!permissions.global.manage_servers);
                assert!(!permissions.global.read_servers);
                assert!(!permissions.global.manage_users);
                assert!(!permissions.global.read_users);
                assert!(!permissions.global.manage_streams);
                assert!(!permissions.global.read_streams);
                assert!(!permissions.global.manage_topics);
                assert!(!permissions.global.read_topics);
                assert!(permissions.streams.is_none());
                assert_eq!(expires_at, u64::MAX, "omitted expires_at defaults to MAX");
            }
            other => panic!("expected InlineGrant, got {other:?}"),
        }
    }

    #[test]
    fn response_inline_grant_missing_principal_should_error() {
        let json = r#"{
            "decision": "inline_grant",
            "permissions": {"global": {}},
            "expires_at": 9999999999
        }"#;
        let err = parse_response(json).unwrap_err();
        assert!(err.to_string().contains("missing principal"));
    }

    #[test]
    fn response_inline_grant_missing_permissions_should_error() {
        let json = r#"{
            "decision": "inline_grant",
            "principal": "dev-1"
        }"#;
        let err = parse_response(json).unwrap_err();
        assert!(err.to_string().contains("missing permissions"));
    }

    // ── response deserialization: deny ───────────────────────────────

    #[test]
    fn response_deny_with_reason() {
        let json = r#"{"decision": "deny", "reason": "certificate revoked"}"#;
        match parse_response(json).unwrap() {
            ExternalAuthDecision::Deny { reason } => {
                assert_eq!(reason, "certificate revoked");
            }
            other => panic!("expected Deny, got {other:?}"),
        }
    }

    #[test]
    fn response_deny_without_reason_uses_default() {
        let json = r#"{"decision": "deny"}"#;
        match parse_response(json).unwrap() {
            ExternalAuthDecision::Deny { reason } => {
                assert_eq!(reason, "denied by external auth");
            }
            other => panic!("expected Deny, got {other:?}"),
        }
    }

    // ── response edge cases ─────────────────────────────────────────

    #[test]
    fn response_unknown_decision_should_fail() {
        let json = r#"{"decision": "pass_through"}"#;
        assert!(parse_response(json).is_err());
    }

    #[test]
    fn response_missing_decision_should_fail() {
        let json = r#"{"user_id": 42}"#;
        assert!(parse_response(json).is_err());
    }

    #[test]
    fn response_extra_fields_accepted() {
        let json = r#"{"decision": "deny", "reason": "no", "audit_id": "abc", "trace": [1,2,3]}"#;
        match parse_response(json).unwrap() {
            ExternalAuthDecision::Deny { reason } => assert_eq!(reason, "no"),
            other => panic!("expected Deny, got {other:?}"),
        }
    }

    #[test]
    fn response_empty_object_should_fail() {
        assert!(parse_response("{}").is_err());
    }

    #[test]
    fn given_disabled_config_when_validating_should_accept_empty_url() {
        let config = ExternalAuthConfig {
            enabled: false,
            url: String::new(),
            ..ExternalAuthConfig::default()
        };
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn given_enabled_config_with_empty_url_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: String::new(),
            ..ExternalAuthConfig::default()
        };
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("external_auth.url must be set"));
    }

    #[test]
    fn given_enabled_config_with_invalid_scheme_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "ftp://auth.example.com".to_owned(),
            ..ExternalAuthConfig::default()
        };
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("must start with http://"));
    }

    #[test]
    fn given_enabled_config_with_https_url_when_validating_should_accept() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "https://auth.example.com/verify".to_owned(),
            ..ExternalAuthConfig::default()
        };
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn given_enabled_config_with_http_url_when_validating_should_accept() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "http://localhost:8080/auth".to_owned(),
            ..ExternalAuthConfig::default()
        };
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn given_request_without_credential_when_serializing_should_omit_field() {
        let req = ExternalAuthRequest {
            credential_type: CredentialType::PersonalAccessToken,
            credential: None,
            username: String::new(),
            transport: "http".to_owned(),
            client_address: "10.0.0.1:443".to_owned(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(!json.contains("credential\":"));
        // credential_type is still present
        assert!(json.contains("\"credential_type\":\"personal_access_token\""));
    }

    #[test]
    fn given_enabled_config_with_zero_timeout_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "https://auth.example.com/verify".to_owned(),
            timeout: "0 s".parse().expect("valid duration"),
            ..ExternalAuthConfig::default()
        };
        let err = validate_config(&config).unwrap_err();
        assert!(
            err.to_string()
                .contains("timeout must be greater than zero"),
        );
    }

    #[test]
    fn given_enabled_config_with_bare_scheme_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "https://".to_owned(),
            ..ExternalAuthConfig::default()
        };
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("has no host"));
    }

    #[test]
    fn given_enabled_config_with_scheme_and_path_only_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "http:///path".to_owned(),
            ..ExternalAuthConfig::default()
        };
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("has no host"));
    }

    #[test]
    fn given_enabled_config_with_user_id_zero_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "https://auth.example.com".to_owned(),
            user_id: 0,
            ..ExternalAuthConfig::default()
        };
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("must not be 0"));
    }
}
