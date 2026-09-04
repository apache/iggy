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

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use configs::external_auth::{ExternalAuthConfig, ExternalAuthErrorStrategy};
use iggy_common::Permissions;
use serde::{Deserialize, Serialize};
use tracing::warn;

const MAX_RESPONSE_BODY_BYTES: usize = 1_048_576;

thread_local! {
    static HTTP_CLIENT: cyper::Client =
        cyper::Client::new().expect("failed to build cyper HTTP client for external auth");
}

fn get_http_client() -> cyper::Client {
    HTTP_CLIENT.with(cyper::Client::clone)
}

/// Credential metadata sent to the external auth service.
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

/// Callout failure (network, timeout, bad response).
#[derive(Debug)]
pub enum ExternalAuthError {
    HttpError(String),
    Timeout,
    BadResponse(String),
}

impl fmt::Display for ExternalAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HttpError(msg) => write!(f, "external auth HTTP error: {msg}"),
            Self::Timeout => write!(f, "external auth callout timed out"),
            Self::BadResponse(msg) => write!(f, "external auth bad response: {msg}"),
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
/// Carried on the connection/session, never persisted.
#[derive(Debug, Clone)]
pub struct SessionPermissions {
    pub principal: String,
    pub permissions: Permissions,
    pub expires_at: u64,
}

pub use iggy_common::{SYNTHETIC_USER_ID_THRESHOLD, is_synthetic_user_id};

/// Process-wide counter for minting synthetic user IDs. Wraps an
/// `Arc<AtomicU32>` so every transport (TCP, QUIC, WS, HTTP) draws from
/// the same sequence and no two transports can mint the same ID.
#[derive(Clone)]
pub struct SyntheticUserIdCounter(Arc<AtomicU32>);

impl SyntheticUserIdCounter {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(AtomicU32::new(u32::MAX)))
    }

    #[must_use]
    pub fn mint(&self) -> Option<u32> {
        loop {
            let current = self.0.load(Ordering::Relaxed);
            if !is_synthetic_user_id(current) {
                return None;
            }
            if self
                .0
                .compare_exchange_weak(current, current - 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Some(current);
            }
        }
    }
}

impl Default for SyntheticUserIdCounter {
    fn default() -> Self {
        Self::new()
    }
}

/// Call the external auth service and parse the response.
///
/// # Errors
///
/// Returns [`ExternalAuthError`] on network/timeout/parse failure. The
/// caller applies the configured `on_error` strategy.
pub async fn callout_external_auth(
    config: &ExternalAuthConfig,
    request: ExternalAuthRequest,
) -> Result<ExternalAuthDecision, ExternalAuthError> {
    let client = get_http_client();
    let timeout = config.timeout.get_duration();

    let body = serde_json::to_vec(&request)
        .map_err(|e| ExternalAuthError::BadResponse(format!("failed to serialize request: {e}")))?;

    let build = || -> Result<_, ExternalAuthError> {
        Ok(client
            .post(&config.url)
            .map_err(|e| ExternalAuthError::HttpError(format!("failed to build request: {e}")))?
            .header("content-type", "application/json")
            .map_err(|e| ExternalAuthError::HttpError(format!("failed to set header: {e}")))?
            .body(body))
    };
    let request_builder = build()?;

    let response = compio::time::timeout(timeout, request_builder.send())
        .await
        .map_err(|_| ExternalAuthError::Timeout)?
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

    let bytes = compio::time::timeout(timeout, response.bytes())
        .await
        .map_err(|_| ExternalAuthError::Timeout)?
        .map_err(|e| ExternalAuthError::HttpError(format!("failed to read body: {e}")))?;

    if bytes.len() > MAX_RESPONSE_BODY_BYTES {
        return Err(ExternalAuthError::BadResponse(
            "response body too large".into(),
        ));
    }

    let resp: ExternalAuthResponse = serde_json::from_slice(&bytes)
        .map_err(|e| ExternalAuthError::BadResponse(format!("invalid JSON: {e}")))?;

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
/// can act on. Applies the `on_error` strategy on callout failure.
///
/// Returns `Ok(Some(decision))` when the external service responded,
/// `Ok(None)` when the callout failed and `on_error = fallback` (caller
/// should fall through to built-in auth).
///
/// # Errors
///
/// Returns [`ExternalAuthError`] when the callout failed and
/// `on_error = deny`.
pub async fn try_external_auth(
    config: &ExternalAuthConfig,
    request: ExternalAuthRequest,
) -> Result<Option<ExternalAuthDecision>, ExternalAuthError> {
    match callout_external_auth(config, request).await {
        Ok(decision) => Ok(Some(decision)),
        Err(error) => {
            warn!(error = %error, "external auth callout failed");
            match config.on_error {
                ExternalAuthErrorStrategy::Fallback => Ok(None),
                ExternalAuthErrorStrategy::Deny => Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_iggy_user_response_when_parsing_should_extract_user_id() {
        let json = r#"{"decision": "iggy_user", "user_id": 42}"#;
        let resp: ExternalAuthResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.decision, DecisionTag::IggyUser);
        assert_eq!(resp.user_id, Some(42));
    }

    #[test]
    fn given_inline_grant_response_when_parsing_should_extract_permissions() {
        let json = r#"{
            "decision": "inline_grant",
            "principal": "device-1234",
            "permissions": {
                "global": {
                    "manage_servers": false,
                    "read_servers": false,
                    "manage_users": false,
                    "read_users": false,
                    "manage_streams": false,
                    "read_streams": true,
                    "manage_topics": false,
                    "read_topics": true,
                    "poll_messages": true,
                    "send_messages": true
                }
            },
            "expires_at": 1700000000
        }"#;
        let resp: ExternalAuthResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.decision, DecisionTag::InlineGrant);
        assert_eq!(resp.principal.as_deref(), Some("device-1234"));
        assert_eq!(resp.expires_at, Some(1_700_000_000));
        let perms = resp.permissions.unwrap();
        assert!(perms.global.poll_messages);
        assert!(perms.global.send_messages);
        assert!(!perms.global.manage_streams);
    }

    #[test]
    fn given_deny_response_when_parsing_should_extract_reason() {
        let json = r#"{"decision": "deny", "reason": "certificate revoked"}"#;
        let resp: ExternalAuthResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.decision, DecisionTag::Deny);
        assert_eq!(resp.reason.as_deref(), Some("certificate revoked"));
    }

    #[test]
    fn given_deny_response_without_reason_when_parsing_should_succeed() {
        let json = r#"{"decision": "deny"}"#;
        let resp: ExternalAuthResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.decision, DecisionTag::Deny);
        assert!(resp.reason.is_none());
    }

    #[test]
    fn given_synthetic_threshold_when_checking_should_identify_correctly() {
        assert!(!is_synthetic_user_id(0));
        assert!(!is_synthetic_user_id(1));
        assert!(!is_synthetic_user_id(SYNTHETIC_USER_ID_THRESHOLD));
        assert!(is_synthetic_user_id(SYNTHETIC_USER_ID_THRESHOLD + 1));
        assert!(is_synthetic_user_id(u32::MAX));
    }

    #[test]
    fn given_request_when_serializing_should_produce_valid_json() {
        let req = ExternalAuthRequest {
            credential_type: CredentialType::Password,
            credential: Some("secret".to_owned()),
            username: "alice".to_owned(),
            transport: "tcp".to_owned(),
            client_address: "127.0.0.1:5000".to_owned(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"credential_type\":\"password\""));
        assert!(json.contains("\"username\":\"alice\""));
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
}
