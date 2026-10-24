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
//! external HTTP service during login. The service decides whether to map the
//! caller to a pre-provisioned Iggy user (by username) or deny the login.
//!
//! Per-thread in-flight callouts are capped at [`MAX_IN_FLIGHT_CALLOUTS`]
//! via [`CalloutGuard`]. There is no per-client rate limit: operators should
//! rate-limit brute-force attempts at the network layer or inside the
//! external auth service itself.

use std::cell::Cell;
use std::fmt;

use configs::external_auth::ExternalAuthConfig;
use serde::{Deserialize, Serialize};

const MAX_RESPONSE_BODY_BYTES: usize = 1_048_576;

/// Reject credentials longer than this before serializing them into the
/// callout body. A multi-MB PAT passes the built-in hash check (mismatch)
/// and would otherwise be forwarded verbatim to the external service.
const MAX_CREDENTIAL_BYTES: usize = 8_192;

const MAX_IN_FLIGHT_CALLOUTS: u32 = 64;

thread_local! {
    static HTTP_CLIENT: cyper::Client =
        cyper::Client::builder()
            .redirect(cyper::redirect::Policy::none())
            .build()
            .expect("failed to build cyper HTTP client for external auth");

    static IN_FLIGHT_CALLOUTS: Cell<u32> = const { Cell::new(0) };
}

struct CalloutGuard;

impl CalloutGuard {
    fn acquire() -> Result<Self, ExternalAuthError> {
        IN_FLIGHT_CALLOUTS.with(|c| {
            let current = c.get();
            if current >= MAX_IN_FLIGHT_CALLOUTS {
                return Err(ExternalAuthError::HttpError(format!(
                    "too many concurrent callouts ({current})"
                )));
            }
            c.set(current + 1);
            Ok(Self)
        })
    }
}

impl Drop for CalloutGuard {
    fn drop(&mut self) {
        IN_FLIGHT_CALLOUTS.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

fn get_http_client() -> cyper::Client {
    HTTP_CLIENT.with(cyper::Client::clone)
}

/// Credential metadata sent to the external auth service.
///
/// Manual `Debug` redacts the `credential` field so passwords never
/// appear in log output.
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
}

/// JSON response from the external auth service.
#[derive(Debug, Deserialize)]
struct ExternalAuthResponse {
    decision: DecisionTag,
    username: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DecisionTag {
    Allow,
    Deny,
}

/// Parsed decision from the external auth service.
#[derive(Debug)]
pub enum ExternalAuthDecision {
    /// Map the caller to a pre-provisioned Iggy user by username.
    Allow { username: String },
    /// Reject the login.
    Deny { reason: String },
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

fn redact_url_userinfo(url: &str) -> std::borrow::Cow<'_, str> {
    let Some((scheme, rest)) = url.split_once("://") else {
        return std::borrow::Cow::Borrowed(url);
    };
    // Restrict the '@' search to the authority segment (before the first
    // '/') so an '@' in a query parameter does not strip the real host.
    let (authority, tail) = rest.split_once('/').unwrap_or((rest, ""));
    match authority.rsplit_once('@') {
        Some((_, after_at)) => {
            if tail.is_empty() {
                std::borrow::Cow::Owned(format!("{scheme}://{after_at}"))
            } else {
                std::borrow::Cow::Owned(format!("{scheme}://{after_at}/{tail}"))
            }
        }
        None => std::borrow::Cow::Borrowed(url),
    }
}

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
                    redact_url_userinfo(&config.url)
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
                reason: format!(
                    "external_auth.url has no host: {}",
                    redact_url_userinfo(&config.url)
                ),
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
             a callout stalls that login attempt for its full duration"
        );
    }
    Ok(())
}

pub fn warn_insecure_url(config: &ExternalAuthConfig) {
    if config.enabled && config.url.starts_with("http://") {
        tracing::warn!(
            url = %redact_url_userinfo(&config.url),
            "external auth URL uses plain HTTP; credentials will be sent in cleartext"
        );
    }
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

    if request.username.len() > MAX_CREDENTIAL_BYTES {
        return Err(ExternalAuthError::BadRequest(format!(
            "username too large ({} bytes, limit {MAX_CREDENTIAL_BYTES})",
            request.username.len()
        )));
    }
    if let Some(ref cred) = request.credential
        && cred.len() > MAX_CREDENTIAL_BYTES
    {
        return Err(ExternalAuthError::BadRequest(format!(
            "credential too large ({} bytes, limit {MAX_CREDENTIAL_BYTES})",
            cred.len()
        )));
    }

    let _guard = CalloutGuard::acquire()?;
    let client = get_http_client();
    let timeout = config.timeout.get_duration();

    let body = serde_json::to_vec(&request)
        .map_err(|e| ExternalAuthError::BadRequest(format!("failed to serialize request: {e}")))?;

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
        let mut buf = Vec::with_capacity(256);
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

    into_decision(resp)
}

/// Validate required fields per decision variant and convert to the
/// public type. Extracted so tests can exercise the mapping without
/// an HTTP round-trip.
fn into_decision(resp: ExternalAuthResponse) -> Result<ExternalAuthDecision, ExternalAuthError> {
    match resp.decision {
        DecisionTag::Allow => {
            let username = resp.username.ok_or_else(|| {
                ExternalAuthError::BadResponse("allow decision missing username".to_owned())
            })?;
            if username.is_empty() {
                return Err(ExternalAuthError::BadResponse(
                    "allow decision has empty username".to_owned(),
                ));
            }
            Ok(ExternalAuthDecision::Allow { username })
        }
        DecisionTag::Deny => {
            let reason = resp
                .reason
                .unwrap_or_else(|| "denied by external auth".to_owned());
            Ok(ExternalAuthDecision::Deny { reason })
        }
    }
}

/// Resolve an externally authenticated username to a pre-provisioned Iggy
/// user. Guards: the user must exist, be active, and not be root.
/// Transport-independent so both the TCP and HTTP login paths share one
/// implementation.
pub fn resolve_username(
    users: &metadata::stm::user::UsersInner,
    username: &str,
) -> Result<(u32, iggy_common::IggyTimestamp), iggy_common::IggyError> {
    use iggy_common::UserStatus;
    use iggy_common::defaults::DEFAULT_ROOT_USER_ID;

    let Some(user) = users.user_by_name(username) else {
        return Err(iggy_common::IggyError::InvalidCredentials);
    };
    if user.id == DEFAULT_ROOT_USER_ID {
        return Err(iggy_common::IggyError::InvalidCredentials);
    }
    if user.status != UserStatus::Active {
        return Err(iggy_common::IggyError::InvalidCredentials);
    }
    Ok((user.id, user.created_at))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deserialize JSON and convert to a decision, same as
    /// `callout_external_auth` does after receiving the HTTP body.
    fn parse_response(json: &str) -> Result<ExternalAuthDecision, ExternalAuthError> {
        let resp: ExternalAuthResponse = serde_json::from_str(json)
            .map_err(|e| ExternalAuthError::BadResponse(format!("invalid JSON: {e}")))?;
        into_decision(resp)
    }

    // ── request serialization ───────────────────────────────────────

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
    fn request_omits_credential_when_none() {
        let req = ExternalAuthRequest {
            credential_type: CredentialType::Password,
            credential: None,
            username: String::new(),
            transport: "http".to_owned(),
            client_address: "10.0.0.1:443".to_owned(),
        };
        let val: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(val["credential_type"], "password");
        assert!(
            val.get("credential").is_none(),
            "credential must be omitted"
        );
        assert_eq!(val["username"], "");
        assert_eq!(val.as_object().unwrap().len(), 4);
    }

    // ── response deserialization: allow ──────────────────────────────

    #[test]
    fn response_allow_with_username() {
        let json = r#"{"decision": "allow", "username": "ext-alice"}"#;
        match parse_response(json).unwrap() {
            ExternalAuthDecision::Allow { username } => assert_eq!(username, "ext-alice"),
            ExternalAuthDecision::Deny { .. } => panic!("expected Allow, got Deny"),
        }
    }

    #[test]
    fn response_allow_missing_username_should_error() {
        let json = r#"{"decision": "allow"}"#;
        let err = parse_response(json).unwrap_err();
        assert!(err.to_string().contains("missing username"));
    }

    #[test]
    fn response_allow_empty_username_should_error() {
        let json = r#"{"decision": "allow", "username": ""}"#;
        let err = parse_response(json).unwrap_err();
        assert!(err.to_string().contains("empty username"));
    }

    // ── response deserialization: deny ───────────────────────────────

    #[test]
    fn response_deny_with_reason() {
        let json = r#"{"decision": "deny", "reason": "certificate revoked"}"#;
        match parse_response(json).unwrap() {
            ExternalAuthDecision::Deny { reason } => {
                assert_eq!(reason, "certificate revoked");
            }
            ExternalAuthDecision::Allow { .. } => panic!("expected Deny, got Allow"),
        }
    }

    #[test]
    fn response_deny_without_reason_uses_default() {
        let json = r#"{"decision": "deny"}"#;
        match parse_response(json).unwrap() {
            ExternalAuthDecision::Deny { reason } => {
                assert_eq!(reason, "denied by external auth");
            }
            ExternalAuthDecision::Allow { .. } => panic!("expected Deny, got Allow"),
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
        let json = r#"{"username": "alice"}"#;
        assert!(parse_response(json).is_err());
    }

    #[test]
    fn response_extra_fields_accepted() {
        let json = r#"{"decision": "deny", "reason": "no", "audit_id": "abc", "trace": [1,2,3]}"#;
        match parse_response(json).unwrap() {
            ExternalAuthDecision::Deny { reason } => assert_eq!(reason, "no"),
            ExternalAuthDecision::Allow { .. } => panic!("expected Deny, got Allow"),
        }
    }

    #[test]
    fn response_empty_object_should_fail() {
        assert!(parse_response("{}").is_err());
    }

    // ── config validation ───────────────────────────────────────────

    #[test]
    fn given_disabled_config_when_validating_should_accept_empty_url() {
        let config = ExternalAuthConfig {
            enabled: false,
            ..ExternalAuthConfig::default()
        };
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn given_enabled_config_with_empty_url_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
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
            credential_type: CredentialType::Password,
            credential: None,
            username: String::new(),
            transport: "http".to_owned(),
            client_address: "10.0.0.1:443".to_owned(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(!json.contains("credential\":"));
        assert!(json.contains("\"credential_type\":\"password\""));
    }

    #[test]
    fn given_enabled_config_with_zero_timeout_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "https://auth.example.com/verify".to_owned(),
            timeout: "0 s".parse().expect("valid duration"),
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
}
