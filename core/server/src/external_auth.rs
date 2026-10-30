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
//! Per-thread in-flight callouts are capped at [`ExternalAuthConfig::max_concurrent_callouts`]
//! via [`CalloutGuard`]. There is no per-client rate limit: operators should
//! rate-limit brute-force attempts at the network layer or inside the
//! external auth service itself.

use std::cell::{Cell, RefCell};
use std::fmt;

use configs::external_auth::ExternalAuthConfig;
use futures::StreamExt;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

const MAX_RESPONSE_BODY_BYTES: usize = 1_048_576;

/// Reject credentials longer than this before serializing them into the
/// callout body.
const MAX_CREDENTIAL_BYTES: usize = 8_192;

thread_local! {
    // Lazily built per shard thread. `preflight` probes the build at boot
    // so a missing CA store fails startup, not the first login.
    static HTTP_CLIENT: RefCell<Option<cyper::Client>> = const { RefCell::new(None) };

    static IN_FLIGHT_CALLOUTS: Cell<u32> = const { Cell::new(0) };
}

#[derive(Debug)]
struct CalloutGuard;

impl CalloutGuard {
    fn acquire(max: u32) -> Result<Self, ExternalAuthError> {
        IN_FLIGHT_CALLOUTS.with(|c| {
            let current = c.get();
            if current >= max {
                return Err(ExternalAuthError::Overloaded(current));
            }
            c.set(current + 1);
            Ok(Self)
        })
    }
}

impl Drop for CalloutGuard {
    fn drop(&mut self) {
        IN_FLIGHT_CALLOUTS.with(|c| c.set(c.get() - 1));
    }
}

fn build_http_client() -> Result<cyper::Client, String> {
    cyper::Client::builder()
        .redirect(cyper::redirect::Policy::none())
        .build()
        .map_err(|e| format!("failed to build HTTP client (missing CA roots?): {e}"))
}

fn get_http_client() -> Result<cyper::Client, ExternalAuthError> {
    HTTP_CLIENT.with(|cell| {
        let mut slot = cell.borrow_mut();
        if let Some(client) = slot.as_ref() {
            return Ok(client.clone());
        }
        let client = build_http_client().map_err(ExternalAuthError::HttpError)?;
        let cloned = client.clone();
        *slot = Some(client);
        Ok(cloned)
    })
}

fn serialize_secret<S: serde::Serializer>(
    value: &SecretString,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(value.expose_secret())
}

/// Transport label for HTTP REST logins, which do not go through the
/// binary `ClientTransportKind` enum. Matches the value documented in
/// config.toml.
pub const HTTP_TRANSPORT: &str = "http";

/// Connection and credential metadata sent to the external auth service.
///
/// Borrows transport metadata and the credential from the caller to avoid
/// copying the secret. `SecretString` redacts itself in `Debug`.
#[derive(Debug, Serialize)]
pub struct ExternalAuthRequest<'a> {
    pub transport: &'a str,
    pub client_address: &'a str,
    pub connection_id: &'a str,
    pub credentials: Credentials<'a>,
}

/// Credential payload for the external auth callout.
///
/// Internally tagged on `"type"` so each variant carries exactly the fields
/// it needs. Future credential kinds (bearer tokens, client certificates)
/// add a variant here without touching the request envelope.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Credentials<'a> {
    UsernamePassword {
        username: &'a str,
        #[serde(serialize_with = "serialize_secret")]
        password: &'a SecretString,
    },
}

fn default_deny_reason() -> String {
    "denied by external auth".to_owned()
}

/// Decision from the external auth service, deserialized directly from the
/// JSON response. Tagged on `"decision"`.
#[derive(Debug, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum ExternalAuthDecision {
    /// Map the caller to a pre-provisioned Iggy user by username.
    Allow { username: String },
    /// Reject the login.
    Deny {
        #[serde(default = "default_deny_reason")]
        reason: String,
    },
}

/// Callout failure (network, timeout, bad response, or rejected request).
///
/// The SDK stores credentials after a successful login and drops them on a
/// terminal failure. Transient variants ([`is_transient`](Self::is_transient))
/// surface as `TransientNotAccepted` so the SDK retries instead of discarding
/// stored credentials during a brief auth-service outage.
#[derive(Debug)]
pub enum ExternalAuthError {
    /// Connect error, TLS failure, or other transport-level fault.
    HttpError(String),
    /// The external service returned 5xx or 429.
    ServiceUnavailable(u16),
    /// The external service returned a non-retryable non-success status (4xx
    /// other than 429).
    HttpStatus(u16),
    Timeout,
    BadResponse(String),
    /// The request was rejected before the callout was made (e.g. credential
    /// too large). Distinguished from `BadResponse` so callers/logs can tell
    /// which side was at fault.
    BadRequest(String),
    /// Too many concurrent callouts on this shard thread. The caller must
    /// return a retryable error so the SDK retries after backoff.
    Overloaded(u32),
}

impl ExternalAuthError {
    /// Transient failures are retryable. The caller maps these to
    /// `TransientNotAccepted` so the SDK backs off instead of dropping
    /// stored credentials.
    pub const fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Overloaded(_) | Self::Timeout | Self::HttpError(_) | Self::ServiceUnavailable(_)
        )
    }
}

impl fmt::Display for ExternalAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HttpError(msg) => write!(f, "external auth HTTP error: {msg}"),
            Self::ServiceUnavailable(status) => {
                write!(f, "external auth service unavailable: HTTP {status}")
            }
            Self::HttpStatus(status) => {
                write!(f, "external auth non-success status: HTTP {status}")
            }
            Self::Timeout => write!(f, "external auth callout timed out"),
            Self::BadResponse(msg) => write!(f, "external auth bad response: {msg}"),
            Self::BadRequest(msg) => write!(f, "external auth bad request: {msg}"),
            Self::Overloaded(n) => {
                write!(f, "too many concurrent ext-auth callouts ({n})")
            }
        }
    }
}

impl std::error::Error for ExternalAuthError {}

/// Extract `scheme://host` from a URL, dropping path, query, and fragment.
/// Falls back to the full URL if the scheme separator is absent.
fn scheme_and_host(url: &str) -> &str {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url;
    };
    // The authority ends at the first of '/', '?', or '#'.
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    &url[..scheme.len() + 3 + authority_end]
}

/// Boot-time checks that need server-side dependencies (TLS roots, tracing).
/// Called after `Validatable::validate` has checked the config values.
pub fn preflight(config: &ExternalAuthConfig) -> Result<(), crate::server_error::ServerError> {
    if !config.enabled {
        return Ok(());
    }
    // The callout is async and yields while waiting, so a large timeout
    // stalls only this login attempt, not the shard reactor.
    if config.timeout.get_duration().as_secs() > 30 {
        tracing::warn!(
            timeout = %config.timeout,
            "external_auth.timeout is unusually large (> 30 s); \
             a callout stalls that login attempt for its full duration"
        );
    }
    if config.url.starts_with("http://") {
        tracing::warn!(
            url = %scheme_and_host(&config.url),
            "external auth URL uses plain HTTP; credentials will be sent in cleartext"
        );
    }
    // Build a throwaway client to verify system TLS roots are loadable.
    // Each shard thread builds its own later; this catches the failure at
    // boot instead of panicking on the first callout.
    build_http_client()
        .map_err(|reason| crate::server_error::ServerError::ExternalAuthClient { reason })?;
    Ok(())
}

fn validate_credential_size(credentials: &Credentials<'_>) -> Result<(), ExternalAuthError> {
    match credentials {
        Credentials::UsernamePassword { username, password } => {
            if username.len() > MAX_CREDENTIAL_BYTES {
                return Err(ExternalAuthError::BadRequest(format!(
                    "username too large ({} bytes, limit {MAX_CREDENTIAL_BYTES})",
                    username.len()
                )));
            }
            let password_len = password.expose_secret().len();
            if password_len > MAX_CREDENTIAL_BYTES {
                return Err(ExternalAuthError::BadRequest(format!(
                    "credential too large ({password_len} bytes, limit {MAX_CREDENTIAL_BYTES})",
                )));
            }
        }
    }
    Ok(())
}

/// Validate the `Allow` decision: username must be present and non-empty.
/// Validated decision: `Allow` carries a guaranteed non-empty username.
#[derive(Debug)]
pub enum ValidatedDecision {
    Allow { username: String },
    Deny { reason: String },
}

fn validate_decision(
    decision: ExternalAuthDecision,
) -> Result<ValidatedDecision, ExternalAuthError> {
    match decision {
        ExternalAuthDecision::Allow { username } => {
            if username.is_empty() {
                return Err(ExternalAuthError::BadResponse(
                    "allow decision has empty username".to_owned(),
                ));
            }
            Ok(ValidatedDecision::Allow { username })
        }
        ExternalAuthDecision::Deny { reason } => Ok(ValidatedDecision::Deny { reason }),
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
    request: ExternalAuthRequest<'_>,
) -> Result<ValidatedDecision, ExternalAuthError> {
    validate_credential_size(&request.credentials)?;

    let _guard = CalloutGuard::acquire(config.max_concurrent_callouts)?;
    let client = get_http_client()?;
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
            let code = status.as_u16();
            return Err(if status.is_server_error() || code == 429 {
                ExternalAuthError::ServiceUnavailable(code)
            } else {
                ExternalAuthError::HttpStatus(code)
            });
        }

        if response
            .content_length()
            .is_some_and(|len| len > MAX_RESPONSE_BODY_BYTES as u64)
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

    let decision: ExternalAuthDecision = serde_json::from_slice(&bytes)
        .map_err(|e| ExternalAuthError::BadResponse(format!("invalid JSON: {e}")))?;

    validate_decision(decision)
}

/// Validate that a pre-provisioned user is eligible for external login:
/// exists, active, not root, and carries no unrecognized critical options.
/// Reused by the trusted-issuer JWT path.
pub fn validate_provisioned_account(
    user: Option<&metadata::stm::user::User>,
) -> Option<(u32, iggy_common::IggyTimestamp)> {
    check_account_policy(user?).ok()
}

/// Why a pre-provisioned account was rejected. Logged for debugging but
/// never exposed on the wire (the caller maps every variant to
/// `InvalidCredentials`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountRejection {
    NotFound,
    Root,
    Inactive,
    CriticalOptions,
}

impl std::fmt::Display for AccountRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NotFound => "user not found",
            Self::Root => "root account cannot be used for external auth",
            Self::Inactive => "user account is inactive",
            Self::CriticalOptions => "user has unrecognized critical options",
        })
    }
}

/// Resolve an externally authenticated username to a pre-provisioned Iggy
/// user. Logs the specific rejection reason; the wire error is always
/// `InvalidCredentials`.
pub fn resolve_username(
    users: &metadata::stm::user::UsersInner,
    username: &str,
) -> Result<(u32, iggy_common::IggyTimestamp), iggy_common::IggyError> {
    let Some(user) = users.user_by_name(username) else {
        tracing::warn!(username, reason = %AccountRejection::NotFound, "ext-auth account rejected");
        return Err(iggy_common::IggyError::InvalidCredentials);
    };
    check_account_policy(user).map_err(|reason| {
        tracing::warn!(username, user_id = user.id, reason = %reason, "ext-auth account rejected");
        iggy_common::IggyError::InvalidCredentials
    })
}

fn check_account_policy(
    user: &metadata::stm::user::User,
) -> Result<(u32, iggy_common::IggyTimestamp), AccountRejection> {
    use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
    use iggy_common::{UserStatus, check_critical_options};

    if user.id == DEFAULT_ROOT_USER_ID {
        return Err(AccountRejection::Root);
    }
    if user.status != UserStatus::Active {
        return Err(AccountRejection::Inactive);
    }
    if check_critical_options(&user.options).is_err() {
        return Err(AccountRejection::CriticalOptions);
    }
    Ok((user.id, user.created_at))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deserialize JSON and validate, same as `callout_external_auth` does
    /// after receiving the HTTP body.
    fn parse_response(json: &str) -> Result<ValidatedDecision, ExternalAuthError> {
        let decision: ExternalAuthDecision = serde_json::from_str(json)
            .map_err(|e| ExternalAuthError::BadResponse(format!("invalid JSON: {e}")))?;
        validate_decision(decision)
    }

    // ── request serialization ───────────────────────────────────────

    #[test]
    fn request_username_password_round_trip() {
        let password = SecretString::from("s3cret".to_owned());
        let req = ExternalAuthRequest {
            transport: "tcp",
            client_address: "10.0.0.1:5000",
            connection_id: "0195f4d7-57d3-7c21-a711-08a147ad6c90",
            credentials: Credentials::UsernamePassword {
                username: "alice",
                password: &password,
            },
        };
        let val: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(val["transport"], "tcp");
        assert_eq!(val["client_address"], "10.0.0.1:5000");
        assert_eq!(val["connection_id"], "0195f4d7-57d3-7c21-a711-08a147ad6c90");
        let creds = &val["credentials"];
        assert_eq!(creds["type"], "username_password");
        assert_eq!(creds["username"], "alice");
        assert_eq!(creds["password"], "s3cret");
        assert_eq!(creds.as_object().unwrap().len(), 3);
    }

    // ── response deserialization: allow ──────────────────────────────

    #[test]
    fn response_allow_with_username() {
        let json = r#"{"decision": "allow", "username": "ext-alice"}"#;
        match parse_response(json).unwrap() {
            ValidatedDecision::Allow { username } => {
                assert_eq!(username, "ext-alice");
            }
            ValidatedDecision::Deny { .. } => panic!("expected Allow, got Deny"),
        }
    }

    #[test]
    fn response_allow_missing_username_should_error() {
        let json = r#"{"decision": "allow"}"#;
        let err = parse_response(json).unwrap_err();
        assert!(
            err.to_string().contains("username"),
            "serde must report the missing field: {err}"
        );
    }

    #[test]
    fn response_allow_empty_username_should_error() {
        let json = r#"{"decision": "allow", "username": ""}"#;
        let err = parse_response(json).unwrap_err();
        assert!(err.to_string().contains("empty username"));
    }

    #[test]
    fn response_allow_with_stray_reason_should_parse() {
        let json = r#"{"decision": "allow", "username": "ext-bob", "reason": "extra"}"#;
        match parse_response(json).unwrap() {
            ValidatedDecision::Allow { username } => {
                assert_eq!(username, "ext-bob");
            }
            ValidatedDecision::Deny { .. } => panic!("expected Allow, got Deny"),
        }
    }

    // ── response deserialization: deny ───────────────────────────────

    #[test]
    fn response_deny_with_reason() {
        let json = r#"{"decision": "deny", "reason": "certificate revoked"}"#;
        match parse_response(json).unwrap() {
            ValidatedDecision::Deny { reason } => {
                assert_eq!(reason, "certificate revoked");
            }
            ValidatedDecision::Allow { .. } => panic!("expected Deny, got Allow"),
        }
    }

    #[test]
    fn response_deny_without_reason_uses_default() {
        let json = r#"{"decision": "deny"}"#;
        match parse_response(json).unwrap() {
            ValidatedDecision::Deny { reason } => {
                assert_eq!(reason, "denied by external auth");
            }
            ValidatedDecision::Allow { .. } => panic!("expected Deny, got Allow"),
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
            ValidatedDecision::Deny { reason } => assert_eq!(reason, "no"),
            ValidatedDecision::Allow { .. } => panic!("expected Deny, got Allow"),
        }
    }

    #[test]
    fn response_empty_object_should_fail() {
        assert!(parse_response("{}").is_err());
    }

    // ── scheme_and_host ────────────────────────────────────────────

    #[test]
    fn scheme_and_host_strips_path_and_query() {
        assert_eq!(
            scheme_and_host("https://auth.example.com/verify?token=secret"),
            "https://auth.example.com"
        );
        assert_eq!(
            scheme_and_host("http://localhost:8080/auth#frag"),
            "http://localhost:8080"
        );
        assert_eq!(scheme_and_host("https://host"), "https://host");
    }

    #[test]
    fn scheme_and_host_does_not_leak_query_at_in_authority() {
        // An '@' in the query must not be mistaken for a userinfo separator.
        assert_eq!(
            scheme_and_host("http://auth.local?next=a@b"),
            "http://auth.local"
        );
    }

    // ── runtime probe ───────────────────────────────────────────────

    #[test]
    fn given_enabled_config_with_https_url_when_probing_runtime_should_accept() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = ExternalAuthConfig {
            enabled: true,
            url: "https://auth.example.com/verify".to_owned(),
            ..ExternalAuthConfig::default()
        };
        assert!(preflight(&config).is_ok());
    }

    #[test]
    fn given_enabled_config_with_http_url_when_probing_runtime_should_accept() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = ExternalAuthConfig {
            enabled: true,
            url: "http://localhost:8080/auth".to_owned(),
            ..ExternalAuthConfig::default()
        };
        assert!(preflight(&config).is_ok());
    }

    #[test]
    fn request_credentials_nested_under_tagged_envelope() {
        let password = SecretString::from("pw".to_owned());
        let req = ExternalAuthRequest {
            transport: "http",
            client_address: "10.0.0.1:443",
            connection_id: "00000000-0000-0000-0000-000000000001",
            credentials: Credentials::UsernamePassword {
                username: "bob",
                password: &password,
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"credentials\":{"));
        assert!(json.contains("\"type\":\"username_password\""));
    }

    // ── callout guard ──────────────────────────────────────────────

    #[test]
    fn given_guard_at_capacity_when_acquired_should_return_overloaded() {
        const CAP: u32 = 4;
        IN_FLIGHT_CALLOUTS.with(|c| c.set(0));

        let mut guards = Vec::with_capacity(CAP as usize);
        for _ in 0..CAP {
            guards.push(CalloutGuard::acquire(CAP).expect("acquire must succeed below cap"));
        }

        let err = CalloutGuard::acquire(CAP).unwrap_err();
        assert!(matches!(err, ExternalAuthError::Overloaded(_)));
        assert!(err.is_transient());
        assert!(err.to_string().contains("too many concurrent"));

        // Drop one guard and verify the next acquire succeeds.
        guards.pop();
        let _recovered = CalloutGuard::acquire(CAP).expect("acquire must succeed after drop");

        drop(guards);
    }

    #[test]
    fn given_transient_errors_when_checked_should_report_transient() {
        assert!(ExternalAuthError::Overloaded(64).is_transient());
        assert!(ExternalAuthError::Timeout.is_transient());
        assert!(ExternalAuthError::HttpError("connection refused".to_owned()).is_transient());
        assert!(ExternalAuthError::ServiceUnavailable(503).is_transient());
        assert!(ExternalAuthError::ServiceUnavailable(429).is_transient());
    }

    #[test]
    fn given_terminal_errors_when_checked_should_not_report_transient() {
        assert!(!ExternalAuthError::HttpStatus(401).is_transient());
        assert!(!ExternalAuthError::BadResponse("bad json".to_owned()).is_transient());
        assert!(!ExternalAuthError::BadRequest("too large".to_owned()).is_transient());
    }

    // ── account validation ─────────────────────────────────────────

    fn test_user(id: u32, status: iggy_common::UserStatus) -> metadata::stm::user::User {
        metadata::stm::user::User {
            id,
            status,
            created_at: iggy_common::IggyTimestamp::from(1_700_000_000_000_000),
            ..metadata::stm::user::User::default()
        }
    }

    #[test]
    fn given_absent_user_when_validating_account_should_reject() {
        assert!(validate_provisioned_account(None).is_none());
    }

    #[test]
    fn given_root_user_when_validating_account_should_reject() {
        let root = test_user(
            iggy_common::defaults::DEFAULT_ROOT_USER_ID,
            iggy_common::UserStatus::Active,
        );
        assert!(validate_provisioned_account(Some(&root)).is_none());
    }

    #[test]
    fn given_inactive_user_when_validating_account_should_reject() {
        let inactive = test_user(1, iggy_common::UserStatus::Inactive);
        assert!(validate_provisioned_account(Some(&inactive)).is_none());
    }

    #[test]
    fn given_active_user_when_validating_account_should_accept() {
        let active = test_user(1, iggy_common::UserStatus::Active);
        let result = validate_provisioned_account(Some(&active));
        assert_eq!(result.unwrap().0, 1);
    }

    #[test]
    fn given_user_with_critical_options_when_validating_account_should_reject() {
        use iggy_common::{HeaderKey, HeaderValue, OptionKey, OptionValue, ResourceOptions};
        let mut user = test_user(2, iggy_common::UserStatus::Active);
        user.options = ResourceOptions::from([
            (
                HeaderKey::from(OptionKey::CriticalKeys),
                OptionValue::explicit(HeaderValue::try_from("a_future_key").expect("marker fits")),
            ),
            (
                HeaderKey::try_from("a_future_key").expect("key fits"),
                OptionValue::explicit(HeaderValue::from(7u64)),
            ),
        ]);
        assert!(validate_provisioned_account(Some(&user)).is_none());
    }
}
