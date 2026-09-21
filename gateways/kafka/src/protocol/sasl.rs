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

//! SASL mechanism parsing and the per-connection authentication state machine.
//!
//! Pure and synchronous: nothing here performs I/O or talks to Iggy. Verifying the credentials
//! this module extracts is the caller's job, which keeps the state machine unit-testable and
//! keeps `handle_request_bounded` free of an `async` signature its ~70 call sites would inherit.
//!
//! See `docs/AUTHENTICATION.md` for the decisions this implements.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use secrecy::SecretString;

use crate::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_SASL_AUTHENTICATE, API_KEY_SASL_HANDSHAKE,
};

/// Mechanisms this gateway can answer for.
///
/// `PLAIN` alone, and not by preference: Iggy stores one Argon2 hash per user, while SCRAM
/// requires the server to hold PBKDF2-derived keys and to send the salt and iteration count to
/// the client. Neither is derivable from an Argon2 hash, so SCRAM needs credential storage that
/// does not exist. `docs/AUTHENTICATION.md` has the full argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    Plain,
}

impl SaslMechanism {
    pub const PLAIN: &'static str = "PLAIN";

    /// Mechanism names advertised in a `SaslHandshake` response, in preference order.
    #[must_use]
    pub const fn advertised() -> &'static [&'static str] {
        &[Self::PLAIN]
    }
}

impl FromStr for SaslMechanism {
    type Err = SaslError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            Self::PLAIN => Ok(Self::Plain),
            _ => Err(SaslError::UnsupportedMechanism),
        }
    }
}

impl Display for SaslMechanism {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain => f.write_str(Self::PLAIN),
        }
    }
}

/// Why a SASL exchange could not proceed.
///
/// Deliberately coarse on the credential path: every malformed-PLAIN case collapses to
/// [`Self::MalformedInitialResponse`] so nothing downstream can accidentally tell a client which
/// half of its credentials was wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslError {
    UnsupportedMechanism,
    MalformedInitialResponse,
}

/// Credentials carried by a PLAIN initial response, forwarded to an Iggy login unchanged.
///
/// No `Debug`: the whole point is that this never reaches a log. `SecretString` would redact the
/// password on its own, but the username is equally sensitive as a user-enumeration signal.
pub struct PlainCredentials {
    pub username: String,
    pub password: SecretString,
}

/// Parses a SASL/PLAIN initial response: `authzid NUL authcid NUL passwd` (RFC 4616).
///
/// Mirrors Kafka's own `PlainSaslServer`: exactly two separators, a non-empty authcid and passwd,
/// and an authzid that is either empty or identical to the authcid. Iggy has no notion of one
/// user acting as another, so a differing authzid is rejected rather than ignored.
///
/// # Errors
///
/// Returns [`SaslError::MalformedInitialResponse`] for any violation of the above, including
/// invalid UTF-8.
pub fn parse_plain(auth_bytes: &[u8]) -> Result<PlainCredentials, SaslError> {
    let mut parts = auth_bytes.split(|&byte| byte == 0);
    let (Some(acting_as), Some(login), Some(secret), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(SaslError::MalformedInitialResponse);
    };

    let acting_as =
        std::str::from_utf8(acting_as).map_err(|_| SaslError::MalformedInitialResponse)?;
    let login = std::str::from_utf8(login).map_err(|_| SaslError::MalformedInitialResponse)?;
    let secret = std::str::from_utf8(secret).map_err(|_| SaslError::MalformedInitialResponse)?;

    if login.is_empty() || secret.is_empty() {
        return Err(SaslError::MalformedInitialResponse);
    }
    if !acting_as.is_empty() && acting_as != login {
        return Err(SaslError::MalformedInitialResponse);
    }

    Ok(PlainCredentials {
        username: login.to_string(),
        password: SecretString::from(secret.to_string()),
    })
}

/// Where one connection stands in the SASL exchange.
///
/// A connection on a gateway with SASL disabled is created [`Self::Authenticated`], so the
/// dispatch path has no second "is SASL on" question to ask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslState {
    /// No mechanism negotiated yet. `api_versions_answered` counts the pre-authentication
    /// `ApiVersions` frames this connection has already been answered, capped by
    /// [`MAX_PRE_AUTH_API_VERSIONS`].
    AwaitHandshake {
        api_versions_answered: u8,
    },
    AwaitToken(SaslMechanism),
    Authenticated,
}

impl SaslState {
    /// Starting state for a connection that must authenticate.
    #[must_use]
    pub const fn new() -> Self {
        Self::AwaitHandshake {
            api_versions_answered: 0,
        }
    }

    /// Records one answered pre-authentication `ApiVersions`.
    ///
    /// Every answer counts, whatever error code it carries. Counting only usable answers lets a
    /// peer repeat a version this gateway refuses for as long as it likes, and each frame resets
    /// the pre-authentication read budget, so the connection holds a `max_connections` permit with
    /// it.
    pub const fn count_api_versions_answer(&mut self) {
        if let Self::AwaitHandshake {
            api_versions_answered,
        } = self
        {
            *api_versions_answered = api_versions_answered.saturating_add(1);
        }
    }
}

impl Default for SaslState {
    fn default() -> Self {
        Self::new()
    }
}

/// What the connection loop should do with a frame, given the state it arrived in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslAction {
    /// Hand the frame to the ordinary request dispatch.
    Dispatch,
    /// Answer a `SaslHandshake` for this mechanism and advance to [`SaslState::AwaitToken`].
    AcceptHandshake(SaslMechanism),
    /// Answer a `SaslHandshake` with `UNSUPPORTED_SASL_MECHANISM`, then close.
    RejectMechanism,
    /// Answer a `SaslHandshake` with `UNSUPPORTED_VERSION`, then close.
    ///
    /// The handshake version selects the token framing (KIP-152): at v0 the tokens that follow
    /// arrive as bare length-prefixed frames with no request header. Refusing v0 here is what
    /// lets the frame reader stay header-parsing-only, and it costs only pre-1.0 clients.
    RejectHandshakeVersion,
    /// Parse the frame as a `SaslAuthenticate` token and verify the credentials it carries.
    Authenticate,
    /// A `SaslAuthenticate` above [`SASL_AUTHENTICATE_MAX_VERSION`]. No response schema exists at
    /// the version the client asked for, so there is nothing parseable to send.
    RejectAuthenticateVersion,
    /// Wrong request for this state. Answer `ILLEGAL_SASL_STATE` shaped for the key, then close.
    IllegalState,
    /// Wrong request, but not fatal: a SASL request on an already-authenticated connection gets
    /// `ILLEGAL_SASL_STATE` and keeps the connection, matching a real broker.
    IllegalStateKeepOpen,
    /// A pre-authentication `ApiVersions` within this connection's allowance. Answer it, then
    /// count it with [`SaslState::count_api_versions_answer`].
    DispatchPreAuthApiVersions,
}

/// Pre-authentication `ApiVersions` frames one connection may be answered.
///
/// Two, not the single frame a conformant exchange needs. A client that opens above this gateway's
/// ceiling is answered `UNSUPPORTED_VERSION` in a v0 body and retries lower (KIP-511), so the
/// downgrade costs a second frame before the handshake.
pub const MAX_PRE_AUTH_API_VERSIONS: u8 = 2;

/// Highest `SaslHandshake` version accepted. See [`SaslAction::RejectHandshakeVersion`].
pub const SASL_HANDSHAKE_VERSION: i16 = 1;

/// Highest `SaslAuthenticate` version accepted, matching what `ApiVersions` advertises.
///
/// Checked in [`SaslState::classify`] rather than left to the decoder. Without it an out-of-range
/// version reaches the credential path, fails to decode, and is reported as an authentication
/// failure, which tells the client its credentials were wrong when the real problem is that it
/// asked at a version this gateway does not speak.
pub const SASL_AUTHENTICATE_MAX_VERSION: i16 = 2;

impl SaslState {
    #[must_use]
    pub const fn is_authenticated(self) -> bool {
        matches!(self, Self::Authenticated)
    }

    /// Classifies an incoming frame without consuming it.
    ///
    /// `mechanism` is the name carried by a `SaslHandshake` body, already decoded by the caller,
    /// and is ignored for every other key.
    #[must_use]
    pub fn classify(self, api_key: i16, api_version: i16, mechanism: Option<&str>) -> SaslAction {
        match (self, api_key) {
            (Self::Authenticated, API_KEY_SASL_HANDSHAKE | API_KEY_SASL_AUTHENTICATE) => {
                SaslAction::IllegalStateKeepOpen
            }
            (Self::Authenticated, _) => SaslAction::Dispatch,
            // A real broker allows exactly one ApiVersions before the handshake
            // (`SaslServerAuthenticator` moves out of `HANDSHAKE_OR_VERSIONS_REQUEST` after
            // answering one); the allowance here covers the KIP-511 downgrade retry on top of it.
            // Allowing an unlimited number lets a connection that never authenticates hold a
            // `max_connections` permit forever, because each frame resets the pre-authentication
            // read budget.
            (
                Self::AwaitHandshake {
                    api_versions_answered,
                },
                API_KEY_API_VERSIONS,
            ) if api_versions_answered < MAX_PRE_AUTH_API_VERSIONS => {
                SaslAction::DispatchPreAuthApiVersions
            }
            (Self::AwaitHandshake { .. }, API_KEY_SASL_HANDSHAKE) => {
                if api_version == SASL_HANDSHAKE_VERSION {
                    mechanism
                        .and_then(|name| SaslMechanism::from_str(name).ok())
                        .map_or(SaslAction::RejectMechanism, SaslAction::AcceptHandshake)
                } else {
                    SaslAction::RejectHandshakeVersion
                }
            }
            (Self::AwaitToken(_), API_KEY_SASL_AUTHENTICATE) => {
                if (0..=SASL_AUTHENTICATE_MAX_VERSION).contains(&api_version) {
                    SaslAction::Authenticate
                } else {
                    SaslAction::RejectAuthenticateVersion
                }
            }
            // Fail closed. A state or key combination not named above is a protocol violation, and
            // a future state added without a rule here must not fall through to `Dispatch`.
            _ => SaslAction::IllegalState,
        }
    }
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    fn plain(payload: &[u8]) -> Result<PlainCredentials, SaslError> {
        parse_plain(payload)
    }

    #[test]
    fn given_a_well_formed_plain_payload_when_parsed_should_yield_credentials() {
        let creds = plain(b"\0alice\0s3cret").expect("well-formed PLAIN payload");
        assert_eq!(creds.username, "alice");
        assert_eq!(creds.password.expose_secret(), "s3cret");
    }

    #[test]
    fn given_an_authzid_equal_to_the_authcid_when_parsed_should_be_accepted() {
        let creds = plain(b"alice\0alice\0s3cret").expect("authzid may repeat the authcid");
        assert_eq!(creds.username, "alice");
    }

    #[test]
    fn given_an_authzid_naming_another_user_when_parsed_should_be_rejected() {
        assert_eq!(
            plain(b"root\0alice\0s3cret").err(),
            Some(SaslError::MalformedInitialResponse),
            "Iggy has no impersonation, so a differing authzid must not be silently ignored"
        );
    }

    #[test]
    fn given_a_password_containing_a_separator_when_parsed_should_be_rejected() {
        // Four fields, not three. Splitting on the first two separators and keeping the rest
        // would let a password smuggle structure into the payload.
        assert_eq!(
            plain(b"\0alice\0s3c\0ret").err(),
            Some(SaslError::MalformedInitialResponse)
        );
    }

    #[test]
    fn given_an_empty_username_or_password_when_parsed_should_be_rejected() {
        assert_eq!(
            plain(b"\0\0s3cret").err(),
            Some(SaslError::MalformedInitialResponse)
        );
        assert_eq!(
            plain(b"\0alice\0").err(),
            Some(SaslError::MalformedInitialResponse)
        );
    }

    #[test]
    fn given_too_few_separators_when_parsed_should_be_rejected() {
        assert_eq!(
            plain(b"alice\0s3cret").err(),
            Some(SaslError::MalformedInitialResponse)
        );
        assert_eq!(plain(b"").err(), Some(SaslError::MalformedInitialResponse));
    }

    #[test]
    fn given_invalid_utf8_when_parsed_should_be_rejected() {
        assert_eq!(
            plain(&[0x00, 0xff, 0xfe, 0x00, b'p']).err(),
            Some(SaslError::MalformedInitialResponse)
        );
    }

    #[test]
    fn given_an_unknown_mechanism_name_when_parsed_should_be_rejected() {
        assert_eq!(
            SaslMechanism::from_str("SCRAM-SHA-256").err(),
            Some(SaslError::UnsupportedMechanism)
        );
        assert_eq!(
            SaslMechanism::from_str("plain").err(),
            Some(SaslError::UnsupportedMechanism),
            "mechanism names are case-sensitive on the wire"
        );
        assert_eq!(SaslMechanism::from_str("PLAIN"), Ok(SaslMechanism::Plain));
    }

    #[test]
    fn given_await_handshake_when_api_versions_arrives_should_dispatch() {
        let state = SaslState::new();
        assert_eq!(
            state.classify(API_KEY_API_VERSIONS, 3, None),
            SaslAction::DispatchPreAuthApiVersions
        );
    }

    #[test]
    fn given_a_spent_api_versions_allowance_should_refuse_the_next_one() {
        // A real broker allows one and the KIP-511 downgrade retry needs a second. Past that, a
        // client that never authenticates keeps resetting the pre-auth read budget and holds a
        // connection permit indefinitely.
        let mut state = SaslState::new();
        for _ in 0..MAX_PRE_AUTH_API_VERSIONS {
            assert_eq!(
                state.classify(API_KEY_API_VERSIONS, 3, None),
                SaslAction::DispatchPreAuthApiVersions
            );
            state.count_api_versions_answer();
        }
        assert_eq!(
            state.classify(API_KEY_API_VERSIONS, 3, None),
            SaslAction::IllegalState
        );
    }

    #[test]
    fn given_await_handshake_when_a_normal_request_arrives_should_be_illegal_state() {
        let state = SaslState::new();
        assert_eq!(state.classify(0, 9, None), SaslAction::IllegalState);
        assert_eq!(state.classify(1, 12, None), SaslAction::IllegalState);
        assert_eq!(
            state.classify(API_KEY_SASL_AUTHENTICATE, 2, None),
            SaslAction::IllegalState,
            "a token before a handshake has no negotiated mechanism to parse against"
        );
    }

    #[test]
    fn given_await_handshake_when_handshake_v0_arrives_should_reject_the_version() {
        let state = SaslState::new();
        assert_eq!(
            state.classify(API_KEY_SASL_HANDSHAKE, 0, Some("PLAIN")),
            SaslAction::RejectHandshakeVersion,
            "v0 selects the headerless token framing the frame reader cannot parse"
        );
    }

    #[test]
    fn given_await_handshake_when_the_mechanism_is_unknown_should_reject_it() {
        let state = SaslState::new();
        assert_eq!(
            state.classify(API_KEY_SASL_HANDSHAKE, 1, Some("GSSAPI")),
            SaslAction::RejectMechanism
        );
        assert_eq!(
            state.classify(API_KEY_SASL_HANDSHAKE, 1, None),
            SaslAction::RejectMechanism
        );
    }

    #[test]
    fn given_await_handshake_when_plain_is_offered_should_accept_it() {
        let state = SaslState::new();
        assert_eq!(
            state.classify(API_KEY_SASL_HANDSHAKE, 1, Some("PLAIN")),
            SaslAction::AcceptHandshake(SaslMechanism::Plain)
        );
    }

    #[test]
    fn given_await_token_when_the_version_is_out_of_range_should_reject_the_version() {
        // Not a credential failure. Reporting one would send the operator to check a password
        // when the client simply asked at a version this gateway does not speak.
        let state = SaslState::AwaitToken(SaslMechanism::Plain);
        assert_eq!(
            state.classify(API_KEY_SASL_AUTHENTICATE, 3, None),
            SaslAction::RejectAuthenticateVersion
        );
        assert_eq!(
            state.classify(API_KEY_SASL_AUTHENTICATE, -1, None),
            SaslAction::RejectAuthenticateVersion
        );
    }

    #[test]
    fn given_await_token_when_every_supported_version_arrives_should_authenticate() {
        let state = SaslState::AwaitToken(SaslMechanism::Plain);
        for version in 0..=SASL_AUTHENTICATE_MAX_VERSION {
            assert_eq!(
                state.classify(API_KEY_SASL_AUTHENTICATE, version, None),
                SaslAction::Authenticate,
                "v{version} is advertised, so it must be accepted"
            );
        }
    }

    #[test]
    fn given_await_token_when_a_token_arrives_should_authenticate() {
        let state = SaslState::AwaitToken(SaslMechanism::Plain);
        assert_eq!(
            state.classify(API_KEY_SASL_AUTHENTICATE, 2, None),
            SaslAction::Authenticate
        );
    }

    #[test]
    fn given_await_token_when_anything_else_arrives_should_be_illegal_state() {
        let state = SaslState::AwaitToken(SaslMechanism::Plain);
        assert_eq!(state.classify(0, 9, None), SaslAction::IllegalState);
        assert_eq!(
            state.classify(API_KEY_API_VERSIONS, 3, None),
            SaslAction::IllegalState,
            "a second ApiVersions mid-exchange is a protocol violation, not a probe"
        );
        assert_eq!(
            state.classify(API_KEY_SASL_HANDSHAKE, 1, Some("PLAIN")),
            SaslAction::IllegalState
        );
    }

    #[test]
    fn given_authenticated_when_normal_requests_arrive_should_dispatch() {
        let state = SaslState::Authenticated;
        assert!(state.is_authenticated());
        assert_eq!(state.classify(0, 9, None), SaslAction::Dispatch);
        assert_eq!(
            state.classify(API_KEY_API_VERSIONS, 3, None),
            SaslAction::Dispatch,
            "the Java client sends ApiVersions again after authenticating"
        );
    }

    #[test]
    fn given_authenticated_when_a_sasl_request_arrives_should_not_close_the_connection() {
        let state = SaslState::Authenticated;
        assert_eq!(
            state.classify(API_KEY_SASL_HANDSHAKE, 1, Some("PLAIN")),
            SaslAction::IllegalStateKeepOpen
        );
        assert_eq!(
            state.classify(API_KEY_SASL_AUTHENTICATE, 2, None),
            SaslAction::IllegalStateKeepOpen
        );
    }
}
