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

//! Login/register failure taxonomy, shared by both spines.
//!
//! Its own leaf because the TCP spine raises these ([`super::session_ops`])
//! while the HTTP spine maps them to wire errors (`crate::http::reply`).
//! Parked in either spine it would make one import the other's session module
//! for a type neither owns.

use crate::session_manager::SessionError;
use metadata::MetadataSubmitError;

/// Login/register failure.
///
/// `#[non_exhaustive]`: external matchers need a wildcard arm.
#[derive(Debug)]
#[non_exhaustive]
pub enum LoginRegisterError {
    InvalidCredentials,
    InvalidToken,
    UserInactive,
    /// The username matched a local user but the password was wrong (or the
    /// account is inactive). Surfaced to the client identically to
    /// `InvalidCredentials` so we don't leak which field failed, but
    /// excluded from `is_invalid_credentials` so the external auth
    /// fallback is never attempted for known local users.
    LocalUserRejected,
    /// Denied by an external authentication service.
    ExternalAuthDenied(String),
    Session(SessionError),
    /// Recoverable consensus failure. The connection stays `Connected`; the
    /// SDK read-timeout replays.
    Transient(MetadataSubmitError),
}

impl LoginRegisterError {
    /// `true` when the credential did not match any local user and an
    /// external auth callout should be attempted as a fallback.
    /// `LocalUserRejected` is intentionally excluded: the user exists
    /// locally so the external path must not override local credentials.
    #[must_use]
    pub const fn is_invalid_credentials(&self) -> bool {
        matches!(self, Self::InvalidCredentials | Self::InvalidToken)
    }

    /// `true` for a terminal failure the client cannot fix by retrying (bad
    /// credentials / token / inactive user / session error); `false` for a
    /// transient consensus failure the SDK replays. The handler fast-fails
    /// terminal errors with an empty reply and stays silent on transient ones.
    #[must_use]
    pub(crate) const fn is_terminal(&self) -> bool {
        match self {
            // Not every submit failure is retryable: the ownership refusal
            // (presented `client_id` belongs to another user) cannot be fixed
            // by replaying anywhere, and surfacing it as transient would make
            // the SDK spin on it forever.
            Self::Transient(error) => !error.is_transient(),
            _ => true,
        }
    }
}

impl std::fmt::Display for LoginRegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCredentials | Self::LocalUserRejected => {
                write!(f, "invalid username or password")
            }
            Self::InvalidToken => write!(f, "invalid or expired personal access token"),
            Self::UserInactive => write!(f, "user account is inactive"),
            Self::ExternalAuthDenied(reason) => write!(f, "external auth denied: {reason}"),
            Self::Session(e) => write!(f, "session error: {e}"),
            Self::Transient(e) => write!(f, "transient consensus failure: {e}"),
        }
    }
}

impl std::error::Error for LoginRegisterError {}
