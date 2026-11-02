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

use iggy_common::IggyError;
use metadata::MetadataSubmitError;

/// Login/register failure.
///
/// `#[non_exhaustive]`: external matchers need a wildcard arm.
#[derive(Debug)]
#[non_exhaustive]
pub enum LoginRegisterError {
    InvalidCredentials,
    /// The username does not exist in the local user store. Distinguished
    /// from `InvalidCredentials` so the ext-auth guard can enter the callout
    /// only for unknown users, not for wrong passwords. On the wire both
    /// surface as `InvalidCredentials`.
    UserNotFound,
    InvalidToken,
    UserInactive,
    Binding(IggyError),
    /// Recoverable consensus failure. The connection stays `Connected`; the
    /// SDK read-timeout replays.
    Transient(MetadataSubmitError),
    /// The external auth callout failed transiently (timeout, connect error,
    /// 5xx, 429, or overloaded). The SDK retries after backoff.
    ExternalAuthUnavailable,
}

impl LoginRegisterError {
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
            Self::Binding(error) => !matches!(
                error,
                IggyError::TransientNotAccepted | IggyError::TransientNotCommitted
            ),
            Self::ExternalAuthUnavailable => false,
            _ => true,
        }
    }
}

impl std::fmt::Display for LoginRegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCredentials | Self::UserNotFound => {
                write!(f, "invalid username or password")
            }
            Self::InvalidToken => write!(f, "invalid or expired personal access token"),
            Self::UserInactive => write!(f, "user account is inactive"),
            Self::Binding(error) => write!(f, "session binding: {error}"),
            Self::Transient(e) => write!(f, "transient consensus failure: {e}"),
            Self::ExternalAuthUnavailable => {
                write!(f, "external auth service temporarily unavailable")
            }
        }
    }
}

impl std::error::Error for LoginRegisterError {}
