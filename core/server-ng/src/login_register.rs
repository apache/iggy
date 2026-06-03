/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Login/register failure taxonomy and its mapping to a wire `EvictionReason`.
//!
//! The login/register flow itself lives in `dispatch` + `auth`; this module
//! owns the error type those handlers return and the
//! `TryFrom<&LoginRegisterError>` triage that decides terminal eviction vs
//! silent close (recoverable failures stay silent so the SDK replays).

use crate::session_manager::SessionError;
use iggy_binary_protocol::EvictionReason;
use metadata::RegisterSubmitError;

/// Login/register failure.
///
/// Most variants -> [`EvictionReason`] (via [`TryFrom<&LoginRegisterError>`]).
/// Two variants are `NotEvictable`:
/// - [`LoginRegisterError::InvalidClientId`], see variant doc.
/// - [`LoginRegisterError::Transient`], recoverable; SDK read-timeout replays.
///
/// `#[non_exhaustive]`: external matchers need a wildcard arm.
#[derive(Debug)]
#[non_exhaustive]
pub enum LoginRegisterError {
    /// `client_id == 0` (reserved sentinel). No frame can address it
    /// ([`EvictionHeader::validate`] rejects); silent close.
    /// `TryFrom` -> `Err(NotEvictable)`.
    ///
    /// [`EvictionHeader::validate`]: iggy_binary_protocol::EvictionHeader
    InvalidClientId,
    InvalidCredentials,
    InvalidToken,
    UserInactive,
    Session(SessionError),
    /// Recoverable consensus failure. The connection stays `Connected`; the
    /// SDK read-timeout replays. `TryFrom` -> `Err(NotEvictable)`.
    Transient(RegisterSubmitError),
}

impl std::fmt::Display for LoginRegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidClientId => write!(f, "client_id must be non-zero"),
            Self::InvalidCredentials => write!(f, "invalid username or password"),
            Self::InvalidToken => write!(f, "invalid or expired personal access token"),
            Self::UserInactive => write!(f, "user account is inactive"),
            Self::Session(e) => write!(f, "session error: {e}"),
            Self::Transient(e) => write!(f, "transient consensus failure: {e}"),
        }
    }
}

impl std::error::Error for LoginRegisterError {}

/// `TryFrom<&LoginRegisterError>` for [`EvictionReason`] returns this when
/// no wire mapping exists. Caller closes silently / lets client retry.
///
/// - `InvalidClientId`: `client_id == 0`, no addressable client.
/// - `Transient`: wire-terminal Eviction would contradict the recoverable
///   contract; the failure is recoverable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotEvictable;

impl std::fmt::Display for NotEvictable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("error has no wireable EvictionReason; close connection silently")
    }
}

impl std::error::Error for NotEvictable {}

/// **No production caller yet**, pins the contract for the eventual
/// wire-eviction dispatcher: `try_from(&err)` -> on `Ok(reason)` build
/// [`iggy_binary_protocol::EvictionHeader`] via
/// [`consensus::build_eviction_message`] and ship+close; on `NotEvictable`
/// close silently.
///
/// `#[non_exhaustive]` + this exhaustive match force every future
/// `LoginRegisterError` through compile-time triage.
impl TryFrom<&LoginRegisterError> for EvictionReason {
    type Error = NotEvictable;

    // Per-variant arms (not `A | B`), so each carries its own NotEvictable
    // reason. Distinct cases (no addressable client_id vs recoverable);
    // merging would lose docs.
    #[allow(clippy::match_same_arms)]
    fn try_from(err: &LoginRegisterError) -> Result<Self, Self::Error> {
        Ok(match err {
            // No client_id to address,  close silently. See variant doc.
            LoginRegisterError::InvalidClientId => return Err(NotEvictable),
            // Recoverable: the connection stays Connected; SDK retries.
            // Wire Eviction would erase a recoverable session.
            LoginRegisterError::Transient(_) => return Err(NotEvictable),
            LoginRegisterError::InvalidCredentials => Self::InvalidCredentials,
            LoginRegisterError::InvalidToken => Self::InvalidToken,
            LoginRegisterError::UserInactive => Self::UserInactive,
            LoginRegisterError::Session(_) => Self::SessionError,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consensus::{EvictionContext, build_eviction_message};
    use iggy_binary_protocol::{Command2, ConsensusHeader, HEADER_SIZE};

    #[test]
    fn login_register_error_maps_to_eviction_reason() {
        let cases: &[(LoginRegisterError, EvictionReason)] = &[
            (
                LoginRegisterError::InvalidCredentials,
                EvictionReason::InvalidCredentials,
            ),
            (
                LoginRegisterError::InvalidToken,
                EvictionReason::InvalidToken,
            ),
            (
                LoginRegisterError::UserInactive,
                EvictionReason::UserInactive,
            ),
            (
                LoginRegisterError::Session(SessionError::ConnectionNotFound(0)),
                EvictionReason::SessionError,
            ),
        ];
        for (err, expected) in cases {
            let mapped = EvictionReason::try_from(err).expect("variant should be wireable");
            assert_eq!(mapped, *expected, "mapping for {err:?}");
        }
    }

    // Transient must NOT be wireable, point of the variant. Callers rely
    // on NotEvictable to stay silent instead of shipping a terminal frame.
    #[test]
    fn transient_is_not_evictable() {
        let err = LoginRegisterError::Transient(RegisterSubmitError::PipelineFull);
        let mapped = EvictionReason::try_from(&err);
        assert_eq!(mapped, Err(NotEvictable));
    }

    // InvalidClientId: client_id == 0 cannot address a frame, caller
    // must handle Err path (silent close).
    #[test]
    fn invalid_client_id_is_not_evictable() {
        let err = LoginRegisterError::InvalidClientId;
        let mapped = EvictionReason::try_from(&err);
        assert_eq!(mapped, Err(NotEvictable));
    }

    #[test]
    fn build_eviction_message_carries_typed_reason() {
        let ctx = EvictionContext {
            cluster: 7,
            view: 3,
            replica: 1,
        };
        let msg = build_eviction_message(ctx, 0xCAFE, EvictionReason::SessionError);
        let header = msg.header();
        assert_eq!(header.command, Command2::Eviction);
        assert_eq!(header.cluster, 7);
        assert_eq!(header.view, 3);
        assert_eq!(header.replica, 1);
        assert_eq!(header.client, 0xCAFE);
        assert_eq!(header.reason, EvictionReason::SessionError);
        assert_eq!(header.size as usize, HEADER_SIZE);
        // validate accepts.
        assert!(header.validate().is_ok());
    }
}
