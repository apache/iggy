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

//! Shared grading of a wire [`EvictionReason`] to the typed [`IggyError`] an
//! evicted session surfaces. The SDK's binary-transport Eviction-frame decoder
//! (TCP / QUIC / WebSocket) and the server-ng HTTP write path both call this,
//! so every transport sees one status per reason.

use iggy_binary_protocol::consensus::EvictionReason;
use iggy_binary_protocol::version::IGGY_PROTOCOL_VERSION;

use super::iggy_error::IggyError;

/// Map a session-terminal [`EvictionReason`], plus the accepted protocol window
/// an `IncompatibleProtocol` frame carries, to the [`IggyError`] the caller
/// renders. Session-terminal reasons collapse to re-authentication statuses;
/// `IncompatibleProtocol` reports the exact window unless it is degenerate (a
/// zero minimum or an inverted range), which also falls back to
/// re-authentication.
///
/// The two callers extract the fields differently - server-ng from an aligned
/// `EvictionHeader`, the SDK by wire offset off an unaligned buffer - then grade
/// through here so the mappings cannot drift apart.
#[must_use]
pub fn eviction_reason_to_error(
    reason: EvictionReason,
    server_protocol_version: u32,
    server_protocol_version_min: u32,
) -> IggyError {
    match reason {
        EvictionReason::InvalidCredentials => IggyError::InvalidCredentials,
        EvictionReason::InvalidToken => IggyError::InvalidPersonalAccessToken,
        EvictionReason::UserInactive
        | EvictionReason::SessionError
        | EvictionReason::NoSession
        | EvictionReason::SessionTooLow
        | EvictionReason::SessionReleaseMismatch => IggyError::Unauthenticated,
        EvictionReason::StaleClient => IggyError::StaleClient,
        EvictionReason::IncompatibleProtocol => {
            if server_protocol_version_min == 0
                || server_protocol_version < server_protocol_version_min
            {
                IggyError::Unauthenticated
            } else {
                IggyError::IncompatibleProtocolVersion(
                    IGGY_PROTOCOL_VERSION,
                    server_protocol_version_min,
                    server_protocol_version,
                )
            }
        }
        EvictionReason::MalformedLogin => IggyError::InvalidFormat,
        _ => IggyError::InvalidCommand,
    }
}
