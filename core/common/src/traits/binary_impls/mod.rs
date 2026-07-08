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

mod cluster;
mod consumer_groups;
mod consumer_offsets;
mod messages;
mod partitions;
mod personal_access_tokens;
mod segments;
mod streams;
mod system;
mod topics;
mod users;

use crate::IggyError;
use crate::http::users::defaults::{
    MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH, MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
};
#[cfg(feature = "vsr")]
use crate::{BinaryClient, ClientState};
use iggy_binary_protocol::WireDecode;
#[cfg(feature = "vsr")]
use iggy_binary_protocol::{ClientVersionInfo, IGGY_PROTOCOL_VERSION, WireName};

/// SDK identifier sent in the login-register version prefix. Foreign SDKs
/// send their own (e.g. `go-sdk`) once they adopt VSR framing.
#[cfg(feature = "vsr")]
pub(crate) const RUST_SDK_NAME: &str = "rust-sdk";

/// Version prefix for both login-register request shapes. `sdk_version`
/// comes from [`crate::VsrSessionControl::sdk_version`] so it is the SDK
/// crate's version, not this crate's.
#[cfg(feature = "vsr")]
pub(crate) fn rust_sdk_version_info(sdk_version: &str) -> Result<ClientVersionInfo, IggyError> {
    Ok(ClientVersionInfo {
        protocol_version: IGGY_PROTOCOL_VERSION,
        sdk_name: WireName::new(RUST_SDK_NAME).expect("RUST_SDK_NAME is 1-255 bytes"),
        sdk_version: WireName::new(sdk_version).map_err(|_| IggyError::InvalidFormat)?,
    })
}

/// Release a live session before a fresh login on the same connection.
///
/// server-ng binds a connection to one `(client, session)` and resolves the
/// acting user from that binding, not the wire header. A re-login on a still-
/// bound connection is served as an idempotent replay that keeps the old
/// identity, so switching users on a live connection requires logging out
/// first: the committed Logout unbinds the connection cluster-wide, so the
/// following Register takes the full register path and rebinds the newly
/// authenticated user. No-op when the client is not authenticated.
#[cfg(feature = "vsr")]
pub(crate) async fn logout_before_relogin<B: BinaryClient>(client: &B) -> Result<(), IggyError> {
    if client.get_state().await == ClientState::Authenticated {
        client.logout_user().await?;
    }
    Ok(())
}

/// Same bounds and error every server (HTTP and binary) enforces, applied
/// before encoding so an oversized password can never desync the u8 length
/// prefix on the wire.
pub(crate) fn validate_password(password: &str) -> Result<(), IggyError> {
    if !(MIN_PASSWORD_LENGTH..=MAX_PASSWORD_LENGTH).contains(&password.len()) {
        return Err(IggyError::InvalidPassword);
    }
    Ok(())
}

/// Same bounds and error the servers enforce for usernames, applied before
/// encoding so an oversized username can never desync the wire name prefix.
pub(crate) fn validate_username(username: &str) -> Result<(), IggyError> {
    if !(MIN_USERNAME_LENGTH..=MAX_USERNAME_LENGTH).contains(&username.len()) {
        return Err(IggyError::InvalidUsername);
    }
    Ok(())
}

/// Decode a wire response, logging the error details before converting to `IggyError`.
pub(crate) fn decode_response<T: WireDecode>(response: &[u8]) -> Result<T, IggyError> {
    T::decode_from(response).map_err(|e| {
        tracing::warn!("failed to decode {}: {e}", std::any::type_name::<T>());
        IggyError::InvalidFormat
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_password_enforces_server_bounds() {
        assert!(validate_password(&"a".repeat(MIN_PASSWORD_LENGTH - 1)).is_err());
        assert!(validate_password(&"a".repeat(MIN_PASSWORD_LENGTH)).is_ok());
        assert!(validate_password(&"a".repeat(MAX_PASSWORD_LENGTH)).is_ok());
        assert!(validate_password(&"a".repeat(MAX_PASSWORD_LENGTH + 1)).is_err());
    }

    #[test]
    fn validate_username_enforces_server_bounds() {
        assert!(validate_username(&"a".repeat(MIN_USERNAME_LENGTH - 1)).is_err());
        assert!(validate_username(&"a".repeat(MIN_USERNAME_LENGTH)).is_ok());
        assert!(validate_username(&"a".repeat(MAX_USERNAME_LENGTH)).is_ok());
        assert!(validate_username(&"a".repeat(MAX_USERNAME_LENGTH + 1)).is_err());
    }
}
