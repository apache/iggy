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

//! One module per Kafka API key.
//!
//! Each module owns its firewall range, its response encoders, and the handler [`dispatch`]
//! routes to. A new API key costs a module, a line in [`dispatch`], and a line in
//! `api::SUPPORTED_RANGES`.
//!
//! `kafka_protocol` owns wire encoding. What lives here is policy: which placeholder values and
//! error codes a request gets back, and when the connection closes instead.

pub mod alter_configs;
pub mod api_versions;
pub mod create_partitions;
pub mod create_topics;
pub mod delete_groups;
pub mod delete_topics;
pub mod describe_cluster;
pub mod describe_configs;
pub mod describe_groups;
pub mod fetch;
pub mod find_coordinator;
pub mod heartbeat;
pub mod incremental_alter_configs;
pub mod init_producer_id;
pub mod join_group;
pub mod leave_group;
pub mod list_groups;
pub mod list_offsets;
pub mod metadata;
pub mod offset_commit;
pub mod offset_delete;
pub mod offset_fetch;
pub mod produce;
pub mod sync_group;
pub(crate) mod topic_config;

use std::fmt::Display;
use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::IggyError;
use iggy_kafka_wire::codec;
use kafka_protocol::messages::TransactionalId;
use kafka_protocol::protocol::{Decodable, Encodable};
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::time::Instant;

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::BridgeError;
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_ALTER_CONFIGS, API_KEY_API_VERSIONS, API_KEY_CREATE_PARTITIONS, API_KEY_CREATE_TOPICS,
    API_KEY_DELETE_GROUPS, API_KEY_DELETE_TOPICS, API_KEY_DESCRIBE_CLUSTER,
    API_KEY_DESCRIBE_CONFIGS, API_KEY_DESCRIBE_GROUPS, API_KEY_FETCH, API_KEY_FIND_COORDINATOR,
    API_KEY_HEARTBEAT, API_KEY_INCREMENTAL_ALTER_CONFIGS, API_KEY_INIT_PRODUCER_ID,
    API_KEY_JOIN_GROUP, API_KEY_LEAVE_GROUP, API_KEY_LIST_GROUPS, API_KEY_LIST_OFFSETS,
    API_KEY_METADATA, API_KEY_OFFSET_COMMIT, API_KEY_OFFSET_DELETE, API_KEY_OFFSET_FETCH,
    API_KEY_PRODUCE, API_KEY_SYNC_GROUP, ConnectionState, ERROR_INVALID_REQUEST,
    ERROR_UNSUPPORTED_VERSION, GatewayState, HandleOutcome, is_supported_version,
    supported_max_version,
};

/// Record encodes and decodes of this many bytes or more run off the async worker.
pub(crate) const CODEC_OFF_WORKER_BYTES: usize = 64 * 1024;
/// A plain copy costs less per byte, so the handoff pays off only from this size.
pub(crate) const COPY_OFF_WORKER_BYTES: usize = 1024 * 1024;

/// Client-supplied `timeout_ms` floor. `0` or negative would abort every topic on arrival.
pub(crate) const MIN_REQUEST_TIMEOUT: Duration = Duration::from_millis(1_000);
/// Client-supplied `timeout_ms` ceiling. Above this the shared `IggyClient` stays busy too long.
pub(crate) const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Clamps KIP-4 `timeout_ms` into `[MIN_REQUEST_TIMEOUT, MAX_REQUEST_TIMEOUT]`.
///
/// `CreateTopics`, `DeleteTopics` and `CreatePartitions` honor the request's own deadline.
/// `ListOffsets` and `Metadata` do not: they have no such field.
pub(crate) fn clamp_request_timeout(timeout_ms: i32) -> Duration {
    let requested = Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0));
    requested.clamp(MIN_REQUEST_TIMEOUT, MAX_REQUEST_TIMEOUT)
}

/// Runs `work`. When `heavy`, other tasks move to another worker first, so they keep running. A
/// current-thread runtime runs it in place.
pub(crate) fn off_worker<T>(heavy: bool, work: impl FnOnce() -> T) -> T {
    let multi_thread = Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == RuntimeFlavor::MultiThread);
    if heavy && multi_thread {
        tokio::task::block_in_place(work)
    } else {
        work()
    }
}

/// Routes one decoded request body to the module that owns its API key.
///
/// An unknown key closes the connection: no response schema exists for it, so any body sent
/// back is misparsed against the schema the client expected.
pub async fn dispatch(
    state: &GatewayState,
    connection: &ConnectionState,
    principal: Option<&AuthenticatedPrincipal>,
    api_key: i16,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    match api_key {
        API_KEY_PRODUCE => produce::handle(state, connection, principal, api_version, body).await,
        API_KEY_FETCH => fetch::handle(state, connection, api_version, body).await,
        API_KEY_LIST_OFFSETS => list_offsets::handle(state, api_version, body).await,
        API_KEY_METADATA => metadata::handle(state, connection, principal, api_version, body).await,
        API_KEY_OFFSET_COMMIT => offset_commit::handle(state, connection, api_version, body).await,
        API_KEY_OFFSET_FETCH => offset_fetch::handle(state, connection, api_version, body).await,
        API_KEY_API_VERSIONS => api_versions::handle(connection, api_version, body).await,
        API_KEY_CREATE_TOPICS => create_topics::handle(state, api_version, body).await,
        API_KEY_DELETE_TOPICS => delete_topics::handle(state, principal, api_version, body).await,
        API_KEY_CREATE_PARTITIONS => {
            create_partitions::handle(state, principal, api_version, body).await
        }
        API_KEY_DESCRIBE_CONFIGS => describe_configs::handle(state, api_version, body).await,
        API_KEY_ALTER_CONFIGS => alter_configs::handle(state, principal, api_version, body).await,
        API_KEY_INCREMENTAL_ALTER_CONFIGS => {
            incremental_alter_configs::handle(state, principal, api_version, body).await
        }
        API_KEY_FIND_COORDINATOR => {
            find_coordinator::handle(state, connection, api_version, body).await
        }
        API_KEY_JOIN_GROUP => join_group::handle(state, connection, api_version, body).await,
        API_KEY_HEARTBEAT => heartbeat::handle(state, connection, api_version, body).await,
        API_KEY_LEAVE_GROUP => leave_group::handle(state, api_version, body).await,
        API_KEY_SYNC_GROUP => sync_group::handle(state, api_version, body).await,
        API_KEY_DESCRIBE_GROUPS => {
            describe_groups::handle(state, connection, api_version, body).await
        }
        API_KEY_DESCRIBE_CLUSTER => {
            describe_cluster::handle(state, connection, api_version, body).await
        }
        API_KEY_LIST_GROUPS => list_groups::handle(state, connection, api_version, body).await,
        API_KEY_DELETE_GROUPS => {
            delete_groups::handle(state, connection, principal, api_version, body).await
        }
        API_KEY_OFFSET_DELETE => {
            offset_delete::handle(state, connection, principal, api_version, body).await
        }
        API_KEY_INIT_PRODUCER_ID => init_producer_id::handle(state, api_version, body).await,
        _ => HandleOutcome::Close,
    }
}

/// Whether a request carries a transactional id, which this gateway never serves.
///
/// An empty id reads as absent: `kafka_protocol` decodes a null wire string to `None` but its
/// own `Default` uses `Some("")`, and a producer that is idempotent-only has no transaction to
/// name either way.
pub(crate) fn is_transactional(transactional_id: Option<&TransactionalId>) -> bool {
    transactional_id.is_some_and(|id| !id.is_empty())
}

/// Whether `principal` may delete a group or its offsets. `None` means no listener of this gateway
/// names principals, with nothing to enforce against, as in `DeleteTopics`.
///
/// Kafka asks for `DELETE` on the group. Iggy has no group permission, and lets any reader of a
/// topic delete a group on it, so the gate asks for what `DeleteTopics` asks for: a global
/// `manage_topics` or `manage_streams`, which the principal's `manage_topics` stands for.
fn may_delete_groups(principal: Option<&AuthenticatedPrincipal>, api: &str) -> bool {
    let Some(principal) = principal else {
        return true;
    };
    if !principal.permissions_known {
        // Denying is the fail-closed choice: granting would authorize an irreversible delete off
        // a value nothing ever actually read.
        tracing::warn!(
            principal = %principal.username,
            "{api} asked on a connection whose permissions were never read; denying"
        );
        return false;
    }
    principal.permissions.manage_topics
}

/// When an offset call on `connection` gives up. See `GroupCoordinator::offset_hold`.
async fn offset_deadline(state: &GatewayState, connection: &ConnectionState) -> Instant {
    let member = connection.heartbeat_member();
    let hold = state.groups.offset_hold(member.as_ref()).await;
    Instant::now() + hold
}

/// When the Iggy calls of a `DeleteGroups` or `OffsetDelete` on `connection` give up. See
/// `GroupCoordinator::deletion_hold`.
async fn deletion_deadline(state: &GatewayState, connection: &ConnectionState) -> Instant {
    let member = connection.heartbeat_member();
    let hold = state.groups.deletion_hold(member.as_ref()).await;
    Instant::now() + hold
}

/// What the operator can do about an offset call that the client sees as -1. The client sees no
/// more, so the log is the one place the cause shows.
const fn offset_refusal_cause(error: &BridgeError) -> &'static str {
    match error {
        BridgeError::Iggy(IggyError::TooManyConsumerOffsets) => {
            "the partition holds partition.consumer_offsets_max keys"
        }
        BridgeError::Iggy(IggyError::InvalidCommand) => {
            "a server without the external group kind answers this"
        }
        error if error.is_bridge_login_rejected() => "Iggy rejected the bridge's credentials",
        _ => "no Kafka code fits this Iggy error",
    }
}

/// [`codec::encode_message`] with the error lifted into this crate's [`Result`].
pub(crate) fn encode_message<T: Encodable>(
    msg: &T,
    version: i16,
    capacity: usize,
) -> Result<Bytes> {
    codec::encode_message(msg, version, capacity).map_err(Into::into)
}

/// [`codec::decode_guarded`] with the error lifted into this crate's [`Result`].
pub(crate) fn decode_guarded<T: Decodable>(
    version: i16,
    body: Bytes,
    validate: impl FnOnce(i16, &Bytes) -> iggy_kafka_wire::Result<()>,
) -> Result<T> {
    codec::decode_guarded(version, body, validate).map_err(Into::into)
}

/// Turn an encode [`Result`] into a [`HandleOutcome`], closing the connection when encoding
/// fails rather than propagating - there is no parseable response to send in that case.
pub(crate) fn respond_or_close<E: Display>(
    result: core::result::Result<Bytes, E>,
    api_name: &str,
) -> HandleOutcome {
    match result {
        Ok(body) => HandleOutcome::Respond(body),
        Err(error) => {
            tracing::warn!(%error, "failed to encode {api_name} response; closing connection");
            HandleOutcome::Close
        }
    }
}

/// The shared shape of every API whose unsupported-version answer is an error response rather
/// than a close, and whose decode failure is reportable.
pub(crate) fn handle_versioned_request<T>(
    api_key: i16,
    api_version: i16,
    body: Bytes,
    decode: impl FnOnce(i16, Bytes) -> Result<T>,
    encode_ok: impl FnOnce(i16, &T) -> Result<Bytes>,
    encode_err: impl Fn(i16, i16) -> Result<Bytes>,
    api_name: &str,
) -> HandleOutcome {
    match decode_request(api_key, api_version, body, decode, encode_err, api_name) {
        Ok(req) => respond_or_close(encode_ok(api_version, &req), api_name),
        Err(outcome) => outcome,
    }
}

/// The decoded request, or the answer to send when the version or the body is bad.
pub(crate) fn decode_request<T>(
    api_key: i16,
    api_version: i16,
    body: Bytes,
    decode: impl FnOnce(i16, Bytes) -> Result<T>,
    encode_err: impl Fn(i16, i16) -> Result<Bytes>,
    api_name: &str,
) -> core::result::Result<T, HandleOutcome> {
    if !is_supported_version(api_key, api_version) {
        return Err(unsupported_version_response(
            api_key,
            api_version,
            |version| encode_err(version, ERROR_UNSUPPORTED_VERSION),
        ));
    }
    decode(api_version, body).map_err(|error| {
        // debug!, not warn!: attacker-controlled, not operator-actionable.
        tracing::debug!(%error, "Failed to decode {api_name} request");
        respond_or_close(encode_err(api_version, ERROR_INVALID_REQUEST), api_name)
    })
}

/// Unsupported-version policy for APIs whose encoders only implement up to
/// `ApiVersionRange::max_version`.
///
/// - `api_version > max`: Close. `SUPPORTED_RANGES` is the governance boundary, not just an
///   encoding-capability limit - `kafka_protocol` can often encode versions above our firewall
///   max just fine, but responding there would silently widen what this gateway accepts.
/// - `api_version < min`: Respond with an error shaped for that version when `kafka_protocol`
///   can encode it, otherwise `encode` fails and [`respond_or_close`] closes instead. In
///   practice every `SUPPORTED_RANGES` min was chosen at or above the oldest version
///   `kafka_protocol` implements for that message, so this always closes today (e.g.
///   `ListOffsets` v0's legacy `old_style_offsets` shape predates the crate's schema) - kept
///   generic rather than hard-coded so a future `kafka_protocol` upgrade that widens a schema's
///   floor is picked up automatically instead of silently staying on `Close`.
pub(crate) fn unsupported_version_response(
    api_key: i16,
    api_version: i16,
    encode: impl FnOnce(i16) -> Result<Bytes>,
) -> HandleOutcome {
    let max_version = supported_max_version(api_key).unwrap_or(0);
    if api_version > max_version {
        tracing::warn!(
            api_key,
            api_version,
            max_version,
            "request version above encoder max; closing connection"
        );
        return HandleOutcome::Close;
    }
    respond_or_close(encode(api_version), "unsupported-version")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_no_runtime_when_work_is_large_should_run_it_in_place() {
        assert_eq!(off_worker(true, || 7), 7);
    }

    #[tokio::test]
    async fn given_a_current_thread_runtime_when_work_is_large_should_run_it_in_place() {
        assert_eq!(off_worker(true, || 7), 7);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn given_a_multi_thread_runtime_when_work_is_large_should_run_it() {
        assert_eq!(off_worker(true, || 7), 7);
    }

    #[test]
    fn clamp_request_timeout_rejects_a_zero_or_negative_value_up_to_the_floor() {
        assert_eq!(clamp_request_timeout(0), MIN_REQUEST_TIMEOUT);
        assert_eq!(clamp_request_timeout(-1), MIN_REQUEST_TIMEOUT);
        assert_eq!(clamp_request_timeout(i32::MIN), MIN_REQUEST_TIMEOUT);
    }

    #[test]
    fn clamp_request_timeout_caps_an_oversized_value_at_the_ceiling() {
        assert_eq!(clamp_request_timeout(i32::MAX), MAX_REQUEST_TIMEOUT);
    }

    #[test]
    fn clamp_request_timeout_passes_through_a_reasonable_value_unchanged() {
        assert_eq!(clamp_request_timeout(5_000), Duration::from_secs(5));
    }
}
