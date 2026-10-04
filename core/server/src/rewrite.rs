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

//! The two pre-consensus request-rewrite chains, side by side.
//!
//! [`tcp_chain`] serves the TCP funnel, [`http_chain`] the HTTP submit. Each
//! step rewrites or validates a request BEFORE consensus, so a rejected
//! request burns no replicated log entry and no plaintext secret enters
//! consensus. The chains enter the PAT rewrite through different functions
//! on purpose: TCP resolves the acting user from the transport
//! `SessionManager` ([`maybe_rewrite_pat_request`]), HTTP authenticates
//! against its own session table and passes the resolved `user_id`
//! ([`rewrite_pat_request_for_user`]).
//!
//! Three more links complete the chains but live in their spines, because
//! they fire partition-read mesh RPCs or are plane-specific. Two are async:
//! the consumer-group Join/Leave enrichment ([`crate::consumer_group`], the
//! TCP funnel calls it after [`tcp_chain`]) and the `DeleteSegments` ->
//! `TruncatePartition` resolution
//! (`dispatch::partition::resolve_delete_segments_truncate`, called by both
//! spines). The third, the consumer-offset rewrite on the partition path
//! (`consumer_group::maybe_rewrite_consumer_offset_request`, called by
//! `dispatch::partition`), is synchronous.

use crate::pat::{maybe_rewrite_pat_request, rewrite_pat_request_for_user};
use crate::segment_cleaner::UNENFORCEABLE_TOPIC_SIZE_WARN;
use crate::session_manager::SessionManager;
use crate::shell::{ShellBus, ShellShard};
use crate::users::maybe_rewrite_user_password_request;
use crate::wire::request_body;
use consensus::MetadataHandle;
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsRequest, DeletePartitionsRequest,
};
use iggy_binary_protocol::requests::streams::{CreateStreamRequest, UpdateStreamRequest};
use iggy_binary_protocol::requests::topics::{CreateTopicRequest, UpdateTopicRequest};
use iggy_binary_protocol::requests::users::{CreateUserRequest, UpdateUserRequest};
use iggy_binary_protocol::{
    MAX_PARTITIONS_PER_REQUEST, Operation, PrepareHeader, RoutedRequestHeader, WireDecode,
    WireIdentifier, WireOptions,
};
use iggy_common::{
    IggyByteSize, IggyError, MaxTopicSize, TopicCreateOptions, UPDATABLE_STREAM_OPTION_KEYS,
    UPDATABLE_TOPIC_OPTION_KEYS, UPDATABLE_USER_OPTION_KEYS, validate_preallocated_topic_bytes,
    validate_topic_segment_size,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::Streams;
use server_common::Message;
use std::cell::RefCell;
use std::rc::Rc;
use tracing::warn;

/// The pre-consensus rewrite stages, in chain order.
///
/// One owner for the set: the funnel's consumer-group rewrite runs outside
/// [`tcp_chain`] but denies through the same path, so it names a variant
/// here instead of passing a bare literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteStage {
    PersonalAccessToken,
    UserPassword,
    StaticBounds,
    ConsumerGroup,
}

impl RewriteStage {
    /// The `context` log label, in the same `snake_case` shape as every other
    /// frame context.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PersonalAccessToken => "personal_access_token",
            Self::UserPassword => "user_password",
            Self::StaticBounds => "static_bounds",
            Self::ConsumerGroup => "consumer_group",
        }
    }
}

/// A staged pre-consensus rejection: `stage` labels the chain step for the
/// deny log line, `error` is the typed code the deny reply carries.
pub struct RewriteDeny {
    pub stage: RewriteStage,
    pub error: IggyError,
}

/// The TCP funnel's pre-consensus rewrite chain, in order: the PAT rewrite
/// (resolving the acting user from the transport `sessions` binding), the
/// password rewrite, then the static bounds gate. Mirrors [`http_chain`]
/// plus that bounds step, which the binary wire needs because it has no
/// `command.validate()` layer. Returns the rewritten request and the raw
/// PAT token the funnel substitutes into the committed reply; a rejection
/// names the failing stage for the deny log line.
pub fn tcp_chain<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    max_tokens_per_user: u32,
    request: Message<RoutedRequestHeader>,
) -> Result<(Message<RoutedRequestHeader>, Option<String>), RewriteDeny>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let (request, raw_pat_token) = maybe_rewrite_pat_request(
        sessions,
        transport_client_id,
        max_tokens_per_user,
        |user_id| {
            shard
                .plane
                .metadata()
                .mux_stm
                .users()
                .read(|users| users.pat_count_of(user_id))
        },
        request,
    )
    // Token cap reached, malformed body, or a lost session binding.
    .map_err(|error| RewriteDeny {
        stage: RewriteStage::PersonalAccessToken,
        error,
    })?;
    // Hash raw passwords and, for ChangePassword, verify the current password
    // on the primary before replication; see `crate::users`. Replicas store the
    // hash directly. A wrong current password is not denied here: it rides
    // consensus and applies as a committed InvalidCredentials no-op, so the only
    // Err returned is a malformed body.
    let request =
        maybe_rewrite_user_password_request(shard, request).map_err(|error| RewriteDeny {
            stage: RewriteStage::UserPassword,
            error,
        })?;
    static_bounds(shard, &request).map_err(|error| RewriteDeny {
        stage: RewriteStage::StaticBounds,
        error,
    })?;
    Ok((request, raw_pat_token))
}

/// The HTTP submit's pre-consensus rewrite chain: the PAT rewrite for the
/// already-authenticated `user_id`, then the password rewrite. Mirrors
/// [`tcp_chain`] minus the session lookup (the HTTP listener authenticates
/// against its own session table and resolves the acting user itself) and
/// minus the static bounds step: HTTP enforces the same bounds in its
/// handlers via `command.validate()` plus the validators below. Returns the
/// rewritten request and the raw PAT token the caller substitutes into the
/// committed reply.
pub fn http_chain<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    user_id: u32,
    max_tokens_per_user: u32,
    request: Message<RoutedRequestHeader>,
) -> Result<(Message<RoutedRequestHeader>, Option<String>), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let (request, raw_token) = rewrite_pat_request_for_user(
        user_id,
        max_tokens_per_user,
        |user_id| {
            shard
                .plane
                .metadata()
                .mux_stm
                .users()
                .read(|users| users.pat_count_of(user_id))
        },
        request,
    )?;
    let request = maybe_rewrite_user_password_request(shard, request)?;
    Ok((request, raw_token))
}

/// Per-request partitions-count cap, shared by create-topic, create-partitions
/// and delete-partitions admission. Runs pre-consensus like
/// [`validate_topic_bounds`]: an oversized count must not burn a replicated
/// log entry (create-partitions admission would also allocate that many
/// consensus-group ids before replicating).
///
/// Zero passes here because a zero-partition TOPIC is legal (legacy
/// `create_topic` admits `0..=MAX`); the add/remove requests reject it in
/// [`validate_partitions_change_count`].
const fn validate_partitions_count(partitions_count: u32) -> Result<(), IggyError> {
    // The two transports carry the cap under different names: this one on the
    // binary path, `MAX_PARTITIONS_COUNT` in the HTTP DTO validators. The
    // parity is a documented contract ([`http_chain`]), so it fails the build
    // rather than the next cross-transport test.
    const _: () = assert!(
        MAX_PARTITIONS_PER_REQUEST == iggy_common::MAX_PARTITIONS_COUNT,
        "the binary and HTTP partitions-count caps must stay equal"
    );

    if partitions_count > MAX_PARTITIONS_PER_REQUEST {
        return Err(IggyError::TooManyPartitions);
    }
    Ok(())
}

/// [`validate_partitions_count`] plus the zero rejection that create-partitions
/// and delete-partitions carry: adding or removing zero partitions is a no-op
/// that would still burn a replicated log entry, bump `Streams::revision` and
/// force every shard through a rebalance pass. Legacy rejects it with
/// `TooManyPartitions` in both handlers (`1..=MAX` on create, `== 0` on
/// delete), so the code matches rather than inventing a new one.
const fn validate_partitions_change_count(partitions_count: u32) -> Result<(), IggyError> {
    if partitions_count == 0 {
        return Err(IggyError::TooManyPartitions);
    }
    validate_partitions_count(partitions_count)
}

/// Static create-topic bounds shared by the TCP and HTTP ingresses. Runs
/// pre-consensus: a rejected request must not burn a replicated log entry,
/// and `prepare_request` errors evict the session instead of denying typed.
/// `ServerDefault` is exempt from the size floor (it resolves against server
/// config at admission, matching legacy); `Unlimited` passes numerically.
/// `segment_size_bytes` is the topic's RESOLVED segment size (explicit
/// option, else this node's default), so a per-topic segment above the
/// global default still floors the topic cap.
pub fn validate_topic_bounds(
    partitions_count: u32,
    max_topic_size: MaxTopicSize,
    segment_size_bytes: u64,
) -> Result<(), IggyError> {
    validate_partitions_count(partitions_count)?;
    validate_topic_size_floor(max_topic_size, segment_size_bytes)
}

/// A topic cap below one segment can never be enforced: the first segment
/// already exceeds it. Split out of [`validate_topic_bounds`] because update
/// admission checks the cap without a partitions count to check.
pub fn validate_topic_size_floor(
    max_topic_size: MaxTopicSize,
    segment_size_bytes: u64,
) -> Result<(), IggyError> {
    if !matches!(max_topic_size, MaxTopicSize::ServerDefault)
        && max_topic_size.as_bytes_u64() < segment_size_bytes
    {
        return Err(IggyError::InvalidTopicSize(
            max_topic_size,
            IggyByteSize::from(segment_size_bytes),
        ));
    }
    Ok(())
}

/// Announce an accepted `max_topic_size` the server cannot enforce as written.
///
/// [`validate_topic_size_floor`] admits any cap of one segment or more, but
/// retention runs PER PARTITION and floors each partition's share at one SEALED
/// segment, which reaches up to one maximum bus frame past `segment_size`. A cap
/// between the two is stored and echoed back verbatim while the server actually
/// keeps `(segment_size + max_message_size) * partitions_count`, so the only
/// moment an operator can be told is the one where they set it.
///
/// Warns rather than rejects: which caps are accepted is client-visible wire
/// behavior, and tightening it would break topics that already exist.
pub fn warn_unenforceable_topic_size(
    max_topic_size: MaxTopicSize,
    segment_size_bytes: u64,
    max_message_size_bytes: usize,
    partitions_count: u32,
) {
    let MaxTopicSize::Custom(configured) = max_topic_size else {
        return;
    };
    let max_message_size_bytes = u64::try_from(max_message_size_bytes).unwrap_or(u64::MAX);
    let per_partition_floor = segment_size_bytes.saturating_add(max_message_size_bytes);
    let topic_floor = per_partition_floor.saturating_mul(u64::from(partitions_count));
    if configured.as_bytes_u64() >= topic_floor {
        return;
    }
    warn!(
        max_topic_size = configured.as_bytes_u64(),
        partitions_count,
        segment_size = segment_size_bytes,
        enforced_per_partition = per_partition_floor,
        "{UNENFORCEABLE_TOPIC_SIZE_WARN}"
    );
}

/// Announce the same unenforceable cap when partitions are ADDED to a topic.
///
/// The cap is topic-wide but enforcement is per partition, so every added
/// partition shrinks the share: a cap that cleared the floor when the topic was
/// created can stop clearing it here. The request carries only the delta, so
/// the stored cap, segment size and current partition count come from metadata.
pub fn warn_unenforceable_topic_size_on_partition_add(
    streams: &Streams,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    max_message_size_bytes: usize,
    added_partitions_count: u32,
) {
    let Some(((stream_slab, topic_slab), _)) = streams.partition_count_context(stream_id, topic_id)
    else {
        return;
    };
    let Some((_, max_topic_size, partitions_count, segment_size)) =
        streams.topic_retention_config(stream_slab, topic_slab)
    else {
        return;
    };
    warn_unenforceable_topic_size(
        max_topic_size,
        segment_size.map_or(iggy_common::DEFAULT_SEGMENT_SIZE, |segment_size| {
            segment_size.as_bytes_u64()
        }),
        max_message_size_bytes,
        u32::try_from(partitions_count)
            .unwrap_or(u32::MAX)
            .saturating_add(added_partitions_count),
    );
}

/// Reject option keys outside the resource's catalog, pre-consensus. Unknown
/// keys are rejected rather than skipped: a silently ignored knob would hand
/// the client server defaults without it ever learning. Streams and users
/// have no catalog keys yet, so `known` is empty for both until one lands.
pub fn validate_option_keys(options: &WireOptions, known: &[&str]) -> Result<(), IggyError> {
    for entry in options {
        // Wire validation already enforced UTF-8 string keys.
        let key = String::from_utf8_lossy(entry.key);
        if !known.contains(&key.as_ref()) {
            return Err(IggyError::UnsupportedOptionKey(key.into_owned()));
        }
    }
    Ok(())
}

/// Static bounds run pre-consensus so a rejected request burns no
/// replicated log entry; HTTP covers the same bounds via
/// `command.validate()`. A body that fails to decode denies typed too
/// (`InvalidCommand`), instead of riding consensus just to fail there.
#[allow(clippy::too_many_lines)]
fn static_bounds<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    request: &Message<RoutedRequestHeader>,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    match request.header().operation {
        Operation::CreateTopic => CreateTopicRequest::decode_from(request_body(request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|create_topic| {
                // `parse` doubles as the catalog gate: an unknown key or a
                // malformed value denies typed here, pre-consensus.
                let options = TopicCreateOptions::parse(&create_topic.options)?;
                if let Some(segment_size) = options.segment_size {
                    validate_topic_segment_size(
                        segment_size.as_bytes_u64(),
                        iggy_common::MAX_TOPIC_SEGMENT_SIZE,
                    )?;
                }
                let segment_size = options.segment_size.map_or_else(
                    || iggy_common::DEFAULT_SEGMENT_SIZE,
                    |segment_size| segment_size.as_bytes_u64(),
                );
                if options
                    .preallocate_segments
                    .unwrap_or(iggy_common::DEFAULT_PREALLOCATE_SEGMENTS)
                {
                    validate_preallocated_topic_bytes(segment_size, create_topic.partitions_count)?;
                }
                let max_topic_size = options
                    .max_topic_size
                    .unwrap_or(MaxTopicSize::ServerDefault);
                validate_topic_bounds(create_topic.partitions_count, max_topic_size, segment_size)?;
                warn_unenforceable_topic_size(
                    max_topic_size,
                    segment_size,
                    shard.bus_max_message_size(),
                    create_topic.partitions_count,
                );
                Ok(())
            }),
        Operation::CreatePartitions => CreatePartitionsRequest::decode_from(request_body(request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|create_partitions| {
                validate_partitions_change_count(create_partitions.partitions_count)?;
                let metadata = shard.plane.metadata();
                warn_unenforceable_topic_size_on_partition_add(
                    metadata.mux_stm.streams(),
                    &create_partitions.stream_id,
                    &create_partitions.topic_id,
                    shard.bus_max_message_size(),
                    create_partitions.partitions_count,
                );
                Ok(())
            }),
        Operation::DeletePartitions => DeletePartitionsRequest::decode_from(request_body(request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|delete_partitions| {
                validate_partitions_change_count(delete_partitions.partitions_count)
            }),
        // Only the updatable subset: the create-time knobs are pushed to
        // partitions when the topic is built and nothing re-pushes them, so
        // accepting one here would store a value no partition ever sees.
        Operation::UpdateTopic => UpdateTopicRequest::decode_from(request_body(request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|update_topic| {
                validate_option_keys(&update_topic.options, UPDATABLE_TOPIC_OPTION_KEYS)?;
                let options = TopicCreateOptions::parse(&update_topic.options)?;
                let Some(max_topic_size) = options.max_topic_size else {
                    return Ok(());
                };
                // An update can lower the cap below one segment just as a
                // create can, and the stored map would then report a size the
                // topic can never enforce. The floor is this topic's own
                // segment size, since that key is create-only.
                let metadata = shard.plane.metadata();
                let streams = metadata.mux_stm.streams();
                let segment_size = streams
                    .topic_segment_size(&update_topic.stream_id, &update_topic.topic_id)
                    .map_or_else(
                        || iggy_common::DEFAULT_SEGMENT_SIZE,
                        |segment_size| segment_size.as_bytes_u64(),
                    );
                validate_topic_size_floor(max_topic_size, segment_size)?;
                let partitions_count = streams
                    .topic_partitions_count(&update_topic.stream_id, &update_topic.topic_id)
                    .unwrap_or(0);
                warn_unenforceable_topic_size(
                    max_topic_size,
                    segment_size,
                    shard.bus_max_message_size(),
                    u32::try_from(partitions_count).unwrap_or(u32::MAX),
                );
                Ok(())
            }),
        Operation::UpdateStream => UpdateStreamRequest::decode_from(request_body(request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|update_stream| {
                validate_option_keys(&update_stream.options, UPDATABLE_STREAM_OPTION_KEYS)
            }),
        Operation::UpdateUser => UpdateUserRequest::decode_from(request_body(request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|update_user| {
                validate_option_keys(&update_user.options, UPDATABLE_USER_OPTION_KEYS)
            }),
        Operation::CreateStream => CreateStreamRequest::decode_from(request_body(request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|create_stream| validate_option_keys(&create_stream.options, &[])),
        Operation::CreateUser => CreateUserRequest::decode_from(request_body(request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|create_user| validate_option_keys(&create_user.options, &[])),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_topic_bounds_deny_pre_consensus() {
        let segment_size = iggy_common::DEFAULT_SEGMENT_SIZE;
        assert!(segment_size > 0, "default segment size must be nonzero");

        assert!(
            validate_topic_bounds(
                MAX_PARTITIONS_PER_REQUEST,
                MaxTopicSize::ServerDefault,
                segment_size
            )
            .is_ok(),
            "the partition cap itself is admissible"
        );
        assert!(
            matches!(
                validate_topic_bounds(
                    MAX_PARTITIONS_PER_REQUEST + 1,
                    MaxTopicSize::ServerDefault,
                    segment_size
                ),
                Err(IggyError::TooManyPartitions)
            ),
            "one past the partition cap must deny"
        );
        // ServerDefault is numerically 0 yet exempt from the segment-size
        // floor: it resolves against server config, matching legacy.
        assert!(validate_topic_bounds(1, MaxTopicSize::ServerDefault, segment_size).is_ok());
        assert!(validate_topic_bounds(1, MaxTopicSize::Unlimited, segment_size).is_ok());
        let below_floor = MaxTopicSize::Custom((segment_size - 1).into());
        assert!(
            matches!(
                validate_topic_bounds(1, below_floor, segment_size),
                Err(IggyError::InvalidTopicSize(size, floor))
                    if size == below_floor && floor == IggyByteSize::from(segment_size)
            ),
            "custom size below the segment size must deny with the bounds"
        );
        let at_floor = MaxTopicSize::Custom(IggyByteSize::from(segment_size));
        assert!(
            validate_topic_bounds(1, at_floor, segment_size).is_ok(),
            "a topic exactly one segment large is admissible"
        );
    }

    #[test]
    fn partitions_count_cap_denies_pre_consensus() {
        assert!(
            validate_partitions_count(MAX_PARTITIONS_PER_REQUEST).is_ok(),
            "the cap itself is admissible"
        );
        assert!(
            matches!(
                validate_partitions_count(MAX_PARTITIONS_PER_REQUEST + 1),
                Err(IggyError::TooManyPartitions)
            ),
            "one past the cap must deny"
        );
        // Zero passes the shared cap because a zero-partition TOPIC is legal
        // (legacy `create_topic` admits `0..=MAX`).
        assert!(validate_partitions_count(0).is_ok());
    }

    #[test]
    fn zero_partitions_change_denies_pre_consensus() {
        // Adding or removing zero partitions is a no-op that would still burn
        // a replicated log entry and force a rebalance. Legacy rejects it with
        // `TooManyPartitions` in both handlers, so the code matches.
        assert!(
            matches!(
                validate_partitions_change_count(0),
                Err(IggyError::TooManyPartitions)
            ),
            "adding or removing zero partitions must deny"
        );
        assert!(validate_partitions_change_count(1).is_ok());
        assert!(validate_partitions_change_count(MAX_PARTITIONS_PER_REQUEST).is_ok());
        assert!(
            matches!(
                validate_partitions_change_count(MAX_PARTITIONS_PER_REQUEST + 1),
                Err(IggyError::TooManyPartitions)
            ),
            "the cap still applies"
        );
    }
}
