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
//! Two more links complete the chains but live in their spines, because
//! they fire partition-read mesh RPCs or are plane-specific: the async
//! `DeleteSegments` -> `TruncatePartition` resolution
//! (`dispatch::partition::resolve_delete_segments_truncate`, called by both
//! spines), and the synchronous consumer-offset rewrite on the partition path
//! (`consumer_group::maybe_rewrite_consumer_offset_request`, called by
//! `dispatch::partition`).

use crate::consumer_group::maybe_rewrite_consumer_group_request;
use crate::pat::{maybe_rewrite_pat_request, rewrite_pat_request_for_user};
use crate::segment_cleaner::UNENFORCEABLE_TOPIC_SIZE_WARN;
use crate::session_manager::SessionManager;
use crate::shell::{ShellBus, ShellShard};
use crate::stream_keys::{
    Bounded, defers_stream_keys, erase_stream_create, maybe_rewrite_secret_options_request,
};
use crate::users::maybe_rewrite_user_password_request;
use crate::wire::{request_body, rewrite_request_body};
use consensus::{MetadataHandle, PartitionsHandle};
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsRequest, DeletePartitionsRequest,
};
use iggy_binary_protocol::requests::streams::CreateStreamRequest;
use iggy_binary_protocol::requests::system::{ResourceTarget, UpdateResourceRequest};
use iggy_binary_protocol::requests::topics::CreateTopicRequest;
use iggy_binary_protocol::requests::users::CreateUserRequest;
use iggy_binary_protocol::{
    MAX_PARTITIONS_PER_REQUEST, Operation, PrepareHeader, RoutedRequestHeader, WireDecode,
    WireEncode, WireIdentifier, WireOptions,
};
use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
use iggy_common::wire_conversions::resource_options_to_wire;
use iggy_common::{
    AdmittedOptions, IggyByteSize, IggyError, MaxTopicSize, OptionAdmissionContext, OptionColumns,
    OptionOperation, OptionsProvenance, OptionsScope, ResourceOptions, TopicCreateOptions,
    TopicResolvedOptions, UserStatus, admit, derived_defaults, validate_preallocated_topic_bytes,
    validate_topic_segment_size,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::Streams;
use metadata::stm::user::Users;
use server_common::Message;
use std::cell::RefCell;
use std::rc::Rc;
use tracing::warn;

/// The pre-consensus rewrite stages of [`tcp_chain`], in chain order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteStage {
    PersonalAccessToken,
    UserPassword,
    StaticBounds,
    SecretOptions,
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
            Self::SecretOptions => "secret_options",
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
/// password rewrite, the static bounds gate (which also replaces a user
/// create's options with the admitted canonical block and hands a stream
/// create on decoded), the secret options rewrite that builds the stream
/// create's body with its key wrapped, then the consumer-group Join/Leave
/// identity rewrite ([`crate::consumer_group`]).
/// Mirrors [`http_chain`] plus the group step, which only the binary wire
/// serves. Returns the rewritten request and the raw PAT token the funnel
/// substitutes into the committed reply. A rejection names the failing stage
/// for the deny log line.
pub fn tcp_chain<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RoutedRequestHeader>,
) -> Result<(Message<RoutedRequestHeader>, Option<String>), RewriteDeny>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let (request, raw_pat_token) =
        maybe_rewrite_pat_request(sessions, transport_client_id, request)
            // Malformed body or a lost session binding.
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
    let user_id = sessions.borrow().get_user_id(transport_client_id);
    let bounded = static_bounds(shard, user_id, request).map_err(|error| RewriteDeny {
        stage: RewriteStage::StaticBounds,
        error,
    })?;
    let key_context = shard.plane.partitions().key_context();
    let request = maybe_rewrite_secret_options_request(&key_context, bounded, || {
        shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|streams| defers_stream_keys(streams, &key_context))
    })
    .map_err(|error| RewriteDeny {
        stage: RewriteStage::SecretOptions,
        error,
    })?;
    let request = maybe_rewrite_consumer_group_request(request).map_err(|error| RewriteDeny {
        stage: RewriteStage::ConsumerGroup,
        error,
    })?;
    Ok((request, raw_pat_token))
}

/// The HTTP submit's pre-consensus rewrite chain: the PAT rewrite for the
/// already-authenticated `user_id`, the password rewrite, the same static
/// bounds as [`tcp_chain`], so both transports admit options through one
/// catalog gate and answer the same request with the same error, then the
/// secret options rewrite. Mirrors
/// [`tcp_chain`] minus the session lookup (the HTTP listener authenticates
/// against its own session table and resolves the acting user itself) and
/// minus the group step, because HTTP serves no join or leave. Returns the
/// rewritten request and the raw PAT token the caller substitutes into the
/// committed reply.
pub fn http_chain<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    user_id: u32,
    request: Message<RoutedRequestHeader>,
) -> Result<(Message<RoutedRequestHeader>, Option<String>), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let (request, raw_token) = rewrite_pat_request_for_user(user_id, request)?;
    let request = maybe_rewrite_user_password_request(shard, request)?;
    let bounded = static_bounds(shard, Some(user_id), request)?;
    let key_context = shard.plane.partitions().key_context();
    let request = maybe_rewrite_secret_options_request(&key_context, bounded, || {
        shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|streams| defers_stream_keys(streams, &key_context))
    })?;
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

/// Static bounds run pre-consensus so a rejected request burns no
/// replicated log entry. Both chains run them, so every option block passes
/// the same catalog admission whatever the transport. A body that fails to
/// decode denies typed too (`InvalidCommand`), instead of riding consensus
/// just to fail there. A user create leaves with the admitted canonical
/// options, so apply stores one kind per key whatever the client sent. A
/// stream create leaves decoded with its admitted options, so the secret
/// stage builds its body without encoding a client key again, and a refused
/// one has its copies of the key erased. `user_id` is the acting user, `None`
/// when the transport has not bound one.
fn static_bounds<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    user_id: Option<u32>,
    request: Message<RoutedRequestHeader>,
) -> Result<Bounded, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    match request.header().operation {
        Operation::CreateTopic => CreateTopicRequest::decode_from(request_body(&request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|create_topic| {
                let admitted = admit_create(
                    OptionsScope::Topic,
                    &create_topic.options,
                    OptionColumns {
                        name: Some(create_topic.name.as_str().to_owned()),
                        ..OptionColumns::default()
                    },
                )?;
                if let Some(segment_size) =
                    TopicCreateOptions::from_resource_options(&admitted.explicit).segment_size
                {
                    validate_topic_segment_size(
                        segment_size.as_bytes_u64(),
                        iggy_common::MAX_TOPIC_SEGMENT_SIZE,
                    )?;
                }
                let mut stored = derived_defaults(OptionsScope::Topic, &admitted.explicit)?;
                stored.extend(admitted.explicit);
                let resolved = TopicResolvedOptions::from(&stored);
                let segment_size = resolved.runtime.effective_segment_size().as_bytes_u64();
                if resolved.runtime.preallocate_segments == Some(true) {
                    validate_preallocated_topic_bytes(segment_size, create_topic.partitions_count)?;
                }
                let max_topic_size = resolved.max_topic_size;
                validate_topic_bounds(create_topic.partitions_count, max_topic_size, segment_size)?;
                warn_unenforceable_topic_size(
                    max_topic_size,
                    segment_size,
                    shard.bus_max_message_size(),
                    create_topic.partitions_count,
                );
                Ok(())
            })?,
        Operation::CreatePartitions => CreatePartitionsRequest::decode_from(request_body(&request))
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
            })?,
        Operation::DeletePartitions => DeletePartitionsRequest::decode_from(request_body(&request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|delete_partitions| {
                validate_partitions_change_count(delete_partitions.partitions_count)
            })?,
        Operation::UpdateResource => UpdateResourceRequest::decode_from(request_body(&request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|patch| patch_bounds(shard, user_id, &patch))?,
        Operation::CreateStream => return bound_stream_create(request),
        Operation::CreateUser => {
            let mut create_user = CreateUserRequest::decode_from(request_body(&request))
                .map_err(|_| IggyError::InvalidCommand)?;
            let admitted = admit_create(
                OptionsScope::User,
                &create_user.options,
                OptionColumns {
                    username: Some(create_user.username.as_str().to_owned()),
                    ..OptionColumns::default()
                },
            )?;
            UserStatus::from_code(create_user.status).map_err(|_| IggyError::InvalidUserStatus)?;
            if let Some(options) = canonical_block(&create_user.options, &admitted)? {
                create_user.options = options;
                return rewrite_request_body(&request, &create_user.to_bytes())
                    .map(Bounded::Request);
            }
        }
        _ => {}
    }
    Ok(Bounded::Request(request))
}

/// Decode and admit a stream create, handed on decoded with its admitted
/// options. A refused one has its copies of a client key erased.
fn bound_stream_create(request: Message<RoutedRequestHeader>) -> Result<Bounded, IggyError> {
    let Ok(create) = CreateStreamRequest::decode_from(request_body(&request)) else {
        erase_stream_create(request, None);
        return Err(IggyError::InvalidCommand);
    };
    let admitted = admit_create(
        OptionsScope::Stream,
        &create.options,
        OptionColumns {
            name: Some(create.name.as_str().to_owned()),
            ..OptionColumns::default()
        },
    );
    match admitted {
        Ok(admitted) => Ok(Bounded::StreamCreate {
            request,
            create,
            options: admitted.explicit,
        }),
        Err(error) => {
            erase_stream_create(request, Some(create));
            Err(error)
        }
    }
}

/// The admitted canonical block when it differs from what the client sent.
fn canonical_block(
    client: &WireOptions,
    admitted: &AdmittedOptions,
) -> Result<Option<WireOptions>, IggyError> {
    let canonical = resource_options_to_wire(&admitted.explicit, OptionsProvenance::All)?;
    Ok((canonical.as_bytes() != client.as_bytes()).then_some(canonical))
}

/// Catalog admission for a generic patch, plus the floor a topic size cap
/// keeps against the topic's own segment size.
///
/// The capacity check and the floor read the target's stored state, so they
/// run only for a caller this node lets update the target. Any other caller
/// is admitted on the request alone and refused by apply, so no answer here
/// tells it what the target stores.
fn patch_bounds<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    user_id: Option<u32>,
    patch: &UpdateResourceRequest,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata = shard.plane.metadata();
    let streams = metadata.mux_stm.streams();
    let users = metadata.mux_stm.users();
    let authorized_user_id =
        user_id.filter(|user_id| may_update(streams, users, *user_id, &patch.target));
    let current = authorized_user_id.and_then(|_| stored_options(streams, users, &patch.target));
    let stored_state_visible = current.is_some();
    let admitted = admit_patch(patch, current)?;
    if let (Some(actor_id), ResourceTarget::User { user_id: target_id }) =
        (authorized_user_id, &patch.target)
    {
        let target_is_root = users
            .read(|inner| inner.resolve_user_id(target_id) == Some(DEFAULT_ROOT_USER_ID as usize));
        if target_is_root
            && (admitted.columns.username.is_some() || admitted.columns.status.is_some())
            && (actor_id != DEFAULT_ROOT_USER_ID
                || admitted.columns.status == Some(UserStatus::Inactive))
        {
            return Err(IggyError::CannotModifyRootUser);
        }
    }
    let ResourceTarget::Topic {
        stream_id,
        topic_id,
    } = &patch.target
    else {
        return Ok(());
    };
    let Some(max_topic_size) = TopicCreateOptions::from_resource_options(&admitted.explicit)
        .max_topic_size
        .filter(|_| stored_state_visible)
    else {
        return Ok(());
    };
    // An update can lower the cap below one segment just as a create can, and
    // the stored map would then report a size the topic can never enforce. The
    // floor is this topic's own segment size, since that key is create-only.
    let segment_size = streams.topic_segment_size(stream_id, topic_id).map_or_else(
        || iggy_common::DEFAULT_SEGMENT_SIZE,
        |segment_size| segment_size.as_bytes_u64(),
    );
    validate_topic_size_floor(max_topic_size, segment_size)?;
    let partitions_count = streams
        .topic_partitions_count(stream_id, topic_id)
        .unwrap_or(0);
    warn_unenforceable_topic_size(
        max_topic_size,
        segment_size,
        shard.bus_max_message_size(),
        u32::try_from(partitions_count).unwrap_or(u32::MAX),
    );
    Ok(())
}

/// Catalog admission for a create's options block, with the create's own
/// fixed fields as columns so the block cannot shadow them.
fn admit_create(
    scope: OptionsScope,
    options: &WireOptions,
    create_columns: OptionColumns,
) -> Result<AdmittedOptions, IggyError> {
    admit(
        scope,
        OptionOperation::Create,
        options,
        &[],
        &OptionAdmissionContext {
            current: None,
            create_columns,
        },
    )
}

/// Whether this node's applied permissions let `user_id` update `target`,
/// the rule apply runs again in committed order. `false` for a target this
/// node cannot resolve, which apply reports.
fn may_update(streams: &Streams, users: &Users, user_id: u32, target: &ResourceTarget) -> bool {
    let decision = match target {
        ResourceTarget::Stream { stream_id } => {
            streams.resolve_stream_id(stream_id).map(|stream_id| {
                users.authorize(|permissioner| permissioner.update_stream(user_id, stream_id))
            })
        }
        ResourceTarget::Topic {
            stream_id,
            topic_id,
        } => streams
            .resolve_topic_ids(stream_id, topic_id)
            .map(|(stream_id, topic_id)| {
                users.authorize(|permissioner| {
                    permissioner.update_topic(user_id, stream_id, topic_id)
                })
            }),
        ResourceTarget::User { .. } => {
            Some(users.authorize(|permissioner| permissioner.update_user(user_id)))
        }
    };
    matches!(decision, Some(Ok(())))
}

/// The options the patch target stores, `None` for a target that does not
/// exist, which apply reports.
fn stored_options(
    streams: &Streams,
    users: &Users,
    target: &ResourceTarget,
) -> Option<ResourceOptions> {
    match target {
        ResourceTarget::Stream { stream_id } => streams.read(|inner| {
            let stream = inner.resolve_stream_id(stream_id)?;
            Some(inner.items.get(stream)?.options.clone())
        }),
        ResourceTarget::Topic {
            stream_id,
            topic_id,
        } => streams.read(|inner| {
            let stream = inner.resolve_stream_id(stream_id)?;
            let topic = inner.resolve_topic_id(stream, topic_id)?;
            Some(inner.items.get(stream)?.topics.get(topic)?.options.clone())
        }),
        ResourceTarget::User { user_id } => users.read(|inner| {
            let user = inner.resolve_user_id(user_id)?;
            Some(inner.items.get(user)?.options.clone())
        }),
    }
}

/// Catalog admission for a generic patch against the target's stored
/// options, with the same rules the metadata primary applies again when it
/// resolves the patch.
fn admit_patch(
    patch: &UpdateResourceRequest,
    current: Option<ResourceOptions>,
) -> Result<AdmittedOptions, IggyError> {
    let reset: Vec<String> = patch
        .reset
        .iter()
        .map(|name| name.as_str().to_owned())
        .collect();
    admit(
        OptionsScope::from_code(patch.target.scope_code())?,
        OptionOperation::Update,
        &patch.options,
        &reset,
        &OptionAdmissionContext {
            current,
            create_columns: OptionColumns::default(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::{
        SpyBus, TestShard, create_one_partition, create_reader_request, prepare_message,
        register_reply, request_message, test_shard, test_shard_with_key_context,
    };
    use bytes::Bytes;
    use iggy_binary_protocol::requests::system::WireResetKeys;
    use iggy_binary_protocol::requests::topics::CreateTopicWithAssignmentsRequest;
    use iggy_binary_protocol::{MAX_OPTIONS, WireName};
    use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
    use iggy_common::wire_conversions::resource_options_from_wire;
    use iggy_common::{
        HeaderKey, HeaderValue, IggyDuration, IggyExpiry, OptionKey, OptionValue, OptionsUpdate,
        ServerKeyContext, StreamCreateOptions, StreamEncryptionKey, StreamKeyOrigin,
        TopicUpdateOptions, UserUpdateOptions, stream_wrap_set, text,
    };
    use message_bus::installer::conn_info::ClientTransportKind;
    use metadata::stm::StateMachine as _;
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Instant;

    const CONFIG_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_patch_constraints_when_using_either_chain_should_return_same_error() {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        create_one_partition(&shard);
        let sessions = root_sessions(&shard, 1);
        let topic = || ResourceTarget::Topic {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
        };
        let stream = || ResourceTarget::Stream {
            stream_id: WireIdentifier::numeric(0),
        };
        let raw_topic_patch = |key: OptionKey, value: &str| {
            let patch = TopicUpdateOptions {
                raw: BTreeMap::from([(key.to_string(), value.to_owned())]),
                ..TopicUpdateOptions::default()
            }
            .to_patch()
            .unwrap();
            UpdateResourceRequest {
                target: topic(),
                reset: patch.reset,
                options: patch.set,
            }
            .to_bytes()
        };
        let short_username = UserUpdateOptions {
            raw: BTreeMap::from([(OptionKey::Username.to_string(), "ab".to_owned())]),
            ..UserUpdateOptions::default()
        }
        .to_patch()
        .unwrap();
        let mut name_column = ResourceOptions::new();
        name_column.insert(
            HeaderKey::from(OptionKey::Name),
            OptionValue::explicit(HeaderValue::try_from("shadow").unwrap()),
        );
        let json_patch = |target: ResourceTarget, body: &str| {
            let patch = serde_json::from_str::<OptionsUpdate>(body)
                .unwrap()
                .to_patch()
                .unwrap();
            UpdateResourceRequest {
                target,
                reset: patch.reset,
                options: patch.set,
            }
            .to_bytes()
        };
        let user = || ResourceTarget::User {
            user_id: WireIdentifier::numeric(0),
        };
        let cases: Vec<(Operation, Bytes, IggyError)> = vec![
            (
                Operation::UpdateResource,
                json_patch(topic(), r#"{"name": 42}"#),
                IggyError::InvalidTopicName,
            ),
            (
                Operation::UpdateResource,
                json_patch(user(), r#"{"status": true}"#),
                IggyError::InvalidUserStatus,
            ),
            (
                Operation::UpdateResource,
                raw_topic_patch(OptionKey::SegmentSize, "64 MiB"),
                IggyError::OptionNotUpdatable(OptionKey::SegmentSize.to_string()),
            ),
            (
                Operation::UpdateResource,
                raw_topic_patch(OptionKey::MaxTopicSize, "1 MiB"),
                IggyError::InvalidTopicSize(
                    MaxTopicSize::from(1024 * 1024u64),
                    IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE),
                ),
            ),
            (
                Operation::UpdateResource,
                UpdateResourceRequest {
                    target: stream(),
                    reset: WireResetKeys::try_from(vec![WireName::new("name").unwrap()]).unwrap(),
                    options: WireOptions::empty(),
                }
                .to_bytes(),
                IggyError::OptionNotResettable(OptionKey::Name.to_string()),
            ),
            (
                Operation::UpdateResource,
                UpdateResourceRequest {
                    target: stream(),
                    reset: WireResetKeys::default(),
                    options: resource_options_to_wire(
                        &ResourceOptions::from([(
                            HeaderKey::from(OptionKey::EncryptionKeySource),
                            OptionValue::explicit(HeaderValue::try_from("stream").unwrap()),
                        )]),
                        OptionsProvenance::All,
                    )
                    .unwrap(),
                }
                .to_bytes(),
                IggyError::ReadOnlyOption(OptionKey::EncryptionKeySource.to_string()),
            ),
            (
                Operation::UpdateResource,
                UpdateResourceRequest {
                    target: ResourceTarget::User {
                        user_id: WireIdentifier::numeric(0),
                    },
                    reset: short_username.reset,
                    options: short_username.set,
                }
                .to_bytes(),
                IggyError::InvalidUsername,
            ),
            (
                Operation::CreateStream,
                CreateStreamRequest {
                    name: WireName::new("orders").unwrap(),
                    options: resource_options_to_wire(&name_column, OptionsProvenance::All)
                        .unwrap(),
                }
                .to_bytes(),
                IggyError::InvalidOptionValue(OptionKey::Name.to_string()),
            ),
            (
                Operation::CreateStream,
                CreateStreamRequest {
                    name: WireName::new("orders").unwrap(),
                    options: StreamCreateOptions {
                        encryption_key: Some(
                            StreamEncryptionKey::try_from([7u8; 32].as_slice()).unwrap(),
                        ),
                        ..StreamCreateOptions::default()
                    }
                    .to_wire()
                    .unwrap(),
                }
                .to_bytes(),
                IggyError::StreamEncryptionUnavailable,
            ),
        ];

        for (operation, body, expected) in cases {
            let tcp = tcp_chain(
                &shard,
                &sessions,
                1,
                request_message(operation, 1, 1, 1, &body),
            )
            .map(|_| ())
            .map_err(|deny| deny.error);
            let http = http_chain(
                &shard,
                DEFAULT_ROOT_USER_ID,
                request_message(operation, 1, 1, 1, &body),
            )
            .map(|_| ());
            assert_eq!(tcp, Err(expected.clone()), "binary chain, {operation:?}");
            assert_eq!(http, Err(expected), "HTTP chain, {operation:?}");
        }
    }

    #[test]
    fn given_caller_without_update_permission_when_patching_topic_below_its_segment_should_leave_the_refusal_to_apply()
     {
        const READER: u32 = 1;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        create_one_partition(&shard);
        let created = shard
            .plane
            .metadata()
            .mux_stm
            .update(prepare_message(
                Operation::CreateUser,
                1,
                3,
                &create_reader_request().to_bytes(),
            ))
            .expect("the reader is created");
        assert_eq!(created.code, 0);
        let patch = TopicUpdateOptions {
            raw: BTreeMap::from([(OptionKey::MaxTopicSize.to_string(), "1 MiB".to_owned())]),
            ..TopicUpdateOptions::default()
        }
        .to_patch()
        .expect("the patch encodes");
        let body = UpdateResourceRequest {
            target: ResourceTarget::Topic {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
            },
            reset: patch.reset,
            options: patch.set,
        }
        .to_bytes();
        let chain = |user_id: u32| {
            http_chain(
                &shard,
                user_id,
                request_message(Operation::UpdateResource, 1, 1, 1, &body),
            )
            .map(|_| ())
        };

        assert_eq!(
            chain(READER),
            Ok(()),
            "a caller apply refuses must not learn the topic's segment size here"
        );
        assert_eq!(
            chain(DEFAULT_ROOT_USER_ID),
            Err(IggyError::InvalidTopicSize(
                MaxTopicSize::from(1024 * 1024u64),
                IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE),
            ))
        );
    }

    #[test]
    fn given_root_deactivation_when_admitting_patch_should_refuse_before_consensus() {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        shard
            .plane
            .metadata()
            .mux_stm
            .users()
            .ensure_root_user("iggy", "hash");
        let patch = UserUpdateOptions {
            status: Some(UserStatus::Inactive),
            ..UserUpdateOptions::default()
        }
        .to_patch()
        .expect("status patch encodes");
        let body = UpdateResourceRequest {
            target: ResourceTarget::User {
                user_id: WireIdentifier::numeric(DEFAULT_ROOT_USER_ID),
            },
            reset: patch.reset,
            options: patch.set,
        }
        .to_bytes();

        let result = http_chain(
            &shard,
            DEFAULT_ROOT_USER_ID,
            request_message(Operation::UpdateResource, 1, 1, 1, &body),
        );
        assert!(
            matches!(result, Err(IggyError::CannotModifyRootUser)),
            "actual: {:?}",
            result.err()
        );
        assert!(bus.replica_sends.borrow().is_empty());
    }

    #[test]
    fn given_manage_users_caller_when_patching_root_should_refuse_rename_and_deactivation() {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        let metadata = shard.plane.metadata();
        metadata.mux_stm.users().ensure_root_user("iggy", "hash");
        let mut manager = create_reader_request();
        manager.username = WireName::new("manager").expect("valid name");
        manager
            .permissions
            .as_mut()
            .expect("permissions")
            .global
            .manage_users = true;
        let created = metadata
            .mux_stm
            .update(prepare_message(
                Operation::CreateUser,
                1,
                3,
                &manager.to_bytes(),
            ))
            .expect("create manager");
        assert_eq!(created.code, 0);

        for update in [
            UserUpdateOptions {
                username: Some("root-renamed".to_owned()),
                ..UserUpdateOptions::default()
            },
            UserUpdateOptions {
                status: Some(UserStatus::Inactive),
                ..UserUpdateOptions::default()
            },
        ] {
            let patch = update.to_patch().expect("patch encodes");
            let body = UpdateResourceRequest {
                target: ResourceTarget::User {
                    user_id: WireIdentifier::numeric(DEFAULT_ROOT_USER_ID),
                },
                reset: patch.reset,
                options: patch.set,
            }
            .to_bytes();
            let result = http_chain(
                &shard,
                1,
                request_message(Operation::UpdateResource, 1, 1, 1, &body),
            );
            assert!(matches!(result, Err(IggyError::CannotModifyRootUser)));
        }
        assert!(bus.replica_sends.borrow().is_empty());
    }

    #[test]
    fn given_out_of_range_status_when_creating_user_with_either_chain_should_refuse_invalid_status()
    {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let body = CreateUserRequest {
            username: WireName::new("status-user").unwrap(),
            password: "secret123".to_owned(),
            status: 9,
            permissions: None,
            options: WireOptions::empty(),
        }
        .to_bytes();

        let tcp = tcp_chain(
            &shard,
            &sessions,
            1,
            request_message(Operation::CreateUser, 1, 1, 1, &body),
        )
        .map(|_| ())
        .map_err(|deny| deny.error);
        let http = http_chain(
            &shard,
            0,
            request_message(Operation::CreateUser, 1, 1, 1, &body),
        )
        .map(|_| ());

        assert_eq!(tcp, Err(IggyError::InvalidUserStatus), "binary chain");
        assert_eq!(http, Err(IggyError::InvalidUserStatus), "HTTP chain");
    }

    #[test]
    fn given_text_stream_key_when_bounding_create_should_encode_no_request_with_the_key() {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        let key = StreamEncryptionKey::try_from([7u8; 32].as_slice()).expect("32 bytes");
        let body = CreateStreamRequest {
            name: WireName::new("orders").expect("a name"),
            options: text_key_block(&key),
        }
        .to_bytes();

        let sent = request_message(Operation::CreateStream, 1, 1, 1, &body);

        let bounded = static_bounds(&shard, Some(DEFAULT_ROOT_USER_ID), sent.clone())
            .expect("the create admits");

        let Bounded::StreamCreate {
            request, options, ..
        } = bounded
        else {
            panic!("a stream create goes on decoded");
        };
        assert_eq!(request.as_slice(), sent.as_slice(), "no request is rebuilt");
        assert_eq!(
            options.get(&HeaderKey::from(OptionKey::EncryptionKey)),
            Some(&OptionValue::explicit(
                HeaderValue::try_from(key.as_bytes().as_slice()).expect("a raw value")
            )),
            "the admitted key is canonical"
        );
    }

    #[test]
    fn given_stream_key_in_either_form_when_running_either_chain_should_forward_only_its_wrap_set()
    {
        let bus = SpyBus::default();
        let context =
            Arc::new(ServerKeyContext::from_config(CONFIG_KEY, &[], true).expect("a valid key"));
        let shard = Rc::new(test_shard_with_key_context(
            &bus,
            0,
            1,
            1,
            Arc::clone(&context),
        ));
        let sessions = root_sessions(&shard, 1);
        let key = StreamEncryptionKey::try_from([7u8; 32].as_slice()).expect("32 bytes");
        let raw_block = StreamCreateOptions {
            encryption_key: Some(key.clone()),
            ..StreamCreateOptions::default()
        }
        .to_wire()
        .expect("encodes");

        for options in [raw_block, text_key_block(&key)] {
            let body = CreateStreamRequest {
                name: WireName::new("orders").expect("a name"),
                options,
            }
            .to_bytes();
            let (tcp, _) = tcp_chain(
                &shard,
                &sessions,
                1,
                request_message(Operation::CreateStream, 1, 1, 1, &body),
            )
            .map_err(|deny| deny.error)
            .expect("binary chain");
            let (http, _) = http_chain(
                &shard,
                DEFAULT_ROOT_USER_ID,
                request_message(Operation::CreateStream, 1, 1, 1, &body),
            )
            .expect("HTTP chain");

            for forwarded in [tcp, http] {
                let forwarded = request_body(&forwarded);
                assert!(!contains(forwarded, key.as_bytes()), "raw key travels on");
                assert!(
                    !contains(forwarded, text::as_base64(key.as_bytes()).as_bytes()),
                    "base64 key travels on"
                );
                let create = CreateStreamRequest::decode_from(forwarded).expect("decodes");
                let stored = resource_options_from_wire(&create.options, true).expect("a block");
                let set = stream_wrap_set(&stored)
                    .expect("readable")
                    .expect("a wrap set");
                assert_eq!(set.origin(), StreamKeyOrigin::Client);
                assert_eq!(context.open(&set).expect("opens"), key);
            }
        }
    }

    #[test]
    fn given_stored_map_at_capacity_when_patching_a_new_key_should_refuse_at_the_edge() {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        create_one_partition(&shard);
        let full: ResourceOptions = (0..MAX_OPTIONS)
            .map(|index| {
                (
                    HeaderKey::from_str(&format!("future_{index:04}")).unwrap(),
                    OptionValue::explicit(HeaderValue::from(true)),
                )
            })
            .collect();
        shard
            .plane
            .metadata()
            .mux_stm
            .update(prepare_message(
                Operation::CreateTopicWithAssignments,
                1,
                3,
                &CreateTopicWithAssignmentsRequest {
                    request: CreateTopicRequest {
                        stream_id: WireIdentifier::numeric(0),
                        partitions_count: 0,
                        name: WireName::new("full").unwrap(),
                        options: resource_options_to_wire(&full, OptionsProvenance::All).unwrap(),
                    },
                    derived_options: WireOptions::empty(),
                    partitions: Vec::new(),
                }
                .to_bytes(),
            ))
            .unwrap();
        let patch = TopicUpdateOptions {
            message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(
                60_000_000u64,
            ))),
            ..TopicUpdateOptions::default()
        }
        .to_patch()
        .unwrap();
        let body = UpdateResourceRequest {
            target: ResourceTarget::Topic {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(1),
            },
            reset: patch.reset,
            options: patch.set,
        }
        .to_bytes();

        let bounded = static_bounds(
            &shard,
            Some(DEFAULT_ROOT_USER_ID),
            request_message(Operation::UpdateResource, 1, 1, 1, &body),
        )
        .map(|_| ());

        assert!(
            matches!(bounded, Err(IggyError::OptionsBlockTooLarge(_))),
            "{bounded:?}"
        );
    }

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

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    fn text_key_block(key: &StreamEncryptionKey) -> WireOptions {
        resource_options_to_wire(
            &ResourceOptions::from([(
                HeaderKey::from(OptionKey::EncryptionKey),
                OptionValue::explicit(
                    HeaderValue::try_from(text::as_base64(key.as_bytes()).as_str())
                        .expect("a text value"),
                ),
            )]),
            OptionsProvenance::All,
        )
        .expect("encodes")
    }

    /// A session table whose `transport` connection is bound to root.
    fn root_sessions(shard: &TestShard, transport: u128) -> Rc<RefCell<SessionManager>> {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        let attachment = {
            let mut client_table = shard.plane.metadata().client_table.borrow_mut();
            client_table
                .commit_register(
                    CLIENT,
                    DEFAULT_ROOT_USER_ID,
                    [0x5a; 32],
                    register_reply(CLIENT, SESSION),
                )
                .expect("root registers");
            client_table
                .attach_session(CLIENT, SESSION, DEFAULT_ROOT_USER_ID)
                .expect("root attaches")
        };
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        sessions.borrow_mut().ensure_connection(
            transport,
            "127.0.0.1:34567".parse().expect("a socket address"),
            ClientTransportKind::Tcp,
            Instant::now(),
        );
        sessions
            .borrow_mut()
            .bind_authenticated_connection(
                transport,
                CLIENT,
                SESSION,
                DEFAULT_ROOT_USER_ID,
                attachment,
                SESSION,
                Instant::now(),
            )
            .expect("root binds");
        sessions
    }
}
