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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::{
    DescribeAclsRequest, SaslAuthenticateRequest, SaslHandshakeRequest,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::bridge::IggyBridge;
use crate::error::Result;
use crate::group::{GroupCoordinator, GroupCoordinatorConfig};
use crate::protocol::acl::{
    self, AclBinding, AclFilter, PrincipalPermissions, encode_describe_acls_error_response,
    encode_describe_acls_response,
};
use crate::protocol::bounds_guard::{
    validate_describe_acls_shape, validate_sasl_authenticate_shape, validate_sasl_handshake_shape,
};
use crate::protocol::handlers::init_producer_id::ProducerIdAllocator;
use crate::protocol::handlers::{
    api_versions, create_topics, decode_guarded, dispatch, fetch, find_coordinator, heartbeat,
    init_producer_id, join_group, leave_group, list_offsets, metadata, produce, respond_or_close,
    sync_group,
};
use crate::protocol::probe_board::ProbeBoard;
use crate::protocol::sasl::{
    SaslMechanism, encode_sasl_authenticate_response, encode_sasl_handshake_response,
};

pub const API_KEY_PRODUCE: i16 = 0;
pub const API_KEY_FETCH: i16 = 1;
pub const API_KEY_LIST_OFFSETS: i16 = 2;
pub const API_KEY_METADATA: i16 = 3;
pub const API_KEY_FIND_COORDINATOR: i16 = 10;
pub const API_KEY_JOIN_GROUP: i16 = 11;
pub const API_KEY_HEARTBEAT: i16 = 12;
pub const API_KEY_LEAVE_GROUP: i16 = 13;
pub const API_KEY_SYNC_GROUP: i16 = 14;
pub const API_KEY_SASL_HANDSHAKE: i16 = 17;
pub const API_KEY_API_VERSIONS: i16 = 18;
pub const API_KEY_CREATE_TOPICS: i16 = 19;
pub const API_KEY_INIT_PRODUCER_ID: i16 = 22;
pub const API_KEY_DESCRIBE_ACLS: i16 = 29;
pub const API_KEY_SASL_AUTHENTICATE: i16 = 36;

pub const DEFAULT_KAFKA_PORT: u16 = 9093;

/// Generic catch-all. Not sent by any stub response today; the `bridge` module's error mapping
/// uses it for an `IggyError` with no closer Kafka analogue.
pub const ERROR_UNKNOWN_SERVER_ERROR: i16 = ResponseError::UnknownServerError.code();
pub const ERROR_NONE: i16 = 0;
/// Fetch: the offset is negative or past the high watermark. The client resets its position.
pub const ERROR_OFFSET_OUT_OF_RANGE: i16 = ResponseError::OffsetOutOfRange.code();
pub const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = ResponseError::UnknownTopicOrPartition.code();
/// Retriable, nothing written. The Produce stub, and a partition refused because the request
/// budget ran out.
pub const ERROR_NOT_LEADER_OR_FOLLOWER: i16 = ResponseError::NotLeaderOrFollower.code();
/// Outcome unknown: the write may have landed. Retriable, so a retry can duplicate it.
/// Idempotent produce (#3545) closes that.
pub const ERROR_REQUEST_TIMED_OUT: i16 = ResponseError::RequestTimedOut.code();
/// Produce: a record, send or partition too large, even alone. Not retried as-is. Java splits a
/// multi-record batch and sends the halves.
pub const ERROR_MESSAGE_TOO_LARGE: i16 = ResponseError::MessageTooLarge.code();
/// `bridge`'s mapping for a Kafka-side topic name that fails Kafka's own naming rules.
///
/// Empty, whitespace-padded, over 249 bytes, or outside `[A-Za-z0-9._-]`, checked before any Iggy
/// call is made - a real Kafka client library validates topic names client-side and would never
/// send one of these, but a raw/non-conformant client could.
pub const ERROR_INVALID_TOPIC_EXCEPTION: i16 = ResponseError::InvalidTopicException.code();
/// Produce: `acks` is not 0, 1 or -1. A conformant client never sends one, since `acks` comes
/// from validated configuration rather than from application input.
pub const ERROR_INVALID_REQUIRED_ACKS: i16 = ResponseError::InvalidRequiredAcks.code();
/// Retriable. Sent when this coordinator is at one of its `GroupCoordinatorConfig` capacity
/// caps: the client should back off and retry rather than treat the group as unusable.
pub const ERROR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// Retriable. Sent to a parked `JoinGroup`/`SyncGroup` waiter when the gateway starts draining,
/// so a shutdown does not hold a connection open for a full rebalance timeout.
pub const ERROR_NOT_COORDINATOR: i16 = 16;
/// The member's generation is not the group's current one; it must rejoin.
pub const ERROR_ILLEGAL_GENERATION: i16 = 22;
/// No protocol name is supported by every member, or the first member sent an empty protocol
/// type / empty protocol list.
pub const ERROR_INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
pub const ERROR_INVALID_GROUP_ID: i16 = 24;
pub const ERROR_UNKNOWN_MEMBER_ID: i16 = 25;
pub const ERROR_INVALID_SESSION_TIMEOUT: i16 = 26;
/// How a follower learns to rejoin: its heartbeat is answered with this while the group prepares.
pub const ERROR_REBALANCE_IN_PROGRESS: i16 = 27;
/// Closest fit for an Iggy permission/credential rejection in `bridge`'s error mapping.
///
/// Still not `SASL_AUTHENTICATION_FAILED`, and now for a firmer reason than when this was written:
/// a connection reaching the bridge has already completed its SASL exchange, so a credential or
/// permission rejection from Iggy at that point is not an authentication failure and saying so
/// would send an operator to the wrong hop. Not sent by any stub response today.
pub const ERROR_TOPIC_AUTHORIZATION_FAILED: i16 = ResponseError::TopicAuthorizationFailed.code();
/// The mechanism a client asked for in `SaslHandshake` is not one this gateway enables. The
/// response still carries the enabled mechanism list, which is what the client prints.
pub const ERROR_UNSUPPORTED_SASL_MECHANISM: i16 = ResponseError::UnsupportedSaslMechanism.code();
/// A request arrived that is legal on the wire but not in this connection's SASL state: a token
/// before a handshake, a normal request before authenticating, or a SASL request after.
pub const ERROR_ILLEGAL_SASL_STATE: i16 = ResponseError::IllegalSaslState.code();
pub const ERROR_UNSUPPORTED_VERSION: i16 = ResponseError::UnsupportedVersion.code();
/// `bridge`'s mapping for `BridgeError::PartitionCountMismatch`: the topic exists, just not with
/// the requested partition count.
///
/// Not [`ERROR_INVALID_PARTITIONS`] - `kafka-protocol`'s own error table (`error.rs`) defines that
/// code's text as "Number of partitions is below 1", which is a different condition (a client
/// asking for zero/negative partitions) than "this topic already exists with a different count".
pub const ERROR_TOPIC_ALREADY_EXISTS: i16 = 36;
pub const ERROR_INVALID_PARTITIONS: i16 = 37;
pub const ERROR_INVALID_REPLICATION_FACTOR: i16 = 38;
/// `CreateTopics`: a manual partition `assignments` list whose partition indices are not exactly
/// `0..assignments.len()` in some order, or repeat an index.
///
/// Matches real Kafka's `ReplicationControlManager.createTopic`, which validates the assignment
/// map's keys the same way regardless of what a client's replica list under each key says (this
/// bridge doesn't model replicas at all, so only the key set is checked).
pub const ERROR_INVALID_REPLICA_ASSIGNMENT: i16 = 39;
/// `CreateTopics` stub: do not claim topics were created (no controller / no Iggy bridge).
pub const ERROR_NOT_CONTROLLER: i16 = 41;
pub const ERROR_INVALID_REQUEST: i16 = 42;
/// `CreateTopics`: a requested topic carried one or more per-topic Kafka configs.
///
/// None of `retention.ms`, `cleanup.policy`, etc. maps onto an Iggy topic option this bridge
/// applies, so every non-empty `configs` list is rejected outright rather than silently dropping
/// a subset an operator might believe took effect.
pub const ERROR_INVALID_CONFIG: i16 = 40;
/// `ListOffsets`' code for a timestamp lookup the broker cannot perform.
///
/// Real brokers send this for an old-message-format log; this bridge sends it for any timestamp
/// other than the two KIP-79 sentinels, since Iggy has no per-message timestamp index at all.
/// Non-retriable, so a Java client resolves immediately instead of retrying
/// [`ERROR_UNKNOWN_SERVER_ERROR`] until its own `default.api.timeout.ms`.
pub const ERROR_UNSUPPORTED_FOR_MESSAGE_FORMAT: i16 =
    ResponseError::UnsupportedForMessageFormat.code();
/// `CreateTopics`: request addressed more distinct topics than this bridge admits in one call.
///
/// A server-imposed limit, not a malformed request - `INVALID_REQUEST` would blame the client for
/// a request Kafka itself would accept.
pub const ERROR_POLICY_VIOLATION: i16 = 44;
/// `FindCoordinator` for a transaction coordinator, which the gateway has none of.
///
/// The one code both the Java client and librdkafka treat as fatal for that lookup: anything else,
/// `INVALID_REQUEST` included, sends librdkafka into a 500ms retry loop that never ends.
pub const ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
/// A credential was refused. Deliberately undifferentiated: Iggy answers a bad password and an
/// unknown user the same way, and distinguishing them here would reintroduce a user-enumeration
/// oracle.
pub const ERROR_SASL_AUTHENTICATION_FAILED: i16 = ResponseError::SaslAuthenticationFailed.code();
/// Fetch: the request continues a session. This gateway opens none, so the client starts over
/// with a full request.
pub const ERROR_FETCH_SESSION_ID_NOT_FOUND: i16 = ResponseError::FetchSessionIdNotFound.code();
/// Produce: zstd before v7.
pub const ERROR_UNSUPPORTED_COMPRESSION_TYPE: i16 =
    ResponseError::UnsupportedCompressionType.code();
/// KIP-394: a `JoinGroup` v4+ with an empty member id is answered with a freshly minted id and
/// this code, and the client rejoins carrying it.
pub const ERROR_MEMBER_ID_REQUIRED: i16 = 79;
pub const ERROR_GROUP_MAX_SIZE_REACHED: i16 = 81;
/// Sent only by `LeaveGroup`: the member id does not hold the `group_instance_id` it named.
pub const ERROR_FENCED_INSTANCE_ID: i16 = 82;
/// Produce: a record or batch this gateway cannot map.
///
/// Not `CORRUPT_MESSAGE` (2), whose text fits but which `kafka-protocol`'s table marks
/// retriable, so a client would resend a batch that can never decode. A client older than Kafka
/// 2.4 reads 87 as a generic server error, which is still terminal and still better than a loop.
pub const ERROR_INVALID_RECORD: i16 = ResponseError::InvalidRecord.code();

/// Kafka's -1 for an offset or a timestamp that is not known.
pub(crate) const UNKNOWN_OFFSET: i64 = -1;

/// Ceiling on the bridge work of one request, wait included. A clock, not a partition cap, so a
/// wide topic still works. Above one bridge call's 15 s timeout. Fits the 25 s shutdown drain.
pub(crate) const REQUEST_DEADLINE: Duration = Duration::from_secs(20);

/// Result of handling one Kafka request body.
#[derive(Debug)]
pub enum HandleOutcome {
    /// Write this response body (with a response header).
    Respond(Bytes),
    /// Produce with `acks=0`: write nothing, keep the connection open.
    NoResponse,
    /// Write this response body, then close the TCP connection.
    ///
    /// Kafka's authentication failures are shaped this way: the client parses a correctly-shaped
    /// error body at its own version and only then sees the connection drop. [`Self::Close`]
    /// alone would leave it guessing, and [`Self::Respond`] would leave an unauthenticated
    /// connection open.
    RespondThenClose(Bytes),
    /// No parseable response exists for this request; close the TCP connection.
    Close,
}

impl HandleOutcome {
    /// Return the response body of a [`Self::Respond`] or [`Self::RespondThenClose`], or panic
    /// with `msg`.
    ///
    /// # Panics
    ///
    /// Panics when the outcome is [`Self::NoResponse`] or [`Self::Close`].
    #[must_use]
    pub fn expect_response(self, msg: &str) -> Bytes {
        match self {
            Self::Respond(body) | Self::RespondThenClose(body) => body,
            Self::NoResponse => panic!("{msg}: got NoResponse"),
            Self::Close => panic!("{msg}: got Close"),
        }
    }

    #[must_use]
    pub const fn is_no_response(&self) -> bool {
        matches!(self, Self::NoResponse)
    }

    #[must_use]
    pub const fn is_close(&self) -> bool {
        matches!(self, Self::Close)
    }
}

#[derive(Debug, Clone)]
pub struct BrokerAdvertise {
    pub host: String,
    pub port: i32,
}

impl Default for BrokerAdvertise {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: i32::from(DEFAULT_KAFKA_PORT),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ApiVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

/// The version firewall, and the exact set `ApiVersions` advertises.
///
/// Absence is load-bearing for the transaction keys (24, 25, 26, 28): a conforming client that
/// does not see a key here never sends it, which is the whole enforcement of "transactions are
/// unsupported". See `docs/SCOPE.md`.
static SUPPORTED_RANGES: &[ApiVersionRange] = &[
    produce::RANGE,
    fetch::RANGE,
    list_offsets::RANGE,
    metadata::RANGE,
    api_versions::RANGE,
    create_topics::RANGE,
    init_producer_id::RANGE,
    find_coordinator::RANGE,
    join_group::RANGE,
    heartbeat::RANGE,
    leave_group::RANGE,
    sync_group::RANGE,
];

#[must_use]
pub fn supported_api_ranges() -> &'static [ApiVersionRange] {
    SUPPORTED_RANGES
}

/// Everything a handler needs that outlives one request.
///
/// `bridge` is `None` until `IGGY_KAFKA_BRIDGE_ENABLED` turns it on. A handler that finds `None`
/// answers with its stub, so APIs can be wired one at a time.
///
/// Every Iggy call but a Fetch poll goes through one lockstep `IggyClient`, so Kafka connections
/// serialize behind whichever of those calls is in flight. The `Arc` does not change that. See the
/// README's "Concurrency ceiling".
pub struct GatewayState {
    pub broker: BrokerAdvertise,
    pub bridge: Option<Arc<IggyBridge>>,
    pub max_frame_size: usize,
    /// Whether `SaslHandshake` and `SaslAuthenticate` are advertised and routed. Kept on the
    /// shared state so `ApiVersions` can answer without a widened handler signature.
    pub sasl_enabled: bool,
    /// Shared across every connection this gateway serves: a producer id has to be unique for
    /// the process, not for the connection that asked for it.
    pub producer_ids: ProducerIdAllocator,
    /// Produce requests that decode and send at once. Caps their memory.
    pub(crate) produce_slots: Semaphore,
    /// Consumer group membership. Process-wide and independent of the bridge: a member outlives
    /// the connection that created it, and group coordination needs no Iggy call.
    pub groups: GroupCoordinator,
    /// Per Kafka topic and partition, how long probes read it as loading. Fetch and `ListOffsets`
    /// share it, since every consumer sees the same probe.
    pub(crate) loading: Mutex<fetch::Spells>,
    /// Topic probes that every Fetch shares.
    pub(crate) probe_board: ProbeBoard,
}

/// Each holds one decoded partition at a time, so about 160 MB at the default 8 MiB frame. Sends
/// run one at a time anyway.
const PRODUCE_SLOTS: usize = 4;

impl GatewayState {
    #[must_use]
    pub fn new(
        broker: BrokerAdvertise,
        bridge: Option<Arc<IggyBridge>>,
        max_frame_size: usize,
        sasl_enabled: bool,
        instance_id: u16,
        groups: GroupCoordinator,
    ) -> Self {
        Self {
            broker,
            bridge,
            max_frame_size,
            sasl_enabled,
            producer_ids: ProducerIdAllocator::new(instance_id),
            produce_slots: Semaphore::const_new(PRODUCE_SLOTS),
            groups,
            loading: Mutex::default(),
            probe_board: ProbeBoard::default(),
        }
    }

    /// State with no bridge, so every handler takes its stub path.
    ///
    /// The coordinator is real but fresh, so two calls never share group state.
    #[must_use]
    pub fn stub(broker: BrokerAdvertise, max_frame_size: usize) -> Self {
        Self::new(
            broker,
            None,
            max_frame_size,
            false,
            0,
            GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
        )
    }
}

/// What a handler keeps for one Kafka connection, across its requests.
///
/// Fetch keeps its -1 holds and the offsets it cannot place here. One consumer's stuck offset then
/// neither skips another consumer's read of it nor replaces the hold of a consumer stuck elsewhere
/// in the partition, and one consumer's wait for an offset never spends another's grace.
#[derive(Default)]
pub struct ConnectionState {
    /// Partitions that answered -1 to this connection's Fetches, per Kafka topic.
    pub(crate) stuck_offsets: Mutex<fetch::StuckOffsets>,
    /// Offsets this connection's Fetches cannot place, per Kafka topic.
    pub(crate) unplaced: Mutex<fetch::Unplaced>,
}

/// Default `max_frame_size` used by [`handle_request`] - the direct call sites across this
/// crate's test suite that don't care about the response-size guard specifically. Production
/// traffic goes through [`handle_request_bounded`] instead (see `server.rs`'s call site), with
/// the connection's actual configured `max_frame_size`.
const DEFAULT_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

/// Handles one decoded request frame and returns how the connection should proceed.
pub async fn handle_request(
    api_key: i16,
    api_version: i16,
    body: Bytes,
    broker: &BrokerAdvertise,
) -> HandleOutcome {
    let state = GatewayState::stub(broker.clone(), DEFAULT_MAX_FRAME_SIZE);
    handle_request_bounded(&state, api_key, api_version, body).await
}

/// Same as [`handle_request`], but rejects a request whose declared array/string lengths project
/// a response larger than `max_frame_size` before decoding it.
///
/// See [`crate::protocol::bounds_guard`]'s `MAX_REQUEST_ELEMENTS`/`RESPONSE_BYTES_PER_ELEMENT`
/// docs for the CPU/memory amplification this closes (a request within the old element budget
/// alone could still produce a multi-megabyte response from a single synchronous, non-yielding
/// call).
///
/// Each call is a new connection's first request, so nothing carries over to the next call.
/// [`handle_connection_request`] keeps a connection's state.
pub async fn handle_request_bounded(
    state: &GatewayState,
    api_key: i16,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    let connection = ConnectionState::default();
    handle_connection_request(state, &connection, api_key, api_version, body).await
}

/// [`handle_request_bounded`] for one request of `connection`.
pub async fn handle_connection_request(
    state: &GatewayState,
    connection: &ConnectionState,
    api_key: i16,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    dispatch(state, connection, api_key, api_version, body).await
}

#[must_use]
pub fn is_supported_version(api_key: i16, api_version: i16) -> bool {
    SUPPORTED_RANGES
        .iter()
        .find(|r| r.api_key == api_key)
        .is_some_and(|r| api_version >= r.min_version && api_version <= r.max_version)
}

/// Highest version this gateway accepts for `api_key`, from the single firewall table.
#[must_use]
pub fn supported_max_version(api_key: i16) -> Option<i16> {
    SUPPORTED_RANGES
        .iter()
        .find(|r| r.api_key == api_key)
        .map(|r| r.max_version)
}

/// Min version advertised in `ApiVersions` (may differ from the firewall min).
///
/// Produce must advertise min=0 per KAFKA-18659 / `PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION`
/// even though this gateway only accepts Produce v3+.
#[must_use]
pub const fn advertised_min_version(api_key: i16, firewall_min: i16) -> i16 {
    if api_key == API_KEY_PRODUCE {
        0
    } else {
        firewall_min
    }
}

/// Advertised only when SASL is switched on, and deliberately absent from [`SUPPORTED_RANGES`].
///
/// These two keys never reach [`handle_request_bounded`]: the connection loop routes them through
/// the SASL state machine before dispatch, whether SASL is on or off. With it off every connection
/// starts authenticated, so both keys are answered `ILLEGAL_SASL_STATE` and the connection stays
/// open, the answer a real broker gives on a PLAINTEXT listener. Keeping them out of the firewall
/// table means dispatch never serves them on its own.
///
/// `SaslHandshake` is pinned to v1 on both ends. v0 selects the headerless token framing (KIP-152)
/// that the frame reader cannot parse, so advertising it would invite exactly the shape this
/// gateway refuses.
static SASL_ADVERTISED_RANGES: &[ApiVersionRange] = &[
    ApiVersionRange {
        // Advertised from v0 even though only v1 is accepted, the same split
        // [`advertised_min_version`] already applies to Produce. librdkafka gates its whole
        // SASL-handshake feature on `SaslHandshake` v0 appearing in the advertisement
        // (`RD_KAFKA_FEATURE_SASL_HANDSHAKE` depends on key 17 at version 0), so advertising v1
        // alone makes it report "SASL Handshake not supported by broker" and give up before it
        // ever sends one. It then picks v1 anyway, because `SaslAuthenticate` being advertised is
        // what selects the KIP-152 framing. A client that really only knows v0 still gets
        // `UNSUPPORTED_VERSION` and a closed connection from the state machine.
        api_key: API_KEY_SASL_HANDSHAKE,
        min_version: 0,
        max_version: 1,
    },
    ApiVersionRange {
        api_key: API_KEY_SASL_AUTHENTICATE,
        min_version: 0,
        max_version: 2,
    },
    // Grouped with the SASL keys rather than with `SUPPORTED_RANGES` because it answers about the
    // authenticated principal. With SASL off there is no principal, so there is nothing it could
    // truthfully describe, and advertising it would invite a question with no answer.
    ApiVersionRange {
        api_key: API_KEY_DESCRIBE_ACLS,
        min_version: 1,
        max_version: 3,
    },
];

/// Sent with every `SASL_AUTHENTICATION_FAILED`, whatever the real cause.
///
/// Iggy's own login runs a dummy hash for an unknown user precisely so that "no such user" and
/// "wrong password" are indistinguishable from the outside. Naming the cause here would undo that
/// server-side care at the gateway. An unreachable Iggy reaches the client the same way, and the
/// gateway's own log is where the difference is recorded.
pub const SASL_AUTH_FAILED_MESSAGE: &str = "Authentication failed";

/// Decodes a `DescribeAcls` filter.
///
/// # Errors
///
/// Returns an error when the body is not a well-formed `DescribeAcls` request at `api_version`.
pub fn decode_acl_filter(api_version: i16, body: Bytes) -> Result<AclFilter> {
    let req = decode_guarded::<DescribeAclsRequest>(api_version, body, |v, b| {
        validate_describe_acls_shape(v, b)
    })?;
    Ok(AclFilter {
        resource_type: req.resource_type_filter,
        resource_name: req.resource_name_filter.map(|name| name.to_string()),
        pattern_type: req.pattern_type_filter,
        principal: req.principal_filter.map(|name| name.to_string()),
        host: req.host_filter.map(|name| name.to_string()),
        operation: req.operation,
        permission_type: req.permission_type,
    })
}

/// Answers `DescribeAcls` for `principal`, selecting from what Iggy already grants them.
#[must_use]
pub fn describe_acls_outcome(
    api_version: i16,
    principal: &str,
    permissions: &PrincipalPermissions,
    filter: &AclFilter,
) -> HandleOutcome {
    let selected: Vec<AclBinding> = acl::bindings_for(permissions)
        .into_iter()
        .filter(|binding| filter.matches(binding, principal))
        .collect();
    respond_or_close(
        encode_describe_acls_response(api_version, principal, &selected),
        "DescribeAcls",
    )
}

/// Versions of `DescribeAcls` this gateway answers, and the range `ApiVersions` advertises.
///
/// Public so the connection loop can enforce it. The key is deliberately absent from this module's
/// `SUPPORTED_RANGES` firewall table, which is what makes this the only bound there is.
pub const SASL_ADVERTISED_DESCRIBE_ACLS_VERSIONS: std::ops::RangeInclusive<i16> = 1..=3;

/// `DescribeAcls` answer carrying only an error code.
///
/// `close` marks the refusals that must end the connection. A caller that keeps it open is saying
/// the client may usefully send something else on it, which is true of a malformed filter and not
/// of a state violation.
#[must_use]
pub fn respond_describe_acls_error(
    api_version: i16,
    error_code: i16,
    close: bool,
) -> HandleOutcome {
    match encode_describe_acls_error_response(api_version, error_code) {
        Ok(body) if close => HandleOutcome::RespondThenClose(body),
        Ok(body) => HandleOutcome::Respond(body),
        Err(error) => {
            tracing::warn!(%error, "failed to encode DescribeAcls error; closing connection");
            HandleOutcome::Close
        }
    }
}

/// Reads the mechanism name out of a `SaslHandshake` body without consuming the caller's copy.
///
/// # Errors
///
/// Returns an error when the body is not a well-formed `SaslHandshake` request at `api_version`.
pub fn decode_sasl_mechanism(api_version: i16, body: Bytes) -> Result<String> {
    let req = decode_guarded::<SaslHandshakeRequest>(api_version, body, |v, b| {
        validate_sasl_handshake_shape(v, b)
    })?;
    Ok(req.mechanism.to_string())
}

/// Reads the opaque token out of a `SaslAuthenticate` body.
///
/// # Errors
///
/// Returns an error when the body is not a well-formed `SaslAuthenticate` request at
/// `api_version`, including when its token exceeds the guard's size cap.
pub fn decode_sasl_auth_bytes(api_version: i16, body: Bytes) -> Result<Bytes> {
    let req = decode_guarded::<SaslAuthenticateRequest>(api_version, body, |v, b| {
        validate_sasl_authenticate_shape(v, b)
    })?;
    Ok(req.auth_bytes)
}

/// `SaslHandshake` answer. `close` marks the refusal paths, where the response is written and the
/// connection then dropped.
#[must_use]
pub fn sasl_handshake_outcome(api_version: i16, error_code: i16, close: bool) -> HandleOutcome {
    // The mechanism list is what a client prints to tell its operator what to configure, so an
    // unsupported-mechanism refusal carries it. `ILLEGAL_SASL_STATE` does not: the request was out
    // of order, not mis-configured, and a real broker answers that one with an empty list. It also
    // means a gateway with SASL switched off stops telling an unauthenticated scanner that it
    // could speak PLAIN.
    let mechanisms: &[&str] = if error_code == ERROR_ILLEGAL_SASL_STATE {
        &[]
    } else {
        SaslMechanism::advertised()
    };
    let encoded = encode_sasl_handshake_response(api_version, error_code, mechanisms);
    finish_sasl(encoded, close, "SaslHandshake")
}

/// `SaslAuthenticate` answer. `close` marks the refusal paths.
#[must_use]
pub fn sasl_authenticate_outcome(api_version: i16, error_code: i16, close: bool) -> HandleOutcome {
    // Only a credential rejection carries the generic message. An `ILLEGAL_SASL_STATE` also
    // travels on this response, and calling a protocol-ordering mistake an authentication failure
    // would send the operator looking at credentials that were never presented.
    let message =
        (error_code == ERROR_SASL_AUTHENTICATION_FAILED).then_some(SASL_AUTH_FAILED_MESSAGE);
    let encoded = encode_sasl_authenticate_response(api_version, error_code, message);
    finish_sasl(encoded, close, "SaslAuthenticate")
}

fn finish_sasl(encoded: Result<Bytes>, close: bool, api_name: &str) -> HandleOutcome {
    match encoded {
        Ok(body) if close => HandleOutcome::RespondThenClose(body),
        Ok(body) => HandleOutcome::Respond(body),
        Err(error) => {
            tracing::warn!(%error, "failed to encode {api_name} response; closing connection");
            HandleOutcome::Close
        }
    }
}

/// The SASL keys, advertised only while the feature is on.
#[must_use]
pub fn sasl_advertised_ranges() -> &'static [ApiVersionRange] {
    SASL_ADVERTISED_RANGES
}

/// Builds a well-formed error response for `api_key`, shaped for `api_version`.
///
/// Used when a request is refused for a reason that belongs to the connection rather than the
/// request body, which today means `ILLEGAL_SASL_STATE` on an unauthenticated connection. A real
/// broker answers from the offending request's own schema so the client can parse the body at the
/// version it asked for, then drops the connection.
///
/// Two keys get [`HandleOutcome::Close`] with no body instead. Produce, because `acks=0` forbids a
/// response and the acks value is not knowable without decoding a body this connection has not
/// earned the right to have decoded. Metadata, because it carries no top-level error field at the
/// versions this gateway supports, so there is no well-formed place to put the code.
#[must_use]
pub fn encode_error_for_key(
    api_key: i16,
    api_version: i16,
    error_code: i16,
    sasl_enabled: bool,
) -> HandleOutcome {
    // The firewall applies here too, and it has to be checked explicitly. `kafka_protocol`'s
    // encoders accept a wider version range than `SUPPORTED_RANGES` does (Fetch to 18 against a
    // firewall max of 12, for instance), so encoding at the version the client asked for would
    // answer a pre-authentication Fetch v13 with a well-formed v13 body on the one path where the
    // caller has proven nothing, while the dispatch path closes on the same request.
    if !is_supported_version(api_key, api_version) {
        return HandleOutcome::Close;
    }
    let encoded = match api_key {
        API_KEY_FETCH => fetch::encode_error_response(api_version, error_code),
        API_KEY_LIST_OFFSETS => list_offsets::encode_error_response(api_version, error_code),
        API_KEY_CREATE_TOPICS => create_topics::encode_error_response(api_version, error_code),
        // Carries the live SASL setting, not a hardcoded `false`: answering an illegal-state
        // ApiVersions with a SASL-less key set contradicts the advertisement sent one frame
        // earlier on the same connection.
        API_KEY_API_VERSIONS => {
            api_versions::encode_response(api_version, error_code, sasl_enabled)
        }
        _ => return HandleOutcome::Close,
    };
    match encoded {
        Ok(body) => HandleOutcome::RespondThenClose(body),
        Err(error) => {
            tracing::debug!(%error, api_key, "no encodable error response; closing connection");
            HandleOutcome::Close
        }
    }
}
