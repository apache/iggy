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

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::{
    DescribeAclsRequest, SaslAuthenticateRequest, SaslHandshakeRequest,
};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::{OwnedMutexGuard, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::IggyBridge;
use crate::cluster_id::ClusterId;
use crate::error::Result;
use crate::group::{GroupCoordinator, GroupCoordinatorConfig, GroupMember, MAX_CLIENT_ID_BYTES};
use crate::protocol::acl::{
    self, AclBinding, AclFilter, PrincipalPermissions, encode_describe_acls_error_response,
    encode_describe_acls_response,
};
use crate::protocol::auto_topic_creation::{AutoTopicCreationConfig, AutoTopicCreator};
use crate::protocol::bounds_guard::{
    validate_describe_acls_shape, validate_sasl_authenticate_shape, validate_sasl_handshake_shape,
};
use crate::protocol::handlers::api_versions::Advertisement;
use crate::protocol::handlers::init_producer_id::ProducerIdAllocator;
use crate::protocol::handlers::{
    alter_configs, api_versions, create_partitions, create_topics, decode_guarded, delete_groups,
    delete_topics, describe_cluster, describe_configs, describe_groups, dispatch, fetch,
    find_coordinator, heartbeat, incremental_alter_configs, init_producer_id, join_group,
    leave_group, list_groups, list_offsets, metadata, offset_commit, offset_delete, offset_fetch,
    produce, respond_or_close, sync_group,
};
use crate::protocol::probe_board::ProbeBoard;
use crate::protocol::sasl::{
    SaslMechanism, encode_sasl_authenticate_response, encode_sasl_handshake_response,
};

pub use iggy_kafka_wire::api::*;

pub const DEFAULT_KAFKA_PORT: u16 = 9093;

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
    offset_commit::RANGE,
    offset_fetch::RANGE,
    api_versions::RANGE,
    create_topics::RANGE,
    delete_topics::RANGE,
    create_partitions::RANGE,
    describe_configs::RANGE,
    alter_configs::RANGE,
    incremental_alter_configs::RANGE,
    init_producer_id::RANGE,
    find_coordinator::RANGE,
    join_group::RANGE,
    heartbeat::RANGE,
    leave_group::RANGE,
    sync_group::RANGE,
    describe_groups::RANGE,
    list_groups::RANGE,
    delete_groups::RANGE,
    offset_delete::RANGE,
    describe_cluster::RANGE,
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
/// Produce sends on one Iggy client per producer. Metadata, `CreateTopics`, `DeleteTopics`,
/// `CreatePartitions`, `DescribeConfigs`, `AlterConfigs` and `IncrementalAlterConfigs` share one
/// lockstep `IggyClient`, so those calls run one at a time across connections. See the README's
/// "Concurrency ceiling".
pub struct GatewayState {
    pub bridge: Option<Arc<IggyBridge>>,
    pub max_frame_size: usize,
    /// Shared across every connection this gateway serves: a producer id has to be unique for
    /// the process, not for the connection that asked for it.
    pub producer_ids: ProducerIdAllocator,
    /// `PRODUCE_CAPS` partition caps, in frame bytes, that Produce may hold decoded at once. Caps
    /// its memory. A request takes what its largest partition may decode, and a send that
    /// outlives the request keeps it while it owns the decoded partition.
    pub(crate) produce_memory: Arc<Semaphore>,
    /// `PRODUCE_LARGE` slots a large Produce request takes before its memory. Small ones skip it.
    pub(crate) produce_large: Arc<Semaphore>,
    /// Consumer group membership. Process-wide and independent of the bridge: a member outlives
    /// the connection that created it, and group coordination needs no Iggy call.
    pub groups: GroupCoordinator,
    /// Per Kafka topic and partition, how long probes read it as loading. Fetch and `ListOffsets`
    /// share it, since every consumer sees the same probe.
    pub(crate) loading: Mutex<fetch::Spells>,
    /// Per Kafka topic and partition, how long `ListOffsets` lookups by time poll empty without
    /// proof that no record matches. Only the first of a spell warns.
    pub(crate) unproven_empty: Mutex<fetch::Spells>,
    /// Topic probes that every Fetch shares.
    pub(crate) probe_board: ProbeBoard,
    /// Held by `CreatePartitions` from reading a topic's partition count until its write is
    /// settled, and by `CreateTopics` and `DeleteTopics` around their writes. Iggy adds a number
    /// of partitions, Kafka asks for a count to reach, and two requests that read the same count
    /// would otherwise both add and overshoot it. Per process: other gateways and direct Iggy
    /// clients are not behind it.
    pub(crate) partition_growth: Arc<tokio::sync::Mutex<()>>,
    /// What Metadata and `DescribeCluster` report. Random unless [`Self::with_cluster_id`] sets
    /// the one stored in Iggy.
    pub cluster_id: ClusterId,
    /// Present only while `IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED` is on.
    pub(crate) auto_topic_creation: Option<AutoTopicCreator>,
}

/// Large Produce requests that decode and send at once. More at once only contend for the
/// gateway's workers and for Iggy: at full load they cost CPU and gave no throughput.
const PRODUCE_LARGE: usize = 4;

/// Partition caps Produce may hold decoded at once, about 200 MB at the default 8 MiB frame.
///
/// Counted in bytes, not requests, so a small request takes a small share. One cap more than the
/// large slots: large requests can hold a cap each, and the small ones still have one.
const PRODUCE_CAPS: usize = PRODUCE_LARGE + 1;

impl GatewayState {
    #[must_use]
    pub fn new(
        bridge: Option<Arc<IggyBridge>>,
        max_frame_size: usize,
        instance_id: u16,
        groups: GroupCoordinator,
    ) -> Self {
        Self {
            bridge,
            max_frame_size,
            producer_ids: ProducerIdAllocator::new(instance_id),
            produce_memory: Arc::new(Semaphore::new(
                PRODUCE_CAPS
                    .saturating_mul(max_frame_size)
                    .min(Semaphore::MAX_PERMITS),
            )),
            produce_large: Arc::new(Semaphore::new(PRODUCE_LARGE)),
            groups,
            loading: Mutex::default(),
            unproven_empty: Mutex::default(),
            probe_board: ProbeBoard::default(),
            partition_growth: Arc::default(),
            cluster_id: ClusterId::random(),
            auto_topic_creation: None,
        }
    }

    /// Turns on Kafka's `auto.create.topics.enable` behaviour for `Metadata` when `config` says
    /// so.
    ///
    /// # Errors
    ///
    /// [`KafkaProtocolError::InvalidConfig`](crate::error::KafkaProtocolError::InvalidConfig)
    /// when creation is on and `config.partitions` is 0 or over Iggy's per-topic cap.
    pub fn with_auto_topic_creation(mut self, config: AutoTopicCreationConfig) -> Result<Self> {
        self.auto_topic_creation = if config.enabled {
            Some(AutoTopicCreator::new(config.partitions)?)
        } else {
            None
        };
        Ok(self)
    }

    /// Cancels the auto topic creations still running and waits up to `deadline` for them to
    /// end. An embedder that builds its own `GatewayState` calls it before closing the bridge.
    pub async fn shutdown_auto_topic_creation(&self, deadline: Duration) {
        let Some(creator) = &self.auto_topic_creation else {
            return;
        };
        if creator.shutdown(deadline).await
            && let Some(bridge) = &self.bridge
        {
            // A cancelled create can keep its client busy, and the bridge's sign-out would queue
            // behind it.
            bridge.discard_auto_create_client();
        }
    }

    /// Holds back the writes of `CreateTopics`, `DeleteTopics` and `CreatePartitions` on this
    /// gateway until the guard drops, the way a `CreatePartitions` settling an unknown outcome
    /// does. For tests that check those handlers wait for it.
    #[doc(hidden)]
    pub async fn hold_topic_changes(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.partition_growth).lock_owned().await
    }

    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: ClusterId) -> Self {
        self.cluster_id = cluster_id;
        self
    }

    /// State with no bridge, so every handler takes its stub path.
    ///
    /// The coordinator is real but fresh, so two calls never share group state.
    #[must_use]
    pub fn stub(max_frame_size: usize) -> Self {
        Self::new(
            None,
            max_frame_size,
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
///
/// The listener the connection arrived through lives here too, because a gateway with several
/// listeners answers Metadata and `ApiVersions` differently on each: a client is pointed back at
/// the port it used, and the SASL keys are advertised only where the exchange is run.
#[derive(Default)]
pub struct ConnectionState {
    /// The broker address this connection's listener advertises.
    pub broker: BrokerAdvertise,
    /// Whether `SaslHandshake` and `SaslAuthenticate` are advertised and routed on this
    /// connection's listener.
    pub sasl_enabled: bool,
    /// Whether `DescribeAcls` is advertised and answered: the listener names a principal, by
    /// SASL or by a verified client certificate.
    pub acls_enabled: bool,
    /// Partitions that answered -1 to this connection's Fetches, per Kafka topic.
    pub(crate) stuck_offsets: Mutex<fetch::StuckOffsets>,
    /// Offsets this connection's Fetches cannot place, per Kafka topic.
    pub(crate) unplaced: Mutex<fetch::Unplaced>,
    /// The member that last sent a Heartbeat here. An offset call caps its hold by that member's
    /// session, since its next heartbeat waits behind the call.
    heartbeat_member: Mutex<Option<GroupMember>>,
    /// The first `client_id` that a request header on this connection named, cut to
    /// [`MAX_CLIENT_ID_BYTES`]. A Kafka client sends one id in every header of a connection.
    client_id: OnceLock<StrBytes>,
    /// The peer's IP address as Kafka reports a member's `client_host`: a `/`, then the address.
    client_host: StrBytes,
    /// Keys the Produce session of a producer without a producer id that this gateway gave out.
    id: ConnectionId,
}

/// A Kafka connection's number, unique in the process.
#[derive(Debug, Clone, Copy)]
struct ConnectionId(u64);

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(0);

impl Default for ConnectionId {
    fn default() -> Self {
        Self(NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl ConnectionState {
    /// State for a connection accepted on the listener that advertises `broker`.
    ///
    /// `acls_enabled` is at least `sasl_enabled`: a SASL listener always names a principal.
    #[must_use]
    pub fn for_listener(broker: BrokerAdvertise, sasl_enabled: bool, acls_enabled: bool) -> Self {
        Self {
            broker,
            sasl_enabled,
            acls_enabled: acls_enabled || sasl_enabled,
            ..Self::default()
        }
    }

    /// The same state, for a connection from `peer`. An IPv4 client of a dual-stack listener is
    /// reported by its IPv4 address, as Kafka does.
    #[must_use]
    pub fn with_peer(mut self, peer: SocketAddr) -> Self {
        self.client_host = StrBytes::from_string(format!("/{}", peer.ip().to_canonical()));
        self
    }

    /// Keeps `client_id` if no earlier header named one. The copy is owned: the header is a view
    /// into its frame, and a group member keeps the id for as long as it stays in the group.
    pub fn note_client_id(&self, client_id: Option<&str>) {
        if let Some(client_id) = client_id.filter(|client_id| !client_id.is_empty()) {
            self.client_id.get_or_init(|| {
                let end = client_id.floor_char_boundary(MAX_CLIENT_ID_BYTES);
                StrBytes::from_string(client_id[..end].to_owned())
            });
        }
    }

    pub(crate) fn client_id(&self) -> StrBytes {
        self.client_id.get().cloned().unwrap_or_default()
    }

    pub(crate) fn client_host(&self) -> StrBytes {
        self.client_host.clone()
    }

    /// What `ApiVersions` tells this connection about the listener-dependent keys.
    #[must_use]
    pub const fn advertisement(&self) -> Advertisement {
        Advertisement {
            sasl: self.sasl_enabled,
            acls: self.acls_enabled,
        }
    }

    pub(crate) const fn id(&self) -> u64 {
        self.id.0
    }

    pub(crate) fn note_heartbeat(&self, member: GroupMember) {
        *self
            .heartbeat_member
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(member);
    }

    pub(crate) fn heartbeat_member(&self) -> Option<GroupMember> {
        self.heartbeat_member
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
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
    let state = GatewayState::stub(DEFAULT_MAX_FRAME_SIZE);
    let connection = ConnectionState::for_listener(broker.clone(), false, false);
    handle_connection_request(&state, &connection, None, api_key, api_version, body).await
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
    handle_connection_request(state, &connection, None, api_key, api_version, body).await
}

/// [`handle_request_bounded`] for one request of `connection`.
///
/// `principal` is `None` when no listener of the gateway names principals, or for a key the
/// connection reached before authenticating. Only `crate::server::route_frame` ever has a
/// principal to pass.
pub async fn handle_connection_request(
    state: &GatewayState,
    connection: &ConnectionState,
    principal: Option<&AuthenticatedPrincipal>,
    api_key: i16,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    dispatch(state, connection, principal, api_key, api_version, body).await
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
];

/// Versions of `DescribeAcls` this gateway answers.
///
/// Public so the connection loop can enforce it. The key is deliberately absent from this module's
/// `SUPPORTED_RANGES` firewall table, which is what makes this the only bound there is, and the
/// advertised range below is derived from it so the two cannot drift apart.
pub const SASL_ADVERTISED_DESCRIBE_ACLS_VERSIONS: std::ops::RangeInclusive<i16> = 1..=3;

/// Advertised only on a listener that names a principal, and absent from [`SUPPORTED_RANGES`]
/// for the same reason the SASL keys are: it answers about the authenticated principal, which
/// only the connection knows. On a plaintext listener with SASL off there is none, so there is
/// nothing it could truthfully describe, and advertising it would invite a question with no
/// answer. An `SSL` listener that verifies client certificates names one too, so it is advertised
/// there without the SASL keys.
static ACL_ADVERTISED_RANGES: &[ApiVersionRange] = &[ApiVersionRange {
    api_key: API_KEY_DESCRIBE_ACLS,
    min_version: *SASL_ADVERTISED_DESCRIBE_ACLS_VERSIONS.start(),
    max_version: *SASL_ADVERTISED_DESCRIBE_ACLS_VERSIONS.end(),
}];

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

fn finish_sasl(
    encoded: iggy_kafka_wire::Result<Bytes>,
    close: bool,
    api_name: &str,
) -> HandleOutcome {
    match encoded {
        Ok(body) if close => HandleOutcome::RespondThenClose(body),
        Ok(body) => HandleOutcome::Respond(body),
        Err(error) => {
            tracing::warn!(%error, "failed to encode {api_name} response; closing connection");
            HandleOutcome::Close
        }
    }
}

/// The SASL keys, advertised only on a listener that runs the exchange.
#[must_use]
pub fn sasl_advertised_ranges() -> &'static [ApiVersionRange] {
    SASL_ADVERTISED_RANGES
}

/// The ACL keys, advertised only on a listener that names a principal.
#[must_use]
pub fn acl_advertised_ranges() -> &'static [ApiVersionRange] {
    ACL_ADVERTISED_RANGES
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
    advertisement: Advertisement,
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
        API_KEY_DESCRIBE_CONFIGS => {
            describe_configs::encode_error_response(api_version, error_code)
        }
        API_KEY_ALTER_CONFIGS => alter_configs::encode_error_response(api_version, error_code),
        API_KEY_INCREMENTAL_ALTER_CONFIGS => {
            incremental_alter_configs::encode_error_response(api_version, error_code)
        }
        API_KEY_DESCRIBE_CLUSTER => {
            describe_cluster::encode_error_response(api_version, error_code)
        }
        // Carries the listener's live advertisement, not a hardcoded `NONE`: answering an
        // illegal-state ApiVersions with a SASL-less key set contradicts the advertisement sent
        // one frame earlier on the same connection.
        API_KEY_API_VERSIONS => {
            api_versions::encode_response(api_version, error_code, advertisement)
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

#[cfg(test)]
mod tests {
    use iggy_kafka_wire::bounds_guard::guarded_range;

    use super::*;

    /// A version this gateway advertises but the guard does not walk would reach the decoder
    /// unchecked, so the two tables must never drift apart.
    #[test]
    fn every_advertised_version_is_guarded() {
        let advertised = supported_api_ranges()
            .iter()
            .chain(sasl_advertised_ranges())
            .chain(acl_advertised_ranges());
        for range in advertised {
            let guarded = guarded_range(range.api_key)
                .unwrap_or_else(|| panic!("api key {} has no guarded range", range.api_key));
            assert!(
                guarded.min_version <= range.min_version
                    && range.max_version <= guarded.max_version,
                "api key {}: advertised {}..={} exceeds guarded {}..={}",
                range.api_key,
                range.min_version,
                range.max_version,
                guarded.min_version,
                guarded.max_version
            );
        }
    }

    #[test]
    fn given_request_headers_should_keep_the_first_client_id_that_one_names() {
        let connection = ConnectionState::default();
        connection.note_client_id(None);
        connection.note_client_id(Some(""));
        assert_eq!(connection.client_id().as_str(), "");

        connection.note_client_id(Some("consumer-g-1"));
        connection.note_client_id(Some("consumer-g-2"));
        assert_eq!(connection.client_id().as_str(), "consumer-g-1");
    }

    /// A 2-byte character that crosses the cap is left out whole, so the id stays UTF-8.
    #[test]
    fn given_a_client_id_past_the_cap_should_keep_its_start() {
        let connection = ConnectionState::default();
        let client_id = format!("{}é", "c".repeat(MAX_CLIENT_ID_BYTES - 1));
        connection.note_client_id(Some(&client_id));
        assert_eq!(
            connection.client_id().as_str(),
            &client_id[..MAX_CLIENT_ID_BYTES - 1]
        );
    }

    /// Kafka shows a member's host as `/` and the address, without the port.
    #[test]
    fn given_a_peer_should_report_its_address_as_kafka_does() {
        let peer: SocketAddr = "192.0.2.10:51234".parse().unwrap();
        assert_eq!(
            ConnectionState::default()
                .with_peer(peer)
                .client_host()
                .as_str(),
            "/192.0.2.10"
        );
        let peer: SocketAddr = "[::ffff:192.0.2.10]:51234".parse().unwrap();
        assert_eq!(
            ConnectionState::default()
                .with_peer(peer)
                .client_host()
                .as_str(),
            "/192.0.2.10"
        );
        let peer: SocketAddr = "[2001:db8::1]:51234".parse().unwrap();
        assert_eq!(
            ConnectionState::default()
                .with_peer(peer)
                .client_host()
                .as_str(),
            "/2001:db8::1"
        );
    }
}
