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

use bytes::{Buf, Bytes};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, BrokerId, CreateTopicsRequest, FetchRequest,
    ListOffsetsRequest, MetadataRequest, MetadataResponse, ProduceRequest, TopicName,
};
use kafka_protocol::protocol::{Decodable, StrBytes};

use crate::bridge::topic_map::validate_kafka_topic_name;
use crate::bridge::{BridgeError, TopicCatalog};
use crate::error::{KafkaProtocolError, Result};
use crate::protocol::bounds_guard::{
    validate_api_versions_shape, validate_create_topics_shape, validate_fetch_shape,
    validate_list_offsets_shape, validate_metadata_shape, validate_produce_shape,
};
use crate::protocol::responses::{
    CreateTopicResult, ListOffsetsPartitionResult, encode_create_topics_error_response,
    encode_create_topics_response, encode_fetch_error_response, encode_fetch_response,
    encode_list_offsets_error_response, encode_list_offsets_response, encode_message,
    encode_produce_error_response, encode_produce_response, validate_create_topic_shape,
};

pub const API_KEY_PRODUCE: i16 = 0;
pub const API_KEY_FETCH: i16 = 1;
pub const API_KEY_LIST_OFFSETS: i16 = 2;
pub const API_KEY_METADATA: i16 = 3;
pub const API_KEY_API_VERSIONS: i16 = 18;
pub const API_KEY_CREATE_TOPICS: i16 = 19;

pub const DEFAULT_KAFKA_PORT: u16 = 9093;

/// Generic catch-all. Not sent by any stub response today; the `bridge` module's error mapping
/// uses it for an `IggyError` with no closer Kafka analogue.
pub const ERROR_UNKNOWN_SERVER_ERROR: i16 = -1;
pub const ERROR_NONE: i16 = 0;
pub const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// Retriable; Produce stub uses this until the Iggy bridge persists records.
pub const ERROR_NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// `bridge`'s mapping for `IggyError::TransientNotCommitted`: the request's outcome is genuinely
/// unknown (neither confirmed applied nor confirmed rejected).
///
/// Retriable in real Kafka too (`TimeoutException extends RetriableException`; the Java producer's
/// `Sender.canRetry` treats it the same as `NOT_LEADER_OR_FOLLOWER`) - this is not chosen to make
/// clients stop retrying. It is chosen because it is the code a real broker sends for the same
/// unknown-outcome shape (an ack that timed out with no confirmation either way), and Kafka has no
/// dedicated "outcome unknown, retry could duplicate" code. The duplicate-write risk on retry is
/// real regardless of which retriable code is sent; it closes only once `#3535` has an idempotent
/// produce path, not by picking a different error code here.
pub const ERROR_REQUEST_TIMED_OUT: i16 = 7;
/// `bridge`'s mapping for a Kafka-side topic name that fails Kafka's own naming rules.
///
/// Empty, whitespace-padded, over 249 bytes, or outside `[A-Za-z0-9._-]`, checked before any Iggy
/// call is made - a real Kafka client library validates topic names client-side and would never
/// send one of these, but a raw/non-conformant client could.
pub const ERROR_INVALID_TOPIC_EXCEPTION: i16 = 17;
/// Closest fit for an Iggy permission/credential rejection in `bridge`'s error mapping.
///
/// There is no bridge-side SASL exchange yet (`#3549`), so `SASL_AUTHENTICATION_FAILED` would
/// misstate the failure point. Not sent by any stub response today.
pub const ERROR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;
pub const ERROR_UNSUPPORTED_VERSION: i16 = 35;
/// `bridge`'s mapping for `BridgeError::PartitionCountMismatch`: the topic exists, just not with
/// the requested partition count.
///
/// Not [`ERROR_INVALID_PARTITIONS`] - `kafka-protocol`'s own error table (`error.rs`) defines that
/// code's text as "Number of partitions is below 1", which is a different condition (a client
/// asking for zero/negative partitions) than "this topic already exists with a different count".
pub const ERROR_TOPIC_ALREADY_EXISTS: i16 = 36;
pub const ERROR_INVALID_PARTITIONS: i16 = 37;
pub const ERROR_INVALID_REPLICATION_FACTOR: i16 = 38;
/// `CreateTopics`: an explicit `num_partitions` disagrees with a non-empty `assignments` list's
/// own length.
///
/// The wire carries no rule for which one wins, so neither is silently preferred over the other.
pub const ERROR_INVALID_REPLICA_ASSIGNMENT: i16 = 39;
pub const ERROR_INVALID_REQUEST: i16 = 42;
/// `CreateTopics`: a requested topic carried one or more per-topic Kafka configs.
///
/// None of `retention.ms`, `cleanup.policy`, etc. maps onto an Iggy topic option this bridge
/// applies, so every non-empty `configs` list is rejected outright rather than silently
/// dropping a subset an operator might believe took effect.
pub const ERROR_INVALID_CONFIG: i16 = 40;

/// Result of handling one Kafka request body.
#[derive(Debug)]
pub enum HandleOutcome {
    /// Write this response body (with a response header).
    Respond(Bytes),
    /// Produce with `acks=0`: write nothing, keep the connection open.
    NoResponse,
    /// No parseable response exists for this request; close the TCP connection.
    Close,
}

impl HandleOutcome {
    /// Return the response body, or panic with `msg` if the outcome is not [`Self::Respond`].
    ///
    /// # Panics
    ///
    /// Panics when the outcome is [`Self::NoResponse`] or [`Self::Close`].
    #[must_use]
    pub fn expect_response(self, msg: &str) -> Bytes {
        match self {
            Self::Respond(body) => body,
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

static SUPPORTED_RANGES: &[ApiVersionRange] = &[
    ApiVersionRange {
        api_key: API_KEY_PRODUCE,
        min_version: 3,
        max_version: 9,
    },
    ApiVersionRange {
        api_key: API_KEY_FETCH,
        min_version: 4,
        max_version: 12,
    },
    ApiVersionRange {
        api_key: API_KEY_LIST_OFFSETS,
        min_version: 1,
        max_version: 6,
    },
    ApiVersionRange {
        api_key: API_KEY_METADATA,
        min_version: 0,
        max_version: 9,
    },
    ApiVersionRange {
        api_key: API_KEY_API_VERSIONS,
        min_version: 0,
        max_version: 3,
    },
    ApiVersionRange {
        api_key: API_KEY_CREATE_TOPICS,
        min_version: 2,
        max_version: 5,
    },
];

#[must_use]
pub fn supported_api_ranges() -> &'static [ApiVersionRange] {
    SUPPORTED_RANGES
}

/// Default `max_frame_size` used by [`handle_request`] - the ~150 direct call sites across this
/// crate's test suite that don't care about the response-size guard specifically. Production
/// traffic goes through [`handle_request_bounded`] instead (see `server.rs`'s call site), with
/// the connection's actual configured `max_frame_size`.
const DEFAULT_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

/// Handles one decoded request frame and returns how the connection should proceed.
///
/// `bridge` is only actually called for Metadata, `CreateTopics` and `ListOffsets` today; every
/// other API key never touches it. It is still a required parameter (not an `Option`) rather
/// than plumbed in only where used: `handle_other_request`'s dispatch is one `match` on
/// `api_key`, and a caller that gets this wrong for one of the three bridge-backed keys would
/// fail at the call site with a type error, not silently at runtime with a missing bridge.
pub async fn handle_request(
    api_key: i16,
    api_version: i16,
    body: Bytes,
    broker: &BrokerAdvertise,
    bridge: &dyn TopicCatalog,
) -> HandleOutcome {
    handle_request_bounded(
        api_key,
        api_version,
        body,
        broker,
        DEFAULT_MAX_FRAME_SIZE,
        bridge,
    )
    .await
}

/// Same as [`handle_request`], but rejects a request whose declared array/string lengths project
/// a response larger than `max_frame_size` before decoding it.
///
/// See [`crate::protocol::bounds_guard`]'s `MAX_REQUEST_ELEMENTS`/`RESPONSE_BYTES_PER_ELEMENT`
/// docs for the CPU/memory amplification this closes (a request within the old element budget
/// alone could still produce a multi-megabyte response from a single synchronous, non-yielding
/// call).
pub async fn handle_request_bounded(
    api_key: i16,
    api_version: i16,
    body: Bytes,
    broker: &BrokerAdvertise,
    max_frame_size: usize,
    bridge: &dyn TopicCatalog,
) -> HandleOutcome {
    if api_key == API_KEY_PRODUCE {
        return handle_produce_request(api_version, body, max_frame_size);
    }
    handle_other_request(api_key, api_version, body, broker, max_frame_size, bridge).await
}

/// Decode `T` from the whole request body and reject unconsumed trailing bytes.
///
/// `kafka_protocol`'s `Decodable` stops once it has read the fields its schema defines; it does
/// not know (or care) whether the caller handed it an exact-length body, so the trailing-bytes
/// check has to live here.
/// A wire trace captured against a real `kcat -L` (librdkafka 2.14.2) showed a `Metadata` v9
/// request - null `topics` array, otherwise fully and correctly consumed by
/// `MetadataRequest::decode` - followed by one extra `0x00` byte past the schema's own end.
/// Tolerated as benign encoder padding up to this many trailing bytes, all of which must be
/// zero (see [`decode_exhaustive`]'s check) - generous above the one byte actually observed,
/// without being unbounded. Safe regardless of the bound chosen: `bounds_guard`'s
/// allocation-size checks run against the wire-declared array/string lengths *before*
/// `decode_exhaustive` is ever reached, so tolerating a few extra zero bytes here cannot change
/// what gets allocated - it only widens what counts as "fully consumed" after decoding already
/// succeeded.
const MAX_TOLERATED_TRAILING_PADDING_BYTES: usize = 8;

fn decode_exhaustive<T: Decodable>(version: i16, mut body: Bytes) -> Result<T> {
    let value =
        T::decode(&mut body, version).map_err(|e| KafkaProtocolError::Malformed(e.to_string()))?;
    if body.has_remaining() {
        let remaining = body.chunk();
        if remaining.len() > MAX_TOLERATED_TRAILING_PADDING_BYTES
            || remaining.iter().any(|&byte| byte != 0)
        {
            return Err(KafkaProtocolError::Malformed(
                "unexpected trailing bytes in request body".to_string(),
            ));
        }
        // debug!, not warn!: every other decode-failure log site in this module treats the
        // request body as attacker-controlled input, not an operator-actionable event - this is
        // the same call site, just a tolerated case of it rather than a rejected one.
        tracing::debug!(
            trailing_bytes = remaining.len(),
            "tolerated trailing zero-byte padding after a fully-decoded request body"
        );
    }
    Ok(value)
}

/// Runs `validate` (see [`crate::protocol::bounds_guard`]) before [`decode_exhaustive`].
///
/// `kafka_protocol` validates wire-declared array/string lengths as non-negative but never
/// against the bytes actually remaining in the frame, so a tiny frame declaring a huge count
/// drives an allocation attempt that aborts the whole process (`handle_alloc_error`, not a
/// panic - uncatchable, and it kills every connection, not just the offending one). `validate`
/// rejects that class of frame first, on a walk that never allocates a collection.
fn decode_guarded<T: Decodable>(
    version: i16,
    body: Bytes,
    validate: impl FnOnce(i16, &Bytes) -> Result<()>,
) -> Result<T> {
    validate(version, &body)?;
    decode_exhaustive(version, body)
}

/// Turn an encode [`Result`] into a [`HandleOutcome`], closing the connection when encoding
/// fails rather than propagating - there is no parseable response to send in that case.
fn respond_or_close(result: Result<Bytes>, api_name: &str) -> HandleOutcome {
    match result {
        Ok(body) => HandleOutcome::Respond(body),
        Err(error) => {
            tracing::warn!(%error, "failed to encode {api_name} response; closing connection");
            HandleOutcome::Close
        }
    }
}

/// Produce is the only request the wire protocol allows to go unanswered
/// (`acks=0`), so it gets its own path that may return [`HandleOutcome::NoResponse`].
///
/// The firewall check runs AFTER decoding the request, not before: `ApiVersions` advertises
/// Produce min=0 (see [`advertised_min_version`]) while the firewall's real floor is 3, so a
/// spec-compliant client can legitimately send Produce v0-2 with `acks=0`. Rejecting those
/// versions before reading `acks` would send an error response the client never expects,
/// desyncing the next correlation id it reads.
fn handle_produce_request(api_version: i16, body: Bytes, max_frame_size: usize) -> HandleOutcome {
    // Above the encoder max there is no response parseable at the client's version, so close
    // rather than decode (same policy the other APIs apply). Fail-closed on a missing row
    // (i16::MIN, not i16::MAX): `handle_request` dispatches Produce on a hard-coded `api_key ==`
    // check, not from this table, so if a future edit ever drops the Produce row to disable the
    // API, a fail-open default here would leave Produce v0-2 acks=0 silently accepted on an API
    // the operator believes is off - unwrap_or(i16::MAX) is not the sound default the sibling
    // lookup at `unsupported_version_response` uses (`map_or(0, ...)`, i.e. fail-closed).
    if api_version > supported_max_version(API_KEY_PRODUCE).unwrap_or(i16::MIN) {
        return HandleOutcome::Close;
    }
    // `kafka_protocol`'s ProduceRequest/ProduceResponse schemas only go back to v3, so v0-2
    // (still advertised as the min in ApiVersions per KAFKA-18659) can be neither decoded nor
    // encoded by the crate - there is no parseable response at these versions regardless of
    // body content. `acks` is always the first i16 on the wire there (`transactional_id` was
    // added in v3), so it's peeked by hand: acks=0 must keep the connection open per the wire
    // protocol's fire-and-forget rule even though no response can ever be encoded for it.
    if api_version < 3 {
        let acks = match body.get(0..2) {
            Some(&[hi, lo]) => Some(i16::from_be_bytes([hi, lo])),
            _ => None,
        };
        return match acks {
            Some(0) | None => HandleOutcome::NoResponse,
            Some(_) => unsupported_version_response(API_KEY_PRODUCE, api_version, |v| {
                encode_produce_error_response(v, ERROR_UNSUPPORTED_VERSION)
            }),
        };
    }
    match decode_guarded::<ProduceRequest>(api_version, body, |v, b| {
        validate_produce_shape(v, b, max_frame_size)
    }) {
        // acks=0 is fire-and-forget: the client isn't reading a response, so
        // sending one desyncs the next correlation id it expects.
        Ok(req) if req.acks == 0 => HandleOutcome::NoResponse,
        // `api_version` is always in `[3, supported_max]` here: the `< 3` case returned above,
        // and the `> max` case returned at the top of this function - so it is always within
        // `SUPPORTED_RANGES`' Produce row and an `is_supported_version` re-check can never fail.
        Ok(req) => respond_or_close(encode_produce_response(api_version, &req), "Produce"),
        Err(error) => {
            // `kafka_protocol` decodes the whole request in one shot; a failure anywhere gives
            // no partial-field access, so `acks` is unknowable here (unlike the pre-migration
            // field-by-field decoder, which could still know `acks` on a later-field failure).
            // Responding risks desyncing an acks=0 fire-and-forget client's correlation stream,
            // so every Produce decode failure now stays silent - a behavior change from the
            // hand-rolled decoder, which answered with INVALID_REQUEST when `acks` was known and
            // nonzero.
            // debug!, not warn!: the body is attacker-controlled, not operator-actionable, and
            // a client looping malformed bodies on one connection (never disconnected - this
            // arm returns NoResponse) has no rate limit here.
            tracing::debug!(%error, "failed to decode Produce request (no response)");
            HandleOutcome::NoResponse
        }
    }
}

async fn handle_other_request(
    api_key: i16,
    api_version: i16,
    body: Bytes,
    broker: &BrokerAdvertise,
    max_frame_size: usize,
    bridge: &dyn TopicCatalog,
) -> HandleOutcome {
    match api_key {
        API_KEY_API_VERSIONS => handle_api_versions(api_version, body),
        API_KEY_METADATA => {
            handle_metadata(api_version, body, broker, max_frame_size, bridge).await
        }
        API_KEY_FETCH => handle_versioned_request(
            API_KEY_FETCH,
            api_version,
            body,
            |v, b| {
                decode_guarded::<FetchRequest>(v, b, |v, b| {
                    validate_fetch_shape(v, b, max_frame_size)
                })
            },
            encode_fetch_response,
            encode_fetch_error_response,
            "Fetch",
        ),
        API_KEY_LIST_OFFSETS => {
            handle_list_offsets(api_version, body, max_frame_size, bridge).await
        }
        API_KEY_CREATE_TOPICS => {
            handle_create_topics(api_version, body, max_frame_size, bridge).await
        }
        // Unknown API key: no api-specific response schema exists, so any body we send is
        // misparsed by the client against the schema it expected. Close is unambiguous.
        _ => HandleOutcome::Close,
    }
}

fn handle_api_versions(api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_API_VERSIONS, api_version) {
        // KIP-511: reply with v0 when the requested version is not understood.
        return respond_or_close(
            encode_api_versions_response(0, ERROR_UNSUPPORTED_VERSION),
            "ApiVersions",
        );
    }
    match decode_guarded::<ApiVersionsRequest>(api_version, body, validate_api_versions_shape) {
        Ok(_) => respond_or_close(
            encode_api_versions_response(api_version, ERROR_NONE),
            "ApiVersions",
        ),
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable (see the same
            // note on the Produce decode-failure arm above).
            tracing::debug!(%error, "failed to decode ApiVersions request");
            respond_or_close(
                encode_api_versions_response(api_version, ERROR_INVALID_REQUEST),
                "ApiVersions",
            )
        }
    }
}

async fn handle_metadata(
    api_version: i16,
    body: Bytes,
    broker: &BrokerAdvertise,
    max_frame_size: usize,
    bridge: &dyn TopicCatalog,
) -> HandleOutcome {
    if !is_supported_version(API_KEY_METADATA, api_version) {
        // Clamping the response to the supported max leaves a body the client parses at its own
        // (unsupported) version, so UNSUPPORTED_VERSION never survives. Clients that skip
        // ApiVersions get a naked close instead.
        tracing::warn!(
            api_version,
            max_supported = supported_max_version(API_KEY_METADATA),
            "Metadata version unsupported; closing connection"
        );
        return HandleOutcome::Close;
    }
    let requested = match decode_metadata_topics(api_version, body, max_frame_size) {
        Ok(topics) => topics,
        Err(error) => {
            // Metadata has no top-level error field; a malformed body cannot carry
            // INVALID_REQUEST in a version-correct way for every client. Close.
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(
                %error,
                api_version,
                "Failed to decode Metadata request; closing connection"
            );
            return HandleOutcome::Close;
        }
    };

    // `None` (null array) means "all topics"; `Some(&[])` (explicit empty array) means "no
    // topics - brokers/cluster metadata only" (KIP-4's `describeCluster()` shape); `Some(names)`
    // means "look up exactly these" - the three must stay distinguishable (see
    // `decode_metadata_topics`'s own doc). Listing is one bridge call per stream this gateway's
    // topic mapping can resolve to; looking up N specific topics is N bridge calls, one per
    // name, since Metadata's per-topic error_code needs to distinguish "doesn't exist" from a
    // real bridge failure for each name independently.
    let resolved: Vec<(String, i16, Option<u32>)> = match requested {
        None => match bridge.list_kafka_topics().await {
            Ok(topics) => topics
                .into_iter()
                .map(|topic| (topic.kafka_topic, ERROR_NONE, Some(topic.partitions_count)))
                .collect(),
            Err(error) => {
                // Metadata has no top-level error field (same reason the decode-failure arm
                // above closes rather than encoding one): silently answering "zero topics
                // exist" here would be a lie, not a graceful degradation - the bridge failed to
                // answer, it did not confirm an empty catalog.
                tracing::warn!(%error, "failed to list Kafka topics from Iggy bridge; closing connection");
                return HandleOutcome::Close;
            }
        },
        Some(names) if names.is_empty() => Vec::new(),
        Some(names) => {
            let mut out = Vec::with_capacity(names.len());
            for name in &names {
                let kafka_topic = name.as_str();
                match bridge.get_kafka_topic(kafka_topic).await {
                    Ok(Some(topic)) => {
                        out.push((topic.kafka_topic, ERROR_NONE, Some(topic.partitions_count)));
                    }
                    Ok(None) => out.push((
                        kafka_topic.to_string(),
                        ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                        None,
                    )),
                    Err(error) => {
                        tracing::warn!(%error, kafka_topic, "failed to look up Kafka topic from Iggy bridge");
                        out.push((kafka_topic.to_string(), error.to_kafka_error_code(), None));
                    }
                }
            }
            out
        }
    };

    respond_or_close(
        encode_metadata_response(api_version, &resolved, broker),
        "Metadata",
    )
}

fn handle_versioned_request<T>(
    api_key: i16,
    api_version: i16,
    body: Bytes,
    decode: impl FnOnce(i16, Bytes) -> Result<T>,
    encode_ok: impl FnOnce(i16, &T) -> Result<Bytes>,
    encode_err: impl Fn(i16, i16) -> Result<Bytes>,
    api_name: &str,
) -> HandleOutcome {
    if is_supported_version(api_key, api_version) {
        match decode(api_version, body) {
            Ok(req) => respond_or_close(encode_ok(api_version, &req), api_name),
            Err(error) => {
                // debug!, not warn!: attacker-controlled, not operator-actionable.
                tracing::debug!(%error, "Failed to decode {api_name} request");
                respond_or_close(encode_err(api_version, ERROR_INVALID_REQUEST), api_name)
            }
        }
    } else {
        unsupported_version_response(api_key, api_version, |version| {
            encode_err(version, ERROR_UNSUPPORTED_VERSION)
        })
    }
}

/// `CreateTopics` (`#3538`): decodes the request, validates and (unless `validate_only`)
/// provisions each requested topic through `bridge.ensure_stream_and_topic`, and reports one
/// result per topic - never fails the whole response for one bad topic among several.
async fn handle_create_topics(
    api_version: i16,
    body: Bytes,
    max_frame_size: usize,
    bridge: &dyn TopicCatalog,
) -> HandleOutcome {
    if !is_supported_version(API_KEY_CREATE_TOPICS, api_version) {
        return unsupported_version_response(API_KEY_CREATE_TOPICS, api_version, |v| {
            encode_create_topics_error_response(v, ERROR_UNSUPPORTED_VERSION)
        });
    }
    let req = match decode_guarded::<CreateTopicsRequest>(api_version, body, |v, b| {
        validate_create_topics_shape(v, b, max_frame_size)
    }) {
        Ok(req) => req,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, "Failed to decode CreateTopics request");
            return respond_or_close(
                encode_create_topics_error_response(api_version, ERROR_INVALID_REQUEST),
                "CreateTopics",
            );
        }
    };

    let mut results: Vec<CreateTopicResult> = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        results.push(create_one_topic(topic, api_version, req.validate_only, bridge).await);
    }
    respond_or_close(
        encode_create_topics_response(&req.topics, &results, api_version),
        "CreateTopics",
    )
}

/// Validates and (unless `validate_only`) creates one requested topic.
///
/// Sequential across a multi-topic request, not concurrent: `IggyBridge` holds one lockstep
/// `IggyClient` (`bridge::iggy_bridge`'s own doc - "the connection is lockstep, one request in
/// flight at a time"), so concurrent calls here would only queue behind each other on the same
/// mutex anyway; sequential keeps each topic's failure independent and easy to reason about,
/// with no ordering surprise from a scheduler interleaving them differently across runs.
async fn create_one_topic(
    topic: &CreatableTopic,
    api_version: i16,
    validate_only: bool,
    bridge: &dyn TopicCatalog,
) -> CreateTopicResult {
    if !topic.configs.is_empty() {
        return Err(ERROR_INVALID_CONFIG);
    }
    // Checked here, not left to `ensure_stream_and_topic`'s own call: that call is skipped
    // entirely below for `validate_only`, and name validation is part of "can this topic be
    // created as specified," the exact thing `validate_only` promises to check - skipping it
    // would report success for a name (empty, padded, over Kafka's 249-byte cap) that the real
    // call always rejects.
    validate_kafka_topic_name("kafka_topic", topic.name.0.as_str())
        .map_err(|error| error.to_kafka_error_code())?;
    let partition_count = validate_create_topic_shape(topic, api_version)?;

    let kafka_topic = topic.name.0.as_str();
    // Checked before the `validate_only` branch, not after: "can this topic be created as
    // specified" includes "does it already exist" - real Kafka's own `validateOnly` still
    // flags an existing topic, it doesn't only check format. `get_kafka_topic` is read-only
    // (the same lookup `handle_metadata` uses), so running it here doesn't violate KIP-4's
    // "don't create anything" promise even when `validate_only` is set.
    //
    // `ensure_stream_and_topic`'s own contract is intentionally idempotent (get-or-create,
    // matching-spec re-call is Ok) - correct for an internal "make sure this exists" helper, but
    // `CreateTopics` is not an upsert: the real `AdminClient.createTopics` contract is
    // `TOPIC_ALREADY_EXISTS` (36) for a topic that's already there even when the requested spec
    // matches exactly. The common "genuinely new topic" path still goes straight through
    // `ensure_stream_and_topic`'s own create-race handling unchanged below; only "it's already
    // there" short-circuits before ever reaching it.
    match bridge.get_kafka_topic(kafka_topic).await {
        Ok(Some(_existing)) => return Err(ERROR_TOPIC_ALREADY_EXISTS),
        Ok(None) => {}
        Err(error) => return Err(error.to_kafka_error_code()),
    }

    if validate_only {
        // KIP-4: "check that the topics can be created as specified, but don't create
        // anything." Format and existence are already checked above; going further (does
        // ensure_topic's eventual PartitionCountMismatch also apply here) would mean calling
        // the bridge with create-on-miss semantics anyway, defeating the "don't create
        // anything" contract - so this reports success on format+existence alone rather than
        // fully simulating the real call.
        return Ok(partition_count);
    }

    bridge
        .ensure_stream_and_topic(kafka_topic, partition_count)
        .await
        .map(|()| partition_count)
        .map_err(|error| error.to_kafka_error_code())
}

/// `ListOffsets` (`#3537`): resolves `earliest`/`latest` timestamp sentinels (`-2`/`-1`) per
/// requested partition through `bridge.high_watermarks`, one bridge call per topic covering all
/// its requested partitions at once.
async fn handle_list_offsets(
    api_version: i16,
    body: Bytes,
    max_frame_size: usize,
    bridge: &dyn TopicCatalog,
) -> HandleOutcome {
    if !is_supported_version(API_KEY_LIST_OFFSETS, api_version) {
        return unsupported_version_response(API_KEY_LIST_OFFSETS, api_version, |v| {
            encode_list_offsets_error_response(v, ERROR_UNSUPPORTED_VERSION)
        });
    }
    let req = match decode_guarded::<ListOffsetsRequest>(api_version, body, |v, b| {
        validate_list_offsets_shape(v, b, max_frame_size)
    }) {
        Ok(req) => req,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, "Failed to decode ListOffsets request");
            return respond_or_close(
                encode_list_offsets_error_response(api_version, ERROR_INVALID_REQUEST),
                "ListOffsets",
            );
        }
    };

    let mut results: Vec<Vec<ListOffsetsPartitionResult>> = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        results.push(resolve_list_offsets_topic(topic, bridge).await);
    }
    respond_or_close(
        encode_list_offsets_response(api_version, &req, &results),
        "ListOffsets",
    )
}

/// Resolves every partition of one `ListOffsetsTopic`, in request order.
///
/// One `high_watermarks` call for the whole topic (covering every partition this topic's
/// request carries, valid or not), not one call per partition - matches
/// `IggyBridge::high_watermarks`'s own one-round-trip batching contract. A partition index that
/// cannot even be a real Iggy partition (negative) is filtered out before the bridge call rather
/// than sent - `IggyBridge` takes `partitions: &[u32]`, so a negative index has no wire
/// representation to send anyway - and reported as `UNKNOWN_TOPIC_OR_PARTITION` directly.
async fn resolve_list_offsets_topic(
    topic: &kafka_protocol::messages::list_offsets_request::ListOffsetsTopic,
    bridge: &dyn TopicCatalog,
) -> Vec<ListOffsetsPartitionResult> {
    let kafka_topic = topic.name.0.as_str();
    let valid_indices: Vec<u32> = topic
        .partitions
        .iter()
        .filter_map(|p| u32::try_from(p.partition_index).ok())
        .collect();

    let watermarks = match bridge.high_watermarks(kafka_topic, &valid_indices).await {
        Ok(watermarks) => watermarks,
        // Call-level failure (bad topic name, mapped stream doesn't exist, bridge timeout) -
        // every partition of this topic fails the same way, matching how a real broker answers
        // every partition of a topic it cannot see identically.
        Err(error) => {
            let error_code = error.to_kafka_error_code();
            return topic.partitions.iter().map(|_| Err(error_code)).collect();
        }
    };
    let watermarks: std::collections::HashMap<u32, std::result::Result<i64, BridgeError>> =
        watermarks.into_iter().collect();

    topic
        .partitions
        .iter()
        .map(|partition| {
            let Ok(index) = u32::try_from(partition.partition_index) else {
                return Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION);
            };
            match watermarks.get(&index) {
                // -2 (earliest): every topic this bridge creates has message_expiry left at
                // ServerDefault (never-expire - see IggyBridge::ensure_topic's own doc), and
                // nothing in this gateway ever trims a partition yet, so the earliest available
                // offset is always 0 for a topic this bridge actually manages. Real Kafka's
                // own `timestamp` field for an earliest/latest sentinel query is -1 regardless
                // (it only carries a real value for an actual timestamp-based lookup, which
                // this bridge does not support - see the `_ =>` arm below).
                Some(Ok(_watermark)) if partition.timestamp == -2 => Ok((-1, 0)),
                Some(Ok(watermark)) if partition.timestamp == -1 => Ok((-1, *watermark)),
                Some(Ok(_)) => Err(ERROR_INVALID_REQUEST),
                Some(Err(error)) => Err(error.to_kafka_error_code()),
                None => Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION),
            }
        })
        .collect()
}

/// Unsupported-version policy for APIs whose encoders only implement up to
/// [`ApiVersionRange::max_version`].
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
fn unsupported_version_response(
    api_key: i16,
    api_version: i16,
    encode: impl FnOnce(i16) -> Result<Bytes>,
) -> HandleOutcome {
    let max_version = SUPPORTED_RANGES
        .iter()
        .find(|r| r.api_key == api_key)
        .map_or(0, |r| r.max_version);
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

fn encode_api_versions_response(api_version: i16, error_code: i16) -> Result<Bytes> {
    let api_keys = SUPPORTED_RANGES
        .iter()
        .map(|r| {
            ApiVersion::default()
                .with_api_key(r.api_key)
                .with_min_version(advertised_min_version(r.api_key, r.min_version))
                .with_max_version(r.max_version)
        })
        .collect();
    let resp = ApiVersionsResponse::default()
        .with_error_code(error_code)
        .with_api_keys(api_keys);
    encode_message(&resp, api_version, 128)
}

/// `topics` is `(kafka_topic, error_code, partitions_count)` - `partitions_count` is `None`
/// exactly when `error_code != ERROR_NONE` (a failed lookup has no partitions to report), `Some`
/// otherwise. Single-broker gateway: every partition of every topic reports broker id 1 as its
/// leader, sole replica and sole in-sync replica - there is no second broker to be anything else.
fn encode_metadata_response(
    response_version: i16,
    topics: &[(String, i16, Option<u32>)],
    broker: &BrokerAdvertise,
) -> Result<Bytes> {
    let response_topics = topics
        .iter()
        .map(|(name, error_code, partitions_count)| {
            let partitions = partitions_count.map_or_else(Vec::new, |count| {
                (0..count)
                    .map(|index| {
                        MetadataResponsePartition::default()
                            .with_error_code(ERROR_NONE)
                            .with_partition_index(i32::try_from(index).unwrap_or(i32::MAX))
                            .with_leader_id(BrokerId(1))
                            .with_leader_epoch(0)
                            .with_replica_nodes(vec![BrokerId(1)])
                            .with_isr_nodes(vec![BrokerId(1)])
                            .with_offline_replicas(Vec::new())
                    })
                    .collect()
            });
            MetadataResponseTopic::default()
                .with_error_code(*error_code)
                .with_name(Some(TopicName(StrBytes::from_string(name.clone()))))
                .with_partitions(partitions)
        })
        .collect();

    let broker_entry = MetadataResponseBroker::default()
        .with_node_id(BrokerId(1))
        .with_host(StrBytes::from_string(broker.host.clone()))
        .with_port(broker.port);

    let resp = MetadataResponse::default()
        .with_brokers(vec![broker_entry])
        .with_controller_id(BrokerId(1))
        .with_topics(response_topics);

    encode_message(&resp, response_version, 256)
}

/// Decodes a Metadata request body so the response can echo topic names.
///
/// A null topics array (`-1` legacy / `varint=0` compact) means "all topics": `Ok(None)`. A
/// non-null array - including an explicitly *empty* one, which real Kafka clients (e.g.
/// `AdminClient.describeCluster()`, KIP-4) send specifically to ask for broker/cluster metadata
/// with no topic listing at all - decodes to `Ok(Some(names))`, `names` empty or not. The two
/// must stay distinguishable: collapsing both to "treat as all topics" (as an earlier version of
/// this function did) answers a `describeCluster()`-style request with the entire topic catalog
/// instead of the empty list it asked for.
///
/// A null per-topic `name` (v10+ allows topic-id-only lookups) has no name to echo, so it errors
/// rather than silently dropping the topic from the response.
fn decode_metadata_topics(
    api_version: i16,
    body: Bytes,
    max_frame_size: usize,
) -> Result<Option<Vec<StrBytes>>> {
    let req = decode_guarded::<MetadataRequest>(api_version, body, |v, b| {
        validate_metadata_shape(v, b, max_frame_size)
    })?;
    req.topics
        .map(|topics| {
            topics
                .into_iter()
                .map(|topic| {
                    topic
                        .name
                        .map(|name| name.0)
                        .ok_or(KafkaProtocolError::NullTopicName)
                })
                .collect()
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

    /// Every `ERROR_*` constant defined in this module, checked against `kafka-protocol`'s own
    /// `ResponseError` table - mirrors `bridge::error`'s identical self-check, which covers only
    /// the constants `IggyError` mappings emit. These are the rest: hand-typed integer literals
    /// used directly by `protocol::api`/`protocol::responses`, verified against nothing
    /// automated before this test (the earlier state of affairs Krishna's #4043 review already
    /// flagged once for the `bridge::error` half - "I checked all twelve constants... nothing in
    /// the crate holds them there" - restated here for the half that check didn't cover).
    #[test]
    fn every_protocol_error_code_matches_kafka_protocols_own_table() {
        use kafka_protocol::error::ResponseError;

        for (ours, theirs) in [
            (
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                ResponseError::UnknownTopicOrPartition,
            ),
            (
                ERROR_NOT_LEADER_OR_FOLLOWER,
                ResponseError::NotLeaderOrFollower,
            ),
            (ERROR_REQUEST_TIMED_OUT, ResponseError::RequestTimedOut),
            (
                ERROR_INVALID_TOPIC_EXCEPTION,
                ResponseError::InvalidTopicException,
            ),
            (
                ERROR_TOPIC_AUTHORIZATION_FAILED,
                ResponseError::TopicAuthorizationFailed,
            ),
            (ERROR_UNSUPPORTED_VERSION, ResponseError::UnsupportedVersion),
            (
                ERROR_TOPIC_ALREADY_EXISTS,
                ResponseError::TopicAlreadyExists,
            ),
            (ERROR_INVALID_PARTITIONS, ResponseError::InvalidPartitions),
            (
                ERROR_INVALID_REPLICATION_FACTOR,
                ResponseError::InvalidReplicationFactor,
            ),
            (
                ERROR_INVALID_REPLICA_ASSIGNMENT,
                ResponseError::InvalidReplicaAssignment,
            ),
            (ERROR_INVALID_CONFIG, ResponseError::InvalidConfig),
            (ERROR_INVALID_REQUEST, ResponseError::InvalidRequest),
        ] {
            assert_eq!(
                ours,
                theirs.code(),
                "{theirs:?} is {} in kafka-protocol, not {ours}",
                theirs.code()
            );
        }
    }

    #[test]
    fn decode_metadata_topics_legacy_null_topic_name_fails() {
        let body = Bytes::from_static(&[
            0x00, 0x00, 0x00, 0x01, // one topic
            0xff, 0xff, // null topic name
        ]);
        let err = decode_metadata_topics(0, body, TEST_MAX_FRAME_SIZE).unwrap_err();
        assert!(matches!(err, KafkaProtocolError::NullTopicName));
    }

    #[test]
    fn decode_metadata_topics_legacy_null_array_means_all_topics() {
        // -1 is the spec-defined "all topics" sentinel for the legacy i32 array count, not a
        // malformed request - must decode to None, distinct from an explicit empty array.
        let body = Bytes::from_static(&[0xff, 0xff, 0xff, 0xff]); // -1
        let topics = decode_metadata_topics(0, body, TEST_MAX_FRAME_SIZE).unwrap();
        assert_eq!(topics, None);
    }

    #[test]
    fn decode_metadata_topics_legacy_empty_array_means_no_topics_not_all_topics() {
        // 0, not -1: an explicit zero-length array - real Kafka clients (describeCluster())
        // send this to mean "brokers only, no topic listing," distinct from the null sentinel.
        let body = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00]); // 0 topics
        let topics = decode_metadata_topics(0, body, TEST_MAX_FRAME_SIZE).unwrap();
        assert_eq!(topics, Some(Vec::new()));
    }

    #[test]
    fn decode_metadata_topics_flexible_empty_array_means_no_topics_not_all_topics() {
        // Compact array: varint 1 = zero elements (not null, which would be varint 0).
        let body = Bytes::from_static(&[
            0x01, // topics: empty compact array, not null
            0x00, // allow_auto_topic_creation
            0x00, // include_cluster_authorized_operations
            0x00, // include_topic_authorized_operations
            0x00, // tagged fields
        ]);
        let topics = decode_metadata_topics(9, body, TEST_MAX_FRAME_SIZE).unwrap();
        assert_eq!(topics, Some(Vec::new()));
    }

    #[test]
    fn decode_metadata_topics_tolerates_librdkafkas_trailing_padding_byte() {
        // Captured verbatim from a real `kcat -L` (librdkafka 2.14.2) against this gateway: a
        // Metadata v9 request with topics=null, all three v8/v9 flags false, one empty tagged
        // field (id=0, len=0), then one extra 0x00 byte past what the schema consumes. Without
        // MAX_TOLERATED_TRAILING_PADDING_BYTES this rejects with "unexpected trailing bytes" and
        // the gateway closes the connection - reproduced against this exact payload before the
        // fix.
        let body = Bytes::from_static(&[
            0x00, // topics: null (all topics)
            0x00, // allow_auto_topic_creation
            0x00, // include_cluster_authorized_operations
            0x00, // include_topic_authorized_operations
            0x01, // tagged field count = 1
            0x00, // tag id = 0
            0x00, // tag data length = 0
            0x00, // trailing padding byte past the schema's end
        ]);
        let topics = decode_metadata_topics(9, body, TEST_MAX_FRAME_SIZE).unwrap();
        assert_eq!(topics, None, "topics=null must still mean all topics");
    }

    #[test]
    fn decode_metadata_topics_rejects_trailing_bytes_beyond_the_tolerated_padding() {
        let mut bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00]; // valid v9 body, no tagged fields
        bytes.extend(std::iter::repeat_n(
            0u8,
            MAX_TOLERATED_TRAILING_PADDING_BYTES + 1,
        ));
        let body = Bytes::from(bytes);
        assert!(decode_metadata_topics(9, body, TEST_MAX_FRAME_SIZE).is_err());
    }

    #[test]
    fn decode_metadata_topics_rejects_nonzero_trailing_bytes() {
        let body = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x01]); // valid + 1 nonzero
        assert!(decode_metadata_topics(9, body, TEST_MAX_FRAME_SIZE).is_err());
    }

    #[test]
    fn decode_metadata_topics_empty_body_is_malformed() {
        assert!(decode_metadata_topics(0, Bytes::new(), TEST_MAX_FRAME_SIZE).is_err());
    }

    #[test]
    fn decode_metadata_topics_flexible_truncated_after_topics_fails() {
        // topics = null (all topics) but missing allow_auto / auth flags / tagged fields.
        let body = Bytes::from_static(&[0x00]);
        assert!(decode_metadata_topics(9, body, TEST_MAX_FRAME_SIZE).is_err());
    }

    #[test]
    fn decode_api_versions_v3_requires_software_fields() {
        assert!(decode_exhaustive::<ApiVersionsRequest>(3, Bytes::new()).is_err());
    }

    #[test]
    fn decode_api_versions_v3_accepts_valid_body() {
        // Hand-encoded rather than round-tripped through `ApiVersionsRequest::encode`: encoding
        // is gated behind the crate's "client" feature, which this broker-only binary doesn't
        // enable.
        let body = Bytes::from_static(&[
            0x0a, b'i', b'g', b'g', b'y', b'-', b't', b'e', b's',
            b't', // compact string (len 9)
            0x06, b'0', b'.', b'1', b'.', b'0', // compact string (len 5)
            0x00, // empty tagged fields
        ]);
        decode_exhaustive::<ApiVersionsRequest>(3, body).unwrap();
    }
}
