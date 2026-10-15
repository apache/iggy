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

//! Produce (API key 0).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::{IggyError, IggyMessage, Sizeable};
use kafka_protocol::messages::produce_request::PartitionProduceData;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::{AcquireError, OwnedSemaphorePermit};

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::{BridgeError, IggyBridge, ProduceTurn, Producer, TopicTarget};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_PRODUCE, ApiVersionRange, ConnectionState, ERROR_INVALID_RECORD,
    ERROR_INVALID_REQUIRED_ACKS, ERROR_MESSAGE_TOO_LARGE, ERROR_NONE, ERROR_NOT_LEADER_OR_FOLLOWER,
    ERROR_REQUEST_TIMED_OUT, ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
    ERROR_UNSUPPORTED_COMPRESSION_TYPE, ERROR_UNSUPPORTED_VERSION, GatewayState, HandleOutcome,
    REQUEST_DEADLINE, UNKNOWN_OFFSET, supported_max_version,
};
use crate::protocol::bounds_guard::validate_produce_shape;
use crate::protocol::handlers::{
    CODEC_OFF_WORKER_BYTES, decode_guarded, encode_message, is_transactional, off_worker,
    respond_or_close, unsupported_version_response,
};
use crate::protocol::probe_board::ProbeBoard;
use crate::records::{
    Allowance, DecompressionBudget, RecordCodecError, TimestampWindow, Zstd, decode_batch,
    is_compressed, producer_id, record_slots, to_iggy,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_PRODUCE,
    min_version: 3,
    max_version: 9,
};

/// The three `acks` values the protocol defines. Anything else is a malformed request.
const ACKS_NONE: i16 = 0;
const ACKS_LEADER: i16 = 1;
const ACKS_ALL: i16 = -1;

/// First Produce version that may carry zstd batches.
const ZSTD_MIN_VERSION: i16 = 7;

/// Frame bytes per record slot. Caps one partition at 131,072 slots, about 40 MB, at the default
/// 8 MiB frame.
///
/// `GatewayState` caps how much of that requests hold at once, at the same rate.
const FRAME_BYTES_PER_RECORD: usize = 64;

/// Partition allowances one request may decode, refused partitions included.
///
/// 64 MiB at the default 8 MiB frame, so a 1 MB request can compress 64x before the rest of it
/// answers 6.
const REQUEST_ALLOWANCES: usize = 8;

const RESPONSE_BASE_BYTES: usize = 512;

/// Produce is the only request the wire protocol allows to go unanswered
/// (`acks=0`), so it gets its own path that may return [`HandleOutcome::NoResponse`].
///
/// The firewall check runs AFTER decoding the request, not before: `ApiVersions` advertises
/// Produce min=0 (see `api::advertised_min_version`) while the firewall's real floor is 3, so a
/// spec-compliant client can legitimately send Produce v0-2 with `acks=0`. Rejecting those
/// versions before reading `acks` would send an error response the client never expects,
/// desyncing the next correlation id it reads.
pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    // Above the encoder max there is no response parseable at the client's version, so close
    // rather than decode (same policy the other APIs apply). Fail-closed on a missing row
    // (i16::MIN, not i16::MAX): dispatch routes Produce on its api key, not from this table, so
    // if a future edit ever drops the Produce row to disable the API, a fail-open default here
    // would leave Produce v0-2 acks=0 silently accepted on an API the operator believes is off.
    if api_version > supported_max_version(API_KEY_PRODUCE).unwrap_or(i16::MIN) {
        return HandleOutcome::Close;
    }
    // `kafka_protocol`'s ProduceRequest/ProduceResponse schemas only go back to v3, so v0-2
    // (still advertised as the min in ApiVersions per KAFKA-18659) can be neither decoded nor
    // encoded by the crate - there is no parseable response at these versions regardless of
    // body content. `acks` is always the first i16 on the wire there (`transactional_id` was
    // added in v3), so it's peeked by hand.
    if api_version < 3 {
        let acks = match body.get(0..2) {
            Some(&[hi, lo]) => Some(i16::from_be_bytes([hi, lo])),
            _ => None,
        };
        return match acks {
            // No bridge: nothing is stored at any version, so keep the connection.
            Some(ACKS_NONE) | None if state.bridge.is_none() => HandleOutcome::NoResponse,
            // With a bridge, v3+ stores records. Close so the client sees these were not.
            Some(ACKS_NONE) | None => HandleOutcome::Close,
            Some(_) => unsupported_version_response(API_KEY_PRODUCE, api_version, |v| {
                encode_error_response(v, ERROR_UNSUPPORTED_VERSION)
            }),
        };
    }
    let request = match decode_guarded::<ProduceRequest>(api_version, body, |v, b| {
        validate_produce_shape(v, b, state.max_frame_size)
    }) {
        Ok(request) => request,
        Err(error) => {
            // `acks` is unknown, so no reply is safe. Close, as Kafka does.
            tracing::debug!(%error, "failed to decode Produce request, closing");
            return HandleOutcome::Close;
        }
    };

    // Ahead of the bridge check, so neither the stub nor the write path answers a transactional
    // request as ordinary records: nothing here tracks a last stable offset or writes an abort
    // marker, so an aborted transaction's records would reach every consumer. 35 is fatal for the
    // producer. acks=0 has no response to carry it, so the refusal is a close, which is what a
    // Kafka broker does on an acks=0 produce error.
    if is_transactional(request.transactional_id.as_ref()) {
        return if request.acks == ACKS_NONE {
            HandleOutcome::Close
        } else {
            respond_or_close(
                encode_uniform_response(api_version, &request, ERROR_UNSUPPORTED_VERSION),
                "Produce",
            )
        };
    }

    let Some(bridge) = state.bridge.as_deref() else {
        return stub_outcome(api_version, &request);
    };

    // `acks=0` is legal, so a request failing this check is one the client waits on.
    if !matches!(request.acks, ACKS_NONE | ACKS_LEADER | ACKS_ALL) {
        tracing::debug!(
            acks = request.acks,
            "Produce request names an unknown acks value"
        );
        return respond_or_close(
            encode_uniform_response(api_version, &request, ERROR_INVALID_REQUIRED_ACKS),
            "Produce",
        );
    }

    // Before planning, not after: the client waits through decompression too.
    let deadline = Deadline::new(request.timeout_ms);
    // The turn before the memory: a request that waits behind its producer's stuck send then
    // holds none, and no large slot.
    let producer = producer_of(state, connection, principal, &request);
    let Some(turn) = bridge.produce_turn(producer, deadline.at).await else {
        tracing::debug!("Produce deadline passed while waiting for the producer's earlier send");
        return timed_out(api_version, &request);
    };
    let cost = decode_cost(&request, state.max_frame_size);
    let Ok(Ok(admission)) = tokio::time::timeout_at(deadline.at, admit(state, cost)).await else {
        tracing::debug!("Produce deadline passed while waiting to decode");
        return timed_out(api_version, &request);
    };
    let partition = Allowance {
        bytes: state.max_frame_size,
        records: state.max_frame_size / FRAME_BYTES_PER_RECORD,
    };
    let budget = DecompressionBudget::new(partition, partition.times(REQUEST_ALLOWANCES));
    let zstd = if api_version >= ZSTD_MIN_VERSION {
        Zstd::Allowed
    } else {
        Zstd::Refused
    };
    let (board, held) = (&state.probe_board, (turn, admission));
    let responses = write_request(bridge, held, board, &request, budget, zstd, deadline).await;

    // Kafka closes an `acks=0` connection on any error, so the client refreshes metadata.
    if request.acks == ACKS_NONE {
        let failed = responses
            .iter()
            .flat_map(|topic| &topic.partition_responses)
            .any(|partition| partition.error_code != ERROR_NONE);
        return if failed {
            HandleOutcome::Close
        } else {
            HandleOutcome::NoResponse
        };
    }
    respond_or_close(encode_written_response(api_version, responses), "Produce")
}

/// No bridge: answer retriable so clients keep their data instead of trusting a discarded write.
fn stub_outcome(api_version: i16, request: &ProduceRequest) -> HandleOutcome {
    // acks=0 is fire-and-forget: the client isn't reading a response, so
    // sending one desyncs the next correlation id it expects.
    if request.acks == ACKS_NONE {
        return HandleOutcome::NoResponse;
    }
    respond_or_close(encode_response(api_version, request), "Produce")
}

/// Answers 7 for every partition. `acks=0` has no response to carry it, so that closes instead.
fn timed_out(api_version: i16, request: &ProduceRequest) -> HandleOutcome {
    if request.acks == ACKS_NONE {
        HandleOutcome::Close
    } else {
        respond_or_close(
            encode_uniform_response(api_version, request, ERROR_REQUEST_TIMED_OUT),
            "Produce",
        )
    }
}

/// What the request may hold decoded, in frame bytes, at most one partition cap.
///
/// Partitions decode one at a time, so the largest counts. Uncompressed, a partition holds its own
/// bytes or its record slots, whichever is more. Compressed, it may fill the cap.
fn decode_cost(request: &ProduceRequest, max_frame_size: usize) -> usize {
    let blobs = || {
        request
            .topic_data
            .iter()
            .flat_map(|topic| &topic.partition_data)
            .filter_map(|partition| partition.records.as_deref())
    };
    let cap_slots = max_frame_size / FRAME_BYTES_PER_RECORD;
    // Only an uncompressed partition under the cap needs its slots counted.
    let counted = |blob: &[u8]| !is_compressed(blob) && blob.len() < max_frame_size;
    let cost = |blob: &[u8]| {
        if counted(blob) {
            let slots = record_slots(blob, cap_slots);
            blob.len().max(slots.saturating_mul(FRAME_BYTES_PER_RECORD))
        } else {
            max_frame_size
        }
    };
    // Counting reads every record, so a large partition counts off the worker, as it decodes.
    let heavy = blobs().any(|blob| counted(blob) && blob.len() >= CODEC_OFF_WORKER_BYTES);
    off_worker(heavy, || blobs().map(cost).max().unwrap_or(0)).min(max_frame_size)
}

/// What a request holds from before its first decode: a large slot when it is large, and its
/// decode memory.
///
/// A send that outlives the request keeps it for as long as it owns the decoded partition: until
/// it ends, or until the bridge gives up on it.
struct Admission {
    _large: Option<OwnedSemaphorePermit>,
    _memory: OwnedSemaphorePermit,
}

/// The producer's turn and the request's admission. They travel together through every send.
type Held = (ProduceTurn, Admission);

/// Waits for the request's decode memory. A large request, `CODEC_OFF_WORKER_BYTES` or more,
/// first waits for a large slot, and holds no memory while it does.
async fn admit(state: &GatewayState, cost: usize) -> std::result::Result<Admission, AcquireError> {
    let large = if cost >= CODEC_OFF_WORKER_BYTES {
        Some(Arc::clone(&state.produce_large).acquire_owned().await?)
    } else {
        None
    };
    let permits = u32::try_from(cost).unwrap_or(u32::MAX);
    let memory = Arc::clone(&state.produce_memory)
        .acquire_many_owned(permits)
        .await?;
    Ok(Admission {
        _large: large,
        _memory: memory,
    })
}

/// The producer id of the request's first batch, when this gateway gave that id out. Otherwise
/// the connection: an id a client made up must not pick a session, or open one per id.
fn producer_of(
    state: &GatewayState,
    connection: &ConnectionState,
    principal: Option<&AuthenticatedPrincipal>,
    request: &ProduceRequest,
) -> Producer {
    request
        .topic_data
        .iter()
        .flat_map(|topic| &topic.partition_data)
        .find_map(|partition| partition.records.as_deref())
        .and_then(producer_id)
        .filter(|id| state.producer_ids.issued(*id))
        .map_or_else(
            || Producer::Connection(connection.id()),
            |id| Producer::Id {
                id,
                user: principal.map(|principal| principal.username.clone()),
            },
        )
}

/// When a request stops writing.
#[derive(Clone, Copy)]
struct Deadline {
    at: tokio::time::Instant,
    /// This gateway's ceiling set it, not the client's `timeout_ms`.
    ceiling: bool,
}

impl Deadline {
    fn new(timeout_ms: i32) -> Self {
        let wait = request_timeout(timeout_ms);
        Self {
            at: tokio::time::Instant::now() + wait,
            ceiling: wait == REQUEST_DEADLINE,
        }
    }

    fn passed(self) -> bool {
        tokio::time::Instant::now() >= self.at
    }
}

/// Why a partition was not written. `message` reaches the client from Produce v8.
struct Refusal {
    code: i16,
    message: Option<String>,
}

impl From<i16> for Refusal {
    fn from(code: i16) -> Self {
        Self {
            code,
            message: None,
        }
    }
}

impl From<RecordCodecError> for Refusal {
    fn from(error: RecordCodecError) -> Self {
        Self {
            code: record_error_code(&error),
            message: Some(error.to_string()),
        }
    }
}

/// Converts and sends one partition at a time, in request order, one Iggy send each.
///
/// One budget for the whole request, so a small request cannot make this inflate without end.
/// Only one partition's messages live at a time. The budget is owned here and borrowed only in
/// sync calls: a borrow held across an await makes the connection task `!Send`.
///
/// Sequential, and `held` carries the producer's turn, so its sends reach Iggy in order. A send
/// that outlives the deadline keeps `held`, and the partitions after it answer 7.
async fn write_request(
    bridge: &IggyBridge,
    held: Held,
    board: &ProbeBoard,
    request: &ProduceRequest,
    budget: DecompressionBudget,
    zstd: Zstd,
    deadline: Deadline,
) -> Vec<TopicProduceResponse> {
    let max_send = bridge.max_send_bytes();
    let mut held = Some(held);
    let mut topics = Vec::with_capacity(request.topic_data.len());
    for topic in &request.topic_data {
        let name = topic.name.as_str();
        let target = bridge.topic_target(name);
        let mut responses = Vec::with_capacity(topic.partition_data.len());
        let mut wrote = false;
        for partition in &topic.partition_data {
            let (outcome, kept) = match (&target, held.take()) {
                (Ok(target), Some(kept)) if !deadline.passed() => {
                    match plan(&budget, zstd, name, partition, max_send) {
                        Ok((id, messages)) => {
                            let (sent, kept) =
                                send_by(bridge, kept, target, name, id, messages, deadline).await;
                            (sent.map_err(Refusal::from), kept)
                        }
                        Err(refusal) => (Err(refusal), Some(kept)),
                    }
                }
                // Skip the decode. The answer is 7 either way: the deadline passed, or a send
                // that outlived it kept the memory this decode would need.
                (Ok(_), kept) => (Err(ERROR_REQUEST_TIMED_OUT.into()), kept),
                (Err(error), kept) => (Err(refusal_code(name, error).into()), kept),
            };
            held = kept;
            wrote |= outcome.is_ok();
            responses.push(partition_outcome(partition.index, outcome));
            // Planning is sync CPU work. Let other connections on this worker run.
            tokio::task::yield_now().await;
        }
        if wrote {
            board.wrote(name);
        }
        topics.push(
            TopicProduceResponse::default()
                .with_name(topic.name.clone())
                .with_partition_responses(responses),
        );
    }
    topics
}

/// [`plan_partition`], off the async worker for a large or compressed partition, so other
/// connections keep running. On a current-thread runtime it runs in place.
fn plan(
    budget: &DecompressionBudget,
    zstd: Zstd,
    kafka_topic: &str,
    partition: &PartitionProduceData,
    max_send: u64,
) -> std::result::Result<(u32, Vec<IggyMessage>), Refusal> {
    let run = || plan_partition(budget, zstd, kafka_topic, partition, max_send);
    // A spent budget refuses before it reads a byte, so there is nothing to move off the worker.
    let heavy = !budget.is_spent()
        && partition
            .records
            .as_ref()
            .is_some_and(|blob| blob.len() >= CODEC_OFF_WORKER_BYTES || is_compressed(blob));
    off_worker(heavy, run)
}

fn partition_outcome(
    index: i32,
    outcome: std::result::Result<Option<u64>, Refusal>,
) -> PartitionProduceResponse {
    match outcome {
        // An offset past `i64::MAX` has no response field to fit in, so it reads as unknown
        // rather than as a saturated number a client would take for a real position.
        Ok(base_offset) => partition_response(index, ERROR_NONE).with_base_offset(
            base_offset.map_or(UNKNOWN_OFFSET, |offset| {
                i64::try_from(offset).unwrap_or(UNKNOWN_OFFSET)
            }),
        ),
        Err(Refusal { code, message }) => {
            partition_response(index, code).with_error_message(message.map(StrBytes::from_string))
        }
    }
}

/// One partition's batch in, its messages out. A batch that cannot be mapped is refused whole,
/// never half-stored.
fn plan_partition(
    budget: &DecompressionBudget,
    zstd: Zstd,
    kafka_topic: &str,
    partition: &PartitionProduceData,
    max_send: u64,
) -> std::result::Result<(u32, Vec<IggyMessage>), Refusal> {
    // The records are not what is wrong with a negative index, so not `INVALID_RECORD`.
    let Ok(partition_id) = u32::try_from(partition.index) else {
        return Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION.into());
    };
    // Nothing to append. Answering success would name an offset for a write that never happened.
    let Some(blob) = partition.records.as_ref() else {
        return Err(ERROR_INVALID_RECORD.into());
    };
    let refuse = |error: RecordCodecError| {
        tracing::debug!(%error, kafka_topic, partition_id, "refused a Produce record batch");
        Refusal::from(error)
    };

    // The clone is a refcount, and `decode_batch` drains what it is given.
    let records = decode_batch(&mut blob.clone(), budget, zstd).map_err(refuse)?;
    // An empty blob decodes to no batch rather than to an error, so this rules on it.
    if records.is_empty() {
        return Err(ERROR_INVALID_RECORD.into());
    }

    // One window for the whole batch, so it fits one send.
    let window = TimestampWindow::of(&records);
    let mut messages = Vec::with_capacity(records.len());
    let mut size = 0u64;
    for record in &records {
        let message = to_iggy(record, window).map_err(refuse)?;
        size = size.saturating_add(message.get_size_bytes().as_bytes_u64());
        messages.push(message);
    }
    // Over this, Iggy drops the frame, and the client retries on 7 until it gives up.
    if size > max_send {
        tracing::debug!(
            size,
            kafka_topic,
            partition_id,
            "Produce partition too large for one Iggy send"
        );
        return Err(ERROR_MESSAGE_TOO_LARGE.into());
    }
    Ok((partition_id, messages))
}

/// The shorter of the client's `timeout_ms` and this gateway's ceiling.
///
/// Honoring the client's budget narrows, not closes, the window for its retry to duplicate a
/// write still running here: this clock starts when the request is read, after any pipelined one.
/// A non-positive value means no preference, not "fail without trying".
fn request_timeout(timeout_ms: i32) -> Duration {
    u64::try_from(timeout_ms)
        .ok()
        .filter(|ms| *ms > 0)
        .map_or(REQUEST_DEADLINE, Duration::from_millis)
        .min(REQUEST_DEADLINE)
}

/// Sends one partition's messages, or answers 7 once the deadline has passed. Hands `held` back,
/// unless the send still runs and keeps it.
///
/// 7 means nothing was written, or the send may still land and a retry duplicate it.
async fn send_by(
    bridge: &IggyBridge,
    (turn, admission): Held,
    target: &TopicTarget,
    kafka_topic: &str,
    partition_id: u32,
    messages: Vec<IggyMessage>,
    deadline: Deadline,
) -> (std::result::Result<Option<u64>, i16>, Option<Held>) {
    let (sent, held) = bridge
        .send_records(turn, admission, target, partition_id, messages, deadline.at)
        .await;
    let sent = match sent {
        Ok(base_offset) => Ok(base_offset),
        Err(BridgeError::Timeout) => {
            // The client picks `timeout_ms`, so its expiry is no signal to the operator.
            if deadline.ceiling {
                tracing::warn!(
                    kafka_topic,
                    partition_id,
                    "Produce ran past the gateway deadline"
                );
            } else {
                tracing::debug!(
                    kafka_topic,
                    partition_id,
                    "Produce ran past the client deadline"
                );
            }
            Err(ERROR_REQUEST_TIMED_OUT)
        }
        Err(error) => Err(refusal_code(kafka_topic, &error)),
    };
    (sent, held)
}

/// The Kafka code for a bridge refusal, logged at a level that matches who can fix it.
fn refusal_code(kafka_topic: &str, error: &BridgeError) -> i16 {
    let code = error.to_kafka_error_code();
    if error.is_bridge_login_rejected() {
        tracing::error!(%error, kafka_topic, "Iggy rejected the bridge's credentials");
    } else if matches!(code, ERROR_UNKNOWN_SERVER_ERROR) {
        tracing::warn!(%error, kafka_topic, "Iggy refused a Produce batch");
    } else {
        // Missing topic, bad name, lost connection: the client sees the code and retries.
        tracing::debug!(%error, kafka_topic, "Iggy refused a Produce batch");
    }
    code
}

/// The Kafka code one record or batch failure answers with.
///
/// Too big is 10, retry is 6, malformed is 87. Not `CORRUPT_MESSAGE` (2) for the malformed half,
/// which `kafka_protocol` marks retriable. Neither 10 nor 87 makes a client resend the same bytes.
/// Java splits a multi-record batch on 10 and sends the halves.
///
/// No wildcard arm, so a variant added later fails the match instead of taking a default.
const fn record_error_code(error: &RecordCodecError) -> i16 {
    match error {
        RecordCodecError::Iggy(IggyError::TooBigMessagePayload | IggyError::TooBigUserHeaders)
        | RecordCodecError::BudgetExceeded { .. }
        | RecordCodecError::RecordBudgetExceeded { .. }
        | RecordCodecError::EnvelopeTooLarge { .. } => ERROR_MESSAGE_TOO_LARGE,
        // The request budget ran out. Nothing was written.
        RecordCodecError::RequestBudgetSpent => ERROR_NOT_LEADER_OR_FOLLOWER,
        // Transactional or control. 35 is a fatal code for these producers, so they stop instead
        // of retrying forever.
        RecordCodecError::TransactionalBatch | RecordCodecError::ControlBatch => {
            ERROR_UNSUPPORTED_VERSION
        }
        RecordCodecError::ZstdTooEarly => ERROR_UNSUPPORTED_COMPRESSION_TYPE,
        // `IggyMessage::new`'s other refusal is an empty payload, which the codec never builds.
        RecordCodecError::Iggy(_)
        // Reachable from Produce.
        | RecordCodecError::TimestampOutOfRange(_)
        | RecordCodecError::Batch(_)
        | RecordCodecError::SeveralBatches(_)
        | RecordCodecError::BatchTrailingBytes(_)
        | RecordCodecError::RecordCountTooLarge { .. }
        | RecordCodecError::RecordCountMismatch { .. }
        | RecordCodecError::HeaderCountTooLarge { .. }
        | RecordCodecError::RecordTruncated
        | RecordCodecError::RecordFieldLength(_)
        | RecordCodecError::RepeatedHeaderName(_)
        // Raised by `from_iggy` only, so Fetch is what actually answers for these.
        | RecordCodecError::UserHeadersUnreadable(_)
        | RecordCodecError::MappingVersion(_)
        | RecordCodecError::ValueMarker(_)
        | RecordCodecError::HeaderNameCollision(_)
        | RecordCodecError::TimestampHeader(_)
        | RecordCodecError::EnvelopeTruncated { .. }
        | RecordCodecError::EnvelopeTrailingBytes(_)
        | RecordCodecError::EnvelopeVersion(_)
        | RecordCodecError::EnvelopeHeaderName => ERROR_INVALID_RECORD,
    }
}

/// Well-formed response with a single placeholder topic and partition.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let resp = ProduceResponse::default().with_responses(vec![
        TopicProduceResponse::default()
            .with_partition_responses(vec![partition_response(0, error_code)]),
    ]);
    encode_message(&resp, version, RESPONSE_BASE_BYTES)
}

/// Stub: discard the payload and answer retriable, rather than advertise a silent success.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, req: &ProduceRequest) -> Result<Bytes> {
    encode_uniform_response(version, req, ERROR_NOT_LEADER_OR_FOLLOWER)
}

/// One code for every partition named, for failures that belong to the request and not to a
/// single partition.
fn encode_uniform_response(version: i16, req: &ProduceRequest, error_code: i16) -> Result<Bytes> {
    let responses = req
        .topic_data
        .iter()
        .map(|topic| {
            TopicProduceResponse::default()
                .with_name(topic.name.clone())
                .with_partition_responses(
                    topic
                        .partition_data
                        .iter()
                        .map(|p| partition_response(p.index, error_code))
                        .collect(),
                )
        })
        .collect();
    encode_written_response(version, responses)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
fn encode_written_response(version: i16, responses: Vec<TopicProduceResponse>) -> Result<Bytes> {
    let resp = ProduceResponse::default().with_responses(responses);
    encode_message(&resp, version, RESPONSE_BASE_BYTES)
}

fn partition_response(index: i32, error_code: i16) -> PartitionProduceResponse {
    PartitionProduceResponse::default()
        .with_index(index)
        .with_error_code(error_code)
        .with_base_offset(UNKNOWN_OFFSET)
        // The real log start costs a `get_topic` per request, and no producer reads it.
        .with_log_start_offset(UNKNOWN_OFFSET)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::messages::TopicName;
    use kafka_protocol::messages::produce_request::TopicProduceData;
    use kafka_protocol::protocol::{Decodable, Encodable};
    use kafka_protocol::records::{
        Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
        Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };
    use tokio_util::sync::CancellationToken;

    use crate::group::{GroupCoordinator, GroupCoordinatorConfig};
    use crate::protocol::acl::PrincipalPermissions;
    use crate::protocol::api::BrokerAdvertise;
    use crate::records::encode_batch;

    const CREATE_TIME: i64 = 1_700_000_000_123;
    const TOPIC: &str = "orders";
    /// Well above anything these batches decompress to.
    const TEST_BUDGET: usize = 1024 * 1024;

    fn record(offset: i64, value: &[u8]) -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: NO_SEQUENCE,
            timestamp: CREATE_TIME,
            key: None,
            value: Some(Bytes::copy_from_slice(value)),
            headers: IndexMap::new(),
        }
    }

    fn compressed_batch(records: &[Record], compression: Compression) -> Bytes {
        let mut buf = BytesMut::new();
        let options = RecordEncodeOptions {
            version: 2,
            compression,
        };
        RecordBatchEncoder::encode(&mut buf, records.iter(), &options).expect("encode batch");
        buf.freeze()
    }

    fn gzip_batch(records: &[Record]) -> Bytes {
        compressed_batch(records, Compression::Gzip)
    }

    fn entry(index: i32, records: Option<Bytes>) -> PartitionProduceData {
        PartitionProduceData::default()
            .with_index(index)
            .with_records(records)
    }

    fn plan_with(
        budget: &DecompressionBudget,
        zstd: Zstd,
        entry: &PartitionProduceData,
    ) -> std::result::Result<(u32, Vec<IggyMessage>), i16> {
        plan_partition(budget, zstd, TOPIC, entry, u64::MAX).map_err(|refusal| refusal.code)
    }

    fn planned(entry: &PartitionProduceData) -> std::result::Result<(u32, Vec<IggyMessage>), i16> {
        plan_with(
            &partition_budget(TEST_BUDGET, usize::MAX),
            Zstd::Allowed,
            entry,
        )
    }

    /// A request allowance of one partition's.
    fn partition_budget(bytes: usize, records: usize) -> DecompressionBudget {
        let allowance = Allowance { bytes, records };
        DecompressionBudget::new(allowance, allowance)
    }

    #[test]
    fn given_a_record_batch_when_planned_should_convert_every_record_in_order() {
        let batch = encode_batch(&mut [record(0, b"first"), record(1, b"second")]).unwrap();
        let (partition_id, messages) = planned(&entry(2, Some(batch))).expect("batch must convert");

        assert_eq!(
            partition_id, 2,
            "the Kafka index passes through unconverted"
        );
        assert_eq!(messages.len(), 2, "one record becomes one message");
        assert_eq!(messages[0].payload.as_ref(), b"first");
        assert_eq!(messages[1].payload.as_ref(), b"second");
    }

    #[test]
    fn given_a_negative_partition_index_when_planned_should_answer_unknown_topic_or_partition() {
        let batch = encode_batch(&mut [record(0, b"v")]).unwrap();
        assert_eq!(
            planned(&entry(-1, Some(batch))).unwrap_err(),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            "the records are not what is wrong with the request"
        );
    }

    #[test]
    fn given_no_records_field_when_planned_should_answer_invalid_record() {
        assert_eq!(planned(&entry(0, None)).unwrap_err(), ERROR_INVALID_RECORD);
    }

    #[test]
    fn given_an_empty_records_blob_when_planned_should_answer_invalid_record() {
        // An empty blob reads as zero batches, not as a malformed one, so nothing below rejects it.
        assert_eq!(
            planned(&entry(0, Some(Bytes::new()))).unwrap_err(),
            ERROR_INVALID_RECORD
        );
    }

    #[test]
    fn given_a_transactional_batch_when_planned_should_answer_unsupported_version() {
        let mut records = [record(0, b"v")];
        records[0].transactional = true;
        let batch = encode_batch(&mut records).unwrap();

        assert_eq!(
            planned(&entry(0, Some(batch))).unwrap_err(),
            ERROR_UNSUPPORTED_VERSION,
            "35 is fatal for a transactional producer"
        );
    }

    fn request_of(partitions: Vec<PartitionProduceData>) -> ProduceRequest {
        ProduceRequest::default().with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partition_data(partitions),
        ])
    }

    /// The default frame.
    const FRAME: usize = 8 * 1024 * 1024;
    /// Long enough for an admission that is free to complete. Time is paused in these tests.
    const WAIT: Duration = Duration::from_millis(50);

    /// A one-record batch that carries `producer_id`.
    fn batch_of(producer_id: i64) -> Bytes {
        let mut records = [record(0, b"v")];
        records[0].producer_id = producer_id;
        records[0].producer_epoch = 0;
        records[0].sequence = 0;
        compressed_batch(&records, Compression::None)
    }

    fn principal(username: &str) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            username: username.to_string(),
            permissions: PrincipalPermissions::default(),
            permissions_known: true,
        }
    }

    #[test]
    fn given_a_producer_id_this_gateway_gave_out_when_keyed_should_take_it_with_its_user() {
        let state = GatewayState::stub(BrokerAdvertise::default(), FRAME);
        let id = state.producer_ids.allocate().expect("an id");
        let request = request_of(vec![entry(0, Some(batch_of(id)))]);
        let connection = ConnectionState::default();

        assert_eq!(
            producer_of(&state, &connection, None, &request),
            Producer::Id { id, user: None }
        );
        assert_eq!(
            producer_of(&state, &connection, Some(&principal("ann")), &request),
            Producer::Id {
                id,
                user: Some("ann".to_string())
            },
            "another user's request with this id gets another session"
        );
    }

    #[test]
    fn given_a_producer_id_a_client_made_up_when_keyed_should_take_the_connection() {
        let state = GatewayState::stub(BrokerAdvertise::default(), FRAME);
        let connection = ConnectionState::default();
        let issued = state.producer_ids.allocate().expect("an id");

        for made_up in [7, issued + 1] {
            let request = request_of(vec![entry(0, Some(batch_of(made_up)))]);
            assert_eq!(
                producer_of(&state, &connection, None, &request),
                Producer::Connection(connection.id()),
                "{made_up} picks no session, and opens none of its own"
            );
        }
    }

    #[test]
    fn given_a_batch_without_a_producer_id_when_keyed_should_take_the_connection() {
        let state = GatewayState::stub(BrokerAdvertise::default(), FRAME);
        let batch = compressed_batch(&[record(0, b"v")], Compression::None);
        let request = request_of(vec![entry(0, Some(batch))]);
        let connection = ConnectionState::default();

        assert_eq!(
            producer_of(&state, &connection, None, &request),
            Producer::Connection(connection.id())
        );
        assert_ne!(
            ConnectionState::default().id(),
            connection.id(),
            "each connection keys a session of its own"
        );
    }

    fn cost_of(partitions: Vec<PartitionProduceData>, max_frame_size: usize) -> usize {
        decode_cost(&request_of(partitions), max_frame_size)
    }

    #[test]
    fn given_an_uncompressed_batch_when_costed_should_take_its_bytes() {
        let batch = encode_batch(&mut [record(0, &[7; 1000]), record(1, &[7; 1000])]).unwrap();
        assert_eq!(
            cost_of(vec![entry(0, Some(batch.clone()))], FRAME),
            batch.len()
        );
    }

    #[test]
    fn given_small_records_when_costed_should_take_a_record_slot_each() {
        let mut records: Vec<Record> = (0..100).map(|offset| record(offset, b"v")).collect();
        let batch = encode_batch(&mut records).unwrap();
        assert!(batch.len() < 100 * FRAME_BYTES_PER_RECORD);
        assert_eq!(
            cost_of(vec![entry(0, Some(batch))], FRAME),
            100 * FRAME_BYTES_PER_RECORD
        );
    }

    #[test]
    fn given_records_with_many_headers_when_costed_should_take_the_slots_the_decode_charges() {
        let mut heavy = record(0, b"v");
        heavy.headers = (0..300)
            .map(|index| (StrBytes::from_string(format!("h{index}")), None))
            .collect();
        let batch = encode_batch(&mut [heavy]).unwrap();
        let cost = cost_of(vec![entry(0, Some(batch.clone()))], FRAME);

        // One slot for the record and one for each three headers.
        assert_eq!(cost, 101 * FRAME_BYTES_PER_RECORD);
        assert!(
            cost > batch.len(),
            "its bytes alone would admit it for less"
        );
        let slots = cost / FRAME_BYTES_PER_RECORD;
        let entry = entry(0, Some(batch));
        assert!(
            plan_with(&partition_budget(FRAME, slots), Zstd::Allowed, &entry).is_ok(),
            "the decode takes no more than the request was admitted for"
        );
        assert_eq!(
            plan_with(&partition_budget(FRAME, slots - 1), Zstd::Allowed, &entry).unwrap_err(),
            ERROR_MESSAGE_TOO_LARGE
        );
    }

    #[test]
    fn given_a_compressed_batch_when_costed_should_take_a_whole_cap() {
        let batch = gzip_batch(&[record(0, b"v")]);
        assert_eq!(
            cost_of(vec![entry(0, Some(batch))], FRAME),
            FRAME,
            "it may inflate to the cap"
        );
    }

    #[test]
    fn given_several_partitions_when_costed_should_take_the_largest() {
        let small = compressed_batch(&[record(0, &[7; 100])], Compression::None);
        let large = compressed_batch(&[record(0, &[7; 5000])], Compression::None);
        let partitions = vec![
            entry(0, Some(small)),
            entry(1, Some(large.clone())),
            entry(2, None),
        ];
        assert_eq!(
            cost_of(partitions, FRAME),
            large.len(),
            "partitions decode one at a time"
        );
    }

    #[test]
    fn given_a_partition_past_the_cap_when_costed_should_take_one_cap() {
        let batch = compressed_batch(&[record(0, &[7; 5000])], Compression::None);
        assert_eq!(cost_of(vec![entry(0, Some(batch))], 4096), 4096);
    }

    /// Admits four requests of `cost`, which takes every large slot.
    async fn large_slots_taken(state: &GatewayState, cost: usize) -> Vec<Admission> {
        let mut admitted = Vec::new();
        for _ in 0..4 {
            admitted.push(admit(state, cost).await.unwrap());
        }
        admitted
    }

    #[tokio::test(start_paused = true)]
    async fn given_the_large_slots_taken_when_a_small_request_comes_should_admit_it() {
        let state = GatewayState::stub(BrokerAdvertise::default(), FRAME);
        let _large = large_slots_taken(&state, FRAME).await;

        let fifth = admit(&state, CODEC_OFF_WORKER_BYTES);
        assert!(
            tokio::time::timeout(WAIT, fifth).await.is_err(),
            "a fifth large request waits its turn"
        );
        let small = admit(&state, CODEC_OFF_WORKER_BYTES - 1);
        assert!(
            tokio::time::timeout(WAIT, small).await.is_ok(),
            "four requests of a whole cap each leave a small one its memory"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_the_large_slots_taken_when_a_large_request_waits_should_hold_no_memory() {
        let state = GatewayState::stub(BrokerAdvertise::default(), FRAME);
        let _large = large_slots_taken(&state, CODEC_OFF_WORKER_BYTES).await;
        let free = state.produce_memory.available_permits();

        let waiting = admit(&state, FRAME);
        tokio::pin!(waiting);
        assert!(tokio::time::timeout(WAIT, &mut waiting).await.is_err());
        assert_eq!(
            state.produce_memory.available_permits(),
            free,
            "small requests can still take that memory"
        );
    }

    /// A gateway whose bridge never dialed. A request that stops before its first send needs no
    /// server.
    fn offline_state() -> GatewayState {
        GatewayState::new(
            BrokerAdvertise::default(),
            Some(Arc::new(IggyBridge::offline())),
            FRAME,
            false,
            0,
            GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
        )
    }

    /// One large Produce v9 request of `connection` for partition 0, through the whole handler.
    async fn handled(
        state: &GatewayState,
        connection: &ConnectionState,
        acks: i16,
    ) -> HandleOutcome {
        let value = vec![7; CODEC_OFF_WORKER_BYTES];
        let batch = compressed_batch(&[record(0, &value)], Compression::None);
        let request = request_of(vec![entry(0, Some(batch))])
            .with_acks(acks)
            .with_timeout_ms(50);
        let mut body = BytesMut::new();
        request.encode(&mut body, 9).expect("encode the request");
        handle(state, connection, None, 9, body.freeze()).await
    }

    #[tokio::test(start_paused = true)]
    async fn given_the_producers_earlier_send_still_running_when_handled_should_answer_timed_out() {
        let state = offline_state();
        let bridge = state.bridge.as_deref().expect("a bridge");
        let connection = ConnectionState::default();
        let far = tokio::time::Instant::now() + Duration::from_secs(60);
        let _earlier_send = bridge
            .produce_turn(Producer::Connection(connection.id()), far)
            .await
            .expect("a free turn");
        let free = |state: &GatewayState| {
            (
                state.produce_large.available_permits(),
                state.produce_memory.available_permits(),
            )
        };
        let before = free(&state);

        let waiting = handled(&state, &connection, ACKS_ALL);
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut waiting)
                .await
                .is_err(),
            "it waits for the turn until its own deadline"
        );
        assert_eq!(
            free(&state),
            before,
            "and holds no large slot and no memory while it waits"
        );
        let mut answer = waiting.await.expect_response("acks=-1 is answered");
        let response = ProduceResponse::decode(&mut answer, 9).expect("decode the response");
        let code = response.responses[0].partition_responses[0].error_code;
        assert_eq!(
            code, ERROR_REQUEST_TIMED_OUT,
            "it must not overtake that send"
        );
        assert!(
            handled(&state, &connection, ACKS_NONE).await.is_close(),
            "acks=0 has no response to carry the 7"
        );
    }

    #[test]
    fn given_an_idempotent_batch_when_planned_should_convert_it() {
        let mut records = [record(0, b"v")];
        records[0].producer_id = 7;
        records[0].producer_epoch = 0;
        records[0].sequence = 0;
        let batch = compressed_batch(&records, Compression::None);

        let (_, messages) =
            planned(&entry(0, Some(batch))).expect("a stock Java producer sends this batch");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload.as_ref(), b"v");
    }

    #[test]
    fn given_zstd_before_v7_when_planned_should_answer_unsupported_compression() {
        let batch = compressed_batch(&[record(0, b"v")], Compression::Zstd);
        let budget = partition_budget(TEST_BUDGET, usize::MAX);

        assert_eq!(
            plan_with(&budget, Zstd::Refused, &entry(0, Some(batch.clone()))).unwrap_err(),
            ERROR_UNSUPPORTED_COMPRESSION_TYPE
        );
        assert!(plan_with(&budget, Zstd::Allowed, &entry(0, Some(batch))).is_ok());
    }

    #[test]
    fn given_a_control_batch_when_planned_should_answer_unsupported_version() {
        let mut records = [record(0, b"v")];
        records[0].control = true;
        let batch = encode_batch(&mut records).unwrap();

        assert_eq!(
            planned(&entry(0, Some(batch))).unwrap_err(),
            ERROR_UNSUPPORTED_VERSION,
            "a stored message carries no control flag, so this one cannot be admitted"
        );
    }

    #[test]
    fn given_a_truncated_batch_when_planned_should_answer_invalid_record() {
        let batch = encode_batch(&mut [record(0, b"value")]).unwrap();
        let truncated = batch.slice(..batch.len() - 3);

        assert_eq!(
            planned(&entry(0, Some(truncated))).unwrap_err(),
            ERROR_INVALID_RECORD,
            "not CORRUPT_MESSAGE (2), which is retriable and would loop"
        );
    }

    #[test]
    fn given_a_batch_that_decompresses_past_the_budget_when_planned_should_answer_too_large() {
        let batch = gzip_batch(&[record(0, &[b'a'; 4096])]);
        let budget = partition_budget(16, usize::MAX);

        assert_eq!(
            plan_with(&budget, Zstd::Allowed, &entry(0, Some(batch))).unwrap_err(),
            ERROR_MESSAGE_TOO_LARGE
        );
    }

    #[test]
    fn given_more_records_than_a_partition_allows_when_planned_should_answer_too_large() {
        let batch = encode_batch(&mut [record(0, b"a"), record(1, b"b")]).unwrap();
        let budget = partition_budget(TEST_BUDGET, 1);

        assert_eq!(
            plan_with(&budget, Zstd::Allowed, &entry(0, Some(batch))).unwrap_err(),
            ERROR_MESSAGE_TOO_LARGE,
            "10, so Java splits the batch"
        );
    }

    #[test]
    fn given_an_earlier_partition_spent_the_budget_when_planned_should_answer_retriable() {
        let budget = partition_budget(6144, usize::MAX);
        let first = entry(0, Some(gzip_batch(&[record(0, &[b'a'; 4096])])));
        let second = entry(1, Some(gzip_batch(&[record(0, &[b'b'; 4096])])));

        assert!(plan_with(&budget, Zstd::Allowed, &first).is_ok());
        assert_eq!(
            plan_with(&budget, Zstd::Allowed, &second).unwrap_err(),
            ERROR_NOT_LEADER_OR_FOLLOWER,
            "fits alone, so not 10"
        );
    }

    #[test]
    fn given_an_entry_over_its_cap_after_another_when_planned_should_answer_too_large() {
        // gzip writes 32 KiB at a time, so the second entry passes its cap in several writes.
        let partition = Allowance {
            bytes: 64 * 1024,
            records: usize::MAX,
        };
        let budget = DecompressionBudget::new(partition, partition.times(8));
        let first = entry(0, Some(gzip_batch(&[record(0, &vec![b'a'; 60 * 1024])])));
        let second = entry(1, Some(gzip_batch(&[record(0, &vec![b'b'; 200 * 1024])])));

        assert!(plan_with(&budget, Zstd::Allowed, &first).is_ok());
        assert_eq!(
            plan_with(&budget, Zstd::Allowed, &second).unwrap_err(),
            ERROR_MESSAGE_TOO_LARGE,
            "too large alone, so a retry cannot help"
        );
    }

    #[test]
    fn given_an_earlier_partition_spent_the_record_budget_when_planned_should_answer_retriable() {
        let budget = partition_budget(TEST_BUDGET, 3);
        let two = || encode_batch(&mut [record(0, b"a"), record(1, b"b")]).unwrap();

        assert!(plan_with(&budget, Zstd::Allowed, &entry(0, Some(two()))).is_ok());
        assert_eq!(
            plan_with(&budget, Zstd::Allowed, &entry(1, Some(two()))).unwrap_err(),
            ERROR_NOT_LEADER_OR_FOLLOWER
        );
    }

    #[test]
    fn given_a_spent_request_when_a_later_partition_is_planned_should_answer_retry_undecoded() {
        let budget = partition_budget(6144, usize::MAX);
        let fill = |index| entry(index, Some(gzip_batch(&[record(0, &[b'a'; 4096])])));
        assert!(plan_with(&budget, Zstd::Allowed, &fill(0)).is_ok());
        assert_eq!(
            plan_with(&budget, Zstd::Allowed, &fill(1)).unwrap_err(),
            ERROR_NOT_LEADER_OR_FOLLOWER
        );

        // Malformed, so a decode would answer 87.
        let garbage = entry(2, Some(Bytes::from_static(b"not a record batch")));
        assert_eq!(
            plan_with(&budget, Zstd::Allowed, &garbage).unwrap_err(),
            ERROR_NOT_LEADER_OR_FOLLOWER,
            "nothing decodes once the request budget is gone"
        );
    }

    #[test]
    fn given_timestamps_past_one_send_when_planned_should_fit_one_send() {
        let mut records = [record(0, b"a"), record(1, b"b"), record(2, b"c")];
        records[1].timestamp = CREATE_TIME + 72 * 60 * 1000;
        records[2].timestamp = -1;
        let (_, messages) = planned(&entry(0, Some(encode_batch(&mut records).unwrap()))).unwrap();

        let stamps: Vec<u64> = messages
            .iter()
            .map(|message| message.header.origin_timestamp)
            .collect();
        let low = stamps.iter().min().unwrap();
        let high = stamps.iter().max().unwrap();
        assert!(
            u32::try_from(high - low).is_ok(),
            "one Iggy send stores each as a u32 delta: {stamps:?}"
        );
    }

    #[test]
    fn given_a_client_timeout_when_bounded_should_take_whichever_is_shorter() {
        assert_eq!(request_timeout(5_000), Duration::from_secs(5));
        assert_eq!(
            request_timeout(30_000),
            REQUEST_DEADLINE,
            "a client asking for longer than this gateway allows gets the gateway's ceiling"
        );
        assert_eq!(request_timeout(i32::MAX), REQUEST_DEADLINE);
    }

    #[test]
    fn given_no_client_timeout_when_bounded_should_still_attempt_the_write() {
        // A value naming no budget means no preference, not "fail now".
        assert_eq!(request_timeout(0), REQUEST_DEADLINE);
        assert_eq!(request_timeout(-1), REQUEST_DEADLINE);
        assert_eq!(request_timeout(i32::MIN), REQUEST_DEADLINE);
    }

    #[test]
    fn given_a_partition_response_when_built_should_name_no_offset_and_no_append_time() {
        let response = partition_response(3, ERROR_INVALID_RECORD);

        assert_eq!(response.index, 3);
        assert_eq!(response.error_code, ERROR_INVALID_RECORD);
        assert_eq!(response.base_offset, UNKNOWN_OFFSET);
        assert_eq!(response.log_start_offset, UNKNOWN_OFFSET);
        assert_eq!(
            response.log_append_time_ms, UNKNOWN_OFFSET,
            "CreateTime, which is the only timestamp type the mapping stores"
        );
    }

    #[test]
    fn given_a_size_failure_when_mapped_should_answer_message_too_large() {
        assert_eq!(
            record_error_code(&RecordCodecError::BudgetExceeded { size: 2, limit: 1 }),
            ERROR_MESSAGE_TOO_LARGE
        );
        assert_eq!(
            record_error_code(&RecordCodecError::EnvelopeTooLarge { size: 1 }),
            ERROR_MESSAGE_TOO_LARGE
        );
        assert_eq!(
            record_error_code(&RecordCodecError::Iggy(IggyError::TooBigMessagePayload)),
            ERROR_MESSAGE_TOO_LARGE
        );
        assert_eq!(
            record_error_code(&RecordCodecError::Iggy(IggyError::TooBigUserHeaders)),
            ERROR_MESSAGE_TOO_LARGE
        );
    }

    #[test]
    fn given_a_malformed_record_when_mapped_should_answer_a_terminal_code() {
        for error in [
            RecordCodecError::Batch("bad".to_string()),
            RecordCodecError::RecordTruncated,
            RecordCodecError::RecordFieldLength(-1),
            RecordCodecError::TimestampOutOfRange(i64::MIN),
            RecordCodecError::RecordCountTooLarge {
                count: i32::MAX,
                limit: 1,
            },
            RecordCodecError::RecordCountMismatch {
                declared: 1,
                walked: 2,
            },
            RecordCodecError::SeveralBatches(2),
            RecordCodecError::BatchTrailingBytes(1),
            RecordCodecError::HeaderCountTooLarge {
                count: i32::MAX,
                limit: 1,
            },
            RecordCodecError::Iggy(IggyError::InvalidMessagePayloadLength),
        ] {
            assert_eq!(
                record_error_code(&error),
                ERROR_INVALID_RECORD,
                "{error} must not answer a retriable code"
            );
        }
    }

    #[test]
    fn given_a_refused_batch_kind_when_mapped_should_answer_unsupported_version() {
        for error in [
            RecordCodecError::TransactionalBatch,
            RecordCodecError::ControlBatch,
        ] {
            assert_eq!(record_error_code(&error), ERROR_UNSUPPORTED_VERSION);
        }
    }

    #[test]
    fn given_a_spent_request_budget_when_mapped_should_answer_retriable() {
        assert_eq!(
            record_error_code(&RecordCodecError::RequestBudgetSpent),
            ERROR_NOT_LEADER_OR_FOLLOWER
        );
    }

    #[test]
    fn given_a_codec_refusal_when_answered_should_carry_its_reason() {
        let refusal = Refusal::from(RecordCodecError::RepeatedHeaderName("a".to_string()));
        let response = partition_outcome(0, Err(refusal));

        assert_eq!(response.error_code, ERROR_INVALID_RECORD);
        assert_eq!(
            response.error_message.as_deref(),
            Some("record repeats header name a"),
            "Java shows this text instead of a generic one"
        );
        assert_eq!(
            partition_outcome(0, Err(Refusal::from(ERROR_REQUEST_TIMED_OUT))).error_message,
            None
        );
    }

    #[test]
    fn given_a_partition_past_the_send_cap_when_planned_should_answer_too_large() {
        let batch = encode_batch(&mut [record(0, b"value")]).unwrap();
        let budget = partition_budget(TEST_BUDGET, usize::MAX);
        let refusal =
            plan_partition(&budget, Zstd::Allowed, TOPIC, &entry(0, Some(batch)), 8).unwrap_err();
        assert_eq!(refusal.code, ERROR_MESSAGE_TOO_LARGE);
    }
}
