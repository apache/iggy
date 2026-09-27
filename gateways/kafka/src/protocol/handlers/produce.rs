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

use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::{IggyError, IggyMessage, Sizeable};
use kafka_protocol::messages::produce_request::PartitionProduceData;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::StrBytes;
use tokio::runtime::{Handle, RuntimeFlavor};

use crate::bridge::{BridgeError, IggyBridge, TopicTarget};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_PRODUCE, ApiVersionRange, ERROR_INVALID_RECORD, ERROR_INVALID_REQUIRED_ACKS,
    ERROR_MESSAGE_TOO_LARGE, ERROR_NONE, ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_REQUEST_TIMED_OUT,
    ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
    ERROR_UNSUPPORTED_COMPRESSION_TYPE, ERROR_UNSUPPORTED_VERSION, GatewayState, HandleOutcome,
    supported_max_version,
};
use crate::protocol::bounds_guard::validate_produce_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, respond_or_close, unsupported_version_response,
};
use crate::records::{
    Allowance, DecompressionBudget, RecordCodecError, TimestampWindow, Zstd, decode_batch,
    is_compressed, to_iggy,
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

/// Kafka's "no offset here", used for `base_offset` on any failure, for a committed send the
/// server named no offset for, and for `log_start_offset` always. The real log start costs a
/// `get_topic` round trip per request and no producer reads it yet.
const UNKNOWN_OFFSET: i64 = -1;

/// Ceiling on how long one request may spend writing, whatever the client asked for.
///
/// One request is many Iggy calls, so without this a request naming many partitions has no
/// bound. Deliberately a clock and not a partition-count cap: a cap refuses a producer writing
/// to a wide topic, and a client set on hogging the shared bridge just sends more requests.
const MAX_REQUEST_DEADLINE: Duration = Duration::from_secs(20);

/// Frame bytes per record slot. Caps one partition at 131,072 slots, about 40 MB, at the default
/// 8 MiB frame.
///
/// `GatewayState` caps how many requests hold that at once.
const FRAME_BYTES_PER_RECORD: usize = 64;

/// Partition allowances one request may decode, refused partitions included.
///
/// 64 MiB at the default 8 MiB frame, so a 1 MB request can compress 64x before the rest of it
/// answers 6.
const REQUEST_ALLOWANCES: usize = 8;

/// Blobs this large, or compressed ones, plan off the async worker.
const BLOCKING_PLAN_BYTES: usize = 64 * 1024;

const RESPONSE_BASE_BYTES: usize = 512;

/// Produce is the only request the wire protocol allows to go unanswered
/// (`acks=0`), so it gets its own path that may return [`HandleOutcome::NoResponse`].
///
/// The firewall check runs AFTER decoding the request, not before: `ApiVersions` advertises
/// Produce min=0 (see `api::advertised_min_version`) while the firewall's real floor is 3, so a
/// spec-compliant client can legitimately send Produce v0-2 with `acks=0`. Rejecting those
/// versions before reading `acks` would send an error response the client never expects,
/// desyncing the next correlation id it reads.
pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
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
    let Ok(Ok(_slot)) = tokio::time::timeout_at(deadline.at, state.produce_slots.acquire()).await
    else {
        tracing::debug!("Produce deadline passed while waiting for a slot");
        return if request.acks == ACKS_NONE {
            HandleOutcome::Close
        } else {
            respond_or_close(
                encode_uniform_response(api_version, &request, ERROR_REQUEST_TIMED_OUT),
                "Produce",
            )
        };
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
    let responses = write_request(bridge, &request, budget, zstd, deadline).await;

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
            ceiling: wait == MAX_REQUEST_DEADLINE,
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
/// Sequential: the one `IggyClient` behind the bridge is lockstep, so concurrent sends would
/// just queue on its stream mutex (see the README's "Concurrency ceiling").
async fn write_request(
    bridge: &IggyBridge,
    request: &ProduceRequest,
    budget: DecompressionBudget,
    zstd: Zstd,
    deadline: Deadline,
) -> Vec<TopicProduceResponse> {
    let max_send = bridge.max_send_bytes();
    let mut topics = Vec::with_capacity(request.topic_data.len());
    for topic in &request.topic_data {
        let name = topic.name.as_str();
        let target = bridge.topic_target(name);
        let mut responses = Vec::with_capacity(topic.partition_data.len());
        for partition in &topic.partition_data {
            let outcome = match &target {
                // Skip the decode. The answer is 7 either way.
                Ok(_) if deadline.passed() => Err(ERROR_REQUEST_TIMED_OUT.into()),
                Ok(target) => {
                    let planned = plan(&budget, zstd, name, partition, max_send);
                    match planned {
                        Ok((id, messages)) => send_by(bridge, target, name, id, messages, deadline)
                            .await
                            .map_err(Refusal::from),
                        Err(refusal) => Err(refusal),
                    }
                }
                Err(error) => Err(refusal_code(name, error).into()),
            };
            responses.push(partition_outcome(partition.index, outcome));
            // Planning is sync CPU work. Let other connections on this worker run.
            tokio::task::yield_now().await;
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
            .is_some_and(|blob| blob.len() >= BLOCKING_PLAN_BYTES || is_compressed(blob));
    if heavy && Handle::current().runtime_flavor() == RuntimeFlavor::MultiThread {
        tokio::task::block_in_place(run)
    } else {
        run()
    }
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
        .map_or(MAX_REQUEST_DEADLINE, Duration::from_millis)
        .min(MAX_REQUEST_DEADLINE)
}

/// Sends one partition's messages, or answers 7 once the deadline has passed.
///
/// 7 means nothing was written, or the send may still land and a retry duplicate it.
async fn send_by(
    bridge: &IggyBridge,
    target: &TopicTarget,
    kafka_topic: &str,
    partition_id: u32,
    messages: Vec<IggyMessage>,
    deadline: Deadline,
) -> std::result::Result<Option<u64>, i16> {
    match bridge
        .send_records(target, partition_id, messages, deadline.at)
        .await
    {
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
    }
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
        .with_log_start_offset(UNKNOWN_OFFSET)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::records::{
        Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
        Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

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
            MAX_REQUEST_DEADLINE,
            "a client asking for longer than this gateway allows gets the gateway's ceiling"
        );
        assert_eq!(request_timeout(i32::MAX), MAX_REQUEST_DEADLINE);
    }

    #[test]
    fn given_no_client_timeout_when_bounded_should_still_attempt_the_write() {
        // A value naming no budget means no preference, not "fail now".
        assert_eq!(request_timeout(0), MAX_REQUEST_DEADLINE);
        assert_eq!(request_timeout(-1), MAX_REQUEST_DEADLINE);
        assert_eq!(request_timeout(i32::MIN), MAX_REQUEST_DEADLINE);
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
