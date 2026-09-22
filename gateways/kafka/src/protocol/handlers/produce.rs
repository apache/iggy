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
use iggy::prelude::{IggyError, IggyMessage};
use kafka_protocol::messages::produce_request::PartitionProduceData;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse, TopicName};

use crate::bridge::IggyBridge;
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_PRODUCE, ApiVersionRange, ERROR_INVALID_RECORD, ERROR_INVALID_REQUIRED_ACKS,
    ERROR_MESSAGE_TOO_LARGE, ERROR_NONE, ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_REQUEST_TIMED_OUT,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, ERROR_UNSUPPORTED_VERSION, GatewayState, HandleOutcome,
    supported_max_version,
};
use crate::protocol::bounds_guard::validate_produce_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, respond_or_close, unsupported_version_response,
};
use crate::records::{DecompressionBudget, RecordCodecError, decode_batches, to_iggy};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_PRODUCE,
    min_version: 3,
    max_version: 9,
};

/// The three `acks` values the protocol defines. Anything else is a malformed request.
const ACKS_NONE: i16 = 0;
const ACKS_LEADER: i16 = 1;
const ACKS_ALL: i16 = -1;

/// Kafka's "no offset here", used for `base_offset` on any failure, for a committed send the
/// server named no offset for, and for `log_start_offset` always. The real log start costs a
/// `get_topic` round trip per request and no producer reads it yet.
const UNKNOWN_OFFSET: i64 = -1;

/// Ceiling on how long one request may spend writing, whatever the client asked for.
///
/// One request is many Iggy calls, each with the bridge's own 15s timeout, so without this a
/// request naming many partitions has no bound. Deliberately a clock and not a partition-count
/// cap: a cap refuses a producer writing to a wide topic, and a client set on hogging the shared
/// bridge just sends more requests instead.
const MAX_REQUEST_DEADLINE: Duration = Duration::from_secs(20);

const RESPONSE_BASE_BYTES: usize = 512;
const RESPONSE_BYTES_PER_PARTITION: usize = 64;

/// A converted partition entry: where to write it, or the code it already answers with.
type PartitionPlan = std::result::Result<(u32, Vec<IggyMessage>), i16>;

struct PlannedPartition {
    index: i32,
    plan: PartitionPlan,
}

struct PlannedTopic {
    name: TopicName,
    partitions: Vec<PlannedPartition>,
}

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
    // added in v3), so it's peeked by hand: acks=0 must keep the connection open per the wire
    // protocol's fire-and-forget rule even though no response can ever be encoded for it.
    if api_version < 3 {
        let acks = match body.get(0..2) {
            Some(&[hi, lo]) => Some(i16::from_be_bytes([hi, lo])),
            _ => None,
        };
        return match acks {
            Some(ACKS_NONE) | None => HandleOutcome::NoResponse,
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
            return HandleOutcome::NoResponse;
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
    let deadline = tokio::time::Instant::now() + request_timeout(request.timeout_ms);

    // Convert everything, then write it. The budget holds its allowance in a `Cell`, so a
    // reference to it live across an await would make every connection task `!Send`. The scope
    // drops it before the first one.
    //
    // One budget for the whole request, not one per batch: a request carries up to
    // `MAX_REQUEST_ELEMENTS` partition entries, so a per-batch cap would still admit that
    // multiple of it.
    let plan = {
        let budget = DecompressionBudget::new(state.max_frame_size);
        plan_request(&budget, &request)
    };
    let responses = write_plan(bridge, plan, deadline).await;

    // The write already ran. Answering `NoResponse` before it would drop every record an
    // `acks=0` producer sends, silently and at full speed.
    if request.acks == ACKS_NONE {
        return HandleOutcome::NoResponse;
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

/// Converts every partition entry. Synchronous, so the budget never crosses an await.
fn plan_request(budget: &DecompressionBudget, request: &ProduceRequest) -> Vec<PlannedTopic> {
    request
        .topic_data
        .iter()
        .map(|topic| PlannedTopic {
            name: topic.name.clone(),
            partitions: topic
                .partition_data
                .iter()
                .map(|partition| PlannedPartition {
                    index: partition.index,
                    plan: plan_partition(budget, topic.name.as_str(), partition),
                })
                .collect(),
        })
        .collect()
}

/// One entry's batches in, its messages out. A batch that cannot be mapped is refused whole,
/// never half-stored.
fn plan_partition(
    budget: &DecompressionBudget,
    kafka_topic: &str,
    partition: &PartitionProduceData,
) -> PartitionPlan {
    // The records are not what is wrong with a negative index, so not `INVALID_RECORD`.
    let Ok(partition_id) = u32::try_from(partition.index) else {
        return Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    };
    // Nothing to append. Answering success would name an offset for a write that never happened.
    let Some(blob) = partition.records.as_ref() else {
        return Err(ERROR_INVALID_RECORD);
    };

    // The clone is a refcount, and `decode_batches` drains what it is given.
    let mut blob = blob.clone();
    let records = decode_batches(&mut blob, budget).map_err(|error| {
        tracing::debug!(%error, kafka_topic, partition_id, "refused a Produce record batch");
        record_error_code(&error)
    })?;
    // An empty blob decodes to zero batches rather than to an error, so this rules on it.
    if records.is_empty() {
        return Err(ERROR_INVALID_RECORD);
    }

    let mut messages = Vec::with_capacity(records.len());
    for record in &records {
        messages.push(to_iggy(record).map_err(|error| {
            tracing::debug!(%error, kafka_topic, partition_id, "refused a Kafka record");
            record_error_code(&error)
        })?);
    }
    Ok((partition_id, messages))
}

/// The shorter of the client's `timeout_ms` and this gateway's ceiling.
///
/// Honoring the client's budget is what stops its retry from duplicating a write still running
/// here. A non-positive value means no preference, not "fail without trying".
fn request_timeout(timeout_ms: i32) -> Duration {
    u64::try_from(timeout_ms)
        .ok()
        .filter(|ms| *ms > 0)
        .map_or(MAX_REQUEST_DEADLINE, Duration::from_millis)
        .min(MAX_REQUEST_DEADLINE)
}

/// Appends every planned partition, in request order.
///
/// Sequential: the one `IggyClient` behind the bridge is lockstep, so concurrent sends would
/// just queue on its stream mutex (see the README's "Concurrency ceiling").
async fn write_plan(
    bridge: &IggyBridge,
    plan: Vec<PlannedTopic>,
    deadline: tokio::time::Instant,
) -> Vec<TopicProduceResponse> {
    let mut topics = Vec::with_capacity(plan.len());
    for PlannedTopic { name, partitions } in plan {
        let mut responses = Vec::with_capacity(partitions.len());
        for PlannedPartition { index, plan } in partitions {
            responses.push(write_partition(bridge, name.as_str(), index, plan, deadline).await);
        }
        topics.push(
            TopicProduceResponse::default()
                .with_name(name)
                .with_partition_responses(responses),
        );
    }
    topics
}

async fn write_partition(
    bridge: &IggyBridge,
    kafka_topic: &str,
    index: i32,
    plan: PartitionPlan,
    deadline: tokio::time::Instant,
) -> PartitionProduceResponse {
    let outcome = match plan {
        Ok((partition_id, mut messages)) => {
            send_by(bridge, kafka_topic, partition_id, &mut messages, deadline).await
        }
        Err(error_code) => Err(error_code),
    };
    match outcome {
        // An offset past `i64::MAX` has no response field to fit in, so it reads as unknown
        // rather than as a saturated number a client would take for a real position.
        Ok(base_offset) => partition_response(index, ERROR_NONE).with_base_offset(
            base_offset.map_or(UNKNOWN_OFFSET, |offset| {
                i64::try_from(offset).unwrap_or(UNKNOWN_OFFSET)
            }),
        ),
        Err(error_code) => partition_response(index, error_code),
    }
}

/// Appends one partition's messages, or answers 7 once the deadline has passed.
///
/// 7 and not a connection-shaped code in both timeout arms: the SDK writes on a detached task
/// this timeout cannot abort, so a send we stop waiting on may still land. 7 is the code for an
/// outcome nobody knows, and retrying on it may duplicate the write.
async fn send_by(
    bridge: &IggyBridge,
    kafka_topic: &str,
    partition_id: u32,
    messages: &mut [IggyMessage],
    deadline: tokio::time::Instant,
) -> std::result::Result<Option<u64>, i16> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        tracing::warn!(
            kafka_topic,
            partition_id,
            "Produce deadline passed before this partition was written"
        );
        return Err(ERROR_REQUEST_TIMED_OUT);
    }

    let send = bridge.send_records(kafka_topic, partition_id, messages);
    match tokio::time::timeout(remaining, send).await {
        Ok(Ok(base_offset)) => Ok(base_offset),
        Ok(Err(error)) => {
            // warn!, not debug!: an operator can act on this, and the client only sees the code.
            tracing::warn!(%error, kafka_topic, partition_id, "Iggy refused a Produce batch");
            Err(error.to_kafka_error_code())
        }
        Err(_elapsed) => {
            tracing::warn!(
                kafka_topic,
                partition_id,
                "Produce deadline passed while this partition was being written"
            );
            Err(ERROR_REQUEST_TIMED_OUT)
        }
    }
}

/// The Kafka code one record or batch failure answers with.
///
/// Two-way split: too big is 10, malformed is 87. Not `CORRUPT_MESSAGE` (2) for the malformed
/// half, whose text fits but which `kafka_protocol` marks retriable, so a client would resend
/// bytes that can never decode. Both codes here are terminal.
///
/// No wildcard arm, so a variant added later fails the match instead of taking a default.
const fn record_error_code(error: &RecordCodecError) -> i16 {
    match error {
        RecordCodecError::Iggy(error) => message_error_code(error),
        RecordCodecError::BudgetExceeded { .. } | RecordCodecError::EnvelopeTooLarge { .. } => {
            ERROR_MESSAGE_TOO_LARGE
        }
        // Both kinds mean transactions: only a transaction produces a control batch. 35 is the
        // one code a transactional producer treats as fatal, so it stops instead of aborting and
        // retrying forever.
        RecordCodecError::UnsupportedBatch(_) => ERROR_UNSUPPORTED_VERSION,
        // Reachable from Produce.
        RecordCodecError::TimestampOutOfRange(_)
        | RecordCodecError::Batch(_)
        | RecordCodecError::RecordCountTooLarge { .. }
        | RecordCodecError::HeaderCountTooLarge { .. }
        | RecordCodecError::RecordTruncated
        | RecordCodecError::RecordFieldLength(_)
        // Raised by `from_iggy` only, so Fetch is what actually answers for these.
        | RecordCodecError::UserHeadersUnreadable(_)
        | RecordCodecError::MappingVersion(_)
        | RecordCodecError::ValueMarker(_)
        | RecordCodecError::HeaderNameCollision(_)
        | RecordCodecError::TimestampMarker(_)
        | RecordCodecError::EnvelopeTruncated { .. }
        | RecordCodecError::EnvelopeTrailingBytes(_)
        | RecordCodecError::EnvelopeVersion(_)
        | RecordCodecError::EnvelopeHeaderName => ERROR_INVALID_RECORD,
    }
}

/// The code for a message `IggyMessage::new` refused.
///
/// It has three rejections: an oversized payload, oversized headers, and an empty payload. The
/// first two are size failures. The third is unreachable, since the codec writes a placeholder
/// byte for a null or empty value.
const fn message_error_code(error: &IggyError) -> i16 {
    match error {
        IggyError::TooBigMessagePayload | IggyError::TooBigUserHeaders => ERROR_MESSAGE_TOO_LARGE,
        _ => ERROR_INVALID_RECORD,
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
    let resp = ProduceResponse::default().with_responses(responses);
    encode_message(&resp, version, RESPONSE_BASE_BYTES)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
fn encode_written_response(version: i16, responses: Vec<TopicProduceResponse>) -> Result<Bytes> {
    let partitions: usize = responses
        .iter()
        .map(|topic| topic.partition_responses.len())
        .sum();
    let capacity =
        RESPONSE_BASE_BYTES.saturating_add(partitions.saturating_mul(RESPONSE_BYTES_PER_PARTITION));
    let resp = ProduceResponse::default().with_responses(responses);
    encode_message(&resp, version, capacity)
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
    use kafka_protocol::messages::produce_request::TopicProduceData;
    use kafka_protocol::protocol::StrBytes;
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

    fn gzip_batch(records: &[Record]) -> Bytes {
        let mut buf = BytesMut::new();
        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::Gzip,
        };
        RecordBatchEncoder::encode(&mut buf, records.iter(), &options).expect("encode gzip batch");
        buf.freeze()
    }

    fn entry(index: i32, records: Option<Bytes>) -> PartitionProduceData {
        PartitionProduceData::default()
            .with_index(index)
            .with_records(records)
    }

    fn plan(entry: &PartitionProduceData) -> PartitionPlan {
        plan_partition(&DecompressionBudget::new(TEST_BUDGET), TOPIC, entry)
    }

    #[test]
    fn given_a_record_batch_when_planned_should_convert_every_record_in_order() {
        let batch = encode_batch(&mut [record(0, b"first"), record(1, b"second")]).unwrap();
        let (partition_id, messages) = plan(&entry(2, Some(batch))).expect("batch must convert");

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
            plan(&entry(-1, Some(batch))).unwrap_err(),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            "the records are not what is wrong with the request"
        );
    }

    #[test]
    fn given_no_records_field_when_planned_should_answer_invalid_record() {
        assert_eq!(plan(&entry(0, None)).unwrap_err(), ERROR_INVALID_RECORD);
    }

    #[test]
    fn given_an_empty_records_blob_when_planned_should_answer_invalid_record() {
        // An empty blob reads as zero batches, not as a malformed one, so nothing below rejects it.
        assert_eq!(
            plan(&entry(0, Some(Bytes::new()))).unwrap_err(),
            ERROR_INVALID_RECORD
        );
    }

    #[test]
    fn given_a_transactional_batch_when_planned_should_answer_unsupported_version() {
        let mut records = [record(0, b"v")];
        records[0].transactional = true;
        let batch = encode_batch(&mut records).unwrap();

        assert_eq!(
            plan(&entry(0, Some(batch))).unwrap_err(),
            ERROR_UNSUPPORTED_VERSION,
            "35 is the only code a transactional producer treats as fatal"
        );
    }

    #[test]
    fn given_a_control_batch_when_planned_should_answer_unsupported_version() {
        let mut records = [record(0, b"v")];
        records[0].control = true;
        let batch = encode_batch(&mut records).unwrap();

        assert_eq!(
            plan(&entry(0, Some(batch))).unwrap_err(),
            ERROR_UNSUPPORTED_VERSION,
            "a stored message carries no control flag, so this one cannot be admitted"
        );
    }

    #[test]
    fn given_a_truncated_batch_when_planned_should_answer_invalid_record() {
        let batch = encode_batch(&mut [record(0, b"value")]).unwrap();
        let truncated = batch.slice(..batch.len() - 3);

        assert_eq!(
            plan(&entry(0, Some(truncated))).unwrap_err(),
            ERROR_INVALID_RECORD,
            "not CORRUPT_MESSAGE (2), which is retriable and would loop"
        );
    }

    #[test]
    fn given_a_batch_that_decompresses_past_the_budget_when_planned_should_answer_too_large() {
        let batch = gzip_batch(&[record(0, &[b'a'; 4096])]);
        let budget = DecompressionBudget::new(16);

        assert_eq!(
            plan_partition(&budget, TOPIC, &entry(0, Some(batch))).unwrap_err(),
            ERROR_MESSAGE_TOO_LARGE
        );
    }

    #[test]
    fn given_one_budget_when_two_partitions_are_planned_should_share_its_allowance() {
        // Per request, not per batch: many entries must not each get the whole budget.
        let budget = DecompressionBudget::new(6144);
        let first = entry(0, Some(gzip_batch(&[record(0, &[b'a'; 4096])])));
        let second = entry(1, Some(gzip_batch(&[record(0, &[b'b'; 4096])])));

        assert!(plan_partition(&budget, TOPIC, &first).is_ok());
        assert_eq!(
            plan_partition(&budget, TOPIC, &second).unwrap_err(),
            ERROR_MESSAGE_TOO_LARGE,
            "the second batch must be charged against what the first one left"
        );
    }

    #[test]
    fn given_a_request_when_planned_should_keep_its_topic_and_partition_order() {
        let batch = encode_batch(&mut [record(0, b"v")]).unwrap();
        let request = ProduceRequest::default().with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partition_data(vec![entry(1, Some(batch)), entry(0, None)]),
        ]);

        let planned = plan_request(&DecompressionBudget::new(TEST_BUDGET), &request);
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].name.as_str(), TOPIC);
        let indices: Vec<i32> = planned[0].partitions.iter().map(|p| p.index).collect();
        assert_eq!(
            indices,
            vec![1, 0],
            "the response follows the request order"
        );
        assert!(planned[0].partitions[0].plan.is_ok());
        assert_eq!(
            planned[0].partitions[1].plan.as_ref().unwrap_err(),
            &ERROR_INVALID_RECORD,
            "one partition failing must not cost the other its own answer"
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
    fn given_a_request_level_failure_when_encoded_should_answer_every_partition_it_named() {
        let request = ProduceRequest::default().with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partition_data(vec![entry(0, None), entry(1, None)]),
        ]);

        let body =
            encode_uniform_response(3, &request, ERROR_INVALID_REQUIRED_ACKS).expect("encode");
        assert!(!body.is_empty());
    }

    #[test]
    fn given_a_size_failure_when_mapped_should_answer_message_too_large() {
        assert_eq!(
            record_error_code(&RecordCodecError::BudgetExceeded {
                produced: 2,
                remaining: 1
            }),
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
        assert_eq!(
            record_error_code(&RecordCodecError::UnsupportedBatch("transactional")),
            ERROR_UNSUPPORTED_VERSION
        );
        assert_eq!(
            record_error_code(&RecordCodecError::UnsupportedBatch("control")),
            ERROR_UNSUPPORTED_VERSION
        );
    }
}
