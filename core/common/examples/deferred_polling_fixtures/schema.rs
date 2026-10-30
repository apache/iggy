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

use bytes::BytesMut;
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BATCH_MESSAGE_HEADER_SIZE, BatchHeader};
use iggy_binary_protocol::codes::{
    CANCEL_DEFERRED_REQUEST_CODE, FETCH_MESSAGES_CODE, POLL_MESSAGES_CODE,
    POLL_MESSAGES_DEFERRED_CODE,
};
use iggy_binary_protocol::requests::messages::{
    DeferredPollMessagesRequest, FETCH_REPLY_MARGIN_US, FetchMessagesRequest, FetchMessagesTarget,
    MAX_DEFERRED_POLL_TIMEOUT_US, MAX_DEFERRED_POLL_WAIT_US, MIN_DEFERRED_POLL_MAX_BYTES,
    MIN_DEFERRED_POLL_REPLY_MARGIN_US, PollMessagesRequest, RawMessage, SendMessagesEncoder,
};
use iggy_binary_protocol::responses::messages::fetch_messages::FETCH_SECTION_HEADER_SIZE;
use iggy_binary_protocol::responses::messages::{
    FetchMessagesResponse, FetchMessagesSection, FetchMessagesSectionHeader,
};
use iggy_binary_protocol::{
    HEADER_SIZE, KIND_CONSUMER, KIND_CONSUMER_GROUP, MAX_PARTITIONS_PER_REQUEST,
    MAX_RECORD_REPLY_SIZE, MAX_RECORD_SIZE, PartitionContext, RequestHeader, STATUS_OK,
    WireConsumer, WireEncode, WireIdentifier, WirePartitioning, WirePollingStrategy,
};
use iggy_common::IggyError;
use serde::Serialize;
use std::collections::BTreeMap;
use std::error::Error;

pub const SCHEMA_VERSION: u32 = 1;
pub const GENERATOR: &str = "core/common/examples/deferred_polling_fixtures.rs";
/// Relative to the `iggy_common` manifest directory.
pub const CANONICAL_PATH: &str = "fixtures/deferred_polling.json";
/// Byte-identical copies each foreign SDK loads from its own test resources.
pub const LANGUAGE_COPIES: [&str; 8] = [
    "../../foreign/python/tests/fixtures/deferred_polling.json",
    "../../foreign/cpp/tests/fixtures/deferred_polling.json",
    "../../foreign/php/tests/fixtures/deferred_polling.json",
    "../../foreign/node/src/wire/fixtures/deferred_polling.json",
    "../../foreign/go/contracts/testdata/deferred_polling.json",
    "../../foreign/java/java-sdk/src/test/resources/deferred_polling.json",
    "../../foreign/csharp/Iggy_SDK_Tests/Fixtures/deferred_polling.json",
    "../../foreign/swift/Tests/IggyTests/Fixtures/deferred_polling.json",
];

const CLIENT: u64 = 7;
const SESSION: u64 = 3;
const ORIGIN_TIMESTAMP: u64 = 1_700_000_000_000_000;
const BASE_TIMESTAMP: u64 = 1_700_000_000_000_500;
const INCARNATION: u64 = 11;
const OWNER_GENERATION: u64 = 4;
const METADATA_OP: u64 = 42;

pub type FixtureResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Serialize)]
pub struct DeferredPollingFixture {
    pub schema_version: u32,
    pub generated_by: String,
    pub conventions: BTreeMap<String, String>,
    pub constants: Constants,
    pub deferred_polls: Vec<DeferredPollVector>,
    pub fetches: Vec<FetchVector>,
    pub cancellations: Vec<CancellationVector>,
    pub fetch_replies: Vec<FetchReplyVector>,
    pub refused: RefusedVectors,
}

#[derive(Debug, Serialize)]
pub struct Constants {
    pub codes: BTreeMap<String, u32>,
    pub status_ok: u32,
    pub errors: BTreeMap<String, u32>,
    pub min_deferred_poll_max_bytes: usize,
    pub max_deferred_poll_wait_us: u64,
    pub max_deferred_poll_timeout_us: u64,
    pub min_deferred_poll_reply_margin_us: u64,
    pub fetch_reply_margin_us: u64,
    pub max_partitions_per_request: u32,
    pub max_record_size: usize,
    pub max_record_reply_size: usize,
    pub request_header_size: usize,
    pub fetch_section_header_size: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Identifier {
    Numeric(u32),
    Named(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct Consumer {
    pub kind: u8,
    pub id: Identifier,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Strategy {
    pub kind: u8,
    pub value: u64,
}

/// A deferred poll (106) and its body.
#[derive(Debug, Serialize)]
pub struct DeferredPollVector {
    pub case: String,
    pub request: DeferredPollFields,
    pub body_hex: String,
}

#[derive(Debug, Serialize)]
pub struct DeferredPollFields {
    pub consumer: Consumer,
    pub stream_id: Identifier,
    pub topic_id: Identifier,
    pub partition_id: Option<u32>,
    pub strategy: Strategy,
    pub count: u32,
    pub auto_commit: bool,
    pub wait_us: u64,
    pub min_count: u32,
    pub max_bytes: u32,
    pub request_timeout_us: u64,
}

/// A fetch (109) and its body.
#[derive(Debug, Serialize)]
pub struct FetchVector {
    pub case: String,
    pub request: FetchFields,
    pub body_hex: String,
}

#[derive(Debug, Serialize)]
pub struct FetchFields {
    pub consumer: Consumer,
    pub stream_id: Identifier,
    pub topic_id: Identifier,
    pub wait_us: u64,
    pub request_timeout_us: u64,
    pub min_count: u32,
    pub count: u32,
    pub max_bytes: u32,
    pub auto_commit: bool,
    pub targets: Vec<TargetFields>,
}

#[derive(Debug, Serialize)]
pub struct TargetFields {
    pub partition_id: u32,
    pub strategy: Strategy,
    pub max_bytes: u32,
    pub incarnation: u64,
    pub owner_generation: u64,
}

/// The cancellation (108) of a deferred request: a request header without a
/// body, with the client, request and session of the request it cancels.
#[derive(Debug, Serialize)]
pub struct CancellationVector {
    pub case: String,
    pub cancels_code: u32,
    pub client: u64,
    pub request: u64,
    pub session: u64,
    pub header_hex: String,
}

/// A fetch reply body and what a client decodes from it.
#[derive(Debug, Serialize)]
pub struct FetchReplyVector {
    pub case: String,
    pub sections: Vec<SectionFields>,
    pub body_hex: String,
}

#[derive(Debug, Serialize)]
pub struct SectionFields {
    pub partition_id: u32,
    pub status: u32,
    pub current_offset: u64,
    pub messages_count: u32,
    pub context: ContextFields,
    pub next_offset: u64,
    pub records_len: u32,
    pub messages: Vec<MessageFields>,
}

#[derive(Debug, Serialize)]
pub struct ContextFields {
    pub incarnation: u64,
    pub owner_generation: u64,
    pub metadata_op: u64,
}

#[derive(Debug, Serialize)]
pub struct MessageFields {
    pub offset: u64,
    /// Decimal, because a 128-bit id does not fit a JSON number.
    pub id: String,
    /// The batch base timestamp that the server stamped.
    pub timestamp: u64,
    pub origin_timestamp: u64,
    pub payload: String,
}

#[derive(Debug, Serialize)]
pub struct RefusedVectors {
    /// Bodies that fail to decode or to validate.
    pub fetches: Vec<RefusedVector>,
    /// Bodies that fail to decode.
    pub fetch_replies: Vec<RefusedVector>,
}

#[derive(Debug, Serialize)]
pub struct RefusedVector {
    pub case: String,
    pub body_hex: String,
}

/// One section of a reply to build: its header fields and its messages as
/// `(id, payload)`, stored from `first_offset` on.
struct SectionSpec {
    partition_id: u32,
    status: u32,
    current_offset: u64,
    next_offset: u64,
    first_offset: u64,
    messages: &'static [(u128, &'static str)],
}

pub fn render() -> FixtureResult<String> {
    let mut json = serde_json::to_string_pretty(&build()?)?;
    json.push('\n');
    Ok(json)
}

/// The fixture, built from the live codecs, so a codec change shows up as a
/// stale file in the drift test.
pub fn build() -> FixtureResult<DeferredPollingFixture> {
    Ok(DeferredPollingFixture {
        schema_version: SCHEMA_VERSION,
        generated_by: GENERATOR.to_string(),
        conventions: conventions(),
        constants: constants(),
        deferred_polls: deferred_polls()?,
        fetches: fetches()?,
        cancellations: cancellations()?,
        fetch_replies: fetch_replies()?,
        refused: refused()?,
    })
}

fn conventions() -> BTreeMap<String, String> {
    [
        (
            "body_hex",
            "Every *_hex field is lowercase hex without separators. body_hex is the command payload without framing.",
        ),
        (
            "header_hex",
            "header_hex is the whole request header of a cancellation, which has no body, before the transport stamps its checksum and checksum_body.",
        ),
        (
            "strategy",
            "strategy.kind is the wire kind: 1 offset, 2 timestamp, 3 first, 4 last, 5 next.",
        ),
        (
            "status",
            "A section status of status_ok was read. Any other status is the error code of that partition alone, and its section holds no records.",
        ),
        (
            "refused",
            "Every refused fetch fails to decode or to validate. Every refused fetch reply fails to decode.",
        ),
        (
            "oversized",
            "A reply may carry a first record above max_bytes, up to max_record_size, so a client reply cap is at least max_record_reply_size.",
        ),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value.to_string()))
    .collect()
}

fn constants() -> Constants {
    Constants {
        codes: [
            ("poll_messages", POLL_MESSAGES_CODE),
            ("poll_messages_deferred", POLL_MESSAGES_DEFERRED_CODE),
            ("cancel_deferred_request", CANCEL_DEFERRED_REQUEST_CODE),
            ("fetch_messages", FETCH_MESSAGES_CODE),
        ]
        .into_iter()
        .map(|(name, code)| (name.to_string(), code))
        .collect(),
        status_ok: STATUS_OK,
        errors: [
            IggyError::TransientNotCommitted,
            IggyError::TransientNotAccepted,
            IggyError::InvalidSizeBytes,
            IggyError::NotPrimary,
            IggyError::DeferredPollCapacityExceeded,
            IggyError::TooManyConsumerOffsets,
            IggyError::HistoryUnavailable,
            IggyError::PartitionLaneBlocked(0, 0),
            IggyError::FetchProgressUncertain(0),
        ]
        .into_iter()
        .map(|error| (<&'static str>::from(&error).to_string(), error.as_code()))
        .collect(),
        min_deferred_poll_max_bytes: MIN_DEFERRED_POLL_MAX_BYTES,
        max_deferred_poll_wait_us: MAX_DEFERRED_POLL_WAIT_US,
        max_deferred_poll_timeout_us: MAX_DEFERRED_POLL_TIMEOUT_US,
        min_deferred_poll_reply_margin_us: MIN_DEFERRED_POLL_REPLY_MARGIN_US,
        fetch_reply_margin_us: FETCH_REPLY_MARGIN_US,
        max_partitions_per_request: MAX_PARTITIONS_PER_REQUEST,
        max_record_size: MAX_RECORD_SIZE,
        max_record_reply_size: MAX_RECORD_REPLY_SIZE,
        request_header_size: HEADER_SIZE,
        fetch_section_header_size: FETCH_SECTION_HEADER_SIZE,
    }
}

fn deferred_polls() -> FixtureResult<Vec<DeferredPollVector>> {
    [
        (
            "consumer_offset_without_commit",
            Consumer {
                kind: KIND_CONSUMER,
                id: Identifier::Numeric(1),
            },
            Strategy { kind: 1, value: 41 },
            false,
        ),
        (
            "group_next_with_commit",
            Consumer {
                kind: KIND_CONSUMER_GROUP,
                id: Identifier::Named("orders-group".to_string()),
            },
            Strategy { kind: 5, value: 0 },
            true,
        ),
    ]
    .into_iter()
    .map(|(case, consumer, strategy, auto_commit)| {
        let request = DeferredPollFields {
            consumer,
            stream_id: Identifier::Numeric(1),
            topic_id: Identifier::Named("orders".to_string()),
            partition_id: Some(2),
            strategy,
            count: 100,
            auto_commit,
            wait_us: 5_000_000,
            min_count: 1,
            max_bytes: 1024 * 1024,
            request_timeout_us: 6_000_000,
        };
        let wire = DeferredPollMessagesRequest {
            poll: PollMessagesRequest {
                consumer: wire_consumer(&request.consumer)?,
                stream_id: wire_identifier(&request.stream_id)?,
                topic_id: wire_identifier(&request.topic_id)?,
                partition_id: request.partition_id,
                strategy: wire_strategy(request.strategy),
                count: request.count,
                auto_commit: request.auto_commit,
            },
            wait_us: request.wait_us,
            min_count: request.min_count,
            max_bytes: request.max_bytes,
            request_timeout_us: request.request_timeout_us,
        };
        Ok(DeferredPollVector {
            case: case.to_string(),
            body_hex: hex::encode(wire.to_bytes()),
            request,
        })
    })
    .collect()
}

fn fetches() -> FixtureResult<Vec<FetchVector>> {
    [
        (
            "consumer_two_targets_without_commit",
            Consumer {
                kind: KIND_CONSUMER,
                id: Identifier::Numeric(1),
            },
            false,
            vec![
                (1, Strategy { kind: 1, value: 41 }),
                (3, Strategy { kind: 5, value: 0 }),
            ],
        ),
        (
            "group_three_targets_with_commit",
            Consumer {
                kind: KIND_CONSUMER_GROUP,
                id: Identifier::Numeric(9),
            },
            true,
            vec![
                (1, Strategy { kind: 5, value: 0 }),
                (2, Strategy { kind: 5, value: 0 }),
                (4, Strategy { kind: 1, value: 7 }),
            ],
        ),
    ]
    .into_iter()
    .map(|(case, consumer, auto_commit, targets)| {
        let request = fetch_fields(consumer, auto_commit, targets);
        Ok(FetchVector {
            case: case.to_string(),
            body_hex: hex::encode(wire_fetch(&request)?.to_bytes()),
            request,
        })
    })
    .collect()
}

fn fetch_fields(
    consumer: Consumer,
    auto_commit: bool,
    targets: Vec<(u32, Strategy)>,
) -> FetchFields {
    // A group member names the owner generation of its partitions.
    let owner_generation = if consumer.kind == KIND_CONSUMER_GROUP {
        OWNER_GENERATION
    } else {
        0
    };
    FetchFields {
        consumer,
        stream_id: Identifier::Numeric(1),
        topic_id: Identifier::Numeric(2),
        wait_us: 5_000_000,
        request_timeout_us: 6_000_000,
        min_count: 1,
        count: 1000,
        max_bytes: 1024 * 1024,
        auto_commit,
        targets: targets
            .into_iter()
            .map(|(partition_id, strategy)| TargetFields {
                partition_id,
                strategy,
                max_bytes: 512 * 1024,
                incarnation: INCARNATION,
                owner_generation,
            })
            .collect(),
    }
}

fn cancellations() -> FixtureResult<Vec<CancellationVector>> {
    [
        ("cancel_deferred_poll", POLL_MESSAGES_DEFERRED_CODE, 21),
        ("cancel_fetch", FETCH_MESSAGES_CODE, 22),
    ]
    .into_iter()
    .map(|(case, code, request)| {
        let original = RequestHeader::for_request(code, u128::from(CLIENT), request, SESSION, &[])?;
        let cancellation = original
            .deferred_cancellation()
            .ok_or("a deferred request has a cancellation")?;
        Ok(CancellationVector {
            case: case.to_string(),
            cancels_code: code,
            client: CLIENT,
            request,
            session: SESSION,
            header_hex: hex::encode(bytemuck::bytes_of(&cancellation)),
        })
    })
    .collect()
}

fn fetch_replies() -> FixtureResult<Vec<FetchReplyVector>> {
    let replies: [(&str, Vec<SectionSpec>); 3] = [
        (
            "ok_section_and_empty_start_section",
            vec![
                SectionSpec {
                    partition_id: 1,
                    status: STATUS_OK,
                    current_offset: 44,
                    next_offset: 44,
                    first_offset: 41,
                    messages: &[(0x0102, "first"), (0x0103, "second"), (0x0104, "third")],
                },
                SectionSpec {
                    partition_id: 3,
                    status: STATUS_OK,
                    current_offset: 9,
                    next_offset: 10,
                    first_offset: 10,
                    messages: &[],
                },
            ],
        ),
        (
            "ok_section_beside_error_sections",
            vec![
                SectionSpec {
                    partition_id: 1,
                    status: IggyError::NotPrimary.as_code(),
                    current_offset: 0,
                    next_offset: 0,
                    first_offset: 0,
                    messages: &[],
                },
                SectionSpec {
                    partition_id: 2,
                    status: STATUS_OK,
                    current_offset: 7,
                    next_offset: 7,
                    first_offset: 6,
                    messages: &[(u128::MAX, "only")],
                },
                SectionSpec {
                    partition_id: 4,
                    status: IggyError::TransientNotAccepted.as_code(),
                    current_offset: 0,
                    next_offset: 0,
                    first_offset: 0,
                    messages: &[],
                },
            ],
        ),
        ("no_sections", Vec::new()),
    ];
    replies
        .into_iter()
        .map(|(case, specs)| {
            let records = specs
                .iter()
                .map(|spec| stored_batch(spec))
                .collect::<FixtureResult<Vec<_>>>()?;
            let sections: Vec<SectionFields> = specs
                .iter()
                .zip(&records)
                .map(|(spec, records)| section_fields(spec, records))
                .collect::<FixtureResult<_>>()?;
            let response = FetchMessagesResponse {
                sections: sections
                    .iter()
                    .zip(&records)
                    .map(|(section, records)| FetchMessagesSection {
                        header: section_header(section),
                        records,
                    })
                    .collect(),
            };
            Ok(FetchReplyVector {
                case: case.to_string(),
                body_hex: hex::encode(response.to_bytes()?),
                sections,
            })
        })
        .collect()
}

fn section_fields(spec: &SectionSpec, records: &[u8]) -> FixtureResult<SectionFields> {
    Ok(SectionFields {
        partition_id: spec.partition_id,
        status: spec.status,
        current_offset: spec.current_offset,
        messages_count: u32::try_from(spec.messages.len())?,
        context: if spec.status == STATUS_OK {
            ContextFields {
                incarnation: INCARNATION,
                owner_generation: OWNER_GENERATION,
                metadata_op: METADATA_OP,
            }
        } else {
            ContextFields {
                incarnation: 0,
                owner_generation: 0,
                metadata_op: 0,
            }
        },
        next_offset: spec.next_offset,
        records_len: u32::try_from(records.len())?,
        messages: spec
            .messages
            .iter()
            .zip(spec.first_offset..)
            .map(|((id, payload), offset)| MessageFields {
                offset,
                id: id.to_string(),
                timestamp: BASE_TIMESTAMP,
                origin_timestamp: ORIGIN_TIMESTAMP,
                payload: (*payload).to_string(),
            })
            .collect(),
    })
}

/// The records of `spec` as the server stores and serves them: one batch,
/// stamped with its partition, base offset and base timestamp.
fn stored_batch(spec: &SectionSpec) -> FixtureResult<Vec<u8>> {
    if spec.messages.is_empty() {
        return Ok(Vec::new());
    }
    let messages: Vec<RawMessage<'_>> = spec
        .messages
        .iter()
        .map(|(id, payload)| RawMessage {
            id: *id,
            origin_timestamp: ORIGIN_TIMESTAMP,
            headers: None,
            payload: payload.as_bytes(),
        })
        .collect();
    let mut body = BytesMut::new();
    SendMessagesEncoder::encode(
        &mut body,
        &WireIdentifier::numeric(1),
        &WireIdentifier::numeric(2),
        &WirePartitioning::PartitionId(spec.partition_id),
        &messages,
    )?;
    let blob: usize = spec
        .messages
        .iter()
        .map(|(_, payload)| BATCH_MESSAGE_HEADER_SIZE + payload.len())
        .sum();
    let mut batch = body[body.len() - BATCH_HEADER_SIZE - blob..].to_vec();
    let mut header = BatchHeader::decode(&batch)?;
    header.partition_id = u64::from(spec.partition_id);
    header.base_offset = spec.first_offset;
    header.base_timestamp = BASE_TIMESTAMP;
    header.batch_checksum = header.checksum_for_blob(&batch[BATCH_HEADER_SIZE..]);
    header.encode_into(&mut batch);
    Ok(batch)
}

fn section_header(section: &SectionFields) -> FetchMessagesSectionHeader {
    FetchMessagesSectionHeader {
        partition_id: section.partition_id,
        status: section.status,
        current_offset: section.current_offset,
        messages_count: section.messages_count,
        context: PartitionContext {
            incarnation: section.context.incarnation,
            owner_generation: section.context.owner_generation,
            metadata_op: section.context.metadata_op,
        },
        next_offset: section.next_offset,
        records_len: section.records_len,
    }
}

fn refused() -> FixtureResult<RefusedVectors> {
    let valid = || {
        fetch_fields(
            Consumer {
                kind: KIND_CONSUMER,
                id: Identifier::Numeric(1),
            },
            false,
            vec![(1, Strategy { kind: 1, value: 0 })],
        )
    };
    let mut no_targets = valid();
    no_targets.targets.clear();
    let mut duplicate = valid();
    duplicate.targets.push(TargetFields {
        partition_id: 1,
        strategy: Strategy { kind: 5, value: 0 },
        max_bytes: 512 * 1024,
        incarnation: INCARNATION,
        owner_generation: 0,
    });
    let mut first_start = valid();
    first_start.targets[0].strategy = Strategy { kind: 3, value: 0 };
    let mut min_count_above_count = valid();
    min_count_above_count.min_count = min_count_above_count.count + 1;
    let mut target_above_request_bytes = valid();
    target_above_request_bytes.targets[0].max_bytes = target_above_request_bytes.max_bytes + 1;
    let mut timeout_without_margin = valid();
    timeout_without_margin.request_timeout_us = timeout_without_margin.wait_us;
    let fetches = [
        ("no_targets", no_targets),
        ("duplicate_partition", duplicate),
        ("first_start_point", first_start),
        ("min_count_above_count", min_count_above_count),
        (
            "target_bytes_above_request_bytes",
            target_above_request_bytes,
        ),
        ("timeout_without_reply_margin", timeout_without_margin),
    ]
    .into_iter()
    .map(|(case, fields)| {
        Ok(RefusedVector {
            case: case.to_string(),
            body_hex: hex::encode(wire_fetch(&fields)?.to_bytes()),
        })
    })
    .collect::<FixtureResult<_>>()?;

    let error_with_records = {
        let mut head = vec![0; FetchMessagesResponse::head_size(1)];
        head[..4].copy_from_slice(&1u32.to_le_bytes());
        let header = FetchMessagesSectionHeader {
            partition_id: 1,
            status: IggyError::NotPrimary.as_code(),
            current_offset: 0,
            messages_count: 0,
            context: PartitionContext::default(),
            next_offset: 0,
            records_len: 4,
        };
        head[4..].copy_from_slice(&header.to_le_bytes());
        head.extend_from_slice(&[0; 4]);
        head
    };
    let truncated_table = {
        let mut head = vec![0; FetchMessagesResponse::head_size(2)];
        head[..4].copy_from_slice(&2u32.to_le_bytes());
        head.truncate(head.len() - 1);
        head
    };
    let fetch_replies = [
        ("error_section_with_records", error_with_records),
        ("truncated_table", truncated_table),
    ]
    .into_iter()
    .map(|(case, body)| RefusedVector {
        case: case.to_string(),
        body_hex: hex::encode(body),
    })
    .collect();
    Ok(RefusedVectors {
        fetches,
        fetch_replies,
    })
}

fn wire_fetch(fields: &FetchFields) -> FixtureResult<FetchMessagesRequest> {
    Ok(FetchMessagesRequest {
        consumer: wire_consumer(&fields.consumer)?,
        stream_id: wire_identifier(&fields.stream_id)?,
        topic_id: wire_identifier(&fields.topic_id)?,
        wait_us: fields.wait_us,
        request_timeout_us: fields.request_timeout_us,
        min_count: fields.min_count,
        count: fields.count,
        max_bytes: fields.max_bytes,
        auto_commit: fields.auto_commit,
        targets: fields
            .targets
            .iter()
            .map(|target| FetchMessagesTarget {
                partition_id: target.partition_id,
                strategy: wire_strategy(target.strategy),
                max_bytes: target.max_bytes,
                incarnation: target.incarnation,
                owner_generation: target.owner_generation,
            })
            .collect(),
    })
}

fn wire_consumer(consumer: &Consumer) -> FixtureResult<WireConsumer> {
    let id = wire_identifier(&consumer.id)?;
    Ok(if consumer.kind == KIND_CONSUMER_GROUP {
        WireConsumer::consumer_group(id)
    } else {
        WireConsumer::consumer(id)
    })
}

fn wire_identifier(identifier: &Identifier) -> FixtureResult<WireIdentifier> {
    Ok(match identifier {
        Identifier::Numeric(id) => WireIdentifier::numeric(*id),
        Identifier::Named(name) => WireIdentifier::named(name.as_str())?,
    })
}

const fn wire_strategy(strategy: Strategy) -> WirePollingStrategy {
    WirePollingStrategy {
        kind: strategy.kind,
        value: strategy.value,
    }
}
