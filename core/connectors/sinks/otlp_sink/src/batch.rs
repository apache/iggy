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

//! Turns the messages of one `consume` call into encoded OTLP export requests.
//!
//! Every message holds the protobuf bytes of one export request, and the bytes of several
//! messages are joined into one request (see `ExportRequest`). A message is forwarded as the
//! bytes it arrived in, so fields this sink does not know reach the receiver.
//!
//! Messages are packed whole: every request carries complete messages, so a failed request
//! accounts for an exact number of them. A request grows until the next message would take
//! it past `max_request_bytes`. A single message larger than that is sent on its own.

use crate::signal::ExportRequest;
use iggy_connector_sdk::{ConsumedMessage, Error, Payload};

/// One encoded export request and the messages it carries.
#[derive(Debug)]
pub(crate) struct Packed {
    pub body: Vec<u8>,
    pub messages: u64,
    pub items: u64,
    /// One message larger than `max_request_bytes`, sent on its own.
    pub oversized: bool,
}

/// The requests built from one `consume` call, plus what could not be turned into OTLP.
#[derive(Debug)]
pub(crate) struct Prepared {
    pub requests: Vec<Packed>,
    pub dropped: u64,
    pub first_error: Option<Error>,
}

/// Check that each message holds one OTLP export request, then pack them.
pub(crate) fn pack_requests<R: ExportRequest>(
    messages: Vec<ConsumedMessage>,
    max_request_bytes: usize,
) -> Prepared {
    let mut encoded = Vec::with_capacity(messages.len());
    let mut dropped = 0u64;
    let mut first_error = None;
    for message in messages {
        match check_request::<R>(message.payload) {
            Ok(request) => {
                let size = request.bytes.len();
                encoded.push((request, size));
            }
            Err(error) => {
                dropped += 1;
                first_error.get_or_insert(error);
            }
        }
    }

    Prepared {
        requests: chunk_by_size(encoded, max_request_bytes)
            .into_iter()
            .map(join)
            .collect(),
        dropped,
        first_error,
    }
}

/// One message as the protobuf bytes of an export request, and the OTLP items in it.
struct Encoded {
    bytes: Vec<u8>,
    items: u64,
}

/// Decode a message to check that it is an export request, and return its own bytes, so
/// nothing is encoded again.
///
/// The decode is a full one, not a walk over the top-level fields, because only a full decode
/// finds a fault inside a resource entry. A faulty message joined with others makes the
/// receiver reject the whole request, and the runtime does not deliver a batch again. The
/// decode also counts the items, and it rejects a message of another signal, whose fields
/// have other wire types.
fn check_request<R: ExportRequest>(payload: Payload) -> Result<Encoded, Error> {
    let Payload::Raw(bytes) = payload else {
        return Err(Error::InvalidPayloadType);
    };
    let request = R::decode(bytes.as_slice()).map_err(|_| Error::InvalidProtobufPayload)?;
    let items = count_items(&request)?;
    Ok(Encoded { bytes, items })
}

/// The OTLP items in `request`. A request without a resource entry has nothing to export.
/// Decoding skips unknown fields, so a message that is not OTLP decodes to such a request.
fn count_items<R: ExportRequest>(request: &R) -> Result<u64, Error> {
    let entries = request.entries();
    if entries.is_empty() {
        return Err(Error::InvalidRecordValue(
            "the export request has no resource entries: it is empty, or the message is not OTLP"
                .to_owned(),
        ));
    }
    Ok(entries.iter().map(R::item_count).sum())
}

/// Join the encoded requests of one chunk into a single request.
fn join(chunk: Chunk<Encoded>) -> Packed {
    let messages = chunk.units.len() as u64;
    let items = chunk.units.iter().map(|unit| unit.items).sum();
    let mut bodies: Vec<Vec<u8>> = chunk.units.into_iter().map(|unit| unit.bytes).collect();
    let body = if bodies.len() == 1 {
        bodies.swap_remove(0)
    } else {
        bodies.concat()
    };
    Packed {
        body,
        messages,
        items,
        oversized: chunk.oversized,
    }
}

struct Chunk<T> {
    units: Vec<T>,
    oversized: bool,
}

/// Greedily group `(unit, size)` pairs so each group's total stays within `limit`. A unit
/// that alone exceeds the limit forms a group of its own.
fn chunk_by_size<T>(units: Vec<(T, usize)>, limit: usize) -> Vec<Chunk<T>> {
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_size = 0;
    for (unit, size) in units {
        if !current.is_empty() && current_size + size > limit {
            chunks.push(Chunk {
                units: std::mem::take(&mut current),
                oversized: false,
            });
            current_size = 0;
        }
        current.push(unit);
        current_size += size;
        if current.len() == 1 && current_size > limit {
            chunks.push(Chunk {
                units: std::mem::take(&mut current),
                oversized: true,
            });
            current_size = 0;
        }
    }
    if !current.is_empty() {
        chunks.push(Chunk {
            units: current,
            oversized: false,
        });
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use prost::Message;

    const NO_LIMIT: usize = usize::MAX;

    fn message(payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 42,
            offset: 7,
            checksum: 0,
            timestamp: 2_000,
            origin_timestamp: 1_000,
            headers: None,
            payload,
        }
    }

    fn logs_request(bodies: &[&str]) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: bodies
                        .iter()
                        .map(|body| LogRecord {
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue((*body).to_owned())),
                            }),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn decoded<R: Message + Default>(packed: &Packed) -> R {
        R::decode(packed.body.as_slice()).expect("a packed body is one export request")
    }

    #[test]
    fn given_proto_messages_should_merge_into_one_request() {
        let first = logs_request(&["a", "b"]).encode_to_vec();
        let second = logs_request(&["c"]).encode_to_vec();

        let prepared = pack_requests::<ExportLogsServiceRequest>(
            vec![message(Payload::Raw(first)), message(Payload::Raw(second))],
            NO_LIMIT,
        );

        assert_eq!(prepared.requests.len(), 1);
        assert_eq!(prepared.requests[0].messages, 2);
        assert_eq!(prepared.requests[0].items, 3);
        let merged: ExportLogsServiceRequest = decoded(&prepared.requests[0]);
        assert_eq!(merged.resource_logs.len(), 2);
        assert_eq!(prepared.dropped, 0);
    }

    #[test]
    fn given_proto_message_with_unknown_field_should_forward_its_bytes_unchanged() {
        let mut first = logs_request(&["a"]).encode_to_vec();
        // Field 15 as a varint: no OTLP version defines it, so prost skips it on decode.
        first.extend_from_slice(&[0x78, 0x01]);
        let second = logs_request(&["b"]).encode_to_vec();

        let prepared = pack_requests::<ExportLogsServiceRequest>(
            vec![
                message(Payload::Raw(first.clone())),
                message(Payload::Raw(second.clone())),
            ],
            NO_LIMIT,
        );

        assert_eq!(prepared.dropped, 0);
        assert_eq!(prepared.requests[0].body, [first, second].concat());
    }

    #[test]
    fn given_message_without_resource_entries_should_drop_it() {
        let prepared = pack_requests::<ExportLogsServiceRequest>(
            vec![message(Payload::Raw(vec![]))],
            NO_LIMIT,
        );

        assert_eq!(prepared.dropped, 1);
        assert!(matches!(
            prepared.first_error,
            Some(Error::InvalidRecordValue(_))
        ));
        assert!(prepared.requests.is_empty());
    }

    #[test]
    fn given_undecodable_proto_should_drop_only_that_message() {
        let valid = logs_request(&["a"]).encode_to_vec();

        let prepared = pack_requests::<ExportLogsServiceRequest>(
            vec![
                message(Payload::Raw(valid.clone())),
                message(Payload::Raw(vec![0xff, 0xff, 0xff])),
                message(Payload::Raw(valid)),
            ],
            NO_LIMIT,
        );

        assert_eq!(prepared.dropped, 1);
        assert_eq!(prepared.first_error, Some(Error::InvalidProtobufPayload));
        assert_eq!(prepared.requests.len(), 1);
        assert_eq!(prepared.requests[0].messages, 2);
    }

    #[test]
    fn given_payload_that_is_not_raw_should_reject_payload_type() {
        let prepared = pack_requests::<ExportLogsServiceRequest>(
            vec![message(Payload::Text(r#"{"resourceLogs": []}"#.to_owned()))],
            NO_LIMIT,
        );

        assert_eq!(prepared.dropped, 1);
        assert_eq!(prepared.first_error, Some(Error::InvalidPayloadType));
        assert!(prepared.requests.is_empty());
    }

    #[test]
    fn given_size_limit_should_split_between_messages_never_inside_one() {
        let payload = logs_request(&["0123456789"]).encode_to_vec();
        let one_message = payload.len();

        let prepared = pack_requests::<ExportLogsServiceRequest>(
            vec![
                message(Payload::Raw(payload.clone())),
                message(Payload::Raw(payload.clone())),
                message(Payload::Raw(payload)),
            ],
            one_message * 2,
        );

        let per_request: Vec<u64> = prepared.requests.iter().map(|r| r.messages).collect();
        assert_eq!(per_request, vec![2, 1]);
        for packed in &prepared.requests {
            assert!(!packed.oversized);
            assert!(packed.body.len() <= one_message * 2);
        }
    }

    #[test]
    fn given_message_larger_than_limit_should_send_it_alone() {
        let small = logs_request(&["a"]).encode_to_vec();
        let large = logs_request(&[&"x".repeat(1_000)]).encode_to_vec();

        let prepared = pack_requests::<ExportLogsServiceRequest>(
            vec![
                message(Payload::Raw(small.clone())),
                message(Payload::Raw(large)),
                message(Payload::Raw(small)),
            ],
            100,
        );

        let per_request: Vec<u64> = prepared.requests.iter().map(|r| r.messages).collect();
        assert_eq!(per_request, vec![1, 1, 1]);
        let oversized: Vec<bool> = prepared.requests.iter().map(|r| r.oversized).collect();
        assert_eq!(oversized, vec![false, true, false]);
    }
}
