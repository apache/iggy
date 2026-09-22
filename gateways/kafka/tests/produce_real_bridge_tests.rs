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

//! Produce (API key 0) driven through the whole handler against a real `iggy-server`.
//!
//! Records go in as Kafka wire bytes and come back out through the Iggy SDK, so a mapping broken
//! the same way in both directions cannot pass.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use iggy::prelude::{
    Consumer, HeaderKey, Identifier, IggyMessage, MessageClient, PollingStrategy, TopicClient,
};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use serial_test::serial;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::protocol::api::{
    API_KEY_PRODUCE, BrokerAdvertise, ERROR_NONE, ERROR_NOT_LEADER_OR_FOLLOWER,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, handle_request_bounded,
};
use iggy_gateway_kafka::records::{KEY_HEADER, MAPPING_VERSION, VERSION_HEADER, from_iggy};

#[path = "common/codec.rs"]
mod codec;
#[path = "common/iggy_server.rs"]
mod iggy_server;

use codec::{Decoder, Encoder};
use iggy_server::{TestServer, raw_client};

/// The stream every unmapped Kafka topic resolves to, per `TopicMapping`'s default rule.
const STREAM: &str = "kafka";
const TOPIC: &str = "orders";
const CREATE_TIME: i64 = 1_700_000_000_123;
const MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

/// Spelled out rather than imported: a test reading its expected code from the same constant the
/// handler answers with cannot catch that constant changing.
const ERROR_INVALID_REQUIRED_ACKS: i16 = 21;
const ERROR_UNSUPPORTED_VERSION: i16 = 35;

/// A live gateway with the topic already provisioned, since Produce creates nothing.
async fn gateway_with_topic(server: &TestServer, partitions: u32) -> GatewayState {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");
    bridge
        .ensure_stream_and_topic(TOPIC, partitions)
        .await
        .expect("the topic must exist before producing to it");
    GatewayState::new(
        BrokerAdvertise::default(),
        Some(Arc::new(bridge)),
        MAX_FRAME_SIZE,
    )
}

fn record(offset: i64, key: Option<&[u8]>, value: &[u8], headers: &[(&str, &[u8])]) -> Record {
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
        key: key.map(Bytes::copy_from_slice),
        value: Some(Bytes::copy_from_slice(value)),
        headers: headers
            .iter()
            .map(|(name, value)| {
                (
                    StrBytes::from_string((*name).to_string()),
                    Some(Bytes::copy_from_slice(value)),
                )
            })
            .collect::<IndexMap<_, _>>(),
    }
}

fn batch(records: &[Record], compression: Compression) -> Bytes {
    let mut buf = BytesMut::new();
    let options = RecordEncodeOptions {
        version: 2,
        compression,
    };
    RecordBatchEncoder::encode(&mut buf, records.iter(), &options).expect("encode record batch");
    buf.freeze()
}

/// One partition entry of a request: the index and the records blob it carries.
type Entry = (i32, Bytes);

/// Builds a request body by hand, at either wire encoding.
///
/// Not through `kafka_protocol`: the crate is taken with the `broker` feature alone, which
/// encodes responses and decodes requests, neither of the halves a client needs. Hand-built
/// bytes also make these tests exercise the real decoder instead of round-tripping one schema.
fn encode_request(version: i16, acks: i16, topic: &str, entries: &[Entry]) -> Bytes {
    let flexible = version >= 9;
    let mut enc = Encoder::with_capacity(1024);

    if flexible {
        enc.write_compact_nullable_string(None); // transactional_id
    } else {
        enc.write_nullable_string(None).expect("null fits");
    }
    enc.write_i16(acks);
    enc.write_i32(5_000); // timeout_ms

    if flexible {
        enc.write_varint(2); // one topic, compact array is N+1
        enc.write_compact_nullable_string(Some(topic));
        enc.write_varint(entries.len() as u64 + 1);
    } else {
        enc.write_i32(1);
        enc.write_nullable_string(Some(topic)).expect("name fits");
        enc.write_i32(i32::try_from(entries.len()).expect("few entries"));
    }

    for (index, records) in entries {
        enc.write_i32(*index);
        if flexible {
            enc.write_compact_nullable_bytes(Some(records));
            enc.write_empty_tagged_fields();
        } else {
            enc.write_nullable_bytes(Some(records))
                .expect("records fit");
        }
    }
    if flexible {
        enc.write_empty_tagged_fields(); // topic
        enc.write_empty_tagged_fields(); // request
    }
    enc.freeze()
}

/// Every partition entry of a response, flattened, in the order the response lists them.
fn partition_results(version: i16, body: Bytes) -> Vec<(i32, i16, i64)> {
    let flexible = version >= 9;
    let mut d = Decoder::new(body);
    let topics = read_count(&mut d, flexible);

    let mut results = Vec::new();
    for _ in 0..topics {
        read_name(&mut d, flexible);
        let partitions = read_count(&mut d, flexible);
        for _ in 0..partitions {
            let index = d.read_i32().expect("partition index");
            let error_code = d.read_i16().expect("error code");
            let base_offset = d.read_i64().expect("base offset");
            let append_time = d.read_i64().expect("log append time");
            assert_eq!(append_time, -1, "CreateTime topics report no append time");
            if version >= 5 {
                d.read_i64().expect("log start offset");
            }
            if version >= 8 {
                assert_eq!(read_count(&mut d, flexible), 0, "no per-record errors");
                read_name(&mut d, flexible); // error_message
            }
            if flexible {
                d.read_tagged_fields().expect("partition tagged fields");
            }
            results.push((index, error_code, base_offset));
        }
        if flexible {
            d.read_tagged_fields().expect("topic tagged fields");
        }
    }

    assert_eq!(d.read_i32().expect("throttle time"), 0);
    if flexible {
        d.read_tagged_fields().expect("response tagged fields");
    }
    assert_eq!(d.remaining(), 0, "the response must decode exactly");
    results
}

fn read_count(d: &mut Decoder, flexible: bool) -> usize {
    if flexible {
        let raw = d.read_varint().expect("compact array length");
        usize::try_from(raw.saturating_sub(1)).expect("array length fits")
    } else {
        usize::try_from(d.read_i32().expect("array length")).expect("array length fits")
    }
}

fn read_name(d: &mut Decoder, flexible: bool) -> Option<String> {
    if flexible {
        d.read_compact_nullable_string().expect("compact string")
    } else {
        d.read_nullable_string().expect("string")
    }
}

/// Sends one request through the handler and returns the partition entries it answered with.
async fn produce(
    state: &GatewayState,
    version: i16,
    acks: i16,
    topic: &str,
    entries: &[Entry],
) -> Vec<(i32, i16, i64)> {
    let body = encode_request(version, acks, topic, entries);
    let response = handle_request_bounded(state, API_KEY_PRODUCE, version, body)
        .await
        .expect_response("a Produce request with acks != 0 must be answered");
    partition_results(version, response)
}

async fn stored(server: &TestServer, partition: u32, count: u32) -> Vec<IggyMessage> {
    raw_client(server)
        .await
        .poll_messages(
            &Identifier::named(STREAM).expect("stream name"),
            &Identifier::named(TOPIC).expect("topic name"),
            Some(partition),
            &Consumer::new(Identifier::named("reader").expect("consumer name")),
            &PollingStrategy::offset(0),
            count,
            false,
        )
        .await
        .expect("poll the partition back")
        .messages
}

#[tokio::test]
#[serial]
async fn given_a_produce_request_when_handled_should_store_every_record_it_carried() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;

    let records = [
        record(0, Some(b"k1"), b"first", &[("trace", b"abc")]),
        record(1, None, b"second", &[]),
    ];
    let entries = [(0, batch(&records, Compression::None))];

    assert_eq!(
        produce(&state, 3, 1, TOPIC, &entries).await,
        vec![(0, ERROR_NONE, 0)],
        "a two-record batch lands at offset 0 and reports it"
    );

    let messages = stored(&server, 0, 10).await;
    assert_eq!(
        messages.len(),
        2,
        "one Kafka record became one Iggy message"
    );
    assert_eq!(messages[0].payload.as_ref(), b"first");
    assert_eq!(messages[1].payload.as_ref(), b"second");

    // Once on the raw stored shape, since a round trip through the codec proves nothing about
    // the shape itself.
    let headers = messages[0]
        .user_headers_map()
        .expect("headers must parse")
        .expect("a gateway-written message always carries headers");
    assert_eq!(
        headers
            .get(&HeaderKey::try_from(VERSION_HEADER).unwrap())
            .map(|value| value.as_bytes().to_vec()),
        Some(vec![MAPPING_VERSION]),
        "the provenance header marks this message as gateway-written"
    );
    assert_eq!(
        headers
            .get(&HeaderKey::try_from(KEY_HEADER).unwrap())
            .map(|value| value.as_bytes().to_vec()),
        Some(b"k1".to_vec()),
        "the Kafka key is stored beside the value, not inside it"
    );

    let back = from_iggy(&messages[0], 0).expect("stored message must decode as a record");
    assert_eq!(back.value.as_deref(), Some(&b"first"[..]));
    assert_eq!(back.timestamp, CREATE_TIME, "CreateTime survives the store");
    assert_eq!(
        back.headers
            .get(&StrBytes::from_static_str("trace"))
            .and_then(Option::as_deref),
        Some(&b"abc"[..])
    );
}

#[tokio::test]
#[serial]
async fn given_a_flexible_produce_request_when_handled_should_store_its_records() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;

    let records = [record(0, Some(b"k"), b"flexible", &[])];
    let entries = [(0, batch(&records, Compression::None))];

    assert_eq!(
        produce(&state, 9, -1, TOPIC, &entries).await,
        vec![(0, ERROR_NONE, 0)],
        "the compact and tagged-field encoding reaches the same write path"
    );
    assert_eq!(stored(&server, 0, 10).await.len(), 1);
}

#[tokio::test]
#[serial]
async fn given_a_gzip_batch_when_handled_should_store_its_records() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;

    let records = [record(0, None, b"compressed", &[])];
    let entries = [(0, batch(&records, Compression::Gzip))];

    assert_eq!(
        produce(&state, 3, 1, TOPIC, &entries).await,
        vec![(0, ERROR_NONE, 0)]
    );
    assert_eq!(
        stored(&server, 0, 10).await[0].payload.as_ref(),
        b"compressed"
    );
}

#[tokio::test]
#[serial]
async fn given_two_batches_in_one_blob_when_handled_should_land_under_one_base_offset() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;

    // One `records` field may hold several batches back to back.
    let mut blob = BytesMut::new();
    blob.extend_from_slice(&batch(&[record(0, None, b"a", &[])], Compression::None));
    blob.extend_from_slice(&batch(&[record(0, None, b"b", &[])], Compression::None));
    let entries = [(0, blob.freeze())];

    assert_eq!(
        produce(&state, 3, 1, TOPIC, &entries).await,
        vec![(0, ERROR_NONE, 0)],
        "both batches append as one Iggy send under one base offset"
    );
    let messages = stored(&server, 0, 10).await;
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[1].payload.as_ref(), b"b");
}

#[tokio::test]
#[serial]
async fn given_a_second_request_when_handled_should_report_where_it_landed() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;

    let first = [record(0, None, b"a", &[]), record(1, None, b"b", &[])];
    let second = [record(0, None, b"c", &[])];

    assert_eq!(
        produce(
            &state,
            3,
            1,
            TOPIC,
            &[(0, batch(&first, Compression::None))]
        )
        .await,
        vec![(0, ERROR_NONE, 0)]
    );
    assert_eq!(
        produce(
            &state,
            3,
            1,
            TOPIC,
            &[(0, batch(&second, Compression::None))]
        )
        .await,
        vec![(0, ERROR_NONE, 2)],
        "the base offset comes from the send confirmation, not from a count this handler kept"
    );
    assert_eq!(stored(&server, 0, 10).await.len(), 3);
}

#[tokio::test]
#[serial]
async fn given_a_partition_the_topic_lacks_when_handled_should_keep_the_other_answers() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 2).await;

    let blob = batch(&[record(0, None, b"v", &[])], Compression::None);
    let entries = [(0, blob.clone()), (9, blob.clone()), (1, blob)];

    assert_eq!(
        produce(&state, 3, 1, TOPIC, &entries).await,
        vec![
            (0, ERROR_NONE, 0),
            (9, ERROR_UNKNOWN_TOPIC_OR_PARTITION, -1),
            (1, ERROR_NONE, 0),
        ],
        "one partition failing costs the others nothing, and a failure names no offset"
    );
    assert_eq!(stored(&server, 0, 10).await.len(), 1);
    assert_eq!(stored(&server, 1, 10).await.len(), 1);
}

#[tokio::test]
#[serial]
async fn given_an_unknown_topic_when_handled_should_answer_unknown_topic_or_partition() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;

    let entries = [(0, batch(&[record(0, None, b"v", &[])], Compression::None))];

    assert_eq!(
        produce(&state, 3, 1, "never-created", &entries).await,
        vec![(0, ERROR_UNKNOWN_TOPIC_OR_PARTITION, -1)],
        "Produce provisions nothing: auto-creation belongs to Metadata"
    );

    let topics = raw_client(&server)
        .await
        .get_topics(&Identifier::named(STREAM).expect("stream name"))
        .await
        .expect("list the topics the stream holds");
    assert_eq!(
        topics.len(),
        1,
        "the refused topic must not have been created on the way"
    );
}

#[tokio::test]
#[serial]
async fn given_acks_zero_when_handled_should_write_the_records_and_answer_nothing() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;

    let entries = [(
        0,
        batch(
            &[record(0, None, b"fire-and-forget", &[])],
            Compression::None,
        ),
    )];
    let body = encode_request(3, 0, TOPIC, &entries);

    let outcome = handle_request_bounded(&state, API_KEY_PRODUCE, 3, body).await;
    assert!(
        outcome.is_no_response(),
        "the wire protocol forbids answering an acks=0 request"
    );

    let messages = stored(&server, 0, 10).await;
    assert_eq!(
        messages.len(),
        1,
        "answering nothing is not the same as storing nothing"
    );
    assert_eq!(messages[0].payload.as_ref(), b"fire-and-forget");
}

#[tokio::test]
#[serial]
async fn given_an_unknown_acks_value_when_handled_should_answer_invalid_required_acks() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;

    let entries = [(0, batch(&[record(0, None, b"v", &[])], Compression::None))];

    assert_eq!(
        produce(&state, 3, 2, TOPIC, &entries).await,
        vec![(0, ERROR_INVALID_REQUIRED_ACKS, -1)]
    );
    assert!(
        stored(&server, 0, 10).await.is_empty(),
        "a request refused before the write must leave the partition empty"
    );
}

#[tokio::test]
#[serial]
async fn given_a_transactional_batch_when_handled_should_answer_unsupported_version() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;

    let mut records = [record(0, None, b"v", &[])];
    records[0].transactional = true;
    let entries = [(0, batch(&records, Compression::None))];

    assert_eq!(
        produce(&state, 3, 1, TOPIC, &entries).await,
        vec![(0, ERROR_UNSUPPORTED_VERSION, -1)],
        "transactions are out of scope, and 35 is the code that stops a transactional producer"
    );
    assert!(
        stored(&server, 0, 10).await.is_empty(),
        "a refused batch must not be half-stored"
    );
}

#[tokio::test]
#[serial]
async fn given_no_bridge_when_handled_should_still_answer_the_retriable_stub() {
    // Every other suite drives this path, so a regression in it surfaces far from Produce.
    // Pinned here, next to the path that replaced it.
    let state = GatewayState::stub(BrokerAdvertise::default(), MAX_FRAME_SIZE);
    let entries = [(0, batch(&[record(0, None, b"v", &[])], Compression::None))];

    assert_eq!(
        produce(&state, 3, 1, TOPIC, &entries).await,
        vec![(0, ERROR_NOT_LEADER_OR_FOLLOWER, -1)],
        "without a bridge nothing is stored, so the answer must stay retriable"
    );
}
