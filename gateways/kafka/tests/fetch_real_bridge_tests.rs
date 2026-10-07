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

//! Fetch (API key 1) driven through the whole handler against a real `iggy-server`.
//!
//! Records go in through the Iggy SDK, mapped by `to_iggy`, and come back as a response that
//! `kafka_protocol`'s client-side decoder reads, the way a consumer would.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use iggy::prelude::{
    HeaderKey, HeaderValue, Identifier, IggyMessage, MessageClient, Partitioning, TopicClient,
};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    FetchRequest, FetchResponse, ProduceRequest, ProduceResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use serial_test::serial;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::api::{
    API_KEY_FETCH, API_KEY_PRODUCE, BrokerAdvertise, ConnectionState, GatewayState,
    handle_connection_request, handle_request_bounded,
};
use iggy_gateway_kafka::records::{MAPPING_VERSION, TimestampWindow, VERSION_HEADER, to_iggy};

#[path = "common/iggy_server.rs"]
mod iggy_server;

use iggy_server::{TestServer, raw_client};

/// The stream every unmapped Kafka topic resolves to, per `TopicMapping`'s default rule.
const STREAM: &str = "kafka";
const TOPIC: &str = "orders";
const CREATE_TIME: i64 = 1_700_000_000_123;
const MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;
/// The highest version a consumer without topic ids sends.
const VERSION: i16 = 12;
/// Java's `max.partition.fetch.bytes` default.
const PARTITION_MAX_BYTES: i32 = 1024 * 1024;
/// A Fetch that finds records or an error answers well inside this.
const PROMPT: Duration = Duration::from_secs(5);

/// Spelled out rather than imported: a test reading its expected code from the same constant the
/// handler answers with cannot catch that constant changing.
const ERROR_UNKNOWN_SERVER_ERROR: i16 = -1;
const ERROR_NONE: i16 = 0;
const ERROR_OFFSET_OUT_OF_RANGE: i16 = 1;
const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERROR_NOT_LEADER_OR_FOLLOWER: i16 = 6;
const ERROR_FETCH_SESSION_ID_NOT_FOUND: i16 = 70;
const UNKNOWN_OFFSET: i64 = -1;

/// A live gateway with the topic in place, since Fetch creates nothing.
async fn gateway_with_topic(server: &TestServer, partitions: u32) -> GatewayState {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");
    bridge
        .ensure_stream_and_topic(TOPIC, partitions)
        .await
        .expect("the topic must exist before fetching from it");
    GatewayState::new(
        BrokerAdvertise::default(),
        Some(Arc::new(bridge)),
        MAX_FRAME_SIZE,
        false,
        0,
        GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
    )
}

fn record(key: Option<&[u8]>, value: Option<&[u8]>, headers: &[(&str, Option<&[u8]>)]) -> Record {
    Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: NO_SEQUENCE,
        timestamp: CREATE_TIME,
        key: key.map(Bytes::copy_from_slice),
        value: value.map(Bytes::copy_from_slice),
        headers: headers
            .iter()
            .map(|(name, value)| {
                (
                    StrBytes::from_string((*name).to_string()),
                    value.map(Bytes::copy_from_slice),
                )
            })
            .collect::<IndexMap<_, _>>(),
    }
}

/// A keyed record holding `value`.
fn keyed(value: &[u8]) -> Record {
    record(Some(b"k"), Some(value), &[])
}

/// Stores `records` in `partition` the way Produce does: `to_iggy`, then one send.
async fn store(server: &TestServer, partition: u32, records: &[Record]) {
    let window = TimestampWindow::of(records);
    let mut messages: Vec<IggyMessage> = records
        .iter()
        .map(|record| to_iggy(record, window).expect("the record maps"))
        .collect();
    send(server, partition, &mut messages).await;
}

async fn send(server: &TestServer, partition: u32, messages: &mut [IggyMessage]) {
    raw_client(server)
        .await
        .send_messages(
            &Identifier::named(STREAM).expect("valid stream name"),
            &Identifier::named(TOPIC).expect("valid topic name"),
            &Partitioning::partition_id(partition),
            messages,
        )
        .await
        .expect("store the messages");
}

/// A message that claims a mapping version this build does not read.
fn unmappable() -> IggyMessage {
    let mut headers = BTreeMap::new();
    headers.insert(
        HeaderKey::try_from(VERSION_HEADER).expect("valid header name"),
        HeaderValue::try_from(&[MAPPING_VERSION + 1][..]).expect("valid header value"),
    );
    IggyMessage::builder()
        .payload(Bytes::from_static(b"v"))
        .user_headers(headers)
        .build()
        .expect("valid message")
}

/// Purges the topic, and waits until every partition reads empty.
async fn purge(server: &TestServer) {
    let client = raw_client(server).await;
    let stream = Identifier::named(STREAM).expect("valid stream name");
    let topic = Identifier::named(TOPIC).expect("valid topic name");
    client
        .purge_topic(&stream, &topic)
        .await
        .expect("purge the topic");
    let deadline = Instant::now() + PROMPT;
    loop {
        let details = client
            .get_topic(&stream, &topic)
            .await
            .expect("read the topic")
            .expect("the topic outlives a purge");
        let empty = details
            .partitions
            .iter()
            .all(|partition| partition.current_offset == 0 && partition.messages_count == 0);
        if empty {
            return;
        }
        assert!(Instant::now() < deadline, "the purge never showed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A consumer's full request for `topic`, each partition as `(index, fetch_offset)`.
fn request(topic: &str, partitions: &[(i32, i64)]) -> FetchRequest {
    FetchRequest::default()
        .with_max_wait_ms(10_000)
        .with_min_bytes(1)
        .with_max_bytes(52_428_800)
        .with_session_epoch(0)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
                .with_partitions(
                    partitions
                        .iter()
                        .map(|&(partition, fetch_offset)| {
                            FetchPartition::default()
                                .with_partition(partition)
                                .with_fetch_offset(fetch_offset)
                                .with_partition_max_bytes(PARTITION_MAX_BYTES)
                        })
                        .collect(),
                ),
        ])
}

/// Sends `request` through the handler at `version`, and decodes the answer as a consumer does.
async fn fetch_at(state: &GatewayState, version: i16, request: &FetchRequest) -> FetchResponse {
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .expect("the request encodes at this version");
    let mut response = handle_request_bounded(state, API_KEY_FETCH, version, body.freeze())
        .await
        .expect_response("Fetch always answers");
    let decoded = FetchResponse::decode(&mut response, version).expect("a Kafka client decodes it");
    assert!(response.is_empty(), "no bytes after the response");
    decoded
}

/// [`fetch_at`] the highest version, for a request that must not wait.
async fn fetch(state: &GatewayState, request: &FetchRequest) -> FetchResponse {
    tokio::time::timeout(PROMPT, fetch_at(state, VERSION, request))
        .await
        .expect("records or an error answer at once")
}

/// [`fetch`] as one request of `connection`, which keeps what its earlier Fetches left.
async fn fetch_on(
    state: &GatewayState,
    connection: &ConnectionState,
    request: &FetchRequest,
) -> FetchResponse {
    let mut body = BytesMut::new();
    request
        .encode(&mut body, VERSION)
        .expect("the request encodes at this version");
    let handled =
        handle_connection_request(state, connection, API_KEY_FETCH, VERSION, body.freeze());
    let mut response = tokio::time::timeout(PROMPT, handled)
        .await
        .expect("records or an error answer at once")
        .expect_response("Fetch always answers");
    let decoded = FetchResponse::decode(&mut response, VERSION).expect("a Kafka client decodes it");
    assert!(response.is_empty(), "no bytes after the response");
    decoded
}

/// The one partition of an answer to a one-partition request.
fn only(response: &FetchResponse) -> &PartitionData {
    assert_eq!(response.error_code, ERROR_NONE);
    assert_eq!(response.session_id, 0, "no session is ever opened");
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].partitions.len(), 1);
    &response.responses[0].partitions[0]
}

/// Every record a partition answered with.
fn records(partition: &PartitionData) -> Vec<Record> {
    let mut bytes = partition.records.clone().unwrap_or_default();
    RecordBatchDecoder::decode_all(&mut bytes)
        .expect("only complete batches")
        .into_iter()
        .flat_map(|set| set.records)
        .collect()
}

fn offsets(partition: &PartitionData) -> Vec<i64> {
    records(partition)
        .iter()
        .map(|record| record.offset)
        .collect()
}

fn values(partition: &PartitionData) -> Vec<Option<Bytes>> {
    records(partition)
        .into_iter()
        .map(|record| record.value)
        .collect()
}

fn assert_refused(partition: &PartitionData, code: i16) {
    assert_eq!(partition.error_code, code);
    assert_eq!(partition.high_watermark, UNKNOWN_OFFSET);
    assert_eq!(records(partition), []);
}

#[tokio::test]
#[serial]
async fn given_records_when_fetching_from_zero_should_return_them_in_order() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    store(
        &server,
        0,
        &[keyed(b"first"), keyed(b"second"), keyed(b"third")],
    )
    .await;

    let response = fetch(&state, &request(TOPIC, &[(0, 0)])).await;
    let partition = only(&response);
    assert_eq!(partition.error_code, ERROR_NONE);
    assert_eq!(partition.high_watermark, 3);
    assert_eq!(offsets(partition), vec![0, 1, 2]);
    assert_eq!(
        values(partition),
        vec![
            Some(Bytes::from_static(b"first")),
            Some(Bytes::from_static(b"second")),
            Some(Bytes::from_static(b"third")),
        ]
    );
    let first = &records(partition)[0];
    assert_eq!(first.key.as_deref(), Some(&b"k"[..]));
    assert_eq!(first.timestamp, CREATE_TIME);

    let from_middle = fetch(&state, &request(TOPIC, &[(0, 1)])).await;
    assert_eq!(offsets(only(&from_middle)), vec![1, 2]);
}

#[tokio::test]
#[serial]
async fn given_native_iggy_messages_when_fetching_should_return_null_keys() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    let mut headers = BTreeMap::new();
    headers.insert(
        HeaderKey::try_from("source").expect("valid header name"),
        HeaderValue::try_from(&b"connector"[..]).expect("valid header value"),
    );
    let mut messages = vec![
        IggyMessage::builder()
            .payload(Bytes::from_static(b"{}"))
            .user_headers(headers)
            .build()
            .expect("valid message"),
        IggyMessage::from("plain".to_string()),
    ];
    send(&server, 0, &mut messages).await;

    let response = fetch(&state, &request(TOPIC, &[(0, 0)])).await;
    let records = records(only(&response));
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|record| record.key.is_none()));
    assert_eq!(records[0].value.as_deref(), Some(&b"{}"[..]));
    assert_eq!(
        records[0].headers.get(&StrBytes::from_static_str("source")),
        Some(&Some(Bytes::from_static(b"connector")))
    );
    assert_eq!(records[1].value.as_deref(), Some(&b"plain"[..]));
    assert!(records[1].headers.is_empty());
}

#[tokio::test]
#[serial]
async fn given_tombstones_and_large_keys_when_fetching_should_round_trip() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    let long_key = vec![b'k'; 300];
    let sent = [
        record(Some(b"gone"), None, &[]),
        record(Some(b"k"), Some(b""), &[]),
        record(Some(&long_key), Some(b"v"), &[]),
        record(Some(b""), Some(b"v"), &[("flag", None)]),
        record(None, Some(b"v"), &[("trace", Some(b"abc"))]),
    ];
    store(&server, 0, &sent).await;

    let response = fetch(&state, &request(TOPIC, &[(0, 0)])).await;
    let fetched = records(only(&response));
    assert_eq!(fetched.len(), sent.len());
    for (fetched, sent) in fetched.iter().zip(&sent) {
        assert_eq!(fetched.key, sent.key);
        assert_eq!(fetched.value, sent.value);
        assert_eq!(fetched.headers, sent.headers);
    }
}

/// Sends `records` as one batch to partition 0 through the Produce handler, and expects them
/// written.
async fn produce(state: &GatewayState, records: &[Record]) {
    const PRODUCE_VERSION: i16 = 9;
    let mut batch = BytesMut::new();
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    RecordBatchEncoder::encode(&mut batch, records.iter(), &options).expect("the batch encodes");
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(batch.freeze())),
                ]),
        ]);
    let mut body = BytesMut::new();
    request
        .encode(&mut body, PRODUCE_VERSION)
        .expect("the request encodes");
    let mut response =
        handle_request_bounded(state, API_KEY_PRODUCE, PRODUCE_VERSION, body.freeze())
            .await
            .expect_response("acks=-1 answers");
    let decoded =
        ProduceResponse::decode(&mut response, PRODUCE_VERSION).expect("a producer decodes it");
    assert_eq!(
        decoded.responses[0].partition_responses[0].error_code,
        ERROR_NONE
    );
}

#[tokio::test]
#[serial]
async fn given_records_written_through_produce_when_fetching_should_return_them_unchanged() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    let stamped = |mut record: Record, timestamp| {
        record.timestamp = timestamp;
        record
    };
    // The epoch sits far outside the window of the rest, so Produce keeps it in `kafka.ts`.
    let sent = [
        record(Some(b"k"), Some(b"v"), &[("trace", Some(b"abc"))]),
        record(None, Some(b"plain"), &[]),
        record(Some(b"gone"), None, &[]),
        record(Some(b""), Some(b""), &[("flag", None)]),
        stamped(record(None, Some(b"old"), &[]), 0),
    ];
    produce(&state, &sent).await;

    let response = fetch(&state, &request(TOPIC, &[(0, 0)])).await;
    let partition = only(&response);
    let fetched = records(partition);
    assert_eq!(partition.high_watermark, 5);
    assert_eq!(fetched.len(), sent.len());
    for (offset, (fetched, sent)) in fetched.iter().zip(&sent).enumerate() {
        assert_eq!(fetched.offset, i64::try_from(offset).unwrap());
        assert_eq!(fetched.key, sent.key);
        assert_eq!(fetched.value, sent.value);
        assert_eq!(fetched.headers, sent.headers);
        assert_eq!(fetched.timestamp, sent.timestamp);
    }
}

#[tokio::test]
#[serial]
async fn given_offset_at_high_watermark_when_fetching_should_wait_and_return_empty() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    store(&server, 0, &[keyed(b"a"), keyed(b"b")]).await;
    let max_wait = Duration::from_millis(300);

    let started = Instant::now();
    let caught_up = request(TOPIC, &[(0, 2)]).with_max_wait_ms(300);
    let response = fetch_at(&state, VERSION, &caught_up).await;
    let waited = started.elapsed();

    assert!(waited >= max_wait, "answered after {waited:?}");
    assert!(waited < PROMPT, "answered after {waited:?}");
    let partition = only(&response);
    assert_eq!(partition.error_code, ERROR_NONE);
    assert_eq!(partition.high_watermark, 2);
    assert_eq!(records(partition), []);
}

#[tokio::test]
#[serial]
async fn given_a_write_during_the_wait_when_fetching_should_return_early() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    let write_after = Duration::from_millis(300);

    let started = Instant::now();
    let empty = request(TOPIC, &[(0, 0)]).with_max_wait_ms(10_000);
    let (response, ()) = tokio::join!(fetch_at(&state, VERSION, &empty), async {
        tokio::time::sleep(write_after).await;
        store(&server, 0, &[keyed(b"late")]).await;
    });
    let waited = started.elapsed();

    assert!(waited >= write_after, "answered after {waited:?}");
    assert!(waited < PROMPT, "the record ends the wait, not max_wait_ms");
    let partition = only(&response);
    assert_eq!(partition.high_watermark, 1);
    assert_eq!(values(partition), vec![Some(Bytes::from_static(b"late"))]);
}

#[tokio::test]
#[serial]
async fn given_a_message_that_does_not_map_when_fetching_should_answer_minus_one_after_the_wait() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    send(&server, 0, &mut [unmappable()]).await;
    let max_wait = Duration::from_millis(300);

    let started = Instant::now();
    let stuck = request(TOPIC, &[(0, 0)]).with_max_wait_ms(300);
    let response = fetch_at(&state, VERSION, &stuck).await;
    let waited = started.elapsed();

    assert!(
        waited >= max_wait,
        "a consumer that retries at once must not spin: {waited:?}"
    );
    assert!(waited < PROMPT, "answered after {waited:?}");
    assert_refused(only(&response), ERROR_UNKNOWN_SERVER_ERROR);
}

#[tokio::test]
#[serial]
async fn given_minus_one_holds_on_two_connections_when_both_retry_should_keep_each_its_own() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 2).await;
    store(&server, 0, &[keyed(b"a")]).await;
    send(&server, 0, &mut [unmappable(), unmappable()]).await;
    store(&server, 1, &[keyed(b"b")]).await;

    // Records on partition 1 end each request at once, and the -1 on partition 0 holds it for
    // the whole wait the request allowed.
    let (stuck_at_1, stuck_at_2) = (ConnectionState::default(), ConnectionState::default());
    for (connection, offset) in [(&stuck_at_1, 1), (&stuck_at_2, 2)] {
        let held = request(TOPIC, &[(0, offset), (1, 0)]).with_max_wait_ms(18_000);
        let response = fetch_on(&state, connection, &held).await;
        let partitions = &response.responses[0].partitions;
        assert_refused(&partitions[0], ERROR_UNKNOWN_SERVER_ERROR);
        assert_eq!(values(&partitions[1]), vec![Some(Bytes::from_static(b"b"))]);
    }

    // A read of partition 0 now finds nothing, so only a hold answers -1 there.
    purge(&server).await;
    let at = |offset| request(TOPIC, &[(0, offset)]).with_max_wait_ms(0);
    assert_refused(
        only(&fetch_on(&state, &stuck_at_1, &at(1)).await),
        ERROR_UNKNOWN_SERVER_ERROR,
    );
    assert_refused(
        only(&fetch_on(&state, &stuck_at_2, &at(2)).await),
        ERROR_UNKNOWN_SERVER_ERROR,
    );
    let fresh = ConnectionState::default();
    assert_refused(
        only(&fetch_on(&state, &fresh, &at(2)).await),
        ERROR_NOT_LEADER_OR_FOLLOWER,
    );
}

#[tokio::test]
#[serial]
async fn given_zero_max_wait_when_fetching_should_answer_at_once() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;

    for no_wait in [
        request(TOPIC, &[(0, 0)]).with_max_wait_ms(0),
        // Kafka has zero bytes in hand, and that meets zero.
        request(TOPIC, &[(0, 0)]).with_min_bytes(0),
    ] {
        let response = fetch(&state, &no_wait).await;
        let partition = only(&response);
        assert_eq!(partition.error_code, ERROR_NONE);
        assert_eq!(partition.high_watermark, 0);
        assert_eq!(records(partition), []);
    }
}

#[tokio::test]
#[serial]
async fn given_offset_past_high_watermark_when_fetching_should_answer_6_within_the_grace() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    store(&server, 0, &[keyed(b"a"), keyed(b"b")]).await;

    // An Iggy node that lags reads the same, so a 1 waits out the connection's 30 s grace.
    let past = fetch(&state, &request(TOPIC, &[(0, 3)]).with_max_wait_ms(0)).await;
    assert_refused(only(&past), ERROR_NOT_LEADER_OR_FOLLOWER);

    let negative = fetch(&state, &request(TOPIC, &[(0, -1)])).await;
    assert_refused(only(&negative), ERROR_OFFSET_OUT_OF_RANGE);
}

#[tokio::test]
#[serial]
async fn given_an_empty_partition_when_fetching_past_it_should_answer_6_after_the_wait() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    let max_wait = Duration::from_millis(300);

    // Iggy reports (0, 0) for a partition it loads or fences too. A 1 would reset the consumer.
    let started = Instant::now();
    let past = request(TOPIC, &[(0, 5)]).with_max_wait_ms(300);
    let response = fetch_at(&state, VERSION, &past).await;
    let waited = started.elapsed();
    assert!(
        waited >= max_wait,
        "a consumer that fetches again at once after a 6 must not spin: {waited:?}"
    );
    assert!(waited < PROMPT, "answered after {waited:?}");
    assert_refused(only(&response), ERROR_NOT_LEADER_OR_FOLLOWER);

    let at_start = request(TOPIC, &[(0, 0)]).with_max_wait_ms(0);
    let response = fetch(&state, &at_start).await;
    let partition = only(&response);
    assert_eq!(partition.error_code, ERROR_NONE, "caught up, not refused");
    assert_eq!(records(partition), []);
}

#[tokio::test]
#[serial]
async fn given_unknown_topic_or_partition_when_fetching_should_return_3() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;

    for (topic, partition) in [
        ("missing", 0),
        (TOPIC, 5),
        (TOPIC, -1),
        // No topic has a name Kafka refuses, and 17 would make the Java consumer throw.
        ("bad name", 0),
    ] {
        let response = fetch(&state, &request(topic, &[(partition, 0)])).await;
        assert_refused(only(&response), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }
}

#[tokio::test]
#[serial]
async fn given_a_small_partition_max_bytes_when_fetching_should_still_return_one_record() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    let big = [b'x'; 2048];
    store(&server, 0, &[keyed(&big), keyed(&big), keyed(&big)]).await;

    let mut small_partition = request(TOPIC, &[(0, 0)]);
    small_partition.topics[0].partitions[0].partition_max_bytes = 10;
    let response = fetch(&state, &small_partition).await;
    let partition = only(&response);
    assert_eq!(offsets(partition), vec![0], "one record, past the limit");
    assert_eq!(partition.high_watermark, 3);

    let small_response = request(TOPIC, &[(0, 0)]).with_max_bytes(10);
    let response = fetch(&state, &small_response).await;
    assert_eq!(offsets(only(&response)), vec![0]);
}

#[tokio::test]
#[serial]
async fn given_small_then_large_messages_when_fetching_should_page_through_them_in_order() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    let small = keyed(&[b's'; 100]);
    let large = keyed(&vec![b'l'; 256 * 1024]);
    store(&server, 0, &vec![small; 100]).await;
    for _ in 0..2 {
        store(&server, 0, &vec![large.clone(); 4]).await;
    }
    let budget = usize::try_from(PARTITION_MAX_BYTES).expect("positive");

    let from_start = fetch(&state, &request(TOPIC, &[(0, 0)])).await;
    let partition = only(&from_start);
    assert_eq!(partition.high_watermark, 108);
    assert_eq!(
        offsets(partition),
        (0..103).collect::<Vec<i64>>(),
        "every small one, then the large ones that fit, in order"
    );
    assert!(partition.records.as_ref().expect("records").len() <= budget);

    let from_large = fetch(&state, &request(TOPIC, &[(0, 100)])).await;
    let partition = only(&from_large);
    assert_eq!(offsets(partition), vec![100, 101, 102]);
    assert!(partition.records.as_ref().expect("records").len() <= budget);
}

#[tokio::test]
#[serial]
async fn given_max_bytes_spent_when_fetching_should_answer_remaining_partitions_empty() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 2).await;
    let value = [b'x'; 1024];
    for partition in 0..2 {
        store(
            &server,
            partition,
            &[keyed(&value), keyed(&value), keyed(&value)],
        )
        .await;
    }

    let response = fetch(&state, &request(TOPIC, &[(0, 0), (1, 0)]).with_max_bytes(1)).await;
    let partitions = &response.responses[0].partitions;
    assert_eq!(partitions.len(), 2, "every requested partition answers");
    assert_eq!(
        offsets(&partitions[0]),
        vec![0],
        "the first gets one record"
    );
    assert_eq!(partitions[1].error_code, ERROR_NONE);
    assert!(records(&partitions[1]).is_empty(), "nothing left to spend");
    assert_eq!(
        partitions[1].high_watermark, 3,
        "the probe's high watermark"
    );
}

#[tokio::test]
#[serial]
async fn given_an_incremental_session_when_fetching_should_return_70() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    store(&server, 0, &[keyed(b"a")]).await;

    let incremental = request(TOPIC, &[(0, 0)])
        .with_session_id(9)
        .with_session_epoch(1);
    for version in [7, VERSION] {
        let response = fetch_at(&state, version, &incremental).await;
        assert_eq!(response.error_code, ERROR_FETCH_SESSION_ID_NOT_FOUND);
        assert_eq!(response.session_id, 0);
        assert!(response.responses.is_empty(), "as Kafka answers");
    }

    // A full request that names a session closes it, so it reads as usual.
    let full = request(TOPIC, &[(0, 0)]).with_session_id(9);
    let response = fetch(&state, &full).await;
    assert_eq!(offsets(only(&response)), vec![0]);
}

#[tokio::test]
#[serial]
async fn given_every_supported_version_when_fetching_should_decode() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    store(
        &server,
        0,
        &[
            keyed(b"a"),
            record(Some(b"k"), Some(b"b"), &[("trace", Some(b"abc"))]),
        ],
    )
    .await;

    for version in 4..=12 {
        let response = tokio::time::timeout(
            PROMPT,
            fetch_at(&state, version, &request(TOPIC, &[(0, 0)])),
        )
        .await
        .expect("records answer at once");
        let partition = only(&response);
        assert_eq!(partition.error_code, ERROR_NONE, "v{version}");
        assert_eq!(partition.high_watermark, 2, "v{version}");
        assert_eq!(partition.last_stable_offset, 2, "v{version}");
        assert_eq!(partition.log_start_offset, UNKNOWN_OFFSET, "v{version}");
        assert_eq!(offsets(partition), vec![0, 1], "v{version}");
        assert_eq!(
            values(partition),
            vec![
                Some(Bytes::from_static(b"a")),
                Some(Bytes::from_static(b"b"))
            ],
            "v{version}"
        );
    }
}

#[tokio::test]
#[serial]
async fn given_read_committed_when_fetching_should_set_last_stable_offset_to_high_watermark() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = gateway_with_topic(&server, 1).await;
    store(&server, 0, &[keyed(b"a"), keyed(b"b")]).await;

    let read_committed = request(TOPIC, &[(0, 0)]).with_isolation_level(1);
    let response = fetch(&state, &read_committed).await;
    let partition = only(&response);
    assert_eq!(partition.high_watermark, 2);
    assert_eq!(
        partition.last_stable_offset, 2,
        "no transactions, so nothing is pending"
    );
    assert_eq!(partition.aborted_transactions, None);
    assert_eq!(offsets(partition), vec![0, 1]);
}
