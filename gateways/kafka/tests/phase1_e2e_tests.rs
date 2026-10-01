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

//! #3539 acceptance test: the whole Phase 1 flow (`CreateTopics` -> `Metadata` -> `Produce` ->
//! `ListOffsets`) driven over real TCP against the real compiled `iggy-gateway-kafka` binary,
//! itself bridged to a real compiled `iggy-server` binary - the same two processes
//! `docker-compose.yml` wires together, spawned directly as host binaries rather than through
//! Docker Compose itself. Not `KafkaGateway::run` spawned in-process the way every other "e2e"
//! suite in this crate does (`common/server.rs`). A regression only the actual binaries'
//! startup, env parsing, or process wiring can produce (as opposed to a handler-level bug the
//! in-process suites already cover) fails here and nowhere else.
//!
//! This does **not** exercise `docker-compose.yml`, the `Dockerfile`s, image builds, container
//! networking, or the health-check gate - those are covered by `docs/MANUAL_TESTING.md`'s
//! Category I (manual) only, not by any automated suite. A broken compose file, a missing
//! runtime library in the image, or a wrong port/network mapping would not be caught here.
//!
//! The final read-back step goes through the Iggy SDK directly rather than a real Kafka Fetch,
//! the same substitute `produce_real_bridge_tests.rs` uses - Fetch is implemented
//! (`src/protocol/handlers/fetch.rs`, exercised by `fetch_real_bridge_tests.rs`), but wiring a
//! hand-built Fetch request/response codec into this specific acceptance test is left as a
//! follow-up rather than bundled into this PR's scope.

use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use iggy::prelude::{Consumer, Identifier, MessageClient, PollingStrategy};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

use iggy_gateway_kafka::protocol::api::{
    API_KEY_CREATE_TOPICS, API_KEY_LIST_OFFSETS, API_KEY_METADATA, API_KEY_PRODUCE, ERROR_NONE,
};

#[path = "common/codec.rs"]
mod codec;
#[path = "common/gateway_process.rs"]
mod gateway_process;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/tcp.rs"]
mod tcp;

use codec::{Decoder, Encoder};
use gateway_process::TestGateway;
use iggy_server::{TestServer, raw_client};
use tcp::round_trip;

const TOPIC: &str = "phase1-e2e-topic";
const STREAM: &str = "kafka";
const CREATE_TOPICS_VERSION: i16 = 5;
const METADATA_VERSION: i16 = 9;
const PRODUCE_VERSION: i16 = 7;
const LIST_OFFSETS_VERSION: i16 = 6;
const CREATE_TIME: i64 = 1_700_000_000_123;

fn build_create_topics_request(topic: &str, num_partitions: i32) -> Bytes {
    let mut enc = Encoder::with_capacity(128);
    enc.write_varint(2); // one topic (compact array N+1)
    enc.write_compact_nullable_string(Some(topic));
    enc.write_i32(num_partitions);
    enc.write_i16(1); // replication_factor
    enc.write_varint(1); // no explicit assignments
    enc.write_varint(1); // no configs
    enc.write_empty_tagged_fields(); // topic tagged fields
    enc.write_i32(5_000); // timeout_ms
    enc.write_bool(false); // validate_only
    enc.write_empty_tagged_fields(); // request tagged fields
    enc.freeze()
}

/// `(error_code, error_message, num_partitions)` from the first (only) topic result.
fn decode_create_topics_response(body: Bytes) -> (i16, Option<String>, i32) {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let _topics_plus_one = d.read_varint().expect("topics array count");
    let _name = d.read_compact_nullable_string().expect("topic name");
    let error_code = d.read_i16().expect("error_code");
    let error_message = d.read_compact_nullable_string().expect("error_message");
    let num_partitions = d.read_i32().expect("num_partitions");
    (error_code, error_message, num_partitions)
}

fn build_metadata_request(topic: &str) -> Bytes {
    let mut enc = Encoder::with_capacity(64);
    enc.write_varint(2); // one requested topic
    enc.write_compact_nullable_string(Some(topic));
    enc.write_empty_tagged_fields(); // per-topic tagged fields
    enc.write_bool(false); // allow_auto_topic_creation
    enc.write_bool(false); // include_cluster_authorized_operations
    enc.write_bool(false); // include_topic_authorized_operations
    enc.write_empty_tagged_fields();
    enc.freeze()
}

/// `(topic error_code, partition error_code, leader_id of partition 0, node_ids in the brokers
/// array, controller_id, first broker's host, first broker's port)` for the single requested
/// topic.
#[allow(clippy::type_complexity)]
fn decode_metadata_response(body: Bytes) -> (i16, i16, i32, Vec<i32>, i32, Option<String>, i32) {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");

    let brokers_plus_one = d.read_varint().expect("brokers array count");
    assert!(brokers_plus_one >= 2, "at least one broker in the list");
    let mut broker_host = None;
    let mut broker_port = 0;
    let mut node_ids = Vec::new();
    for i in 1..brokers_plus_one {
        let node_id = d.read_i32().expect("node_id");
        let host = d.read_compact_nullable_string().expect("host");
        let port = d.read_i32().expect("port");
        d.read_compact_nullable_string().expect("rack");
        d.read_tagged_fields().expect("broker tagged fields");
        node_ids.push(node_id);
        if i == 1 {
            broker_host = host;
            broker_port = port;
        }
    }
    d.read_compact_nullable_string().expect("cluster_id");
    let controller_id = d.read_i32().expect("controller_id");

    let topics_plus_one = d.read_varint().expect("topics array count");
    assert_eq!(topics_plus_one, 2, "exactly one topic was requested");
    let error_code = d.read_i16().expect("topic error_code");
    d.read_compact_nullable_string().expect("topic name");
    d.read_bool().expect("is_internal");

    let partitions_plus_one = d.read_varint().expect("partitions array count");
    assert_eq!(partitions_plus_one, 2, "one partition was created");
    let partition_error_code = d.read_i16().expect("partition error_code");
    d.read_i32().expect("partition_index");
    let leader_id = d.read_i32().expect("leader_id");
    (
        error_code,
        partition_error_code,
        leader_id,
        node_ids,
        controller_id,
        broker_host,
        broker_port,
    )
}

fn assert_metadata_response(body: Bytes, gateway_addr: SocketAddr) {
    let (
        error_code,
        partition_error_code,
        leader_id,
        node_ids,
        controller_id,
        broker_host,
        broker_port,
    ) = decode_metadata_response(body);
    assert_eq!(error_code, ERROR_NONE, "Metadata must report the new topic");
    assert_eq!(
        partition_error_code, ERROR_NONE,
        "the single partition must report no error"
    );
    assert_eq!(leader_id, 1, "this gateway is the sole broker");
    assert!(
        node_ids.contains(&leader_id),
        "leader_id {leader_id} must name a broker actually present in the brokers array \
         {node_ids:?} - a real client cannot route to a leader it was never told about"
    );
    assert_eq!(
        controller_id, 1,
        "single-broker cluster: the controller is the same broker"
    );
    assert_eq!(
        broker_host.as_deref(),
        Some(gateway_addr.ip().to_string().as_str()),
        "Metadata must advertise the address a client can actually reach this gateway on"
    );
    assert_eq!(
        broker_port,
        i32::from(gateway_addr.port()),
        "Metadata must advertise the port this gateway is actually listening on"
    );
}

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
        sequence: NO_SEQUENCE + i32::try_from(offset).expect("small offset"),
        timestamp: CREATE_TIME,
        key: None,
        value: Some(Bytes::copy_from_slice(value)),
        headers: IndexMap::new(),
    }
}

fn build_produce_request(topic: &str, records: &[Record]) -> Bytes {
    let mut batch_buf = BytesMut::new();
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    RecordBatchEncoder::encode(&mut batch_buf, records.iter(), &options)
        .expect("encode record batch");

    let mut enc = Encoder::with_capacity(1024);
    enc.write_nullable_string(None)
        .expect("null transactional_id");
    enc.write_i16(1); // acks: leader
    enc.write_i32(5_000); // timeout_ms
    enc.write_i32(1); // one topic
    enc.write_nullable_string(Some(topic))
        .expect("topic name fits");
    enc.write_i32(1); // one partition
    enc.write_i32(0); // partition_index
    enc.write_nullable_bytes(Some(&batch_buf))
        .expect("record batch fits");
    enc.freeze()
}

/// `(error_code, base_offset)` from the first (only) partition result.
fn decode_produce_response(body: Bytes) -> (i16, i64) {
    let mut d = Decoder::new(body);
    let _topics = d.read_i32().expect("topics array count");
    let _name = d.read_nullable_string().expect("topic name");
    let _partitions = d.read_i32().expect("partitions array count");
    let _index = d.read_i32().expect("partition_index");
    let error_code = d.read_i16().expect("error_code");
    let base_offset = d.read_i64().expect("base_offset");
    let _log_append_time = d.read_i64().expect("log_append_time");
    let _log_start_offset = d.read_i64().expect("log_start_offset");
    (error_code, base_offset)
}

fn build_list_offsets_request(topic: &str) -> Bytes {
    let mut enc = Encoder::with_capacity(64);
    enc.write_i32(-1); // replica_id
    enc.write_i8(0); // isolation_level: READ_UNCOMMITTED
    enc.write_varint(2); // one topic
    enc.write_compact_nullable_string(Some(topic));
    enc.write_varint(2); // one partition
    enc.write_i32(0); // partition_index
    enc.write_i32(-1); // current_leader_epoch
    enc.write_i64(-1); // timestamp: latest
    enc.write_empty_tagged_fields(); // partition tagged fields
    enc.write_empty_tagged_fields(); // topic tagged fields
    enc.write_empty_tagged_fields(); // request tagged fields
    enc.freeze()
}

/// `(error_code, offset)` for the requested partition's latest-offset result.
fn decode_list_offsets_response(body: Bytes) -> (i16, i64) {
    let mut d = Decoder::new(body);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let _topics_plus_one = d.read_varint().expect("topics array count");
    let _name = d.read_compact_nullable_string().expect("topic name");
    let _partitions_plus_one = d.read_varint().expect("partitions array count");
    let _partition_index = d.read_i32().expect("partition_index");
    let error_code = d.read_i16().expect("error_code");
    let _timestamp = d.read_i64().expect("timestamp");
    let offset = d.read_i64().expect("offset");
    (error_code, offset)
}

#[tokio::test]
async fn phase1_produce_flow_through_real_gateway_process_and_real_iggy_server() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let gateway = TestGateway::spawn(&server).await;
    let addr: SocketAddr = gateway.address.parse().expect("gateway address");

    let (corr, body) = round_trip(
        addr,
        API_KEY_CREATE_TOPICS,
        CREATE_TOPICS_VERSION,
        1,
        &build_create_topics_request(TOPIC, 1),
    )
    .await;
    assert_eq!(corr, 1, "correlation id must echo the request");
    let (error_code, error_message, num_partitions) = decode_create_topics_response(body);
    assert_eq!(error_code, ERROR_NONE, "CreateTopics must succeed");
    assert_eq!(
        error_message, None,
        "no error_message on a successful result"
    );
    assert_eq!(num_partitions, 1);

    let (corr, body) = round_trip(
        addr,
        API_KEY_METADATA,
        METADATA_VERSION,
        2,
        &build_metadata_request(TOPIC),
    )
    .await;
    assert_eq!(corr, 2, "correlation id must echo the request");
    assert_metadata_response(body, addr);

    let records = [record(0, b"phase1-hello"), record(1, b"phase1-world")];
    let (corr, body) = round_trip(
        addr,
        API_KEY_PRODUCE,
        PRODUCE_VERSION,
        3,
        &build_produce_request(TOPIC, &records),
    )
    .await;
    assert_eq!(corr, 3, "correlation id must echo the request");
    let (error_code, base_offset) = decode_produce_response(body);
    assert_eq!(error_code, ERROR_NONE, "Produce must succeed");
    assert_eq!(base_offset, 0, "first batch into an empty partition");

    let (corr, body) = round_trip(
        addr,
        API_KEY_LIST_OFFSETS,
        LIST_OFFSETS_VERSION,
        4,
        &build_list_offsets_request(TOPIC),
    )
    .await;
    assert_eq!(corr, 4, "correlation id must echo the request");
    let (error_code, offset) = decode_list_offsets_response(body);
    assert_eq!(error_code, ERROR_NONE, "ListOffsets must succeed");
    assert_eq!(offset, 2, "high watermark after two produced records");

    // Read the produced records back through the Iggy SDK directly rather than a real Kafka
    // Fetch - see this file's module doc for why (Fetch itself is implemented).
    let raw = raw_client(&server).await;
    let polled = raw
        .poll_messages(
            &Identifier::named(STREAM).expect("stream name"),
            &Identifier::named(TOPIC).expect("topic name"),
            Some(0),
            &Consumer::new(Identifier::named("phase1-e2e-reader").expect("consumer name")),
            &PollingStrategy::offset(0),
            2,
            false,
        )
        .await
        .expect("poll produced messages")
        .messages;
    assert_eq!(polled.len(), 2, "both produced records must be readable");
    assert_eq!(polled[0].payload.as_ref(), b"phase1-hello");
    assert_eq!(polled[1].payload.as_ref(), b"phase1-world");
}
