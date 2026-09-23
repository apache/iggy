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

//! `InitProducerId` allocation and the transactional refusals that keep delivery honest.

#[path = "common/codec.rs"]
mod codec;
#[path = "common/scope.rs"]
mod scope;
#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use iggy_gateway_kafka::GatewayConfig;
use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_INIT_PRODUCER_ID, API_KEY_PRODUCE, ERROR_NONE,
    ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_UNSUPPORTED_VERSION, GatewayState, handle_request,
    handle_request_bounded, is_supported_version,
};

use codec::Decoder;
use scope::default_broker;
use server::{spawn_test_server, spawn_test_server_with_config};
use tcp::{ByteRead, build_request_frame, read_byte_with_timeout, round_trip};
use wire::{build_api_versions_flexible_request, build_init_producer_id_request};

/// Transaction APIs this gateway must never advertise. A client that cannot see them in
/// `ApiVersions` never sends them, which is the entire enforcement of "no transactions".
const TRANSACTION_API_KEYS: &[(i16, &str)] = &[
    (24, "AddPartitionsToTxn"),
    (25, "AddOffsetsToTxn"),
    (26, "EndTxn"),
    (28, "TxnOffsetCommit"),
];

const MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

/// Counter width in a producer id; the instance number sits above it.
const COUNTER_BITS: i64 = 47;

struct InitProducerIdResponse {
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
}

/// Decode a response body for `version`, asserting nothing is left over so a wrong flexible
/// threshold (tagged fields written at v1, or omitted at v2) fails here rather than passing.
fn decode_init_producer_id_response(version: i16, body: &Bytes) -> InitProducerIdResponse {
    let mut d = Decoder::new(body.clone());
    assert_eq!(d.read_i32().unwrap(), 0, "throttle_time_ms");
    let decoded = InitProducerIdResponse {
        error_code: d.read_i16().unwrap(),
        producer_id: d.read_i64().unwrap(),
        producer_epoch: d.read_i16().unwrap(),
    };
    if version >= 2 {
        d.read_tagged_fields().unwrap();
    }
    assert_eq!(d.remaining(), 0, "v{version} response has trailing bytes");
    decoded
}

async fn init_producer_id(
    state: &GatewayState,
    version: i16,
    transactional_id: Option<&str>,
) -> InitProducerIdResponse {
    let request = build_init_producer_id_request(version, transactional_id);
    let body = handle_request_bounded(state, API_KEY_INIT_PRODUCER_ID, version, request)
        .await
        .expect_response("InitProducerId always answers, it is never fire-and-forget");
    decode_init_producer_id_response(version, &body)
}

fn stub_state(instance_id: u16) -> GatewayState {
    GatewayState::new(default_broker(), None, MAX_FRAME_SIZE, instance_id)
}

/// Produce v3 body with one topic and one partition, so a per-partition error code has somewhere
/// to land.
fn produce_v3_body(acks: i16, transactional_id: Option<&str>) -> Bytes {
    let mut body = BytesMut::new();
    match transactional_id {
        Some(id) => {
            body.put_i16(i16::try_from(id.len()).expect("transactional id fits i16"));
            body.put_slice(id.as_bytes());
        }
        None => body.put_i16(-1),
    }
    body.put_i16(acks);
    body.put_i32(1_000); // timeout_ms
    body.put_i32(1); // one topic
    body.put_i16(6);
    body.put_slice(b"orders");
    body.put_i32(1); // one partition
    body.put_i32(0); // partition index
    body.put_i32(4); // records length
    body.put_slice(&[0x00, 0x00, 0x00, 0x00]);
    body.freeze()
}

/// Skips to the first per-partition `error_code` of a Produce v3 response.
fn first_produce_partition_error(body: &Bytes) -> i16 {
    let mut d = Decoder::new(body.clone());
    assert_eq!(d.read_i32().unwrap(), 1, "one topic in the response");
    assert_eq!(d.read_nullable_string().unwrap().as_deref(), Some("orders"));
    assert_eq!(d.read_i32().unwrap(), 1, "one partition in the response");
    assert_eq!(d.read_i32().unwrap(), 0, "partition index");
    d.read_i16().unwrap()
}

#[tokio::test]
async fn given_no_transactional_id_when_init_producer_id_should_allocate_an_id_with_epoch_zero() {
    let state = stub_state(0);
    for version in 0i16..=5 {
        let response = init_producer_id(&state, version, None).await;
        assert_eq!(response.error_code, ERROR_NONE, "v{version} error_code");
        assert_eq!(response.producer_epoch, 0, "v{version} producer_epoch");
        assert!(
            response.producer_id >= 0,
            "v{version} producer id must stay non-negative: -1 means no producer id"
        );
    }
}

#[tokio::test]
async fn given_one_gateway_when_two_producers_init_should_receive_distinct_ids() {
    let state = stub_state(0);
    let first = init_producer_id(&state, 4, None).await;
    let second = init_producer_id(&state, 4, None).await;
    assert_eq!(first.error_code, ERROR_NONE);
    assert_eq!(second.error_code, ERROR_NONE);
    assert_ne!(
        first.producer_id, second.producer_id,
        "each InitProducerId must advance the counter"
    );
}

#[tokio::test]
async fn given_a_configured_instance_id_when_init_producer_id_should_return_it_in_the_high_bits() {
    let instance_id = 0x0123u16;
    let response = init_producer_id(&stub_state(instance_id), 4, None).await;
    assert_eq!(response.error_code, ERROR_NONE);
    assert_eq!(
        response.producer_id >> COUNTER_BITS,
        i64::from(instance_id),
        "the configured instance number is what makes ids unique across gateways"
    );
}

#[tokio::test]
async fn given_a_transactional_id_when_init_producer_id_should_answer_unsupported_version() {
    let state = stub_state(0);
    for version in 0i16..=5 {
        let response = init_producer_id(&state, version, Some("orders-txn")).await;
        assert_eq!(
            response.error_code, ERROR_UNSUPPORTED_VERSION,
            "v{version} must refuse a transactional producer"
        );
        // A real broker's error path leaves both at their schema defaults, and -1 is the
        // "no producer id" sentinel the whole bit-63-clear layout exists to keep distinct
        // from a real allocation.
        assert_eq!(
            response.producer_id, -1,
            "v{version} a refusal must not hand back an allocated id"
        );
        assert_eq!(response.producer_epoch, 0, "v{version} producer_epoch");
    }
}

#[tokio::test]
async fn given_an_empty_transactional_id_when_init_producer_id_should_still_allocate() {
    let response = init_producer_id(&stub_state(0), 4, Some("")).await;
    assert_eq!(
        response.error_code, ERROR_NONE,
        "an empty transactional id names no transaction"
    );
    assert!(response.producer_id >= 0);
}

#[tokio::test]
async fn given_a_transactional_id_when_producing_should_answer_unsupported_version_per_partition() {
    let body = handle_request(
        API_KEY_PRODUCE,
        3,
        produce_v3_body(1, Some("orders-txn")),
        &default_broker(),
    )
    .await
    .expect_response("acks=1 expects a response");
    assert_eq!(
        first_produce_partition_error(&body),
        ERROR_UNSUPPORTED_VERSION,
        "a transactional batch must be refused, not stored as ordinary records"
    );
}

#[tokio::test]
async fn given_no_transactional_id_when_producing_should_keep_the_retriable_stub_error() {
    let body = handle_request(
        API_KEY_PRODUCE,
        3,
        produce_v3_body(1, None),
        &default_broker(),
    )
    .await
    .expect_response("acks=1 expects a response");
    assert_eq!(
        first_produce_partition_error(&body),
        ERROR_NOT_LEADER_OR_FOLLOWER,
        "a non-transactional produce keeps the retriable stub error, not the transactional refusal"
    );
}

#[tokio::test]
async fn given_acks_zero_and_a_transactional_id_when_producing_should_close_the_connection() {
    // acks=0 has no response to carry the refusal, and dropping the batch silently would leave
    // the refusal to whatever write path lands later. The acks=1 half pins that the same body is
    // refused when a response exists, so the close is the acks=0 form of that refusal.
    let refused = handle_request(
        API_KEY_PRODUCE,
        3,
        produce_v3_body(1, Some("orders-txn")),
        &default_broker(),
    )
    .await
    .expect_response("acks=1 expects a response");
    assert_eq!(
        first_produce_partition_error(&refused),
        ERROR_UNSUPPORTED_VERSION,
        "the same body must be refused when the client is reading a response"
    );

    let outcome = handle_request(
        API_KEY_PRODUCE,
        3,
        produce_v3_body(0, Some("orders-txn")),
        &default_broker(),
    )
    .await;
    assert!(
        outcome.is_close(),
        "acks=0 cannot carry 35, so a transactional batch must be refused by closing"
    );
}

#[tokio::test]
async fn given_the_advertised_api_list_when_a_client_reads_it_should_omit_the_transaction_keys() {
    let legacy = handle_request(API_KEY_API_VERSIONS, 1, Bytes::new(), &default_broker())
        .await
        .expect_response("ApiVersions always answers");
    let mut d = Decoder::new(legacy);
    assert_eq!(d.read_i16().unwrap(), ERROR_NONE);
    let count = d.read_i32().unwrap();
    let mut advertised = Vec::new();
    for _ in 0..count {
        advertised.push(d.read_i16().unwrap());
        d.read_i16().unwrap(); // min_version
        d.read_i16().unwrap(); // max_version
    }

    let flexible = handle_request(
        API_KEY_API_VERSIONS,
        3,
        build_api_versions_flexible_request("iggy-test", "0.1.0"),
        &default_broker(),
    )
    .await
    .expect_response("ApiVersions always answers");
    let mut d = Decoder::new(flexible);
    assert_eq!(d.read_i16().unwrap(), ERROR_NONE);
    let count = d.read_varint().unwrap() - 1;
    let mut advertised_flexible = Vec::new();
    for _ in 0..count {
        advertised_flexible.push(d.read_i16().unwrap());
        d.read_i16().unwrap(); // min_version
        d.read_i16().unwrap(); // max_version
        d.read_tagged_fields().unwrap();
    }

    assert!(
        advertised.contains(&API_KEY_INIT_PRODUCER_ID),
        "InitProducerId must be advertised or no producer ever sends it"
    );
    for &(api_key, name) in TRANSACTION_API_KEYS {
        assert!(
            !advertised.contains(&api_key),
            "{name} (key {api_key}) must stay out of the v1 advertisement"
        );
        assert!(
            !advertised_flexible.contains(&api_key),
            "{name} (key {api_key}) must stay out of the v3 advertisement"
        );
        assert!(
            !is_supported_version(api_key, 0),
            "{name} (key {api_key}) must not pass the version firewall"
        );
    }
}

#[tokio::test]
async fn given_a_transaction_api_key_when_sent_anyway_should_close_the_connection() {
    for &(api_key, name) in TRANSACTION_API_KEYS {
        let outcome = handle_request(api_key, 0, Bytes::new(), &default_broker()).await;
        assert!(
            outcome.is_close(),
            "{name} (key {api_key}) has no response schema here and must close"
        );
    }
}

#[tokio::test]
async fn given_a_live_server_when_a_transactional_request_is_refused_should_keep_the_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    for (api_key, version, body) in [
        (
            API_KEY_INIT_PRODUCER_ID,
            4i16,
            build_init_producer_id_request(4, Some("orders-txn")),
        ),
        (API_KEY_PRODUCE, 3, produce_v3_body(1, Some("orders-txn"))),
    ] {
        let frame = build_request_frame(api_key, version, 7_000, Some("txn-test"), &body);
        stream.write_all(&frame).await.expect("write request");
        let payload = tcp::read_response_frame(&mut stream, MAX_FRAME_SIZE).await;
        let (correlation_id, _) = tcp::parse_response_payload(api_key, version, payload);
        assert_eq!(correlation_id, 7_000, "key {api_key} correlation id");
        assert_ne!(
            read_byte_with_timeout(&mut stream, Duration::from_millis(250)).await,
            ByteRead::Closed,
            "key {api_key} must refuse the transaction without dropping the connection"
        );
    }
}

/// Config value has to reach the allocator, not just the struct. Nothing else pins that hop:
/// one test pins env to config, another pins `GatewayState::new` to the high bits, and the
/// server's own `config.instance_id` argument sat between them uncovered.
#[tokio::test]
async fn given_a_server_configured_with_an_instance_id_when_init_producer_id_should_reflect_it() {
    let instance_id = 0x0042u16;
    let (addr, _shutdown) = spawn_test_server_with_config(GatewayConfig {
        bind_addr: String::new(),
        advertised_host: None,
        advertised_port: None,
        max_frame_size: MAX_FRAME_SIZE,
        max_connections: 1024,
        idle_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
        shutdown_drain_timeout: Duration::from_secs(5),
        instance_id,
    })
    .await;

    let request = build_init_producer_id_request(4, None);
    let (_correlation_id, body) =
        round_trip(addr, API_KEY_INIT_PRODUCER_ID, 4, 7_200, &request).await;
    let response = decode_init_producer_id_response(4, &body);
    assert_eq!(response.error_code, ERROR_NONE);
    assert_eq!(
        response.producer_id >> COUNTER_BITS,
        i64::from(instance_id),
        "the configured instance number must reach the allocator, not stop at the config struct"
    );
}

/// One allocator per process, shared across connections. Building a `GatewayState` per accepted
/// connection instead would restart every counter at 0 and put duplicate producer ids on the
/// wire, which Kafka requires to be unique; the in-process test above cannot see that because it
/// never opens a second connection.
#[tokio::test]
async fn given_one_server_when_two_connections_init_should_receive_distinct_ids() {
    let (addr, _shutdown) = spawn_test_server().await;
    let request = build_init_producer_id_request(4, None);

    let (_first_id, first_body) =
        round_trip(addr, API_KEY_INIT_PRODUCER_ID, 4, 7_300, &request).await;
    let (_second_id, second_body) =
        round_trip(addr, API_KEY_INIT_PRODUCER_ID, 4, 7_301, &request).await;

    let first = decode_init_producer_id_response(4, &first_body);
    let second = decode_init_producer_id_response(4, &second_body);
    assert_eq!(first.error_code, ERROR_NONE);
    assert_eq!(second.error_code, ERROR_NONE);
    assert_ne!(
        first.producer_id, second.producer_id,
        "separate connections must draw from one allocator"
    );
}

#[tokio::test]
async fn given_a_live_server_when_init_producer_id_round_trips_should_return_an_allocated_id() {
    let (addr, _shutdown) = spawn_test_server().await;
    let request = build_init_producer_id_request(4, None);
    let (correlation_id, body) =
        round_trip(addr, API_KEY_INIT_PRODUCER_ID, 4, 7_100, &request).await;
    assert_eq!(correlation_id, 7_100);
    let response = decode_init_producer_id_response(4, &body);
    assert_eq!(response.error_code, ERROR_NONE);
    assert_eq!(response.producer_epoch, 0);
    assert!(response.producer_id >= 0);
}
