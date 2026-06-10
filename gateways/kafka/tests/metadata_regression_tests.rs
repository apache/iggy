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

//! Metadata API regression — all supported versions, broker advertise, topic counts.

#[path = "common/scope.rs"]
mod scope;

use bytes::Bytes;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_METADATA, BrokerAdvertise, ERROR_UNKNOWN_TOPIC_OR_PARTITION, handle_request,
};
use iggy_gateway_kafka::protocol::codec::{Decoder, Encoder};

use scope::default_broker;

fn metadata_request_legacy(topic_count: i32) -> Bytes {
    let mut enc = Encoder::with_capacity(8);
    enc.write_i32(topic_count);
    enc.freeze()
}

fn metadata_request_flexible(topic_count: usize) -> Bytes {
    let mut enc = Encoder::with_capacity(8);
    enc.write_varint((topic_count + 1) as u64);
    enc.freeze()
}

fn read_broker_legacy(d: &mut Decoder) -> (String, i32) {
    let count = d.read_i32().unwrap();
    assert_eq!(count, 1);
    let _node = d.read_i32().unwrap();
    let host = d.read_nullable_string().unwrap().unwrap();
    let port = d.read_i32().unwrap();
    (host, port)
}

fn read_broker_flexible(d: &mut Decoder) -> (String, i32) {
    let count_plus_one = d.read_varint().unwrap();
    assert_eq!(count_plus_one, 2); // one broker
    let _node = d.read_i32().unwrap();
    let host = d.read_compact_nullable_string().unwrap().unwrap();
    let port = d.read_i32().unwrap();
    let _rack = d.read_compact_nullable_string().unwrap();
    d.read_tagged_fields().unwrap();
    (host, port)
}

#[test]
fn metadata_v0_empty_topics_stub_broker() {
    let body = handle_request(
        API_KEY_METADATA,
        0,
        metadata_request_legacy(0),
        &default_broker(),
    );
    let mut d = Decoder::new(body);
    let (host, port) = read_broker_legacy(&mut d);
    assert_eq!(host, "127.0.0.1");
    assert_eq!(port, 9093);
    assert_eq!(d.read_i32().unwrap(), 0);
}

#[test]
fn metadata_v0_three_topics_each_unknown() {
    let body = handle_request(
        API_KEY_METADATA,
        0,
        metadata_request_legacy(3),
        &default_broker(),
    );
    let mut d = Decoder::new(body);
    let _ = read_broker_legacy(&mut d);
    assert_eq!(d.read_i32().unwrap(), 3);
    for _ in 0..3 {
        assert_eq!(d.read_i16().unwrap(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(d.read_nullable_string().unwrap().unwrap(), "unknown-topic");
        assert_eq!(d.read_i32().unwrap(), 0);
    }
}

#[test]
fn metadata_v1_includes_controller_id() {
    let body = handle_request(
        API_KEY_METADATA,
        1,
        metadata_request_legacy(0),
        &default_broker(),
    );
    let mut d = Decoder::new(body);
    // Metadata v1 has no throttle_time_ms (added in v3).
    let _ = read_broker_legacy(&mut d);
    let _rack = d.read_nullable_string().unwrap();
    let controller = d.read_i32().unwrap();
    assert_eq!(controller, 1);
}

#[test]
fn metadata_v2_includes_cluster_id_field() {
    let body = handle_request(
        API_KEY_METADATA,
        2,
        metadata_request_legacy(0),
        &default_broker(),
    );
    let mut d = Decoder::new(body);
    let _ = read_broker_legacy(&mut d);
    let _rack = d.read_nullable_string().unwrap();
    let _cluster_id = d.read_nullable_string().unwrap();
    let _controller = d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 0);
}

#[test]
fn metadata_all_legacy_versions_produce_valid_response() {
    for version in 0i16..=8 {
        let body = handle_request(
            API_KEY_METADATA,
            version,
            metadata_request_legacy(1),
            &default_broker(),
        );
        let mut d = Decoder::new(body);
        if version >= 3 {
            let _throttle = d.read_i32().unwrap();
        }
        let _ = read_broker_legacy(&mut d);
        if version >= 1 {
            let _rack = d.read_nullable_string().unwrap();
        }
        if version >= 2 {
            let _cluster = d.read_nullable_string().unwrap();
        }
        if version >= 1 {
            let _controller = d.read_i32().unwrap();
        }
        assert_eq!(d.read_i32().unwrap(), 1);
        assert_eq!(d.read_i16().unwrap(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }
}

#[test]
fn metadata_v9_flexible_encoding() {
    let body = handle_request(
        API_KEY_METADATA,
        9,
        metadata_request_flexible(2),
        &default_broker(),
    );
    let mut d = Decoder::new(body);
    let _throttle = d.read_i32().unwrap();
    let (host, port) = read_broker_flexible(&mut d);
    assert_eq!(host, "127.0.0.1");
    assert_eq!(port, 9093);
    let _cluster = d.read_compact_nullable_string().unwrap();
    let controller = d.read_i32().unwrap();
    assert_eq!(controller, 1);

    let topics_plus_one = d.read_varint().unwrap();
    assert_eq!(topics_plus_one, 3); // 2 topics
    for _ in 0..2 {
        assert_eq!(d.read_i16().unwrap(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            d.read_compact_nullable_string().unwrap().unwrap(),
            "unknown-topic"
        );
        let _internal = d.read_bool().unwrap();
        let parts_plus_one = d.read_varint().unwrap();
        assert_eq!(parts_plus_one, 1); // empty partitions
        assert_eq!(d.read_i32().unwrap(), i32::MIN); // topic_authorized_operations (v8+)
        d.read_tagged_fields().unwrap();
    }
    assert_eq!(d.read_i32().unwrap(), i32::MIN); // cluster_authorized_operations (v8+)
    d.read_tagged_fields().unwrap();
    assert_eq!(d.remaining(), 0);
}

#[test]
fn metadata_v8_includes_authorized_operations_legacy() {
    let body = handle_request(
        API_KEY_METADATA,
        8,
        metadata_request_legacy(1),
        &default_broker(),
    );
    let mut d = Decoder::new(body);
    let _throttle = d.read_i32().unwrap();
    let _ = read_broker_legacy(&mut d);
    let _rack = d.read_nullable_string().unwrap();
    let _cluster = d.read_nullable_string().unwrap();
    let _controller = d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 1);
    let _topic_error = d.read_i16().unwrap();
    let _topic = d.read_nullable_string().unwrap();
    let _internal = d.read_bool().unwrap();
    assert_eq!(d.read_i32().unwrap(), 0); // empty partitions
    assert_eq!(d.read_i32().unwrap(), i32::MIN); // topic_authorized_operations
    assert_eq!(d.read_i32().unwrap(), i32::MIN); // cluster_authorized_operations
    assert_eq!(d.remaining(), 0);
}

#[test]
fn metadata_uses_custom_broker_advertise() {
    let broker = BrokerAdvertise {
        host: "10.0.0.42".to_string(),
        port: 29093,
    };
    let body = handle_request(API_KEY_METADATA, 0, metadata_request_legacy(0), &broker);
    let mut d = Decoder::new(body);
    let (host, port) = read_broker_legacy(&mut d);
    assert_eq!(host, "10.0.0.42");
    assert_eq!(port, 29093);
}
