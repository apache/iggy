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

#[path = "common/codec.rs"]
mod codec;

use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::fetch_request::FetchTopic;
use kafka_protocol::messages::list_offsets_request::ListOffsetsTopic;
use kafka_protocol::messages::produce_request::TopicProduceData;
use kafka_protocol::messages::{
    BrokerId, CreateTopicsRequest, FetchRequest, ListOffsetsRequest, TopicName,
};
use kafka_protocol::protocol::StrBytes;

use iggy_gateway_kafka::protocol::api::{
    ERROR_INVALID_PARTITIONS, ERROR_INVALID_REPLICATION_FACTOR, ERROR_INVALID_REQUEST,
    ERROR_NOT_CONTROLLER, ERROR_UNSUPPORTED_VERSION,
};
use iggy_gateway_kafka::protocol::responses::{
    encode_create_topics_error_response, encode_create_topics_response,
    encode_fetch_error_response, encode_fetch_response, encode_list_offsets_error_response,
    encode_produce_error_response,
};

use codec::Decoder;

fn topic_name(name: &str) -> TopicName {
    TopicName(StrBytes::from_string(name.to_string()))
}

fn creatable_topic(name: &str, num_partitions: i32, replication_factor: i16) -> CreatableTopic {
    CreatableTopic::default()
        .with_name(topic_name(name))
        .with_num_partitions(num_partitions)
        .with_replication_factor(replication_factor)
}

#[test]
fn create_topics_response_flags_non_positive_partition_count_v2() {
    let req = CreateTopicsRequest::default().with_topics(vec![creatable_topic("bad-topic", 0, 1)]);
    let body = encode_create_topics_response(2, &req).unwrap();
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.read_i32().unwrap(), 1); // topics len
    assert_eq!(
        d.read_nullable_string().unwrap(),
        Some("bad-topic".to_string())
    );
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_PARTITIONS);
}

#[test]
fn create_topics_v5_broker_default_partitions_is_not_invalid_partitions() {
    // KIP-464: -1 = broker default on CreateTopics v4+; stub still returns NOT_CONTROLLER.
    let req = CreateTopicsRequest::default()
        .with_topics(vec![creatable_topic("default-parts", -1, 2)])
        .with_validate_only(true);
    let body = encode_create_topics_response(5, &req).unwrap();
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.read_varint().unwrap(), 2); // one topic
    assert_eq!(
        d.read_compact_nullable_string().unwrap(),
        Some("default-parts".to_string())
    );
    assert_eq!(d.read_i16().unwrap(), ERROR_NOT_CONTROLLER);
    assert_eq!(d.read_compact_nullable_string().unwrap(), None);
    assert_eq!(d.read_i32().unwrap(), -1);
    assert_eq!(d.read_i16().unwrap(), 2);
}

#[test]
fn create_topics_v5_flags_zero_and_below_minus_one_partition_count() {
    for num_partitions in [0i32, -2] {
        let req = CreateTopicsRequest::default().with_topics(vec![creatable_topic(
            "bad-parts",
            num_partitions,
            1,
        )]);
        let body = encode_create_topics_response(5, &req).unwrap();
        let mut d = Decoder::new(body);
        assert_eq!(d.read_i32().unwrap(), 0);
        assert_eq!(d.read_varint().unwrap(), 2);
        assert_eq!(
            d.read_compact_nullable_string().unwrap(),
            Some("bad-parts".to_string())
        );
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_INVALID_PARTITIONS,
            "num_partitions={num_partitions}"
        );
    }
}

#[test]
fn create_topics_v5_flags_invalid_replication_factor() {
    for replication_factor in [0i16, -2] {
        let req = CreateTopicsRequest::default().with_topics(vec![creatable_topic(
            "bad-rf",
            1,
            replication_factor,
        )]);
        let body = encode_create_topics_response(5, &req).unwrap();
        let mut d = Decoder::new(body);
        assert_eq!(d.read_i32().unwrap(), 0);
        assert_eq!(d.read_varint().unwrap(), 2);
        assert_eq!(
            d.read_compact_nullable_string().unwrap(),
            Some("bad-rf".to_string())
        );
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_INVALID_REPLICATION_FACTOR,
            "replication_factor={replication_factor}"
        );
    }
}

#[test]
fn create_topics_v2_rejects_broker_default_sentinel() {
    // KIP-464 defaults apply from v4; on v2 without assignments, -1 is INVALID_PARTITIONS.
    let req = CreateTopicsRequest::default().with_topics(vec![creatable_topic("legacy", -1, 1)]);
    let body = encode_create_topics_response(2, &req).unwrap();
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(
        d.read_nullable_string().unwrap(),
        Some("legacy".to_string())
    );
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_PARTITIONS);
}

#[test]
fn create_topics_v2_with_assignments_allows_broker_default_sentinels() {
    // KIP-464: on v2/v3, -1 partitions/replication are valid when assignments are present.
    use kafka_protocol::messages::create_topics_request::CreatableReplicaAssignment;

    let assignment = CreatableReplicaAssignment::default()
        .with_partition_index(0)
        .with_broker_ids(vec![BrokerId(1)]);
    let req = CreateTopicsRequest::default().with_topics(vec![
        creatable_topic("assigned", -1, -1).with_assignments(vec![assignment]),
    ]);
    let body = encode_create_topics_response(2, &req).unwrap();
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(
        d.read_nullable_string().unwrap(),
        Some("assigned".to_string())
    );
    assert_eq!(d.read_i16().unwrap(), ERROR_NOT_CONTROLLER);
}

#[test]
fn create_topics_error_response_carries_explicit_error_code() {
    let body = encode_create_topics_error_response(5, ERROR_UNSUPPORTED_VERSION).unwrap();
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_varint().unwrap(), 2);
    assert_eq!(
        d.read_compact_nullable_string().unwrap(),
        Some(String::new())
    );
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
}

#[test]
fn fetch_error_response_v7_uses_top_level_error_and_no_topics() {
    let body = encode_fetch_error_response(7, ERROR_INVALID_REQUEST).unwrap();
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
    assert_eq!(d.read_i32().unwrap(), 0); // session_id
    assert_eq!(d.read_i32().unwrap(), 0); // empty topics
    assert_eq!(d.remaining(), 0);
}

#[test]
fn fetch_error_response_v12_uses_flexible_empty_topics() {
    let body = encode_fetch_error_response(12, ERROR_UNSUPPORTED_VERSION).unwrap();
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    assert_eq!(d.read_i32().unwrap(), 0); // session_id
    assert_eq!(d.read_varint().unwrap(), 1); // empty topics compact array
    d.read_tagged_fields().unwrap();
    assert_eq!(d.remaining(), 0);
}

#[test]
fn list_offsets_error_response_v6_uses_flexible_shape() {
    // v0's legacy `old_style_offsets` shape has no `kafka_protocol` encoder - see
    // `responses::encode_list_offsets_error_response` and the version-firewall e2e coverage for
    // that behavior. This exercises the lowest version the crate can actually encode.
    let body = encode_list_offsets_error_response(6, ERROR_UNSUPPORTED_VERSION).unwrap();
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.read_varint().unwrap(), 2); // one topic
    assert_eq!(
        d.read_compact_nullable_string().unwrap(),
        Some(String::new())
    );
    assert_eq!(d.read_varint().unwrap(), 2); // one partition
    assert_eq!(d.read_i32().unwrap(), 0); // partition index
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    assert_eq!(d.read_i64().unwrap(), -1); // timestamp
    assert_eq!(d.read_i64().unwrap(), -1); // offset
    assert_eq!(d.read_i32().unwrap(), -1); // leader_epoch
    d.read_tagged_fields().unwrap();
}

#[test]
fn produce_error_response_v9_uses_flexible_record_errors_shape() {
    let body = encode_produce_error_response(9, ERROR_INVALID_REQUEST).unwrap();
    let mut d = Decoder::new(body);
    assert_eq!(d.read_varint().unwrap(), 2); // one topic
    assert_eq!(
        d.read_compact_nullable_string().unwrap(),
        Some(String::new())
    );
    assert_eq!(d.read_varint().unwrap(), 2); // one partition
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
    assert_eq!(d.read_i64().unwrap(), 0);
    assert_eq!(d.read_i64().unwrap(), -1);
    assert_eq!(d.read_i64().unwrap(), 0);
    assert_eq!(d.read_varint().unwrap(), 1); // empty record_errors array
    assert_eq!(d.read_compact_nullable_string().unwrap(), None);
}

#[test]
fn success_responses_can_still_encode_empty_request_vectors() {
    let produce = kafka_protocol::messages::ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(1_000)
        .with_topic_data(Vec::<TopicProduceData>::new());
    assert!(
        !iggy_gateway_kafka::protocol::responses::encode_produce_response(3, &produce)
            .unwrap()
            .is_empty()
    );

    let fetch = FetchRequest::default().with_topics(Vec::<FetchTopic>::new());
    assert!(!encode_fetch_response(4, &fetch).unwrap().is_empty());

    let list_offsets = ListOffsetsRequest::default().with_topics(Vec::<ListOffsetsTopic>::new());
    assert!(
        !iggy_gateway_kafka::protocol::responses::encode_list_offsets_response(1, &list_offsets)
            .unwrap()
            .is_empty()
    );

    let create_topics = CreateTopicsRequest::default().with_topics(Vec::<CreatableTopic>::new());
    assert!(
        !encode_create_topics_response(2, &create_topics)
            .unwrap()
            .is_empty()
    );
}
