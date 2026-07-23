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

use iggy_gateway_kafka::protocol::api::{
    ERROR_INVALID_PARTITIONS, ERROR_INVALID_REPLICATION_FACTOR, ERROR_INVALID_REQUEST,
    ERROR_NOT_CONTROLLER, ERROR_UNSUPPORTED_VERSION,
};
use iggy_gateway_kafka::protocol::codec::Decoder;
use iggy_gateway_kafka::protocol::requests::{
    CreatableTopic, CreateTopicsRequest, FetchPartition, FetchRequest, FetchTopic,
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic, ProducePartitionData,
    ProduceRequest, ProduceTopicData,
};
use iggy_gateway_kafka::protocol::responses::{
    encode_create_topics_error_response, encode_create_topics_response,
    encode_fetch_error_response, encode_list_offsets_error_response, encode_produce_error_response,
};

#[test]
fn create_topics_response_flags_non_positive_partition_count_v2() {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "bad-topic".to_string(),
            num_partitions: 0,
            replication_factor: 1,
        }],
        timeout_ms: 5_000,
        validate_only: false,
    };
    let mut d = Decoder::new(encode_create_topics_response(2, &req));
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
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "default-parts".to_string(),
            num_partitions: -1,
            replication_factor: 2,
        }],
        timeout_ms: 5_000,
        validate_only: true,
    };
    let mut d = Decoder::new(encode_create_topics_response(5, &req));
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
        let req = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "bad-parts".to_string(),
                num_partitions,
                replication_factor: 1,
            }],
            timeout_ms: 5_000,
            validate_only: false,
        };
        let mut d = Decoder::new(encode_create_topics_response(5, &req));
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
        let req = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "bad-rf".to_string(),
                num_partitions: 1,
                replication_factor,
            }],
            timeout_ms: 5_000,
            validate_only: false,
        };
        let mut d = Decoder::new(encode_create_topics_response(5, &req));
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
    // KIP-464 defaults apply from v4; on v2, -1 is still INVALID_PARTITIONS.
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "legacy".to_string(),
            num_partitions: -1,
            replication_factor: 1,
        }],
        timeout_ms: 5_000,
        validate_only: false,
    };
    let mut d = Decoder::new(encode_create_topics_response(2, &req));
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(
        d.read_nullable_string().unwrap(),
        Some("legacy".to_string())
    );
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_PARTITIONS);
}

#[test]
fn create_topics_error_response_carries_explicit_error_code() {
    let mut d = Decoder::new(encode_create_topics_error_response(
        5,
        ERROR_UNSUPPORTED_VERSION,
    ));
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
    let mut d = Decoder::new(encode_fetch_error_response(7, ERROR_INVALID_REQUEST));
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
    assert_eq!(d.read_i32().unwrap(), 0); // session_id
    assert_eq!(d.read_i32().unwrap(), 0); // empty topics
    assert_eq!(d.remaining(), 0);
}

#[test]
fn fetch_error_response_v12_uses_flexible_empty_topics() {
    let mut d = Decoder::new(encode_fetch_error_response(12, ERROR_UNSUPPORTED_VERSION));
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    assert_eq!(d.read_i32().unwrap(), 0); // session_id
    assert_eq!(d.read_varint().unwrap(), 1); // empty topics compact array
    d.read_tagged_fields().unwrap();
    assert_eq!(d.remaining(), 0);
}

#[test]
fn list_offsets_error_response_v0_uses_legacy_old_style_offsets_array() {
    let mut d = Decoder::new(encode_list_offsets_error_response(
        0,
        ERROR_UNSUPPORTED_VERSION,
    ));
    assert_eq!(d.read_i32().unwrap(), 1); // topics
    assert_eq!(d.read_nullable_string().unwrap(), Some(String::new()));
    assert_eq!(d.read_i32().unwrap(), 1); // partitions
    assert_eq!(d.read_i32().unwrap(), 0); // partition index
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    assert_eq!(d.read_i32().unwrap(), 0); // old_style_offsets len
    assert_eq!(d.remaining(), 0);
}

#[test]
fn produce_error_response_v9_uses_flexible_record_errors_shape() {
    let mut d = Decoder::new(encode_produce_error_response(9, ERROR_INVALID_REQUEST));
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
    let produce = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 1_000,
        topics: Vec::<ProduceTopicData>::new(),
    };
    assert!(
        !iggy_gateway_kafka::protocol::responses::encode_produce_response(3, &produce).is_empty()
    );

    let fetch = FetchRequest {
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes: 0,
        isolation_level: 0,
        topics: Vec::<FetchTopic>::new(),
    };
    assert!(!iggy_gateway_kafka::protocol::responses::encode_fetch_response(4, &fetch).is_empty());

    let list_offsets = ListOffsetsRequest {
        isolation_level: 0,
        topics: Vec::<ListOffsetsTopic>::new(),
    };
    assert!(
        !iggy_gateway_kafka::protocol::responses::encode_list_offsets_response(1, &list_offsets)
            .is_empty()
    );

    let create_topics = CreateTopicsRequest {
        topics: Vec::<CreatableTopic>::new(),
        timeout_ms: 0,
        validate_only: false,
    };
    assert!(
        !iggy_gateway_kafka::protocol::responses::encode_create_topics_response(2, &create_topics)
            .is_empty()
    );
}

#[allow(clippy::let_unit_value)]
fn _type_anchors() {
    let _ = FetchPartition {
        partition: 0,
        fetch_offset: 0,
        partition_max_bytes: 0,
    };
    let _ = ListOffsetsPartition {
        partition: 0,
        timestamp: 0,
    };
    let _ = ProducePartitionData {
        partition: 0,
        records: None,
    };
}
