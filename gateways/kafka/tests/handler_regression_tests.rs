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

//! Full handler regression — every scoped API key × version through `handle_request`.

#[path = "common/fixtures.rs"]
mod fixtures;
#[path = "common/scope.rs"]
mod scope;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_CREATE_TOPICS, API_KEY_FETCH, API_KEY_LIST_OFFSETS, API_KEY_PRODUCE, ERROR_NONE,
    ERROR_NOT_CONTROLLER, ERROR_NOT_LEADER_OR_FOLLOWER, handle_request,
};
use iggy_gateway_kafka::protocol::codec::Decoder;

use fixtures::load_fixture_body_or_skip;
use scope::{SCOPED_API_KEYS, default_broker};

#[test]
fn handle_request_succeeds_for_every_supported_version_with_fixture() {
    for &(api_key, name, min_ver, max_ver) in SCOPED_API_KEYS {
        if api_key == 3 || api_key == 18 {
            // Metadata / ApiVersions: empty body is valid
            for version in min_ver..=max_ver {
                let resp = handle_request(api_key, version, bytes::Bytes::new(), &default_broker())
                    .expect_response("test request has acks != 0 and expects a response");
                assert!(
                    !resp.is_empty(),
                    "{name} v{version} returned empty response"
                );
            }
            continue;
        }

        for version in min_ver..=max_ver {
            let Some(body) = load_fixture_body_or_skip(api_key, name, version) else {
                continue;
            };
            let resp = handle_request(api_key, version, body, &default_broker())
                .expect_response("test request has acks != 0 and expects a response");
            assert!(
                !resp.is_empty(),
                "{name} v{version} returned empty response"
            );
        }
    }
}

#[test]
fn produce_stub_response_returns_retriable_not_leader() {
    for version in 3i16..=9 {
        let Some(body) = load_fixture_body_or_skip(0, "Produce", version) else {
            continue;
        };
        let resp = handle_request(API_KEY_PRODUCE, version, body, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        let flexible = version >= 9;
        let mut d = Decoder::new(resp);
        if flexible {
            let _topics = d.read_varint().unwrap();
            let _topic = d.read_compact_nullable_string().unwrap();
            let _parts = d.read_varint().unwrap();
        } else {
            let _topics = d.read_i32().unwrap();
            let _topic = d.read_nullable_string().unwrap();
            let _parts = d.read_i32().unwrap();
        }
        let _partition = d.read_i32().unwrap();
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_NOT_LEADER_OR_FOLLOWER,
            "Produce v{version}"
        );
    }
}

#[test]
fn fetch_stub_response_has_zero_partition_error() {
    for version in 4i16..=12 {
        let Some(body) = load_fixture_body_or_skip(1, "Fetch", version) else {
            continue;
        };
        let resp = handle_request(API_KEY_FETCH, version, body, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        let flexible = version >= 12;
        let mut d = Decoder::new(resp);
        if version >= 1 {
            let _throttle = d.read_i32().unwrap();
        }
        if version >= 7 {
            assert_eq!(d.read_i16().unwrap(), ERROR_NONE);
            let _session = d.read_i32().unwrap();
        }
        if flexible {
            let _topics = d.read_varint().unwrap();
            let _topic = d.read_compact_nullable_string().unwrap();
            let _parts = d.read_varint().unwrap();
        } else {
            let _topics = d.read_i32().unwrap();
            let _topic = d.read_nullable_string().unwrap();
            let _parts = d.read_i32().unwrap();
        }
        let _partition = d.read_i32().unwrap();
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_NONE,
            "Fetch v{version} partition error"
        );
    }
}

#[test]
fn list_offsets_stub_response_has_zero_error() {
    for version in 1i16..=6 {
        let Some(body) = load_fixture_body_or_skip(2, "ListOffsets", version) else {
            continue;
        };
        let resp = handle_request(API_KEY_LIST_OFFSETS, version, body, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        let flexible = version >= 6;
        let mut d = Decoder::new(resp);
        if version >= 2 {
            let _throttle = d.read_i32().unwrap();
        }
        if flexible {
            let _topics = d.read_varint().unwrap();
            let _topic = d.read_compact_nullable_string().unwrap();
            let _parts = d.read_varint().unwrap();
        } else {
            let _topics = d.read_i32().unwrap();
            let _topic = d.read_nullable_string().unwrap();
            let _parts = d.read_i32().unwrap();
        }
        let _partition = d.read_i32().unwrap();
        assert_eq!(d.read_i16().unwrap(), ERROR_NONE, "ListOffsets v{version}");
    }
}

#[test]
fn create_topics_stub_response_returns_not_controller() {
    for version in 2i16..=5 {
        let Some(body) = load_fixture_body_or_skip(19, "CreateTopics", version) else {
            continue;
        };
        let resp = handle_request(API_KEY_CREATE_TOPICS, version, body, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        let flexible = version >= 5;
        let mut d = Decoder::new(resp);
        if version >= 2 {
            let _throttle = d.read_i32().unwrap();
        }
        if flexible {
            let _topics = d.read_varint().unwrap();
            let _topic = d.read_compact_nullable_string().unwrap();
        } else {
            let _topics = d.read_i32().unwrap();
            let _topic = d.read_nullable_string().unwrap();
        }
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_NOT_CONTROLLER,
            "CreateTopics v{version}"
        );
    }
}
