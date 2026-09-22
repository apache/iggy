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

//! Fetch (API key 1).

use bytes::Bytes;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::{FetchRequest, FetchResponse};

use crate::error::Result;
use crate::protocol::api::{
    API_KEY_FETCH, ApiVersionRange, ERROR_NONE, ERROR_NOT_LEADER_OR_FOLLOWER, GatewayState,
    HandleOutcome,
};
use crate::protocol::bounds_guard::validate_fetch_shape;
use crate::protocol::handlers::{decode_guarded, encode_message, handle_versioned_request};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_FETCH,
    min_version: 4,
    max_version: 12,
};

#[expect(
    clippy::unused_async,
    reason = "the shared handler signature, kept until a handler awaits the bridge"
)]
pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    handle_versioned_request(
        API_KEY_FETCH,
        api_version,
        body,
        |v, b| {
            decode_guarded::<FetchRequest>(v, b, |v, b| {
                validate_fetch_shape(v, b, state.max_frame_size)
            })
        },
        encode_response,
        encode_error_response,
        "Fetch",
    )
}

/// Well-formed Fetch response. Uses top-level `error_code` at v7+, or a single
/// placeholder topic/partition with per-partition `error_code` below v7.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    if version >= 7 {
        return encode_inner(version, Vec::new(), error_code);
    }
    // No top-level error field below v7; the error surfaces on the placeholder partition instead.
    let topics = vec![
        FetchableTopicResponse::default().with_partitions(vec![partition_response(0, error_code)]),
    ];
    encode_inner(version, topics, ERROR_NONE)
}

/// Stub: discard the payload and return a retriable error so clients don't mistake "no real
/// data yet" for a genuinely empty partition (same philosophy as Produce).
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, req: &FetchRequest) -> Result<Bytes> {
    let topics = req
        .topics
        .iter()
        .map(|topic| {
            FetchableTopicResponse::default()
                .with_topic(topic.topic.clone())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|p| partition_response(p.partition, ERROR_NOT_LEADER_OR_FOLLOWER))
                        .collect(),
                )
        })
        .collect();
    encode_inner(version, topics, ERROR_NONE)
}

fn encode_inner(
    version: i16,
    topics: Vec<FetchableTopicResponse>,
    top_level_error: i16,
) -> Result<Bytes> {
    let resp = FetchResponse::default()
        .with_error_code(top_level_error)
        .with_responses(topics);
    encode_message(&resp, version, 512)
}

fn partition_response(partition: i32, error_code: i16) -> PartitionData {
    PartitionData::default()
        .with_partition_index(partition)
        .with_error_code(error_code)
        .with_last_stable_offset(0)
        .with_log_start_offset(0)
        .with_records(None)
}
