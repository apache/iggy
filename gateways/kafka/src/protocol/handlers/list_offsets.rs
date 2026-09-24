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

//! `ListOffsets` (API key 2).

use bytes::Bytes;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::{ListOffsetsRequest, ListOffsetsResponse};

use crate::error::Result;
use crate::protocol::api::{
    API_KEY_LIST_OFFSETS, ApiVersionRange, ERROR_NOT_LEADER_OR_FOLLOWER, GatewayState,
    HandleOutcome,
};
use crate::protocol::bounds_guard::validate_list_offsets_shape;
use crate::protocol::handlers::{decode_guarded, encode_message, handle_versioned_request};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_LIST_OFFSETS,
    min_version: 1,
    max_version: 6,
};

#[expect(
    clippy::unused_async,
    reason = "the shared handler signature, kept until a handler awaits the bridge"
)]
pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    handle_versioned_request(
        API_KEY_LIST_OFFSETS,
        api_version,
        body,
        |v, b| {
            decode_guarded::<ListOffsetsRequest>(v, b, |v, b| {
                validate_list_offsets_shape(v, b, state.max_frame_size)
            })
        },
        encode_response,
        encode_error_response,
        "ListOffsets",
    )
}

/// Well-formed `ListOffsets` response with a single placeholder topic/partition.
///
/// `kafka_protocol` has no encodable representation for `ListOffsets` v0 (the legacy
/// `old_style_offsets` shape predates the schema this crate generates from); a v0 request now
/// falls through `super::unsupported_version_response`'s encode-failure path to `Close`
/// instead of the pre-migration downgraded response.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version` (always the
/// case for `version == 0`).
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let topics = vec![
        ListOffsetsTopicResponse::default()
            .with_partitions(vec![partition_response(0, error_code)]),
    ];
    encode_inner(version, topics)
}

/// Stub: discard the payload and return a retriable error, matching Produce/Fetch - a genuine
/// offset lookup requires the same partition-leadership the stub doesn't have yet.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, req: &ListOffsetsRequest) -> Result<Bytes> {
    let topics = req
        .topics
        .iter()
        .map(|topic| {
            ListOffsetsTopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|p| {
                            partition_response(p.partition_index, ERROR_NOT_LEADER_OR_FOLLOWER)
                        })
                        .collect(),
                )
        })
        .collect();
    encode_inner(version, topics)
}

fn encode_inner(version: i16, topics: Vec<ListOffsetsTopicResponse>) -> Result<Bytes> {
    let resp = ListOffsetsResponse::default().with_topics(topics);
    encode_message(&resp, version, 256)
}

fn partition_response(partition: i32, error_code: i16) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(partition)
        .with_error_code(error_code)
}
