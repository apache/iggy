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

//! `CreateTopics` (API key 19).

use bytes::Bytes;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse};

use crate::error::Result;
use crate::protocol::api::{
    API_KEY_CREATE_TOPICS, ApiVersionRange, ERROR_INVALID_PARTITIONS,
    ERROR_INVALID_REPLICATION_FACTOR, ERROR_NONE, ERROR_NOT_CONTROLLER, GatewayState,
    HandleOutcome,
};
use crate::protocol::bounds_guard::validate_create_topics_shape;
use crate::protocol::handlers::{decode_guarded, encode_message, handle_versioned_request};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_CREATE_TOPICS,
    min_version: 2,
    max_version: 5,
};

#[expect(
    clippy::unused_async,
    reason = "the shared handler signature, kept until a handler awaits the bridge"
)]
pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    handle_versioned_request(
        API_KEY_CREATE_TOPICS,
        api_version,
        body,
        |v, b| {
            decode_guarded::<CreateTopicsRequest>(v, b, |v, b| {
                validate_create_topics_shape(v, b, state.max_frame_size)
            })
        },
        encode_response,
        encode_error_response,
        "CreateTopics",
    )
}

/// Well-formed `CreateTopics` response with a single placeholder topic.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let topics = vec![
        CreatableTopic::default()
            .with_num_partitions(1)
            .with_replication_factor(1),
    ];
    encode_inner(version, &topics, error_code)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, req: &CreateTopicsRequest) -> Result<Bytes> {
    encode_inner(version, &req.topics, ERROR_NONE)
}

/// Resolve per-topic `CreateTopics` error.
///
/// KIP-464: `num_partitions = -1` / `replication_factor = -1` mean broker default when either
/// (a) the version is v4+, or (b) the topic carries a manual partition assignment (valid on
/// v2/v3 as well). Otherwise non-positive values are [`ERROR_INVALID_PARTITIONS`] /
/// [`ERROR_INVALID_REPLICATION_FACTOR`]. When validation passes, the stub returns
/// [`ERROR_NOT_CONTROLLER`] so clients do not believe the topic was created.
const fn topic_error(version: i16, topic: &CreatableTopic, forced_error: i16) -> i16 {
    if forced_error != ERROR_NONE {
        return forced_error;
    }

    let broker_default_ok = version >= 4 || !topic.assignments.is_empty();

    let partitions_ok = if broker_default_ok {
        topic.num_partitions == -1 || topic.num_partitions > 0
    } else {
        topic.num_partitions > 0
    };
    if !partitions_ok {
        return ERROR_INVALID_PARTITIONS;
    }

    let replication_ok = if broker_default_ok {
        topic.replication_factor == -1 || topic.replication_factor > 0
    } else {
        topic.replication_factor > 0
    };
    if !replication_ok {
        return ERROR_INVALID_REPLICATION_FACTOR;
    }

    ERROR_NOT_CONTROLLER
}

fn encode_inner(version: i16, topics: &[CreatableTopic], forced_error: i16) -> Result<Bytes> {
    let results = topics
        .iter()
        .map(|topic| {
            CreatableTopicResult::default()
                .with_name(topic.name.clone())
                .with_error_code(topic_error(version, topic, forced_error))
                .with_error_message(None)
                .with_num_partitions(topic.num_partitions)
                .with_replication_factor(topic.replication_factor)
        })
        .collect();
    let resp = CreateTopicsResponse::default().with_topics(results);
    encode_message(&resp, version, 256)
}
