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

//! `FindCoordinator` (API key 10).
//!
//! The answer is always this gateway, matching the single broker entry Metadata advertises. No
//! group state is consulted: where the coordinator lives does not depend on which group is asked
//! about.

use bytes::Bytes;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::{BrokerId, FindCoordinatorRequest, FindCoordinatorResponse};
use kafka_protocol::protocol::StrBytes;

use crate::error::Result;
use crate::protocol::api::{
    API_KEY_FIND_COORDINATOR, ApiVersionRange, BrokerAdvertise, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED, ERROR_UNSUPPORTED_VERSION, GatewayState,
    HandleOutcome, is_supported_version,
};
use crate::protocol::bounds_guard::validate_find_coordinator_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_FIND_COORDINATOR,
    min_version: 0,
    max_version: 4,
};

/// `key_type` 0. Types 1 (transaction) and 2 (share) have no coordinator here.
const COORDINATOR_TYPE_GROUP: i8 = 0;
const COORDINATOR_TYPE_TRANSACTION: i8 = 1;

/// The node id this gateway advertises for itself, in Metadata and here alike.
const SELF_NODE_ID: i32 = 1;

const UNSUPPORTED_KEY_TYPE_MESSAGE: &str = "only group coordination is supported";

#[expect(
    clippy::unused_async,
    reason = "the shared handler signature, kept until a handler awaits the bridge"
)]
pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_FIND_COORDINATOR, api_version) {
        return unsupported_version_response(API_KEY_FIND_COORDINATOR, api_version, |version| {
            encode_error_response(version, ERROR_UNSUPPORTED_VERSION)
        });
    }
    match decode_guarded::<FindCoordinatorRequest>(api_version, body, |version, body| {
        validate_find_coordinator_shape(version, body, state.max_frame_size)
    }) {
        Ok(request) => respond_or_close(
            encode_response(api_version, &request, &state.broker),
            "FindCoordinator",
        ),
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, api_version, "Failed to decode FindCoordinator request");
            if api_version >= 4 {
                // v4 carries its error per requested key and the keys are exactly what failed to
                // decode, so there is no honest body to send.
                return HandleOutcome::Close;
            }
            respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "FindCoordinator",
            )
        }
    }
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(
    version: i16,
    request: &FindCoordinatorRequest,
    broker: &BrokerAdvertise,
) -> Result<Bytes> {
    let keys = if version >= 4 {
        request.coordinator_keys.clone()
    } else {
        vec![request.key.clone()]
    };
    let error_code = match request.key_type {
        COORDINATOR_TYPE_GROUP => {
            return encode_inner(version, &keys, ERROR_NONE, None, Some(broker));
        }
        // Not a retriable code: transactions are out of scope for good, and anything librdkafka
        // does not treat as fatal here (INVALID_REQUEST included) leaves a transactional producer
        // re-querying the coordinator forever.
        COORDINATOR_TYPE_TRANSACTION => ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        _ => ERROR_INVALID_REQUEST,
    };
    encode_inner(
        version,
        &keys,
        error_code,
        Some(StrBytes::from_static_str(UNSUPPORTED_KEY_TYPE_MESSAGE)),
        None,
    )
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    encode_inner(version, &[StrBytes::new()], error_code, None, None)
}

/// v4 moved every field into `coordinators[]` and the encoder refuses a response that sets both
/// shapes, so each one is built on its own.
fn encode_inner(
    version: i16,
    keys: &[StrBytes],
    error_code: i16,
    error_message: Option<StrBytes>,
    broker: Option<&BrokerAdvertise>,
) -> Result<Bytes> {
    let (node_id, host, port) = broker.map_or_else(
        || (-1, StrBytes::new(), -1),
        |broker| {
            (
                SELF_NODE_ID,
                StrBytes::from_string(broker.host.clone()),
                broker.port,
            )
        },
    );

    let response = if version >= 4 {
        let coordinators = keys
            .iter()
            .map(|key| {
                Coordinator::default()
                    .with_key(key.clone())
                    .with_node_id(BrokerId(node_id))
                    .with_host(host.clone())
                    .with_port(port)
                    .with_error_code(error_code)
                    .with_error_message(error_message.clone())
            })
            .collect();
        FindCoordinatorResponse::default().with_coordinators(coordinators)
    } else {
        FindCoordinatorResponse::default()
            .with_error_code(error_code)
            .with_error_message(error_message)
            .with_node_id(BrokerId(node_id))
            .with_host(host)
            .with_port(port)
    };
    encode_message(&response, version, 128)
}
