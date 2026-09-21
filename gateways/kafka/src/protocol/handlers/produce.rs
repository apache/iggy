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

//! Produce (API key 0).

use bytes::Bytes;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};

use crate::error::Result;
use crate::protocol::api::{
    API_KEY_PRODUCE, ApiVersionRange, ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_UNSUPPORTED_VERSION,
    GatewayState, HandleOutcome, supported_max_version,
};
use crate::protocol::bounds_guard::validate_produce_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_PRODUCE,
    min_version: 3,
    max_version: 9,
};

/// Produce is the only request the wire protocol allows to go unanswered
/// (`acks=0`), so it gets its own path that may return [`HandleOutcome::NoResponse`].
///
/// The firewall check runs AFTER decoding the request, not before: `ApiVersions` advertises
/// Produce min=0 (see `api::advertised_min_version`) while the firewall's real floor is 3, so a
/// spec-compliant client can legitimately send Produce v0-2 with `acks=0`. Rejecting those
/// versions before reading `acks` would send an error response the client never expects,
/// desyncing the next correlation id it reads.
#[expect(
    clippy::unused_async,
    reason = "the shared handler signature, kept until a handler awaits the bridge"
)]
pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    // Above the encoder max there is no response parseable at the client's version, so close
    // rather than decode (same policy the other APIs apply). Fail-closed on a missing row
    // (i16::MIN, not i16::MAX): dispatch routes Produce on its api key, not from this table, so
    // if a future edit ever drops the Produce row to disable the API, a fail-open default here
    // would leave Produce v0-2 acks=0 silently accepted on an API the operator believes is off.
    if api_version > supported_max_version(API_KEY_PRODUCE).unwrap_or(i16::MIN) {
        return HandleOutcome::Close;
    }
    // `kafka_protocol`'s ProduceRequest/ProduceResponse schemas only go back to v3, so v0-2
    // (still advertised as the min in ApiVersions per KAFKA-18659) can be neither decoded nor
    // encoded by the crate - there is no parseable response at these versions regardless of
    // body content. `acks` is always the first i16 on the wire there (`transactional_id` was
    // added in v3), so it's peeked by hand: acks=0 must keep the connection open per the wire
    // protocol's fire-and-forget rule even though no response can ever be encoded for it.
    if api_version < 3 {
        let acks = match body.get(0..2) {
            Some(&[hi, lo]) => Some(i16::from_be_bytes([hi, lo])),
            _ => None,
        };
        return match acks {
            Some(0) | None => HandleOutcome::NoResponse,
            Some(_) => unsupported_version_response(API_KEY_PRODUCE, api_version, |v| {
                encode_error_response(v, ERROR_UNSUPPORTED_VERSION)
            }),
        };
    }
    match decode_guarded::<ProduceRequest>(api_version, body, |v, b| {
        validate_produce_shape(v, b, state.max_frame_size)
    }) {
        // acks=0 is fire-and-forget: the client isn't reading a response, so
        // sending one desyncs the next correlation id it expects.
        Ok(req) if req.acks == 0 => HandleOutcome::NoResponse,
        // `api_version` is always in `[3, supported_max]` here: the `< 3` case returned above,
        // and the `> max` case returned at the top of this function - so it is always within
        // `SUPPORTED_RANGES`' Produce row and an `is_supported_version` re-check can never fail.
        Ok(req) => respond_or_close(encode_response(api_version, &req), "Produce"),
        Err(error) => {
            // `kafka_protocol` decodes the whole request in one shot; a failure anywhere gives
            // no partial-field access, so `acks` is unknowable here (unlike the pre-migration
            // field-by-field decoder, which could still know `acks` on a later-field failure).
            // Responding risks desyncing an acks=0 fire-and-forget client's correlation stream,
            // so every Produce decode failure now stays silent - a behavior change from the
            // hand-rolled decoder, which answered with INVALID_REQUEST when `acks` was known and
            // nonzero.
            // debug!, not warn!: the body is attacker-controlled, not operator-actionable, and
            // a client looping malformed bodies on one connection (never disconnected - this
            // arm returns NoResponse) has no rate limit here.
            tracing::debug!(%error, "failed to decode Produce request (no response)");
            HandleOutcome::NoResponse
        }
    }
}

/// Well-formed Produce response with a single placeholder topic/partition.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let resp = ProduceResponse::default().with_responses(vec![
        TopicProduceResponse::default()
            .with_partition_responses(vec![partition_response(0, error_code)]),
    ]);
    encode_message(&resp, version, 512)
}

/// Stub: discard the payload and return a retriable error so clients keep data locally until
/// the Iggy bridge lands (do not advertise silent success).
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, req: &ProduceRequest) -> Result<Bytes> {
    let responses = req
        .topic_data
        .iter()
        .map(|topic| {
            TopicProduceResponse::default()
                .with_name(topic.name.clone())
                .with_partition_responses(
                    topic
                        .partition_data
                        .iter()
                        .map(|p| partition_response(p.index, ERROR_NOT_LEADER_OR_FOLLOWER))
                        .collect(),
                )
        })
        .collect();
    let resp = ProduceResponse::default().with_responses(responses);
    encode_message(&resp, version, 512)
}

fn partition_response(index: i32, error_code: i16) -> PartitionProduceResponse {
    PartitionProduceResponse::default()
        .with_index(index)
        .with_error_code(error_code)
        .with_log_start_offset(0)
}
