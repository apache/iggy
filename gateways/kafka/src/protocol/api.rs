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

use std::sync::Arc;

use bytes::Bytes;

use crate::bridge::IggyBridge;
use crate::protocol::handlers::{
    api_versions, create_topics, dispatch, fetch, list_offsets, metadata, produce,
};

pub const API_KEY_PRODUCE: i16 = 0;
pub const API_KEY_FETCH: i16 = 1;
pub const API_KEY_LIST_OFFSETS: i16 = 2;
pub const API_KEY_METADATA: i16 = 3;
pub const API_KEY_API_VERSIONS: i16 = 18;
pub const API_KEY_CREATE_TOPICS: i16 = 19;

pub const DEFAULT_KAFKA_PORT: u16 = 9093;

/// Generic catch-all. Not sent by any stub response today; the `bridge` module's error mapping
/// uses it for an `IggyError` with no closer Kafka analogue.
pub const ERROR_UNKNOWN_SERVER_ERROR: i16 = -1;
pub const ERROR_NONE: i16 = 0;
pub const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// Retriable; Produce stub uses this until the Iggy bridge persists records.
pub const ERROR_NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// `bridge`'s mapping for `IggyError::TransientNotCommitted`: the request's outcome is genuinely
/// unknown (neither confirmed applied nor confirmed rejected).
///
/// Retriable in real Kafka too (`TimeoutException extends RetriableException`; the Java producer's
/// `Sender.canRetry` treats it the same as `NOT_LEADER_OR_FOLLOWER`) - this is not chosen to make
/// clients stop retrying. It is chosen because it is the code a real broker sends for the same
/// unknown-outcome shape (an ack that timed out with no confirmation either way), and Kafka has no
/// dedicated "outcome unknown, retry could duplicate" code. The duplicate-write risk on retry is
/// real regardless of which retriable code is sent; it closes only once `#3535` has an idempotent
/// produce path, not by picking a different error code here.
pub const ERROR_REQUEST_TIMED_OUT: i16 = 7;
/// `bridge`'s mapping for a Kafka-side topic name that fails Kafka's own naming rules.
///
/// Empty, whitespace-padded, over 249 bytes, or outside `[A-Za-z0-9._-]`, checked before any Iggy
/// call is made - a real Kafka client library validates topic names client-side and would never
/// send one of these, but a raw/non-conformant client could.
pub const ERROR_INVALID_TOPIC_EXCEPTION: i16 = 17;
/// Closest fit for an Iggy permission/credential rejection in `bridge`'s error mapping.
///
/// There is no bridge-side SASL exchange yet (`#3549`), so `SASL_AUTHENTICATION_FAILED` would
/// misstate the failure point. Not sent by any stub response today.
pub const ERROR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;
pub const ERROR_UNSUPPORTED_VERSION: i16 = 35;
/// `bridge`'s mapping for `BridgeError::PartitionCountMismatch`: the topic exists, just not with
/// the requested partition count.
///
/// Not [`ERROR_INVALID_PARTITIONS`] - `kafka-protocol`'s own error table (`error.rs`) defines that
/// code's text as "Number of partitions is below 1", which is a different condition (a client
/// asking for zero/negative partitions) than "this topic already exists with a different count".
pub const ERROR_TOPIC_ALREADY_EXISTS: i16 = 36;
pub const ERROR_INVALID_PARTITIONS: i16 = 37;
pub const ERROR_INVALID_REPLICATION_FACTOR: i16 = 38;
/// `CreateTopics` stub: do not claim topics were created (no controller / no Iggy bridge).
pub const ERROR_NOT_CONTROLLER: i16 = 41;
pub const ERROR_INVALID_REQUEST: i16 = 42;

/// Result of handling one Kafka request body.
#[derive(Debug)]
pub enum HandleOutcome {
    /// Write this response body (with a response header).
    Respond(Bytes),
    /// Produce with `acks=0`: write nothing, keep the connection open.
    NoResponse,
    /// No parseable response exists for this request; close the TCP connection.
    Close,
}

impl HandleOutcome {
    /// Return the response body, or panic with `msg` if the outcome is not [`Self::Respond`].
    ///
    /// # Panics
    ///
    /// Panics when the outcome is [`Self::NoResponse`] or [`Self::Close`].
    #[must_use]
    pub fn expect_response(self, msg: &str) -> Bytes {
        match self {
            Self::Respond(body) => body,
            Self::NoResponse => panic!("{msg}: got NoResponse"),
            Self::Close => panic!("{msg}: got Close"),
        }
    }

    #[must_use]
    pub const fn is_no_response(&self) -> bool {
        matches!(self, Self::NoResponse)
    }

    #[must_use]
    pub const fn is_close(&self) -> bool {
        matches!(self, Self::Close)
    }
}

#[derive(Debug, Clone)]
pub struct BrokerAdvertise {
    pub host: String,
    pub port: i32,
}

impl Default for BrokerAdvertise {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: i32::from(DEFAULT_KAFKA_PORT),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ApiVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

static SUPPORTED_RANGES: &[ApiVersionRange] = &[
    produce::RANGE,
    fetch::RANGE,
    list_offsets::RANGE,
    metadata::RANGE,
    api_versions::RANGE,
    create_topics::RANGE,
];

#[must_use]
pub fn supported_api_ranges() -> &'static [ApiVersionRange] {
    SUPPORTED_RANGES
}

/// Everything a handler needs that outlives one request.
///
/// `bridge` is `None` until `IGGY_KAFKA_BRIDGE_ENABLED` turns it on. A handler that finds `None`
/// answers with its stub, so APIs can be wired one at a time.
///
/// One `IggyBridge` is one `IggyClient` and its TCP transport is lockstep, so Kafka connections
/// serialize behind whichever Iggy request is in flight. The `Arc` does not change that. See the
/// README's "Concurrency ceiling".
pub struct GatewayState {
    pub broker: BrokerAdvertise,
    pub bridge: Option<Arc<IggyBridge>>,
    pub max_frame_size: usize,
}

impl GatewayState {
    #[must_use]
    pub const fn new(
        broker: BrokerAdvertise,
        bridge: Option<Arc<IggyBridge>>,
        max_frame_size: usize,
    ) -> Self {
        Self {
            broker,
            bridge,
            max_frame_size,
        }
    }

    /// State with no bridge, so every handler takes its stub path.
    #[must_use]
    pub const fn stub(broker: BrokerAdvertise, max_frame_size: usize) -> Self {
        Self::new(broker, None, max_frame_size)
    }
}

/// Default `max_frame_size` used by [`handle_request`] - the direct call sites across this
/// crate's test suite that don't care about the response-size guard specifically. Production
/// traffic goes through [`handle_request_bounded`] instead (see `server.rs`'s call site), with
/// the connection's actual configured `max_frame_size`.
const DEFAULT_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

/// Handles one decoded request frame and returns how the connection should proceed.
pub async fn handle_request(
    api_key: i16,
    api_version: i16,
    body: Bytes,
    broker: &BrokerAdvertise,
) -> HandleOutcome {
    let state = GatewayState::stub(broker.clone(), DEFAULT_MAX_FRAME_SIZE);
    handle_request_bounded(&state, api_key, api_version, body).await
}

/// Same as [`handle_request`], but rejects a request whose declared array/string lengths project
/// a response larger than `max_frame_size` before decoding it.
///
/// See [`crate::protocol::bounds_guard`]'s `MAX_REQUEST_ELEMENTS`/`RESPONSE_BYTES_PER_ELEMENT`
/// docs for the CPU/memory amplification this closes (a request within the old element budget
/// alone could still produce a multi-megabyte response from a single synchronous, non-yielding
/// call).
pub async fn handle_request_bounded(
    state: &GatewayState,
    api_key: i16,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    dispatch(state, api_key, api_version, body).await
}

#[must_use]
pub fn is_supported_version(api_key: i16, api_version: i16) -> bool {
    SUPPORTED_RANGES
        .iter()
        .find(|r| r.api_key == api_key)
        .is_some_and(|r| api_version >= r.min_version && api_version <= r.max_version)
}

/// Highest version this gateway accepts for `api_key`, from the single firewall table.
#[must_use]
pub fn supported_max_version(api_key: i16) -> Option<i16> {
    SUPPORTED_RANGES
        .iter()
        .find(|r| r.api_key == api_key)
        .map(|r| r.max_version)
}

/// Min version advertised in `ApiVersions` (may differ from the firewall min).
///
/// Produce must advertise min=0 per KAFKA-18659 / `PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION`
/// even though this gateway only accepts Produce v3+.
#[must_use]
pub const fn advertised_min_version(api_key: i16, firewall_min: i16) -> i16 {
    if api_key == API_KEY_PRODUCE {
        0
    } else {
        firewall_min
    }
}
