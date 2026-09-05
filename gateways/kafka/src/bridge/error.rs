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

use iggy::prelude::IggyError;
use thiserror::Error;

use crate::protocol::api::{ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_UNKNOWN_TOPIC_OR_PARTITION};

/// Kafka's generic `UNKNOWN_SERVER_ERROR` (`-1`). Not in `protocol::api`'s `ERROR_*` set - that
/// table only lists codes the foundation's stub responses actually send; this is the bridge's own
/// catch-all for an `IggyError` variant with no closer Kafka analogue.
const ERROR_UNKNOWN_SERVER_ERROR: i16 = -1;
/// Kafka's `TOPIC_AUTHORIZATION_FAILED`. Closest fit for an Iggy permission/credential rejection -
/// there is no bridge-side SASL exchange yet (`#3549`), so `SASL_AUTHENTICATION_FAILED` would
/// misstate the failure point.
const ERROR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;

/// Errors from the `IggyBridge`: connection lifecycle, config, and Iggy SDK calls.
#[derive(Debug, Error)]
pub enum BridgeError {
    /// Bridge config is structurally invalid (empty address, missing credentials, malformed
    /// topic-mapping TOML) - caught before any connection attempt.
    #[error("invalid bridge configuration: {0}")]
    InvalidConfig(String),
    /// The Iggy client could not connect or authenticate, or a call failed after connecting.
    /// Wraps the SDK's own error rather than re-deriving a parallel taxonomy.
    #[error("Iggy client error: {0}")]
    Iggy(#[from] IggyError),
    /// `high_watermark` was asked about a partition index the topic doesn't have.
    #[error(
        "partition {partition} out of range for topic '{topic}' ({partitions_count} partitions)"
    )]
    PartitionOutOfRange {
        topic: String,
        partition: u32,
        partitions_count: u32,
    },
}

impl BridgeError {
    /// Maps this error to the Kafka protocol error code a handler should answer with.
    ///
    /// Connection-shaped failures reuse `NOT_LEADER_OR_FOLLOWER` (6) - the same retriable code
    /// the foundation's Produce/Fetch stubs already send - so a client backs off and retries
    /// rather than treating a transient Iggy outage as a permanent failure. Not-found maps to
    /// `UNKNOWN_TOPIC_OR_PARTITION` (3). Anything without a closer analogue falls back to
    /// `UNKNOWN_SERVER_ERROR` (-1).
    #[must_use]
    pub const fn to_kafka_error_code(&self) -> i16 {
        match self {
            Self::Iggy(err) => iggy_error_to_kafka_code(err),
            Self::PartitionOutOfRange { .. } => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            // Not a wire-response case in practice: an invalid bridge config is caught at
            // `IggyBridge::connect` before any handler exists to answer a Kafka request, so this
            // is reachable only if a future caller starts constructing configs at request time.
            // `UNKNOWN_SERVER_ERROR` at least doesn't claim a specific, wrong cause the way
            // `UNSUPPORTED_VERSION` (misleadingly implies a Kafka API version mismatch) would.
            Self::InvalidConfig(_) => ERROR_UNKNOWN_SERVER_ERROR,
        }
    }
}

/// Kept private so this association can change without touching call sites.
const fn iggy_error_to_kafka_code(err: &IggyError) -> i16 {
    match err {
        IggyError::StreamIdNotFound(_)
        | IggyError::StreamNameNotFound(_)
        | IggyError::TopicIdNotFound(_, _)
        | IggyError::TopicNameNotFound(_, _) => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        IggyError::Unauthenticated
        | IggyError::Unauthorized
        | IggyError::InvalidCredentials
        | IggyError::InvalidUsername
        | IggyError::InvalidPassword => ERROR_TOPIC_AUTHORIZATION_FAILED,
        IggyError::Disconnected | IggyError::CannotEstablishConnection => {
            ERROR_NOT_LEADER_OR_FOLLOWER
        }
        _ => ERROR_UNKNOWN_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::Identifier;

    #[test]
    fn stream_id_not_found_maps_to_unknown_topic_or_partition() {
        let err = BridgeError::Iggy(IggyError::StreamIdNotFound(Identifier::numeric(1).unwrap()));
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn stream_name_not_found_maps_to_unknown_topic_or_partition() {
        let err = BridgeError::Iggy(IggyError::StreamNameNotFound("orders".to_string()));
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn topic_name_not_found_maps_to_unknown_topic_or_partition() {
        let err = BridgeError::Iggy(IggyError::TopicNameNotFound(
            "orders".to_string(),
            "kafka".to_string(),
        ));
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn unauthenticated_maps_to_topic_authorization_failed() {
        let err = BridgeError::Iggy(IggyError::Unauthenticated);
        assert_eq!(err.to_kafka_error_code(), ERROR_TOPIC_AUTHORIZATION_FAILED);
    }

    #[test]
    fn unauthorized_maps_to_topic_authorization_failed() {
        let err = BridgeError::Iggy(IggyError::Unauthorized);
        assert_eq!(err.to_kafka_error_code(), ERROR_TOPIC_AUTHORIZATION_FAILED);
    }

    #[test]
    fn invalid_config_maps_to_unknown_server_error_not_unsupported_version() {
        let err = BridgeError::InvalidConfig("bad config".to_string());
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_SERVER_ERROR);
    }

    #[test]
    fn disconnected_maps_to_not_leader_or_follower_for_retry() {
        let err = BridgeError::Iggy(IggyError::Disconnected);
        assert_eq!(err.to_kafka_error_code(), ERROR_NOT_LEADER_OR_FOLLOWER);
    }

    #[test]
    fn cannot_establish_connection_maps_to_not_leader_or_follower_for_retry() {
        let err = BridgeError::Iggy(IggyError::CannotEstablishConnection);
        assert_eq!(err.to_kafka_error_code(), ERROR_NOT_LEADER_OR_FOLLOWER);
    }

    #[test]
    fn unmatched_iggy_error_falls_back_to_unknown_server_error() {
        let err = BridgeError::Iggy(IggyError::InvalidConfiguration);
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_SERVER_ERROR);
    }

    #[test]
    fn partition_out_of_range_maps_to_unknown_topic_or_partition() {
        let err = BridgeError::PartitionOutOfRange {
            topic: "t".to_string(),
            partition: 5,
            partitions_count: 2,
        };
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }
}
