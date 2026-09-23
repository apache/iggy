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

use crate::protocol::api::{
    ERROR_INVALID_PARTITIONS, ERROR_INVALID_REQUEST, ERROR_INVALID_TOPIC_EXCEPTION,
    ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_REQUEST_TIMED_OUT, ERROR_TOPIC_ALREADY_EXISTS,
    ERROR_TOPIC_AUTHORIZATION_FAILED, ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};

/// Errors from the `IggyBridge`: connection lifecycle, config, and Iggy SDK calls.
#[derive(Debug, Error)]
pub enum BridgeError {
    /// Bridge config is structurally invalid (empty address, missing credentials, malformed
    /// topic-mapping TOML) - caught before any connection attempt.
    #[error("invalid bridge configuration: {0}")]
    InvalidConfig(String),
    /// The Iggy client could not connect or authenticate, or a call failed after connecting.
    /// Wraps the SDK's own error rather than re-deriving a parallel taxonomy.
    ///
    /// Caution logging or otherwise trusting this variant's `Display`: a server-side rejection
    /// crosses the wire as a bare numeric code (`ReplyHeader.status` / a committed metadata
    /// result), and the SDK reconstructs the full `IggyError` from just that code via
    /// `IggyError::from_code` (`vsr.rs`). Any data-carrying variant's fields get
    /// `Default::default()` in that path, not the real value - `Identifier::default()` is numeric
    /// `0`, itself a valid id, so e.g. a reconstructed `StreamIdNotFound` can print "ID: 0" whether
    /// or not stream 0 is the one that actually failed. Locally-constructed variants (this
    /// module's own `ok_or_else` calls, for instance) carry real data; only ones that came back
    /// from an actual server rejection are affected.
    #[error("Iggy client error: {0}")]
    Iggy(#[from] IggyError),
    /// A bridge call exceeded its wall-clock budget (`with_request_timeout`'s `REQUEST_TIMEOUT`)
    /// without a definitive answer from Iggy.
    ///
    /// Distinct from `Iggy(IggyError::CannotEstablishConnection)`: that variant means the SDK
    /// itself gave up on establishing or using the connection, a known failure safe to retry. A
    /// timeout here means only that *this caller* stopped waiting - the SDK does its write/read
    /// inside a detached task this timeout cannot abort (`with_request_timeout`'s own doc has the
    /// mechanism), so the request may already be on the wire, or already applied server-side, by
    /// the time this fires. That is the same unknown-outcome shape `IggyError::TransientNotCommitted`
    /// is, which is why this maps like that variant, not like a known connection failure.
    #[error("bridge call timed out waiting for a reply from Iggy")]
    Timeout,
    /// `high_watermark` was asked about a partition index the topic doesn't have.
    #[error(
        "partition {partition} out of range for topic '{topic}' ({partitions_count} partitions)"
    )]
    PartitionOutOfRange {
        topic: String,
        partition: u32,
        partitions_count: u32,
    },
    /// `ensure_topic` was asked to ensure a topic that already exists with a different partition
    /// count. `ensure_topic`'s whole contract is "the topic has `partition_count` partitions
    /// afterward" - silently keeping the old count and returning `Ok(())` would let two
    /// concurrent callers requesting different counts for the same topic both believe they
    /// succeeded.
    #[error(
        "topic '{topic}' already exists with {existing} partitions, but {requested} were requested"
    )]
    PartitionCountMismatch {
        topic: String,
        existing: u32,
        requested: u32,
    },
    /// A Kafka-side topic name failed Kafka's own naming rules (empty, whitespace-padded, over
    /// 249 bytes, or outside `[A-Za-z0-9._-]`) before any Iggy call was made. A conformant Kafka
    /// client library validates topic names before sending them, so this is defense against a
    /// raw or non-conformant client, not an expected path.
    #[error("invalid Kafka topic name '{kafka_topic}': {reason}")]
    InvalidKafkaTopicName { kafka_topic: String, reason: String },
    /// `ensure_stream_and_topic` was asked for `partition_count == 0`. Enforced here, not only
    /// at the `CreateTopics` wire-validation layer that's this bridge's only caller today - the
    /// method is `pub`, so a future caller (a test, or a later Produce auto-create path)
    /// bypassing that layer must not be able to provision a topic nothing can produce to.
    #[error("partition count must be at least 1, got 0 for topic '{kafka_topic}'")]
    InvalidPartitionCount { kafka_topic: String },
}

impl BridgeError {
    /// Maps this error to the Kafka protocol error code a handler should answer with.
    ///
    /// Connection-shaped failures reuse `NOT_LEADER_OR_FOLLOWER` (6) - the same retriable code
    /// the foundation's Produce/Fetch stubs already send - so a client backs off and retries
    /// rather than treating a transient Iggy outage as a permanent failure. Not-found maps to
    /// `UNKNOWN_TOPIC_OR_PARTITION` (3). A bridge-side [`Self::Timeout`] maps to
    /// `REQUEST_TIMED_OUT` (7), the same code `IggyError::TransientNotCommitted` gets and for the
    /// same reason: the outcome is unknown, not known-safe-to-retry, so it must not share a code
    /// with the connection-shaped set. Anything without a closer analogue falls back to
    /// `UNKNOWN_SERVER_ERROR` (-1).
    #[must_use]
    pub const fn to_kafka_error_code(&self) -> i16 {
        match self {
            Self::Iggy(err) => iggy_error_to_kafka_code(err),
            Self::Timeout => ERROR_REQUEST_TIMED_OUT,
            Self::PartitionOutOfRange { .. } => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            Self::PartitionCountMismatch { .. } => ERROR_TOPIC_ALREADY_EXISTS,
            Self::InvalidKafkaTopicName { .. } => ERROR_INVALID_TOPIC_EXCEPTION,
            // Same code the wire-validation layer already uses for this exact condition - this
            // is the same failure reached through a different door, not a new kind of error.
            Self::InvalidPartitionCount { .. } => ERROR_INVALID_PARTITIONS,
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
///
/// The connection-shaped set (`Disconnected`, `EmptyResponse`, `Unauthenticated`, `StaleClient`,
/// `NotConnected`, `CannotEstablishConnection`, `TcpError`) mirrors exactly what
/// `TcpClient::send_raw_with_response` (`tcp_client.rs`) itself treats as worth a reconnect retry.
/// A mapping that only covered some of these would send a permanent-looking Kafka error for a
/// condition the SDK itself considers transient. `Unauthenticated` belongs here, not with the
/// credential-rejection group below: `fail_if_not_authenticated` (`binary_auth.rs`) returns it for
/// `ClientState::Connected`, the ordinary window between a reconnect's TCP handshake completing
/// and its auto-sign-in landing, not for a rejected login.
///
/// `TransientNotAccepted` (Iggy replica-side "retry, on any replica") is retriable the same way.
/// `TransientNotCommitted` is not folded into that set: its outcome is genuinely unknown rather
/// than known-safe-to-retry, so it maps to `REQUEST_TIMED_OUT` instead of
/// `NOT_LEADER_OR_FOLLOWER` - a caller must not treat it as an ordinary retriable failure and risk
/// a duplicate write.
///
/// `Unauthorized` is the only one of the four credential-shaped variants that stays on
/// `TOPIC_AUTHORIZATION_FAILED` (29): it means the authenticated user lacks a permission, which is
/// a real, fixable-by-the-Kafka-operator ACL problem. `InvalidCredentials`/`InvalidUsername`/
/// `InvalidPassword` mean the *bridge's own* `IGGY_KAFKA_IGGY_USERNAME`/`_PASSWORD` are wrong
/// (`tcp_client.rs`'s own sign-in path raises exactly these for a rejected login, with the comment
/// "the caller's to fix" - meaning the gateway operator, not the Kafka client). Sending 29 for
/// these blames the Kafka client's own ACLs for a problem it cannot see or fix, and it is not
/// startup-only: the SDK re-runs sign-in on every reconnect (`tcp_client.rs`), so a since-rotated
/// bridge password surfaces this mid-request, not just at boot. They fall to
/// `UNKNOWN_SERVER_ERROR` instead - correctly fatal (retrying won't fix a wrong password), but
/// without asserting a cause the Kafka client cannot act on. A handler wiring this in
/// (`#3535`/`#3536`) should log the real `IggyError` at `error!` level server-side, since the
/// Kafka client will never see more than "-1" for it.
const fn iggy_error_to_kafka_code(err: &IggyError) -> i16 {
    match err {
        IggyError::StreamIdNotFound(_)
        | IggyError::StreamNameNotFound(_)
        | IggyError::TopicIdNotFound(_, _)
        | IggyError::TopicNameNotFound(_, _)
        | IggyError::PartitionNotFound(_, _, _) => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        IggyError::Unauthorized => ERROR_TOPIC_AUTHORIZATION_FAILED,
        IggyError::Disconnected
        | IggyError::EmptyResponse
        | IggyError::Unauthenticated
        | IggyError::StaleClient
        | IggyError::NotConnected
        | IggyError::CannotEstablishConnection
        | IggyError::TcpError
        | IggyError::TransientNotAccepted => ERROR_NOT_LEADER_OR_FOLLOWER,
        IggyError::TransientNotCommitted => ERROR_REQUEST_TIMED_OUT,
        // Not `ERROR_INVALID_PARTITIONS` (37): that code's own text, per `kafka-protocol`'s
        // table, is "Number of partitions is below 1" - the opposite condition from "too many"
        // (Iggy's server-side cap, above 1000). Reusing 37 for both directions would return a
        // client-visible error message that contradicts the actual request it sent.
        IggyError::TooManyPartitions => ERROR_INVALID_REQUEST,
        // Deliberately NOT special-cased here to ERROR_NONE: this function is shared by every
        // handler's error path, but "the operation did commit, so report success" only holds for
        // a caller that issued a *write* - the SDK's own reconnect path replayed a write whose
        // first attempt already applied, and the server's client-table dedup caught the replay.
        // A read that somehow reaches this variant has no write to have "already applied"; a
        // write-side caller (CreateTopics) special-cases it locally, close to the write it
        // concerns, instead of baking a write-only assumption into a mapping every read also
        // goes through.
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
    fn unauthenticated_maps_to_not_leader_or_follower_for_retry() {
        // Not a credential rejection: fail_if_not_authenticated (binary_auth.rs) returns this for
        // ClientState::Connected, the ordinary window mid-reconnect before auto-sign-in lands.
        let err = BridgeError::Iggy(IggyError::Unauthenticated);
        assert_eq!(err.to_kafka_error_code(), ERROR_NOT_LEADER_OR_FOLLOWER);
    }

    #[test]
    fn partition_not_found_maps_to_unknown_topic_or_partition() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let err = BridgeError::Iggy(IggyError::PartitionNotFound(3, stream_id, topic_id));
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn too_many_partitions_maps_to_invalid_request_not_invalid_partitions() {
        // INVALID_PARTITIONS (37) means "count is below 1" (kafka-protocol's own error table) -
        // the opposite condition from "too many": reusing it here would send a client-visible
        // message that contradicts the request it just sent.
        let err = BridgeError::Iggy(IggyError::TooManyPartitions);
        assert_eq!(err.to_kafka_error_code(), ERROR_INVALID_REQUEST);
    }

    #[test]
    fn request_already_applied_falls_to_the_generic_mapping_here() {
        // This shared mapping has no write to know "already applied" refers to - CreateTopics
        // (the only caller for whom that's a success, not a fault) special-cases it locally
        // instead (`create_topics.rs`), close to the write it concerns.
        let err = BridgeError::Iggy(IggyError::RequestAlreadyApplied);
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_SERVER_ERROR);
    }

    #[test]
    fn invalid_partition_count_maps_to_invalid_partitions() {
        let err = BridgeError::InvalidPartitionCount {
            kafka_topic: "orders".to_string(),
        };
        assert_eq!(err.to_kafka_error_code(), ERROR_INVALID_PARTITIONS);
    }

    // The bridge's own credentials failing (a wrong IGGY_KAFKA_IGGY_USERNAME/_PASSWORD) is not
    // the Kafka client's fault and not something it can fix - these three must NOT share
    // Unauthorized's TOPIC_AUTHORIZATION_FAILED (29), which would blame the Kafka client's own
    // ACLs for a bridge-side misconfiguration.
    #[test]
    fn invalid_credentials_maps_to_unknown_server_error_not_authorization_failed() {
        let err = BridgeError::Iggy(IggyError::InvalidCredentials);
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_SERVER_ERROR);
    }

    #[test]
    fn invalid_username_maps_to_unknown_server_error_not_authorization_failed() {
        let err = BridgeError::Iggy(IggyError::InvalidUsername);
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_SERVER_ERROR);
    }

    #[test]
    fn invalid_password_maps_to_unknown_server_error_not_authorization_failed() {
        let err = BridgeError::Iggy(IggyError::InvalidPassword);
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_SERVER_ERROR);
    }

    #[test]
    fn invalid_kafka_topic_name_maps_to_invalid_topic_exception() {
        let err = BridgeError::InvalidKafkaTopicName {
            kafka_topic: " orders ".to_string(),
            reason: "must not have leading or trailing whitespace".to_string(),
        };
        assert_eq!(err.to_kafka_error_code(), ERROR_INVALID_TOPIC_EXCEPTION);
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
    fn empty_response_maps_to_not_leader_or_follower_for_retry() {
        let err = BridgeError::Iggy(IggyError::EmptyResponse);
        assert_eq!(err.to_kafka_error_code(), ERROR_NOT_LEADER_OR_FOLLOWER);
    }

    #[test]
    fn stale_client_maps_to_not_leader_or_follower_for_retry() {
        let err = BridgeError::Iggy(IggyError::StaleClient);
        assert_eq!(err.to_kafka_error_code(), ERROR_NOT_LEADER_OR_FOLLOWER);
    }

    #[test]
    fn not_connected_maps_to_not_leader_or_follower_for_retry() {
        let err = BridgeError::Iggy(IggyError::NotConnected);
        assert_eq!(err.to_kafka_error_code(), ERROR_NOT_LEADER_OR_FOLLOWER);
    }

    #[test]
    fn tcp_error_maps_to_not_leader_or_follower_for_retry() {
        let err = BridgeError::Iggy(IggyError::TcpError);
        assert_eq!(err.to_kafka_error_code(), ERROR_NOT_LEADER_OR_FOLLOWER);
    }

    #[test]
    fn transient_not_accepted_maps_to_not_leader_or_follower_for_retry() {
        let err = BridgeError::Iggy(IggyError::TransientNotAccepted);
        assert_eq!(err.to_kafka_error_code(), ERROR_NOT_LEADER_OR_FOLLOWER);
    }

    #[test]
    fn transient_not_committed_maps_to_request_timed_out_not_plain_retry() {
        // Outcome is unknown, not known-safe-to-retry - must not share a code with the
        // unambiguous retriable set, or a caller could blindly retry a Produce into a duplicate.
        let err = BridgeError::Iggy(IggyError::TransientNotCommitted);
        assert_eq!(err.to_kafka_error_code(), ERROR_REQUEST_TIMED_OUT);
    }

    #[test]
    fn timeout_maps_to_request_timed_out_not_not_leader_or_follower() {
        // A caller-side timeout is the same unknown-outcome shape as TransientNotCommitted (the
        // SDK's write/read run in a detached task this timeout cannot abort, so the request may
        // already be on the wire) - must not borrow CannotEstablishConnection's known-safe code.
        let err = BridgeError::Timeout;
        assert_eq!(err.to_kafka_error_code(), ERROR_REQUEST_TIMED_OUT);
        assert_ne!(err.to_kafka_error_code(), ERROR_NOT_LEADER_OR_FOLLOWER);
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

    #[test]
    fn partition_count_mismatch_maps_to_topic_already_exists_not_invalid_partitions() {
        // INVALID_PARTITIONS (37) means "count is below 1" (kafka-protocol's own error table) -
        // a different condition than "this topic already exists with a different count".
        let err = BridgeError::PartitionCountMismatch {
            topic: "t".to_string(),
            existing: 3,
            requested: 5,
        };
        assert_eq!(err.to_kafka_error_code(), ERROR_TOPIC_ALREADY_EXISTS);
    }

    /// Every `ERROR_*` constant this module sends, checked against `kafka-protocol`'s own
    /// `ResponseError` table - the crate's canonical copy of Kafka's real wire numbers, not this
    /// module's own. Every other test above pins routing (which `IggyError` maps to which
    /// constant); without this, a wrong constant value would still pass all of them, since they
    /// only ever compare against the same constants the function returns.
    #[test]
    fn every_sent_error_code_matches_kafka_protocols_own_table() {
        use kafka_protocol::error::ResponseError;

        for (ours, theirs) in [
            (
                ERROR_UNKNOWN_SERVER_ERROR,
                ResponseError::UnknownServerError,
            ),
            (
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                ResponseError::UnknownTopicOrPartition,
            ),
            (
                ERROR_NOT_LEADER_OR_FOLLOWER,
                ResponseError::NotLeaderOrFollower,
            ),
            (ERROR_REQUEST_TIMED_OUT, ResponseError::RequestTimedOut),
            (
                ERROR_INVALID_TOPIC_EXCEPTION,
                ResponseError::InvalidTopicException,
            ),
            (
                ERROR_TOPIC_AUTHORIZATION_FAILED,
                ResponseError::TopicAuthorizationFailed,
            ),
            (
                ERROR_TOPIC_ALREADY_EXISTS,
                ResponseError::TopicAlreadyExists,
            ),
            (ERROR_INVALID_PARTITIONS, ResponseError::InvalidPartitions),
            (ERROR_INVALID_REQUEST, ResponseError::InvalidRequest),
        ] {
            assert_eq!(
                ours,
                theirs.code(),
                "{theirs:?} is {} in kafka-protocol, not {ours}",
                theirs.code()
            );
        }
    }
}
