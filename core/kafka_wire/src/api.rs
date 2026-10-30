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

//! API keys, error codes and the version-range type every host of the protocol shares.

use kafka_protocol::error::ResponseError;

pub const API_KEY_PRODUCE: i16 = 0;
pub const API_KEY_FETCH: i16 = 1;
pub const API_KEY_LIST_OFFSETS: i16 = 2;
pub const API_KEY_METADATA: i16 = 3;
pub const API_KEY_OFFSET_COMMIT: i16 = 8;
pub const API_KEY_OFFSET_FETCH: i16 = 9;
pub const API_KEY_FIND_COORDINATOR: i16 = 10;
pub const API_KEY_JOIN_GROUP: i16 = 11;
pub const API_KEY_HEARTBEAT: i16 = 12;
pub const API_KEY_LEAVE_GROUP: i16 = 13;
pub const API_KEY_SYNC_GROUP: i16 = 14;
pub const API_KEY_DESCRIBE_GROUPS: i16 = 15;
pub const API_KEY_LIST_GROUPS: i16 = 16;
pub const API_KEY_SASL_HANDSHAKE: i16 = 17;
pub const API_KEY_API_VERSIONS: i16 = 18;
pub const API_KEY_CREATE_TOPICS: i16 = 19;
pub const API_KEY_DELETE_TOPICS: i16 = 20;
pub const API_KEY_INIT_PRODUCER_ID: i16 = 22;
pub const API_KEY_DESCRIBE_ACLS: i16 = 29;
pub const API_KEY_DESCRIBE_CONFIGS: i16 = 32;
pub const API_KEY_ALTER_CONFIGS: i16 = 33;
pub const API_KEY_SASL_AUTHENTICATE: i16 = 36;
pub const API_KEY_CREATE_PARTITIONS: i16 = 37;
pub const API_KEY_DELETE_GROUPS: i16 = 42;
pub const API_KEY_INCREMENTAL_ALTER_CONFIGS: i16 = 44;
pub const API_KEY_OFFSET_DELETE: i16 = 47;
pub const API_KEY_DESCRIBE_CLUSTER: i16 = 60;

/// Generic catch-all. Not sent by any stub response today; the `bridge` module's error mapping
/// uses it for an `IggyError` with no closer Kafka analogue.
pub const ERROR_UNKNOWN_SERVER_ERROR: i16 = ResponseError::UnknownServerError.code();
pub const ERROR_NONE: i16 = 0;
/// Fetch: the offset is negative or past the high watermark. The client resets its position.
pub const ERROR_OFFSET_OUT_OF_RANGE: i16 = ResponseError::OffsetOutOfRange.code();
pub const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = ResponseError::UnknownTopicOrPartition.code();
/// Retriable, nothing written. The Produce stub, and a partition refused because the request
/// budget ran out.
pub const ERROR_NOT_LEADER_OR_FOLLOWER: i16 = ResponseError::NotLeaderOrFollower.code();
/// Outcome unknown: the write may have landed. Retriable. A retry of an idempotent batch writes
/// nothing new, and a retry of any other batch can write it twice.
pub const ERROR_REQUEST_TIMED_OUT: i16 = ResponseError::RequestTimedOut.code();
/// Produce: a record, send or partition too large, even alone. Not retried as-is. Java splits a
/// multi-record batch and sends the halves.
pub const ERROR_MESSAGE_TOO_LARGE: i16 = ResponseError::MessageTooLarge.code();
/// `bridge`'s mapping for a Kafka-side topic name that fails Kafka's own naming rules.
///
/// Empty, whitespace-padded, over 249 bytes, or outside `[A-Za-z0-9._-]`, checked before any Iggy
/// call is made - a real Kafka client library validates topic names client-side and would never
/// send one of these, but a raw/non-conformant client could.
pub const ERROR_INVALID_TOPIC_EXCEPTION: i16 = ResponseError::InvalidTopicException.code();
/// Produce: `acks` is not 0, 1 or -1. A conformant client never sends one, since `acks` comes
/// from validated configuration rather than from application input.
pub const ERROR_INVALID_REQUIRED_ACKS: i16 = ResponseError::InvalidRequiredAcks.code();
/// Retriable, and the client keeps its coordinator. An offset call to Iggy that may work on retry.
pub const ERROR_COORDINATOR_LOAD_IN_PROGRESS: i16 = ResponseError::CoordinatorLoadInProgress.code();
/// Retriable. Sent when this coordinator is at one of its `GroupCoordinatorConfig` capacity
/// caps: the client should back off and retry rather than treat the group as unusable.
pub const ERROR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// Retriable. Sent to a parked `JoinGroup`/`SyncGroup` waiter when the gateway starts draining,
/// so a shutdown does not hold a connection open for a full rebalance timeout.
pub const ERROR_NOT_COORDINATOR: i16 = 16;
/// The member's generation is not the group's current one; it must rejoin.
pub const ERROR_ILLEGAL_GENERATION: i16 = 22;
/// No protocol name is supported by every member, or the first member sent an empty protocol
/// type / empty protocol list.
pub const ERROR_INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
pub const ERROR_INVALID_GROUP_ID: i16 = 24;
pub const ERROR_UNKNOWN_MEMBER_ID: i16 = 25;
/// `DescribeGroups` v6: the named group is not on this coordinator.
///
/// Earlier versions of the same response report that miss as state `Dead` with error 0.
/// Kafka 4.0's `describeGroups` splits it the same way, and a v6 client treats error 0 as a
/// group that exists.
pub const ERROR_GROUP_ID_NOT_FOUND: i16 = ResponseError::GroupIdNotFound.code();
pub const ERROR_INVALID_SESSION_TIMEOUT: i16 = 26;
/// How a follower learns to rejoin: its heartbeat is answered with this while the group prepares.
pub const ERROR_REBALANCE_IN_PROGRESS: i16 = 27;
/// `DeleteGroups` and `OffsetDelete`: the principal may not delete groups or their offsets.
///
/// `DeleteGroups` also sends it when Iggy refuses the bridge user, either the delete of the Iggy
/// group that holds the offsets, or the listing of the groups of a topic where it can hold them.
pub const ERROR_GROUP_AUTHORIZATION_FAILED: i16 = ResponseError::GroupAuthorizationFailed.code();
/// `DeleteGroups`: the group has members. `OffsetDelete`: it has members of a protocol other than
/// `consumer`.
pub const ERROR_NON_EMPTY_GROUP: i16 = ResponseError::NonEmptyGroup.code();
/// `OffsetDelete`: a member subscribes to the topic, so its offsets stay.
pub const ERROR_GROUP_SUBSCRIBED_TO_TOPIC: i16 = ResponseError::GroupSubscribedToTopic.code();
/// Closest fit for an Iggy permission/credential rejection in `bridge`'s error mapping.
///
/// Still not `SASL_AUTHENTICATION_FAILED`, and now for a firmer reason than when this was written:
/// a connection reaching the bridge has already completed its SASL exchange, so a credential or
/// permission rejection from Iggy at that point is not an authentication failure and saying so
/// would send an operator to the wrong hop. Not sent by any stub response today.
pub const ERROR_TOPIC_AUTHORIZATION_FAILED: i16 = ResponseError::TopicAuthorizationFailed.code();
/// The mechanism a client asked for in `SaslHandshake` is not one this gateway enables. The
/// response still carries the enabled mechanism list, which is what the client prints.
pub const ERROR_UNSUPPORTED_SASL_MECHANISM: i16 = ResponseError::UnsupportedSaslMechanism.code();
/// A request arrived that is legal on the wire but not in this connection's SASL state: a token
/// before a handshake, a normal request before authenticating, or a SASL request after.
pub const ERROR_ILLEGAL_SASL_STATE: i16 = ResponseError::IllegalSaslState.code();
pub const ERROR_UNSUPPORTED_VERSION: i16 = ResponseError::UnsupportedVersion.code();
/// `bridge`'s mapping for `BridgeError::PartitionCountMismatch`: the topic exists, just not with
/// the requested partition count.
///
/// Not [`ERROR_INVALID_PARTITIONS`] - `kafka-protocol`'s own error table (`error.rs`) defines that
/// code's text as "Number of partitions is below 1", which is a different condition (a client
/// asking for zero/negative partitions) than "this topic already exists with a different count".
pub const ERROR_TOPIC_ALREADY_EXISTS: i16 = 36;
pub const ERROR_INVALID_PARTITIONS: i16 = 37;
pub const ERROR_INVALID_REPLICATION_FACTOR: i16 = 38;
/// `CreateTopics`: a manual partition `assignments` list whose partition indices are not exactly
/// `0..assignments.len()` in some order, or repeat an index.
///
/// Matches real Kafka's `ReplicationControlManager.createTopic`, which validates the assignment
/// map's keys the same way regardless of what a client's replica list under each key says (this
/// bridge doesn't model replicas at all, so only the key set is checked).
pub const ERROR_INVALID_REPLICA_ASSIGNMENT: i16 = 39;
/// No controller and no Iggy bridge. `CreateTopics`, `DescribeConfigs`, and `AlterConfigs`
/// answer this so a client does not treat a stub as a completed create, read, or alter.
pub const ERROR_NOT_CONTROLLER: i16 = 41;
pub const ERROR_INVALID_REQUEST: i16 = 42;
/// A config this gateway will not apply.
///
/// `CreateTopics` rejects any per-topic config list and points the client at `AlterConfigs`.
/// `AlterConfigs` stores `retention.ms`, `retention.minutes`, or `retention.hours` as
/// `message_expiry` in milliseconds and rejects every other key, and a value that cannot
/// be stored. `DescribeConfigs` sends this when a requested key is unknown or a stored expiry
/// cannot be shown as `retention.ms`.
pub const ERROR_INVALID_CONFIG: i16 = 40;
/// `CreateTopics`, `DescribeConfigs`, and `AlterConfigs`: the request addressed more distinct
/// topics than this bridge admits in one call.
///
/// A server-imposed limit, not a malformed request - `INVALID_REQUEST` would blame the client for
/// a request Kafka itself would accept.
pub const ERROR_POLICY_VIOLATION: i16 = 44;
/// `FindCoordinator` for a transaction coordinator, which the gateway has none of.
///
/// The one code both the Java client and librdkafka treat as fatal for that lookup: anything else,
/// `INVALID_REQUEST` included, sends librdkafka into a 500ms retry loop that never ends.
pub const ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
/// A credential was refused. Deliberately undifferentiated: Iggy answers a bad password and an
/// unknown user the same way, and distinguishing them here would reintroduce a user-enumeration
/// oracle.
pub const ERROR_SASL_AUTHENTICATION_FAILED: i16 = ResponseError::SaslAuthenticationFailed.code();
/// Fetch: the request continues a session. This gateway opens none, so the client starts over
/// with a full request.
pub const ERROR_FETCH_SESSION_ID_NOT_FOUND: i16 = ResponseError::FetchSessionIdNotFound.code();
/// Produce: zstd before v7.
pub const ERROR_UNSUPPORTED_COMPRESSION_TYPE: i16 =
    ResponseError::UnsupportedCompressionType.code();
/// KIP-394: a `JoinGroup` v4+ with an empty member id is answered with a freshly minted id and
/// this code, and the client rejoins carrying it.
pub const ERROR_MEMBER_ID_REQUIRED: i16 = 79;
pub const ERROR_GROUP_MAX_SIZE_REACHED: i16 = 81;
/// The member id does not hold the `group_instance_id` it named, or a static member that
/// returned has since replaced it.
pub const ERROR_FENCED_INSTANCE_ID: i16 = 82;
/// Produce: the batch does not follow the last batch of its producer.
///
/// A batch older than the batches that the partition keeps gets it too. The Java producer retries
/// it, and moves to a new epoch when the gap is real.
pub const ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = ResponseError::OutOfOrderSequenceNumber.code();
/// Produce: a newer epoch of the producer wrote to the partition.
pub const ERROR_INVALID_PRODUCER_EPOCH: i16 = ResponseError::InvalidProducerEpoch.code();
/// Produce: the partition no longer holds the producer, and the batch does not start at
/// sequence 0. The Java producer moves to a new epoch and sends the batch again from 0.
pub const ERROR_UNKNOWN_PRODUCER_ID: i16 = ResponseError::UnknownProducerId.code();
/// Produce: a record or batch this gateway cannot map.
///
/// Not `CORRUPT_MESSAGE` (2), whose text fits but which `kafka-protocol`'s table marks
/// retriable, so a client would resend a batch that can never decode. A client older than Kafka
/// 2.4 reads 87 as a generic server error, which is still terminal and still better than a loop.
pub const ERROR_INVALID_RECORD: i16 = ResponseError::InvalidRecord.code();
/// `DescribeCluster` v1+ asked a broker for the controller endpoints.
pub const ERROR_MISMATCHED_ENDPOINT_TYPE: i16 = ResponseError::MismatchedEndpointType.code();
/// `DescribeCluster` v1+ named an endpoint type Kafka does not define.
pub const ERROR_UNSUPPORTED_ENDPOINT_TYPE: i16 = ResponseError::UnsupportedEndpointType.code();

#[derive(Debug, Clone, Copy)]
pub struct ApiVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produce_advertises_min_zero_whatever_the_firewall_accepts() {
        assert_eq!(advertised_min_version(API_KEY_PRODUCE, 3), 0);
        assert_eq!(advertised_min_version(API_KEY_FETCH, 4), 4);
    }
}
