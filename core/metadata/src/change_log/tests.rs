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

use super::*;
use crate::MuxStateMachine;
use crate::change_log::{ChangeLogCapacities, CurrentState, MetadataChangeLog, current_page};
use crate::impls::metadata::{StreamsFrontend, apply_committed_prepare};
use crate::stm::consumer_group::{
    CompleteConsumerGroupRevocationRequest, JoinConsumerGroupRequest, LeaveConsumerGroupRequest,
};
use crate::stm::lifecycle::{CompleteLifecycleRequest, LifecycleCompletion};
use crate::stm::stream::Streams;
use crate::stm::user::{
    CreatePersonalAccessTokenRequest, DeletePersonalAccessTokenRequest, PAT_TOKEN_HASH_BYTES,
    ROOT_USER_CREATED_AT_MICROS, Users,
};
use consensus::{CLIENTS_TABLE_MAX, ClientTable, EXPIRED_SESSION_REQUEST_ID};
use iggy_binary_protocol::primitives::metadata_changes::{
    WireConsumerGroupCreatedBody, WireConsumerGroupDeletedBody, WireMemberJoinedBody,
    WireMemberLeftReason, WireMetadataChangeStatus, WireMetadataCurrentItem,
    WireMetadataCurrentPageToken, WirePartitionHandedOverBody, WirePartitionsChangedBody,
    WirePermissionsUpdatedReason, WireSessionBody, WireSessionClosedBody, WireSessionClosedReason,
    WireStreamDeletedBody, WireTokenBody, WireTokenDeletedBody, WireTokenDeletedReason,
    WireTopicDeletedBody, WireUserIdentityBody, WireUserPermissionsBody,
};
use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
use iggy_binary_protocol::primitives::permissions::{
    WireGlobalPermissions, WirePermissions, WireStreamPermissions, WireTopicPermissions,
};
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
};
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsRequest, CreatePartitionsWithAssignmentsRequest, DeletePartitionsRequest,
};
use iggy_binary_protocol::requests::streams::{
    CreateStreamRequest, DeleteStreamRequest, RewrapStreamKeyEntry, RewrapStreamKeysRequest,
    UpdateStreamResolvedRequest,
};
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest, CreateTopicWithAssignmentsRequest, DeleteTopicRequest,
    UpdateTopicResolvedRequest,
};
use iggy_binary_protocol::requests::users::login_register::RegisterRequest;
use iggy_binary_protocol::requests::users::{
    ChangePasswordRequest, CreateUserRequest, DeleteUserRequest, UpdatePermissionsRequest,
    UpdateUserResolvedRequest,
};
use iggy_binary_protocol::responses::system::{GetMetadataChangesResponse, WireMetadataItem};
use iggy_binary_protocol::{Command, Operation, PrepareHeader, WireEncode, WireIdentifier};
use iggy_binary_protocol::{WireName, WireOptions};
use iggy_common::wire_conversions::resource_options_to_wire;
use iggy_common::{
    HeaderKey, HeaderValue, MetadataChangeGroup, MetadataChangeGroups, OptionKey, OptionStorage,
    OptionValue, OptionVisibility, OptionsProvenance, ServerKeyContext, StreamEncryptionKey,
    StreamKeyOrigin, UserStatus, WrapSet, descriptors_for, variadic,
};
use server_common::Message;
use server_common::iobuf::Owned;
use std::cell::{Cell, RefCell};

const HEADER_SIZE: usize = size_of::<PrepareHeader>();
const ROOT: u32 = 0;
const ROOT_CLIENT: u128 = 7;
const ALICE: u32 = 1;
const BOB: u32 = 2;
const CLIENT_A: u128 = 101;
const CLIENT_B: u128 = 102;
const OLD_CONFIG_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
const NEW_CONFIG_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";
const SENTINEL: &str = "change-log-sentinel-6f1d";
const NO_CHANGES: [AppliedChange; 0] = [];
const NO_ITEMS: [WireMetadataItem; 0] = [];

#[test]
fn given_created_stream_when_applying_should_record_its_identity_and_public_options() {
    let harness = Harness::new();
    let committed = harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );
    let op = harness.last_op();

    let stream = harness.stream(0);
    assert_eq!(actors(&committed), [Some(ROOT)]);
    assert_eq!(
        decoded(&committed),
        [WireMetadataChange::StreamCreated(WireStreamBody {
            key: WireStreamKey {
                stream_id: 0,
                stream_created_op: op,
            },
            created_at: stream.created_at.as_micros(),
            name: name("orders"),
            options: read_projection(OptionsScope::Stream, &stream.options),
        })]
    );
}

#[test]
fn given_equal_stream_patch_when_applying_should_record_nothing() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );

    let committed = harness.commit(
        ROOT_CALLER,
        Operation::UpdateStreamResolved,
        &UpdateStreamResolvedRequest {
            stream_id: WireIdentifier::numeric(0),
            name: Some(name("orders")),
            explicit_options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        },
    );

    assert_eq!(committed, NO_CHANGES);
}

#[test]
fn given_stream_rename_when_applying_should_record_stream_updated_with_values_after_it() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );

    let committed = harness.commit(
        ROOT_CALLER,
        Operation::UpdateStreamResolved,
        &UpdateStreamResolvedRequest {
            stream_id: WireIdentifier::numeric(0),
            name: Some(name("invoices")),
            explicit_options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        },
    );

    let stream = harness.stream(0);
    assert_eq!(
        decoded(&committed),
        [WireMetadataChange::StreamUpdated(
            stream_body(&stream).expect("valid stream")
        )]
    );
    assert_eq!(stream.name.as_ref(), "invoices");
}

#[test]
fn given_rewrap_when_kek_fingerprint_changes_should_record_stream_updated_without_actor() {
    let harness = Harness::new();
    let old = key_context(OLD_CONFIG_KEY, &[]);
    let rotated = key_context(NEW_CONFIG_KEY, &[OLD_CONFIG_KEY]);
    let stored = WrapSet::from(
        old.wrap(
            &StreamEncryptionKey::try_from([3u8; 32].as_slice()).expect("valid key"),
            StreamKeyOrigin::Server,
        )
        .expect("wrap"),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &encrypted_stream("vault", &stored),
    );
    let before = stream_body(&harness.stream(0)).expect("valid stream");
    let rewrapped = rotated
        .target_set(&stored)
        .expect("rewrap")
        .expect("rotation changes the set");

    let committed = harness.commit(
        SERVER,
        Operation::RewrapStreamKeys,
        &RewrapStreamKeysRequest {
            entries: vec![RewrapStreamKeyEntry {
                stream_id: 0,
                created_op: harness.stream(0).created_op,
                key_fingerprint: stored
                    .key_fingerprint_bytes()
                    .try_into()
                    .expect("fingerprint width"),
                wrap_set: Bytes::copy_from_slice(rewrapped.as_ref()),
            }],
        },
    );

    let after = stream_body(&harness.stream(0)).expect("valid stream");
    assert_ne!(before.options, after.options, "the KEK fingerprint moved");
    assert_eq!(actors(&committed), [None]);
    assert_eq!(
        decoded(&committed),
        [WireMetadataChange::StreamUpdated(after)]
    );
    for body in committed.iter().map(|change| &change.body) {
        assert!(!contains(body, stored.as_ref()));
        assert!(!contains(body, rewrapped.as_ref()));
    }
}

#[test]
fn given_rewrap_entry_that_changes_nothing_when_applying_should_record_nothing() {
    let harness = Harness::new();
    let old = key_context(OLD_CONFIG_KEY, &[]);
    let stored = WrapSet::from(
        old.wrap(
            &StreamEncryptionKey::try_from([3u8; 32].as_slice()).expect("valid key"),
            StreamKeyOrigin::Server,
        )
        .expect("wrap"),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &encrypted_stream("vault", &stored),
    );

    let committed = harness.commit(
        SERVER,
        Operation::RewrapStreamKeys,
        &RewrapStreamKeysRequest {
            entries: vec![RewrapStreamKeyEntry {
                stream_id: 0,
                created_op: harness.stream(0).created_op,
                key_fingerprint: stored
                    .key_fingerprint_bytes()
                    .try_into()
                    .expect("fingerprint width"),
                wrap_set: Bytes::copy_from_slice(stored.as_ref()),
            }],
        },
    );

    assert_eq!(committed, NO_CHANGES);
}

#[test]
fn given_stream_without_partitions_when_deleting_should_record_stream_deleted_at_once() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );
    let stream = harness.stream(0);

    let committed = harness.commit(
        ROOT_CALLER,
        Operation::DeleteStream,
        &DeleteStreamRequest {
            stream_id: WireIdentifier::numeric(0),
        },
    );

    assert_eq!(
        decoded(&committed),
        [WireMetadataChange::StreamDeleted(WireStreamDeletedBody {
            key: WireStreamKey::from(&stream),
            created_at: stream.created_at.as_micros(),
            name: name("orders"),
            requested_op: None,
        })]
    );
}

#[test]
fn given_stream_delete_with_grants_when_applying_should_record_permission_updates_in_user_order() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("audit"),
    );
    for (username, streams) in [("alice", vec![0, 1]), ("bob", vec![0]), ("carol", vec![1])] {
        harness.commit(
            ROOT_CALLER,
            Operation::CreateUser,
            &create_user(username, Some(stream_grants(&streams))),
        );
    }

    let committed = harness.commit(
        ROOT_CALLER,
        Operation::DeleteStream,
        &DeleteStreamRequest {
            stream_id: WireIdentifier::numeric(0),
        },
    );

    let records = decoded(&committed);
    assert_eq!(records.len(), 3);
    assert!(matches!(records[0], WireMetadataChange::StreamDeleted(_)));
    for (record, user_id) in records[1..].iter().zip([ALICE, BOB]) {
        let user = harness.user(user_id);
        assert_eq!(
            *record,
            WireMetadataChange::UserPermissionsUpdated(WireUserPermissionsBody {
                key: WireUserKey::from(&user),
                username: name(&user.username),
                permissions: user.permissions.as_deref().map(permissions_to_wire),
                reason: WirePermissionsUpdatedReason::ResourceDeleted,
            })
        );
    }
    assert_eq!(actors(&committed), [Some(ROOT); 3]);
}

#[test]
fn given_created_topic_when_applying_should_record_its_partitions_and_created_op() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );

    let committed = harness.commit(
        ROOT_CALLER,
        Operation::CreateTopicWithAssignments,
        &create_topic("events", 3),
    );
    let op = harness.last_op();

    let stream = harness.stream(0);
    let topic = &stream.topics[0];
    assert_eq!(topic.created_op, op);
    assert_eq!(
        decoded(&committed),
        [WireMetadataChange::TopicCreated(WireTopicBody {
            key: WireTopicKey {
                stream: WireStreamKey::from(&stream),
                topic_id: 0,
                topic_created_op: op,
            },
            created_at: topic.created_at.as_micros(),
            name: name("events"),
            partitions_count: 3,
            options: read_projection(OptionsScope::Topic, &topic.options),
        })]
    );
}

#[test]
fn given_reused_topic_id_when_recreating_should_have_distinct_created_op() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );
    let first = harness.commit(
        ROOT_CALLER,
        Operation::CreateTopicWithAssignments,
        &create_topic("events", 0),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::DeleteTopic,
        &DeleteTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
        },
    );

    let second = harness.commit(
        ROOT_CALLER,
        Operation::CreateTopicWithAssignments,
        &create_topic("events", 0),
    );

    let (first, second) = (decoded(&first), decoded(&second));
    let ([WireMetadataChange::TopicCreated(first)], [WireMetadataChange::TopicCreated(second)]) =
        (first.as_slice(), second.as_slice())
    else {
        panic!("each create records one topic");
    };
    assert_eq!(first.key.topic_id, second.key.topic_id);
    assert_ne!(first.key.topic_created_op, second.key.topic_created_op);
}

#[test]
fn given_topic_patch_when_values_change_should_record_topic_updated_and_nothing_when_equal() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateTopicWithAssignments,
        &create_topic("events", 1),
    );
    let patch = |name: &str| UpdateTopicResolvedRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        name: Some(WireName::new(name).expect("valid name")),
        explicit_options: WireOptions::empty(),
        derived_options: WireOptions::empty(),
    };

    let equal = harness.commit(
        ROOT_CALLER,
        Operation::UpdateTopicResolved,
        &patch("events"),
    );
    let renamed = harness.commit(ROOT_CALLER, Operation::UpdateTopicResolved, &patch("audit"));

    let stream = harness.stream(0);
    assert_eq!(equal, NO_CHANGES);
    assert_eq!(
        decoded(&renamed),
        [WireMetadataChange::TopicUpdated(
            topic_body(&stream, &stream.topics[0]).expect("valid topic")
        )]
    );
}

#[test]
fn given_two_phase_topic_delete_when_completing_should_record_requested_op_and_requester() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateTopicWithAssignments,
        &create_topic("events", 2),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateUser,
        &create_user("alice", Some(topic_grant(0, 0))),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateUser,
        &create_user("bob", Some(managing_topics())),
    );
    let stream = harness.stream(0);
    let topic_key = stream.topic_key(&stream.topics[0]);
    let topic_created_at = stream.topics[0].created_at.as_micros();
    let bob = Caller {
        client: CLIENT_B,
        user_id: BOB,
        session: harness.register(CLIENT_B, BOB),
    };

    let intent = harness.commit(
        bob,
        Operation::DeleteTopic,
        &DeleteTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
        },
    );
    let intent_op = harness.last_op();
    let partial = harness.complete_lifecycle(intent_op, &[0]);
    let completed = harness.complete_lifecycle(intent_op, &[1]);

    assert_eq!(intent, NO_CHANGES, "the topic still exists at the intent");
    assert_eq!(partial, NO_CHANGES, "a partial batch finishes nothing");
    let alice = harness.user(ALICE);
    assert_eq!(
        decoded(&completed),
        [
            WireMetadataChange::TopicDeleted(WireTopicDeletedBody {
                key: topic_key,
                created_at: topic_created_at,
                name: name("events"),
                requested_op: Some(intent_op),
            }),
            WireMetadataChange::UserPermissionsUpdated(WireUserPermissionsBody {
                key: WireUserKey::from(&alice),
                username: name("alice"),
                permissions: alice.permissions.as_deref().map(permissions_to_wire),
                reason: WirePermissionsUpdatedReason::ResourceDeleted,
            }),
        ]
    );
    assert_eq!(actors(&completed), [Some(BOB), Some(BOB)]);
}

#[test]
fn given_partitions_added_and_removed_when_applying_should_record_counts_and_removed_tail() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateTopicWithAssignments,
        &create_topic("events", 1),
    );
    let topic_key = {
        let stream = harness.stream(0);
        stream.topic_key(&stream.topics[0])
    };

    let added = harness.commit(
        ROOT_CALLER,
        Operation::CreatePartitionsWithAssignments,
        &CreatePartitionsWithAssignmentsRequest {
            request: CreatePartitionsRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partitions_count: 2,
            },
            partitions: assignments(2, 10),
        },
    );
    let intent = harness.commit(
        ROOT_CALLER,
        Operation::DeletePartitions,
        &DeletePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 2,
        },
    );
    let intent_op = harness.last_op();
    let removed = harness.complete_lifecycle(intent_op, &[1, 2]);

    assert_eq!(
        decoded(&added),
        [WireMetadataChange::PartitionsChanged(
            WirePartitionsChangedBody {
                topic: topic_key,
                partitions_count: 3,
                removed_partition_ids: vec![],
                requested_op: None,
            }
        )]
    );
    assert_eq!(intent, NO_CHANGES);
    assert_eq!(
        decoded(&removed),
        [WireMetadataChange::PartitionsChanged(
            WirePartitionsChangedBody {
                topic: topic_key,
                partitions_count: 1,
                removed_partition_ids: vec![1, 2],
                requested_op: Some(intent_op),
            }
        )]
    );
}

#[test]
fn given_consumer_group_lifecycle_when_applying_should_record_counts_after_each_change() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateTopicWithAssignments,
        &create_topic("events", 0),
    );
    let topic_key = {
        let stream = harness.stream(0);
        stream.topic_key(&stream.topics[0])
    };

    let first = harness.commit(
        ROOT_CALLER,
        Operation::CreateConsumerGroup,
        &create_group("billing"),
    );
    let second = harness.commit(
        ROOT_CALLER,
        Operation::CreateConsumerGroup,
        &create_group("shipping"),
    );
    let deleted = harness.commit(
        ROOT_CALLER,
        Operation::DeleteConsumerGroup,
        &DeleteConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            group_id: WireIdentifier::numeric(0),
        },
    );

    assert_eq!(
        [decoded(&first), decoded(&second), decoded(&deleted)],
        [
            vec![WireMetadataChange::ConsumerGroupCreated(
                WireConsumerGroupCreatedBody {
                    topic: topic_key,
                    group_id: 0,
                    name: name("billing"),
                    consumer_groups_count: 1,
                }
            )],
            vec![WireMetadataChange::ConsumerGroupCreated(
                WireConsumerGroupCreatedBody {
                    topic: topic_key,
                    group_id: 1,
                    name: name("shipping"),
                    consumer_groups_count: 2,
                }
            )],
            vec![WireMetadataChange::ConsumerGroupDeleted(
                WireConsumerGroupDeletedBody {
                    topic: topic_key,
                    group_id: 0,
                    name: name("billing"),
                    consumer_groups_count: 1,
                    requested_op: None,
                }
            )],
        ]
    );
}

#[test]
fn given_group_on_partitions_when_delete_completes_should_record_requested_op() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateTopicWithAssignments,
        &create_topic("events", 1),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateConsumerGroup,
        &create_group("billing"),
    );

    let intent = harness.commit(
        ROOT_CALLER,
        Operation::DeleteConsumerGroup,
        &DeleteConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            group_id: WireIdentifier::numeric(0),
        },
    );
    let intent_op = harness.last_op();
    let completed = harness.complete_lifecycle(intent_op, &[0]);

    assert_eq!(intent, NO_CHANGES);
    let records = decoded(&completed);
    let [WireMetadataChange::ConsumerGroupDeleted(body)] = records.as_slice() else {
        panic!("the completion records the group delete");
    };
    assert_eq!(body.requested_op, Some(intent_op));
    assert_eq!(body.consumer_groups_count, 0);
    assert_eq!(actors(&completed), [Some(ROOT)]);
}

#[test]
fn given_new_member_when_joining_should_record_it_once_and_nothing_on_rejoin() {
    let harness = Harness::with_group(2);
    let session = harness.register(CLIENT_A, ROOT);
    let topic_key = harness.topic_key();

    let joined = harness.join(CLIENT_A, session);
    let rejoined = harness.join(CLIENT_A, session);

    assert_eq!(
        decoded(&joined),
        [WireMetadataChange::ConsumerGroupMemberJoined(
            WireMemberJoinedBody {
                topic: topic_key,
                group_id: 0,
                member_id: 0,
                client_id: CLIENT_A,
                members_count: 1,
                partition_ids: vec![],
            }
        )]
    );
    assert_eq!(rejoined, NO_CHANGES);
}

#[test]
fn given_member_when_leaving_should_record_left_with_resulting_count() {
    let harness = Harness::with_group(2);
    let session = harness.register(CLIENT_A, ROOT);
    harness.join(CLIENT_A, session);
    let topic_key = harness.topic_key();

    let left = harness.commit(
        ROOT_CALLER,
        Operation::LeaveConsumerGroup,
        &LeaveConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            group_id: WireIdentifier::numeric(0),
            client_id: CLIENT_A,
        },
    );

    assert_eq!(
        decoded(&left),
        [WireMetadataChange::ConsumerGroupMemberLeft(
            WireMemberLeftBody {
                topic: topic_key,
                group_id: 0,
                member_id: 0,
                client_id: CLIENT_A,
                members_count: 0,
                reason: WireMemberLeftReason::Left,
            }
        )]
    );
}

#[test]
fn given_completed_handover_when_applying_should_record_owner_matching_group_details() {
    let harness = Harness::with_group(2);
    let session = harness.register(CLIENT_A, ROOT);
    harness.join(CLIENT_A, session);
    let topic_key = harness.topic_key();

    let mut records = Vec::new();
    for transition in harness.mux.streams().consumer_group_pending_revocations() {
        records.extend(decoded(&harness.commit(
            SERVER,
            Operation::CompleteConsumerGroupRevocation,
            &CompleteConsumerGroupRevocationRequest {
                stream_id: WireIdentifier::numeric(transition.stream_id),
                topic_id: WireIdentifier::numeric(transition.topic_id),
                partition_id: transition.partition_id,
                installation: transition.installation,
                partition_op: 1,
            },
        )));
    }

    let details = harness
        .mux
        .streams()
        .consumer_group_details(
            &WireIdentifier::numeric(0),
            &WireIdentifier::numeric(0),
            &WireIdentifier::numeric(0),
        )
        .expect("group exists");
    let expected: Vec<_> = details.members[0]
        .partitions
        .iter()
        .map(|&partition_id| {
            WireMetadataChange::ConsumerGroupPartitionHandedOver(WirePartitionHandedOverBody {
                topic: topic_key,
                group_id: 0,
                partition_id,
                member_id: Some(details.members[0].id),
            })
        })
        .collect();
    assert_eq!(expected.len(), 2);
    assert_eq!(records, expected);
}

#[test]
fn given_owner_rejoining_with_new_session_when_applying_should_record_partitions_left_without_owner()
 {
    let harness = Harness::with_group(2);
    let session = harness.register(CLIENT_A, ROOT);
    harness.join(CLIENT_A, session);
    for transition in harness.mux.streams().consumer_group_pending_revocations() {
        harness.commit(
            SERVER,
            Operation::CompleteConsumerGroupRevocation,
            &CompleteConsumerGroupRevocationRequest {
                stream_id: WireIdentifier::numeric(transition.stream_id),
                topic_id: WireIdentifier::numeric(transition.topic_id),
                partition_id: transition.partition_id,
                installation: transition.installation,
                partition_op: 1,
            },
        );
    }
    let topic_key = harness.topic_key();

    let rejoined = harness.join(CLIENT_A, session + 100);

    assert_eq!(
        decoded(&rejoined),
        [0, 1].map(|partition_id| {
            WireMetadataChange::ConsumerGroupPartitionHandedOver(WirePartitionHandedOverBody {
                topic: topic_key,
                group_id: 0,
                partition_id,
                member_id: None,
            })
        })
    );
    let details = harness
        .mux
        .streams()
        .consumer_group_details(
            &WireIdentifier::numeric(0),
            &WireIdentifier::numeric(0),
            &WireIdentifier::numeric(0),
        )
        .expect("group exists");
    assert_eq!(details.members[0].partitions, [] as [u32; 0]);
}

#[test]
fn given_revocation_of_clients_in_overlapping_groups_when_permuting_slots_should_emit_identical_records()
 {
    let registered_in_order = |clients: [u128; 2]| {
        let harness = Harness::with_group(1);
        harness.commit(
            ROOT_CALLER,
            Operation::CreateConsumerGroup,
            &create_group("shipping"),
        );
        harness.commit(
            ROOT_CALLER,
            Operation::CreateUser,
            &create_user("alice", None),
        );
        for client in clients {
            harness.register(client, ALICE);
        }
        for group_id in [0, 1] {
            for client in [CLIENT_A, CLIENT_B] {
                let session = harness
                    .client_table
                    .borrow()
                    .get_epoch(client)
                    .expect("registered");
                harness.join_group(client, session, group_id);
            }
        }
        harness.commit(
            ROOT_CALLER,
            Operation::DeleteUser,
            &DeleteUserRequest {
                user_id: WireIdentifier::numeric(ALICE),
            },
        )
    };

    let first = decoded(&registered_in_order([CLIENT_A, CLIENT_B]));
    let second = decoded(&registered_in_order([CLIENT_B, CLIENT_A]));

    // Session epochs follow the registration order, nothing else may.
    let closed_clients = |records: &[WireMetadataChange]| {
        records
            .iter()
            .filter_map(|record| match record {
                WireMetadataChange::SessionClosed(body) => Some(body.client_id),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(closed_clients(&first), [CLIENT_A, CLIENT_B]);
    assert_eq!(closed_clients(&second), [CLIENT_A, CLIENT_B]);
    assert_eq!(first[3..], second[3..]);
    assert!(matches!(first[0], WireMetadataChange::UserDeleted(_)));
    let removed: Vec<_> = first[3..]
        .iter()
        .map(|record| match record {
            WireMetadataChange::ConsumerGroupMemberLeft(body) => (
                body.group_id,
                body.member_id,
                body.client_id,
                body.members_count,
                body.reason,
            ),
            other => panic!("expected a member removal, got {other:?}"),
        })
        .collect();
    assert_eq!(
        removed,
        [
            (0, 0, CLIENT_A, 0, WireMemberLeftReason::SessionEnded),
            (0, 1, CLIENT_B, 0, WireMemberLeftReason::SessionEnded),
            (1, 0, CLIENT_A, 0, WireMemberLeftReason::SessionEnded),
            (1, 1, CLIENT_B, 0, WireMemberLeftReason::SessionEnded),
        ]
    );
}

#[test]
fn given_created_user_when_applying_should_record_permissions_matching_get_user() {
    let harness = Harness::new();
    for stream_name in ["orders", "audit", "billing"] {
        harness.commit(
            ROOT_CALLER,
            Operation::CreateStream,
            &create_stream(stream_name),
        );
    }

    let committed = harness.commit(
        ROOT_CALLER,
        Operation::CreateUser,
        &create_user("alice", Some(stream_grants(&[2, 1]))),
    );

    let user = harness.user(ALICE);
    assert_eq!(
        decoded(&committed),
        [WireMetadataChange::UserCreated(WireUserBody {
            key: WireUserKey::from(&user),
            username: name("alice"),
            status: UserStatus::Active.as_code(),
            permissions: user.permissions.as_deref().map(permissions_to_wire),
            options: read_projection(OptionsScope::User, &user.options),
        })]
    );
}

#[test]
fn given_user_patches_when_applying_should_record_only_changed_values() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateUser,
        &create_user("alice", None),
    );
    let patch = |username: &str, status: UserStatus| UpdateUserResolvedRequest {
        user_id: WireIdentifier::numeric(ALICE),
        username: Some(WireName::new(username).expect("valid name")),
        status: Some(status.as_code()),
        explicit_options: WireOptions::empty(),
        derived_options: WireOptions::empty(),
    };

    let equal = harness.commit(
        ROOT_CALLER,
        Operation::UpdateUserResolved,
        &patch("alice", UserStatus::Active),
    );
    let renamed = harness.commit(
        ROOT_CALLER,
        Operation::UpdateUserResolved,
        &patch("alicia", UserStatus::Active),
    );

    assert_eq!(equal, NO_CHANGES);
    assert_eq!(
        decoded(&renamed),
        [WireMetadataChange::UserUpdated(
            user_body(&harness.user(ALICE)).expect("valid user")
        )]
    );
}

#[test]
fn given_deactivated_user_with_member_when_applying_should_record_update_session_closed_then_member_left()
 {
    let harness = Harness::with_group(1);
    harness.commit(
        ROOT_CALLER,
        Operation::CreateUser,
        &create_user("alice", None),
    );
    let session = harness.register(CLIENT_A, ALICE);
    harness.join(CLIENT_A, session);
    let alice = WireUserKey::from(&harness.user(ALICE));

    let committed = harness.commit(
        ROOT_CALLER,
        Operation::UpdateUserResolved,
        &UpdateUserResolvedRequest {
            user_id: WireIdentifier::numeric(ALICE),
            username: None,
            status: Some(UserStatus::Inactive.as_code()),
            explicit_options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        },
    );

    let records = decoded(&committed);
    assert_eq!(records.len(), 3);
    assert!(matches!(&records[0], WireMetadataChange::UserUpdated(body)
        if body.status == UserStatus::Inactive.as_code()));
    assert_eq!(
        records[1],
        WireMetadataChange::SessionClosed(WireSessionClosedBody {
            user: alice,
            client_id: CLIENT_A,
            session,
            reason: WireSessionClosedReason::Revoked,
        })
    );
    assert!(
        matches!(&records[2], WireMetadataChange::ConsumerGroupMemberLeft(body)
        if body.client_id == CLIENT_A && body.reason == WireMemberLeftReason::SessionEnded)
    );
    assert_eq!(actors(&committed), [Some(ROOT), Some(ROOT), Some(ROOT)]);
}

#[test]
fn given_deleted_user_with_sessions_when_applying_should_close_each_session_in_client_order() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateUser,
        &create_user("alice", None),
    );
    let second = harness.register(CLIENT_B, ALICE);
    let first = harness.register(CLIENT_A, ALICE);
    let alice = WireUserKey::from(&harness.user(ALICE));

    let committed = harness.commit(
        ROOT_CALLER,
        Operation::DeleteUser,
        &DeleteUserRequest {
            user_id: WireIdentifier::numeric(ALICE),
        },
    );

    let closed = |client_id, session| {
        WireMetadataChange::SessionClosed(WireSessionClosedBody {
            user: alice,
            client_id,
            session,
            reason: WireSessionClosedReason::Revoked,
        })
    };
    let records = decoded(&committed);
    assert!(matches!(&records[0], WireMetadataChange::UserDeleted(_)));
    assert_eq!(
        records[1..],
        [closed(CLIENT_A, first), closed(CLIENT_B, second)]
    );
    assert_eq!(actors(&committed), [Some(ROOT); 3]);
}

#[test]
fn given_deleted_user_and_changed_password_when_applying_should_record_identity_only() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateUser,
        &create_user("alice", None),
    );
    let alice = harness.user(ALICE);
    let identity = WireUserIdentityBody {
        key: WireUserKey::from(&alice),
        username: name("alice"),
    };
    let change_password = |new_password: &str| ChangePasswordRequest {
        user_id: WireIdentifier::numeric(ALICE),
        current_password: String::new(),
        new_password: new_password.to_owned(),
    };

    let refused = harness.commit(ROOT_CALLER, Operation::ChangePassword, &change_password(""));
    let changed = harness.commit(
        ROOT_CALLER,
        Operation::ChangePassword,
        &change_password("new-hash"),
    );
    let deleted = harness.commit(
        ROOT_CALLER,
        Operation::DeleteUser,
        &DeleteUserRequest {
            user_id: WireIdentifier::numeric(ALICE),
        },
    );

    assert_eq!(refused, NO_CHANGES);
    assert_eq!(
        decoded(&changed),
        [WireMetadataChange::UserPasswordChanged(identity.clone())]
    );
    assert_eq!(
        decoded(&deleted),
        [WireMetadataChange::UserDeleted(identity)]
    );
    assert!(!contains(&changed[0].body, b"new-hash"));
}

#[test]
fn given_permission_replacements_when_applying_should_record_only_a_different_set() {
    let harness = Harness::new();
    for stream_name in ["orders", "audit", "billing"] {
        harness.commit(
            ROOT_CALLER,
            Operation::CreateStream,
            &create_stream(stream_name),
        );
    }
    harness.commit(
        ROOT_CALLER,
        Operation::CreateUser,
        &create_user("alice", Some(stream_grants(&[1]))),
    );
    let replace = |permissions: Option<WirePermissions>| UpdatePermissionsRequest {
        user_id: WireIdentifier::numeric(ALICE),
        permissions,
    };

    let equal = harness.commit(
        ROOT_CALLER,
        Operation::UpdatePermissions,
        &replace(Some(stream_grants(&[1]))),
    );
    let changed = harness.commit(
        ROOT_CALLER,
        Operation::UpdatePermissions,
        &replace(Some(stream_grants(&[1, 2]))),
    );

    let alice = harness.user(ALICE);
    assert_eq!(equal, NO_CHANGES);
    assert_eq!(
        decoded(&changed),
        [WireMetadataChange::UserPermissionsUpdated(
            WireUserPermissionsBody {
                key: WireUserKey::from(&alice),
                username: name("alice"),
                permissions: alice.permissions.as_deref().map(permissions_to_wire),
                reason: WirePermissionsUpdatedReason::Updated,
            }
        )]
    );
}

#[test]
fn given_personal_access_tokens_when_created_and_deleted_should_record_names_without_secrets() {
    let harness = Harness::new();
    let root = harness.user(ROOT);
    let token_hash = [b'f'; PAT_TOKEN_HASH_BYTES];
    let create = |name: &str, expiry: u64| CreatePersonalAccessTokenRequest {
        user_id: ROOT,
        name: WireName::new(name).expect("valid name"),
        expiry,
        token_hash,
    };
    let delete = |name: &str, only_if_expired: bool| DeletePersonalAccessTokenRequest {
        user_id: ROOT,
        name: WireName::new(name).expect("valid name"),
        only_if_expired,
    };

    let created = harness.commit(
        ROOT_CALLER,
        Operation::CreatePersonalAccessToken,
        &create("ci", 0),
    );
    let renewed_skip = harness.commit(
        SERVER,
        Operation::DeletePersonalAccessToken,
        &delete("ci", true),
    );
    let deleted = harness.commit(
        ROOT_CALLER,
        Operation::DeletePersonalAccessToken,
        &delete("ci", false),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreatePersonalAccessToken,
        &create("short", 1),
    );
    let expired = harness.commit_bytes(
        SERVER,
        Operation::DeletePersonalAccessToken,
        &delete("short", true).to_bytes(),
        |header| header.timestamp += 10_000_000_000,
    );

    assert_eq!(
        decoded(&created),
        [WireMetadataChange::PersonalAccessTokenCreated(
            WireTokenBody {
                user: WireUserKey::from(&root),
                name: name("ci"),
                expiry: 0,
            }
        )]
    );
    assert!(!contains(&created[0].body, &token_hash));
    assert_eq!(renewed_skip, NO_CHANGES, "a token that never expires stays");
    assert_eq!(
        decoded(&deleted),
        [WireMetadataChange::PersonalAccessTokenDeleted(
            WireTokenDeletedBody {
                user: WireUserKey::from(&root),
                name: name("ci"),
                reason: WireTokenDeletedReason::Deleted,
            }
        )]
    );
    assert_eq!(
        decoded(&expired),
        [WireMetadataChange::PersonalAccessTokenDeleted(
            WireTokenDeletedBody {
                user: WireUserKey::from(&root),
                name: name("short"),
                reason: WireTokenDeletedReason::Expired,
            }
        )]
    );
    assert_eq!(actors(&expired), [None]);
}

#[test]
fn given_first_registration_when_applying_should_record_session_with_its_epoch() {
    let harness = Harness::new();

    let committed = harness.commit_register(CLIENT_A, ROOT, ROOT_USER_CREATED_AT_MICROS);
    let op = harness.last_op();

    assert_eq!(harness.client_table.borrow().get_epoch(CLIENT_A), Some(op));
    assert_eq!(
        decoded(&committed),
        [WireMetadataChange::SessionRegistered(WireSessionBody {
            user: WireUserKey::from(&harness.user(ROOT)),
            client_id: CLIENT_A,
            session: op,
        })]
    );
    assert_eq!(actors(&committed), [Some(ROOT)]);
}

#[test]
fn given_unauthenticated_register_when_applying_should_record_nothing() {
    let harness = Harness::new();

    let committed = harness.commit_register(CLIENT_A, ROOT, ROOT_USER_CREATED_AT_MICROS + 1);

    assert_eq!(committed, NO_CHANGES);
    assert!(!harness.client_table.borrow().contains(CLIENT_A));
}

#[test]
fn given_retried_register_when_applying_should_record_nothing_and_keep_epoch() {
    let harness = Harness::new();
    let epoch = harness.register(CLIENT_A, ROOT);

    let retried = harness.commit_register(CLIENT_A, ROOT, ROOT_USER_CREATED_AT_MICROS);

    assert_eq!(retried, NO_CHANGES);
    assert_eq!(
        harness.client_table.borrow().get_epoch(CLIENT_A),
        Some(epoch)
    );
}

#[test]
fn given_logout_of_member_when_applying_should_record_session_closed_before_member_left() {
    let harness = Harness::with_group(1);
    let session = harness.register(CLIENT_A, ROOT);
    harness.join(CLIENT_A, session);
    let caller = Caller {
        client: CLIENT_A,
        user_id: ROOT,
        session,
    };

    let committed = harness.commit_bytes(caller, Operation::Logout, &[], |_| {});
    let repeated = harness.commit_bytes(caller, Operation::Logout, &[], |_| {});

    let records = decoded(&committed);
    assert_eq!(
        records[0],
        WireMetadataChange::SessionClosed(WireSessionClosedBody {
            user: WireUserKey::from(&harness.user(ROOT)),
            client_id: CLIENT_A,
            session,
            reason: WireSessionClosedReason::Logout,
        })
    );
    assert!(
        matches!(&records[1], WireMetadataChange::ConsumerGroupMemberLeft(body)
        if body.client_id == CLIENT_A && body.reason == WireMemberLeftReason::SessionEnded)
    );
    assert_eq!(records.len(), 2);
    assert_eq!(actors(&committed), [Some(ROOT), Some(ROOT)]);
    assert!(
        repeated.is_empty(),
        "only the logout that ends the session records"
    );
}

#[test]
fn given_lease_expiry_logout_when_applying_should_record_expired_close_without_actor() {
    let harness = Harness::new();
    let session = harness.register(CLIENT_A, ROOT);

    let committed = harness.commit_bytes(
        Caller {
            client: CLIENT_A,
            user_id: ROOT,
            session,
        },
        Operation::Logout,
        &[],
        |header| header.request = EXPIRED_SESSION_REQUEST_ID,
    );

    assert_eq!(
        decoded(&committed),
        [WireMetadataChange::SessionClosed(WireSessionClosedBody {
            user: WireUserKey::from(&harness.user(ROOT)),
            client_id: CLIENT_A,
            session,
            reason: WireSessionClosedReason::Expired,
        })]
    );
    assert_eq!(actors(&committed), [None]);
}

#[test]
fn given_refused_denied_or_stale_operations_when_applying_should_record_nothing() {
    let harness = Harness::new();
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateUser,
        &create_user("alice", None),
    );
    let session = harness.register(CLIENT_A, ALICE);

    let duplicate = harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );
    let denied = harness.commit(
        Caller {
            client: CLIENT_A,
            user_id: ALICE,
            session,
        },
        Operation::CreateStream,
        &create_stream("audit"),
    );
    let stale = harness.commit(
        Caller {
            client: CLIENT_A,
            user_id: ALICE,
            session: session + 1,
        },
        Operation::CreateStream,
        &create_stream("audit"),
    );

    assert_eq!(duplicate, NO_CHANGES);
    assert_eq!(denied, NO_CHANGES);
    assert_eq!(stale, NO_CHANGES);
    assert!(
        harness
            .mux
            .streams()
            .read(|streams| streams.items.len() == 1)
    );
}

#[test]
fn given_sentinel_in_every_hidden_option_when_recording_should_never_encode_it() {
    for scope in [
        OptionsScope::Stream,
        OptionsScope::Topic,
        OptionsScope::User,
    ] {
        for descriptor in descriptors_for(scope).filter(|descriptor| {
            descriptor.visibility != OptionVisibility::Public
                || descriptor.storage != OptionStorage::Map
        }) {
            let mut options = ResourceOptions::new();
            options.insert(
                HeaderKey::from(descriptor.key),
                OptionValue::explicit(HeaderValue::try_from(SENTINEL).expect("valid value")),
            );
            let body = match scope {
                OptionsScope::User => WireMetadataChange::UserCreated(
                    user_body(&User {
                        username: "alice".into(),
                        options,
                        ..User::default()
                    })
                    .expect("valid user"),
                ),
                OptionsScope::Topic => {
                    let stream = Stream {
                        name: "orders".into(),
                        ..Stream::default()
                    };
                    let topic = Topic {
                        name: "events".into(),
                        options,
                        ..Topic::default()
                    };
                    WireMetadataChange::TopicCreated(topic_body(&stream, &topic).expect("valid"))
                }
                OptionsScope::Stream => WireMetadataChange::StreamCreated(
                    stream_body(&Stream {
                        name: "orders".into(),
                        options,
                        ..Stream::default()
                    })
                    .expect("valid stream"),
                ),
            };
            assert!(
                !contains(&body.body_bytes(), SENTINEL.as_bytes()),
                "{scope:?} record echoed {:?}",
                descriptor.key
            );
        }
    }
}

#[test]
fn given_unreadable_critical_state_when_recording_should_mark_options_unreadable() {
    let mut options = ResourceOptions::new();
    options.insert(
        HeaderKey::from(OptionKey::EncryptionKeyWrapped),
        OptionValue::explicit(HeaderValue::try_from(SENTINEL).expect("valid value")),
    );

    let body = stream_body(&Stream {
        name: "orders".into(),
        options,
        ..Stream::default()
    })
    .expect("valid stream");

    assert_eq!(body.options, WireChangeOptions::Unreadable);
}

#[test]
fn given_state_when_paging_one_item_at_a_time_should_equal_one_page_in_group_and_key_order() {
    let harness = Harness::with_group(2);
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("audit"),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateConsumerGroup,
        &create_group("shipping"),
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateUser,
        &create_user("alice", None),
    );
    let session = harness.register(CLIENT_A, ALICE);
    harness.join(CLIENT_A, session);
    harness.commit(
        ROOT_CALLER,
        Operation::CreatePersonalAccessToken,
        &CreatePersonalAccessTokenRequest {
            user_id: ALICE,
            name: name("ci"),
            expiry: 0,
            token_hash: [b'f'; PAT_TOKEN_HASH_BYTES],
        },
    );

    let whole = harness.scan(&[], 1000);
    let one_by_one = harness.scan(&[], 1);

    assert_eq!(one_by_one, whole);
    let groups: Vec<_> = whole.iter().map(WireMetadataCurrentItem::group).collect();
    let mut sorted = groups.clone();
    sorted.sort();
    assert_eq!(groups, sorted, "items come in group order");
    let count = |group| groups.iter().filter(|item| **item == group).count();
    assert_eq!(count(WireMetadataChangeGroup::Streams), 2);
    assert_eq!(count(WireMetadataChangeGroup::Topics), 1);
    assert_eq!(count(WireMetadataChangeGroup::Partitions), 1);
    assert_eq!(count(WireMetadataChangeGroup::ConsumerGroups), 2);
    assert_eq!(count(WireMetadataChangeGroup::ConsumerGroupMembers), 1);
    assert_eq!(count(WireMetadataChangeGroup::Users), 2);
    assert_eq!(count(WireMetadataChangeGroup::PersonalAccessTokens), 1);
    assert_eq!(count(WireMetadataChangeGroup::Sessions), 2);
}

#[test]
fn given_delete_and_rename_between_pages_when_continuing_should_seek_past_the_last_key() {
    let harness = Harness::new();
    for stream_name in ["orders", "audit", "billing"] {
        harness.commit(
            ROOT_CALLER,
            Operation::CreateStream,
            &create_stream(stream_name),
        );
    }
    let streams = [MetadataChangeGroup::Streams];
    let first = harness.current(&streams, None, 1, true);
    let token = first
        .current_page_token
        .clone()
        .expect("more streams follow");

    harness.commit(
        ROOT_CALLER,
        Operation::DeleteStream,
        &DeleteStreamRequest {
            stream_id: WireIdentifier::numeric(0),
        },
    );
    harness.commit(
        ROOT_CALLER,
        Operation::UpdateStreamResolved,
        &UpdateStreamResolvedRequest {
            stream_id: WireIdentifier::numeric(1),
            name: Some(name("invoices")),
            explicit_options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        },
    );
    harness.commit(
        ROOT_CALLER,
        Operation::CreateStream,
        &create_stream("orders"),
    );
    let second = harness.current(&streams, Some(&token), 10, true);

    let names: Vec<_> = second
        .items
        .iter()
        .map(|item| match decoded_item(item) {
            WireMetadataCurrentItem::Stream(body) => {
                (body.key.stream_id, body.name.as_str().to_owned())
            }
            other => panic!("expected a stream, got {other:?}"),
        })
        .collect();
    assert_eq!(
        names,
        [(1, "invoices".to_owned()), (2, "billing".to_owned())],
        "the deleted key still seeks, and the recreated id below it is a change after the anchor"
    );
    assert_eq!(second.next, first.next, "every page carries the anchor");
    assert_eq!(second.current_page_token, None);
}

#[test]
fn given_continuation_above_the_applied_head_when_paging_should_answer_behind_with_the_token() {
    let harness = Harness::new();
    for stream_name in ["orders", "audit"] {
        harness.commit(
            ROOT_CALLER,
            Operation::CreateStream,
            &create_stream(stream_name),
        );
    }
    let streams = [MetadataChangeGroup::Streams];
    let mut token = harness
        .current(&streams, None, 1, true)
        .current_page_token
        .expect("more streams follow");
    token.required_head = harness.last_op() + 1;

    let page = harness.current(&streams, Some(&token), 10, true);

    assert_eq!(page.status, WireMetadataChangeStatus::Behind);
    assert_eq!(page.current_page_token, Some(token));
    assert_eq!(page.items, NO_ITEMS);
}

#[test]
fn given_anchor_evicted_between_pages_when_continuing_should_expire_without_items() {
    let harness = Harness::with_capacities(ChangeLogCapacities {
        resource: 2,
        activity: 16,
    });
    for stream_name in ["orders", "audit"] {
        harness.commit(
            ROOT_CALLER,
            Operation::CreateStream,
            &create_stream(stream_name),
        );
    }
    let streams = [MetadataChangeGroup::Streams];
    let token = harness
        .current(&streams, None, 1, true)
        .current_page_token
        .expect("more streams follow");
    for stream_name in ["billing", "shipping"] {
        harness.commit(
            ROOT_CALLER,
            Operation::CreateStream,
            &create_stream(stream_name),
        );
    }

    let page = harness.current(&streams, Some(&token), 10, true);

    assert_eq!(page.status, WireMetadataChangeStatus::Expired);
    assert_eq!(page.next, None);
    assert_eq!(page.items, NO_ITEMS);
    assert_eq!(page.current_page_token, None);
}

#[test]
fn given_mismatched_current_anchor_when_continuing_should_reset_without_a_cursor() {
    let harness = Harness::new();
    for stream_name in ["orders", "audit"] {
        harness.commit(
            ROOT_CALLER,
            Operation::CreateStream,
            &create_stream(stream_name),
        );
    }
    let streams = [MetadataChangeGroup::Streams];
    let token = harness
        .current(&streams, None, 1, true)
        .current_page_token
        .expect("more streams follow");

    for different_history in [true, false] {
        let mut mismatched = token.clone();
        if different_history {
            mismatched.anchor.log_id ^= 1;
        } else {
            mismatched.anchor.checksum ^= 1;
        }
        let page = harness.current(&streams, Some(&mismatched), 10, true);

        assert_eq!(page.status, WireMetadataChangeStatus::Reset);
        assert_eq!(page.next, None);
        assert_eq!(page.items, NO_ITEMS);
        assert_eq!(page.current_page_token, None);
    }
}

#[test]
fn given_protection_table_ahead_when_paging_sessions_should_answer_behind() {
    let harness = Harness::new();

    let sessions = harness.current(
        &[MetadataChangeGroup::Users, MetadataChangeGroup::Sessions],
        None,
        10,
        false,
    );
    let users = harness.current(&[MetadataChangeGroup::Users], None, 10, false);

    assert_eq!(sessions.status, WireMetadataChangeStatus::Behind);
    assert_eq!(users.status, WireMetadataChangeStatus::Ok);
}

#[test]
fn given_sessions_and_tokens_when_paging_should_carry_identities_only() {
    let harness = Harness::new();
    let token_hash = [b'f'; PAT_TOKEN_HASH_BYTES];
    harness.commit(
        ROOT_CALLER,
        Operation::CreatePersonalAccessToken,
        &CreatePersonalAccessTokenRequest {
            user_id: ROOT,
            name: name("ci"),
            expiry: 0,
            token_hash,
        },
    );

    let page = harness.current(
        &[
            MetadataChangeGroup::Users,
            MetadataChangeGroup::PersonalAccessTokens,
            MetadataChangeGroup::Sessions,
        ],
        None,
        100,
        true,
    );

    let root = WireUserKey::from(&harness.user(ROOT));
    let items: Vec<_> = page.items.iter().map(decoded_item).collect();
    assert!(
        items.contains(&WireMetadataCurrentItem::Session(WireSessionBody {
            user: root,
            client_id: ROOT_CLIENT,
            session: 1,
        }))
    );
    assert!(
        items.contains(&WireMetadataCurrentItem::PersonalAccessToken(
            WireTokenBody {
                user: root,
                name: name("ci"),
                expiry: 0,
            }
        ))
    );
    for item in &page.items {
        assert!(!contains(&item.body, &[0x5a; 32]), "no bind verifier");
        assert!(!contains(&item.body, &token_hash), "no token hash");
        assert!(!contains(&item.body, b"hash"), "no password hash");
    }
}

#[test]
fn given_users_and_members_when_paging_should_match_their_read_projections() {
    let harness = Harness::with_group(2);
    harness.commit(
        ROOT_CALLER,
        Operation::CreateUser,
        &create_user("alice", Some(stream_grants(&[0]))),
    );
    let session = harness.register(CLIENT_A, ALICE);
    harness.join(CLIENT_A, session);
    for transition in harness.mux.streams().consumer_group_pending_revocations() {
        harness.commit(
            SERVER,
            Operation::CompleteConsumerGroupRevocation,
            &CompleteConsumerGroupRevocationRequest {
                stream_id: WireIdentifier::numeric(transition.stream_id),
                topic_id: WireIdentifier::numeric(transition.topic_id),
                partition_id: transition.partition_id,
                installation: transition.installation,
                partition_op: 1,
            },
        );
    }

    let items = harness.scan(&[], 100);

    let alice = harness.user(ALICE);
    assert!(items.contains(&WireMetadataCurrentItem::User(
        user_body(&alice).expect("valid user")
    )));
    let details = harness
        .mux
        .streams()
        .consumer_group_details(
            &WireIdentifier::numeric(0),
            &WireIdentifier::numeric(0),
            &WireIdentifier::numeric(0),
        )
        .expect("group exists");
    let members: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            WireMetadataCurrentItem::ConsumerGroupMember(member) => {
                Some((member.member_id, member.partition_ids.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        members,
        [(details.members[0].id, details.members[0].partitions.clone())]
    );
    assert_eq!(members[0].1, [0, 1]);
}

#[derive(Clone, Copy)]
struct Caller {
    client: u128,
    user_id: u32,
    session: u64,
}

const ROOT_CALLER: Caller = Caller {
    client: ROOT_CLIENT,
    user_id: ROOT,
    session: 0,
};

const SERVER: Caller = Caller {
    client: 0,
    user_id: ROOT,
    session: 0,
};

struct Harness {
    mux: MuxStateMachine<variadic!(Users, Streams)>,
    client_table: RefCell<ClientTable>,
    change_log: MetadataChangeLog,
    op: Cell<u64>,
}

impl Harness {
    fn new() -> Self {
        Self::with_capacities(ChangeLogCapacities::default())
    }

    fn with_capacities(capacities: ChangeLogCapacities) -> Self {
        let users = Users::default();
        users.ensure_root_user("iggy", "hash");
        let streams = Streams::default();
        let harness = Self {
            mux: MuxStateMachine::new(variadic!(users, streams)),
            client_table: RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX)),
            change_log: MetadataChangeLog::new(capacities),
            op: Cell::new(0),
        };
        // A client commit without a session is a fatal receipt error.
        harness.register(ROOT_CLIENT, ROOT);
        harness
    }

    /// A stream with a topic of `partitions` partitions and one group on it.
    fn with_group(partitions: u32) -> Self {
        let harness = Self::new();
        harness.commit(
            ROOT_CALLER,
            Operation::CreateStream,
            &create_stream("orders"),
        );
        harness.commit(
            ROOT_CALLER,
            Operation::CreateTopicWithAssignments,
            &create_topic("events", partitions),
        );
        harness.commit(
            ROOT_CALLER,
            Operation::CreateConsumerGroup,
            &create_group("billing"),
        );
        harness
    }

    fn last_op(&self) -> u64 {
        self.op.get()
    }

    fn commit(
        &self,
        caller: Caller,
        operation: Operation,
        request: &impl WireEncode,
    ) -> Vec<AppliedChange> {
        self.commit_bytes(caller, operation, &request.to_bytes(), |_| {})
    }

    fn commit_bytes(
        &self,
        caller: Caller,
        operation: Operation,
        body: &[u8],
        adjust: impl FnOnce(&mut PrepareHeader),
    ) -> Vec<AppliedChange> {
        let op = self.op.get() + 1;
        self.op.set(op);
        let total = HEADER_SIZE + body.len();
        let mut buffer = Owned::<4096>::zeroed(total);
        {
            let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
                &mut buffer.as_mut_slice()[..HEADER_SIZE],
            );
            header.command = Command::Prepare;
            header.operation = operation;
            header.op = op;
            header.commit = op - 1;
            header.timestamp = op * 1_000;
            header.client = caller.client;
            header.user_id = caller.user_id;
            header.session = caller.session;
            header.request = if operation == Operation::Register {
                0
            } else {
                op
            };
            header.retry_capacity = u32::try_from(CLIENTS_TABLE_MAX).expect("capacity");
            header.size = u32::try_from(total).expect("size");
            adjust(header);
        }
        buffer.as_mut_slice()[HEADER_SIZE..].copy_from_slice(body);
        let prepare: Message<PrepareHeader> = Message::try_from(buffer).expect("valid prepare");
        let header = *prepare.header();
        let changes =
            apply_committed_prepare(&self.mux, &self.client_table, true, |_| {}, prepare).changes;
        self.change_log
            .append(&header, changes.clone())
            .expect("committed ops append in order");
        changes
    }

    /// One current state page over the applied state of this harness.
    fn current(
        &self,
        groups: &[MetadataChangeGroup],
        token: Option<&WireMetadataCurrentPageToken>,
        limit: u32,
        sessions_aligned: bool,
    ) -> GetMetadataChangesResponse {
        let table = self.client_table.borrow();
        self.mux.streams().read(|streams| {
            self.mux.users().read(|users| {
                self.change_log
                    .read(|log| {
                        current_page(
                            log,
                            self.last_op(),
                            MetadataChangeGroups::requested(groups),
                            token,
                            limit,
                            &CurrentState {
                                streams,
                                users,
                                sessions: sessions_aligned.then_some(&*table),
                            },
                        )
                    })
                    .expect("readable")
                    .expect("valid page")
            })
        })
    }

    /// Every item of a scan, page by page.
    fn scan(&self, groups: &[MetadataChangeGroup], limit: u32) -> Vec<WireMetadataCurrentItem> {
        let mut items = Vec::new();
        let mut token = None;
        loop {
            let page = self.current(groups, token.as_ref(), limit, true);
            assert_eq!(page.status, WireMetadataChangeStatus::Ok);
            assert!(page.items.len() <= limit as usize);
            items.extend(page.items.iter().map(decoded_item));
            match page.current_page_token {
                Some(next) => token = Some(next),
                None => return items,
            }
        }
    }

    fn commit_register(
        &self,
        client: u128,
        user_id: u32,
        user_created_at: u64,
    ) -> Vec<AppliedChange> {
        self.commit(
            Caller {
                client,
                user_id,
                session: 0,
            },
            Operation::Register,
            &RegisterRequest {
                user_created_at,
                bind_verifier: [0x5a; 32],
            },
        )
    }

    /// Registers `client` for `user_id` and returns its session epoch.
    fn register(&self, client: u128, user_id: u32) -> u64 {
        let user_created_at = self.user(user_id).created_at.as_micros();
        self.commit_register(client, user_id, user_created_at);
        self.client_table
            .borrow()
            .get_epoch(client)
            .expect("registration commits")
    }

    fn join(&self, client: u128, session: u64) -> Vec<AppliedChange> {
        self.join_group(client, session, 0)
    }

    fn join_group(&self, client: u128, session: u64, group_id: u32) -> Vec<AppliedChange> {
        self.commit(
            ROOT_CALLER,
            Operation::JoinConsumerGroup,
            &JoinConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                group_id: WireIdentifier::numeric(group_id),
                client_id: client,
                session,
            },
        )
    }

    /// Reports the fences of `partitions` of the intent at `intent_op` as
    /// installed, from a server-internal client.
    fn complete_lifecycle(&self, intent_op: u64, partitions: &[u32]) -> Vec<AppliedChange> {
        let completions = partitions
            .iter()
            .map(|&partition_id| LifecycleCompletion {
                topic_id: 0,
                partition_id,
                partition_op: 1,
            })
            .collect();
        self.commit(
            Caller {
                client: message_bus::server_internal_client_id(intent_op),
                user_id: ROOT,
                session: 0,
            },
            Operation::CompleteLifecycle,
            &CompleteLifecycleRequest::new(intent_op, 0, completions).expect("valid batch"),
        )
    }

    fn stream(&self, stream_id: usize) -> Stream {
        self.mux
            .streams()
            .read(|streams| streams.items.get(stream_id).cloned())
            .expect("stream exists")
    }

    fn user(&self, user_id: u32) -> User {
        self.mux
            .users()
            .read(|users| users.items.get(user_id as usize).cloned())
            .expect("user exists")
    }

    fn topic_key(&self) -> WireTopicKey {
        let stream = self.stream(0);
        stream.topic_key(&stream.topics[0])
    }
}

fn decoded(changes: &[AppliedChange]) -> Vec<WireMetadataChange> {
    changes
        .iter()
        .map(|change| {
            WireMetadataChange::decode(change.group().as_u8(), change.kind.as_u8(), &change.body)
                .expect("valid body")
                .expect("known kind")
        })
        .collect()
}

fn decoded_item(item: &WireMetadataItem) -> WireMetadataCurrentItem {
    WireMetadataCurrentItem::decode(item.group, &item.body)
        .expect("valid item")
        .expect("known group")
}

fn actors(changes: &[AppliedChange]) -> Vec<Option<u32>> {
    changes.iter().map(|change| change.actor_user_id).collect()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn name(value: &str) -> WireName {
    WireName::new(value).expect("valid name")
}

/// The option blocks the read routes return for `stored`.
fn read_projection(scope: OptionsScope, stored: &ResourceOptions) -> WireChangeOptions {
    let (explicit, derived) =
        resource_options_to_wire_split(&public_options(scope, stored).expect("readable"))
            .expect("encodable");
    WireChangeOptions::Readable { explicit, derived }
}

fn key_context(key: &str, previous: &[&str]) -> ServerKeyContext {
    let previous: Vec<String> = previous.iter().map(|key| (*key).to_owned()).collect();
    ServerKeyContext::from_config(key, &previous, true).expect("valid key")
}

fn create_stream(stream_name: &str) -> CreateStreamRequest {
    CreateStreamRequest {
        name: name(stream_name),
        options: WireOptions::empty(),
    }
}

fn encrypted_stream(stream_name: &str, set: &WrapSet) -> CreateStreamRequest {
    let mut options = ResourceOptions::new();
    options.insert(
        HeaderKey::from(OptionKey::EncryptionKeyWrapped),
        OptionValue::explicit(HeaderValue::try_from(set.as_ref()).expect("valid set")),
    );
    CreateStreamRequest {
        name: name(stream_name),
        options: resource_options_to_wire(&options, OptionsProvenance::All).expect("encodable"),
    }
}

fn create_topic(topic_name: &str, partitions: u32) -> CreateTopicWithAssignmentsRequest {
    CreateTopicWithAssignmentsRequest {
        request: CreateTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            partitions_count: partitions,
            name: name(topic_name),
            options: WireOptions::empty(),
        },
        derived_options: WireOptions::empty(),
        partitions: assignments(partitions, 1),
    }
}

fn assignments(count: u32, first_group: u64) -> Vec<CreatedPartitionAssignment> {
    (0..count)
        .map(|partition_id| CreatedPartitionAssignment {
            partition_id,
            consensus_group_id: first_group + u64::from(partition_id),
            initial_view: 0,
        })
        .collect()
}

fn create_group(group_name: &str) -> CreateConsumerGroupRequest {
    CreateConsumerGroupRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        name: name(group_name),
    }
}

fn create_user(username: &str, permissions: Option<WirePermissions>) -> CreateUserRequest {
    CreateUserRequest {
        username: name(username),
        password: "hash".to_owned(),
        status: UserStatus::Active.as_code(),
        permissions,
        options: WireOptions::empty(),
    }
}

fn no_global() -> WireGlobalPermissions {
    WireGlobalPermissions {
        manage_servers: false,
        read_servers: false,
        manage_users: false,
        read_users: false,
        manage_streams: false,
        read_streams: false,
        manage_topics: false,
        read_topics: false,
        poll_messages: false,
        send_messages: false,
    }
}

fn managing_topics() -> WirePermissions {
    WirePermissions {
        global: WireGlobalPermissions {
            manage_topics: true,
            read_topics: true,
            ..no_global()
        },
        streams: Vec::new(),
    }
}

fn stream_grants(stream_ids: &[u32]) -> WirePermissions {
    WirePermissions {
        global: no_global(),
        streams: stream_ids
            .iter()
            .map(|&stream_id| WireStreamPermissions {
                stream_id,
                manage_stream: false,
                read_stream: true,
                manage_topics: false,
                read_topics: false,
                poll_messages: true,
                send_messages: false,
                topics: Vec::new(),
            })
            .collect(),
    }
}

fn topic_grant(stream_id: u32, topic_id: u32) -> WirePermissions {
    WirePermissions {
        global: no_global(),
        streams: vec![WireStreamPermissions {
            stream_id,
            manage_stream: false,
            read_stream: true,
            manage_topics: false,
            read_topics: false,
            poll_messages: false,
            send_messages: false,
            topics: vec![WireTopicPermissions {
                topic_id,
                manage_topic: false,
                read_topic: true,
                poll_messages: true,
                send_messages: false,
            }],
        }],
    }
}
