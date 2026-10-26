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

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::{Duration, Instant};

use iggy::prelude::locking::IggyRwLockFn;
use iggy::prelude::*;
use iggy_binary_protocol::WireEncode;
use iggy_binary_protocol::codes::GET_METADATA_CHANGES_CODE;
use iggy_binary_protocol::primitives::metadata_changes::WireMetadataChangeGroup;
use iggy_binary_protocol::requests::system::{GetMetadataChangesRequest, SessionIdentity};
use iggy_common::{
    MAX_METADATA_CHANGES_LIMIT, MetadataChangeGroups, MetadataMemberLeftReason, MetadataOptions,
    MetadataPermissionsUpdatedReason, MetadataSessionClosedReason, MetadataTokenDeletedReason,
    OptionKey, ServerKeyContext, StreamEncryptionKey, VsrSessionControl, text,
};
use integration::harness::{TestHarness, wait_for_consumer_group_assignment};
use integration::iggy_harness;
use tokio::time::sleep;

const STREAM_NAME: &str = "changes-stream";
const RENAMED_STREAM_NAME: &str = "changes-stream-renamed";
const TOPIC_NAME: &str = "changes-topic";
const DROPPED_TOPIC_NAME: &str = "changes-dropped-topic";
const GROUP_NAME: &str = "changes-group";
const USERNAME: &str = "changes-user";
const RENAMED_USERNAME: &str = "changes-user-renamed";
const PASSWORD: &str = "changes-secret";
const NEW_PASSWORD: &str = "changes-secret-2";
const TOKEN_NAME: &str = "changes-token";
const OWN_KEY_STREAM: &str = "changes-own-key";
const GENERATED_KEY_STREAM: &str = "changes-generated-key";
const SERVER_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
const ROTATED_SERVER_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";
const ROOT_USER_ID: u32 = 0;
const FOLLOW_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const EVERY_KIND: [MetadataChangeKind; 21] = [
    MetadataChangeKind::StreamCreated,
    MetadataChangeKind::StreamUpdated,
    MetadataChangeKind::StreamDeleted,
    MetadataChangeKind::TopicCreated,
    MetadataChangeKind::TopicUpdated,
    MetadataChangeKind::TopicDeleted,
    MetadataChangeKind::PartitionsChanged,
    MetadataChangeKind::ConsumerGroupCreated,
    MetadataChangeKind::ConsumerGroupDeleted,
    MetadataChangeKind::ConsumerGroupMemberJoined,
    MetadataChangeKind::ConsumerGroupMemberLeft,
    MetadataChangeKind::ConsumerGroupPartitionHandedOver,
    MetadataChangeKind::UserCreated,
    MetadataChangeKind::UserUpdated,
    MetadataChangeKind::UserDeleted,
    MetadataChangeKind::UserPasswordChanged,
    MetadataChangeKind::UserPermissionsUpdated,
    MetadataChangeKind::PersonalAccessTokenCreated,
    MetadataChangeKind::PersonalAccessTokenDeleted,
    MetadataChangeKind::SessionRegistered,
    MetadataChangeKind::SessionClosed,
];

// Harness clients never ping, so a short heartbeat would end the writer's
// session mid-test and add an expiry record the assertions do not expect.
#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn given_every_kind_of_write_when_following_should_record_each_kind_with_its_actor(
    harness: &TestHarness,
) {
    let reader = harness.root_client().await.expect("reader");
    let start = latest(&reader, &[]).await;
    let writer = harness.tcp_root_client().await.expect("writer");
    let writer_session = session_identity(&writer).await;
    let stream_id = Identifier::named(STREAM_NAME).expect("stream id");
    let topic_id = Identifier::named(TOPIC_NAME).expect("topic id");
    let group_id = Identifier::named(GROUP_NAME).expect("group id");

    let stream = writer
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    let topic = writer
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(2),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    writer
        .update_topic(
            &stream_id,
            &topic_id,
            &TopicUpdateOptions {
                message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(
                    Duration::from_secs(3600),
                ))),
                ..TopicUpdateOptions::default()
            },
        )
        .await
        .expect("update topic");
    writer
        .create_topic(
            &stream_id,
            DROPPED_TOPIC_NAME,
            &TopicCreateOptions::default(),
        )
        .await
        .expect("create dropped topic");
    writer
        .delete_topic(
            &stream_id,
            &Identifier::named(DROPPED_TOPIC_NAME).expect("dropped topic id"),
        )
        .await
        .expect("delete topic");
    writer
        .create_partitions(&stream_id, &topic_id, 1)
        .await
        .expect("create partitions");
    writer
        .delete_partitions(&stream_id, &topic_id, 1)
        .await
        .expect("delete partitions");
    writer
        .create_consumer_group(&stream_id, &topic_id, GROUP_NAME)
        .await
        .expect("create group");
    writer
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("join group");
    let assigned = wait_for_consumer_group_assignment(
        &writer,
        &stream_id,
        &topic_id,
        &group_id,
        1,
        FOLLOW_TIMEOUT,
    )
    .await;
    writer
        .leave_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("leave group");
    writer
        .delete_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("delete group");
    let user = writer
        .create_user(
            USERNAME,
            PASSWORD,
            UserStatus::Active,
            Some(stream_grant(stream.id, false)),
            &UserCreateOptions::default(),
        )
        .await
        .expect("create user");
    let user_id = Identifier::numeric(user.id).expect("user id");
    writer
        .update_user(
            &user_id,
            &UserUpdateOptions {
                username: Some(RENAMED_USERNAME.to_owned()),
                ..UserUpdateOptions::default()
            },
        )
        .await
        .expect("update user");
    writer
        .change_password(&user_id, PASSWORD, NEW_PASSWORD)
        .await
        .expect("change password");
    writer
        .update_permissions(&user_id, Some(stream_grant(stream.id, true)))
        .await
        .expect("update permissions");
    writer
        .create_personal_access_token(TOKEN_NAME, PersonalAccessTokenExpiry::NeverExpire)
        .await
        .expect("create token");
    writer
        .delete_personal_access_token(TOKEN_NAME)
        .await
        .expect("delete token");
    writer
        .update_stream(
            &stream_id,
            &StreamUpdateOptions {
                name: Some(RENAMED_STREAM_NAME.to_owned()),
                ..StreamUpdateOptions::default()
            },
        )
        .await
        .expect("rename stream");
    writer
        .delete_stream(&Identifier::named(RENAMED_STREAM_NAME).expect("renamed id"))
        .await
        .expect("delete stream");
    let pruned = wait_for_user(&writer, &user_id, |details| {
        details
            .permissions
            .as_ref()
            .is_none_or(|permissions| permissions.streams.is_none())
    })
    .await;
    writer.delete_user(&user_id).await.expect("delete user");
    writer.logout_user().await.expect("logout");

    let records = follow_until(&reader, &start, &[], |records| {
        records.iter().any(|record| {
            matches!(
                &record.change,
                MetadataChange::SessionClosed(closed) if closed.client_id == writer_session.client_id
            )
        }) && records
            .iter()
            .any(|record| matches!(record.change, MetadataChange::StreamDeleted(_)))
    })
    .await;

    let seen: HashSet<MetadataChangeKind> = records
        .iter()
        .filter_map(|record| record.change.kind())
        .collect();
    assert_eq!(seen, EVERY_KIND.into_iter().collect::<HashSet<_>>());
    assert_positions_increase(&start, &records);
    for record in &records {
        let expected_actor = match &record.change {
            MetadataChange::ConsumerGroupPartitionHandedOver(_) => None,
            MetadataChange::SessionRegistered(session)
                if session.client_id != writer_session.client_id =>
            {
                continue;
            }
            MetadataChange::SessionClosed(closed)
                if closed.client_id != writer_session.client_id =>
            {
                continue;
            }
            _ => Some(ROOT_USER_ID),
        };
        assert_eq!(record.actor_user_id, expected_actor, "{record:?}");
    }

    let stream_created = only(&records, |change| match change {
        MetadataChange::StreamCreated(body) => Some(body.clone()),
        _ => None,
    });
    assert_eq!(stream_created.stream_id, stream.id);
    assert_eq!(stream_created.name, STREAM_NAME);
    assert_eq!(stream_created.created_at, stream.created_at.as_micros());
    assert_eq!(
        stream_created.options,
        MetadataOptions::Readable(stream.options.clone())
    );
    let stream_key = (stream_created.stream_id, stream_created.stream_created_op);
    let creation_op = records
        .iter()
        .find(|record| matches!(record.change, MetadataChange::StreamCreated(_)))
        .map(|record| record.op);
    assert_eq!(Some(stream_created.stream_created_op), creation_op);

    let renamed = only(&records, |change| match change {
        MetadataChange::StreamUpdated(body) => Some(body.clone()),
        _ => None,
    });
    assert_eq!((renamed.stream_id, renamed.stream_created_op), stream_key);
    assert_eq!(renamed.name, RENAMED_STREAM_NAME);
    let deleted = only(&records, |change| match change {
        MetadataChange::StreamDeleted(body) => Some(body.clone()),
        _ => None,
    });
    assert_eq!((deleted.stream_id, deleted.stream_created_op), stream_key);
    assert_eq!(deleted.name, RENAMED_STREAM_NAME);
    let deletion_op = records
        .iter()
        .find(|record| matches!(record.change, MetadataChange::StreamDeleted(_)))
        .map(|record| record.op)
        .expect("deletion record");
    // The stream held partitions, so the delete finished at a later completion.
    assert!(
        deleted
            .requested_op
            .is_some_and(|requested| requested < deletion_op),
        "{deleted:?}"
    );

    let topics_created: Vec<_> = records
        .iter()
        .filter_map(|record| match &record.change {
            MetadataChange::TopicCreated(body) => Some(body),
            _ => None,
        })
        .collect();
    assert_eq!(topics_created.len(), 2);
    assert_eq!(topics_created[0].topic_id, topic.id);
    assert_eq!(topics_created[0].name, TOPIC_NAME);
    assert_eq!(topics_created[0].partitions_count, 2);
    assert_eq!(
        (
            topics_created[0].stream_id,
            topics_created[0].stream_created_op
        ),
        stream_key
    );
    let topic_updated = only(&records, |change| match change {
        MetadataChange::TopicUpdated(body) => Some(body.clone()),
        _ => None,
    });
    assert_eq!(topic_updated.topic_id, topic.id);
    assert_eq!(
        topic_updated.topic_created_op,
        topics_created[0].topic_created_op
    );

    let partitions: Vec<_> = records
        .iter()
        .filter_map(|record| match &record.change {
            MetadataChange::PartitionsChanged(body) => {
                Some((body.partitions_count, body.removed_partition_ids.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(partitions, vec![(3, vec![]), (2, vec![2])]);

    let member = &assigned.members[0];
    let joined = only(&records, |change| match change {
        MetadataChange::ConsumerGroupMemberJoined(body) => Some(body.clone()),
        _ => None,
    });
    assert_eq!(joined.member_id, member.id);
    assert_eq!(joined.client_id, writer_session.client_id);
    assert_eq!(joined.members_count, 1);
    let mut installed: BTreeSet<u32> = joined.partition_ids.iter().copied().collect();
    installed.extend(records.iter().filter_map(|record| match &record.change {
        MetadataChange::ConsumerGroupPartitionHandedOver(body)
            if body.member_id == Some(member.id) =>
        {
            Some(body.partition_id)
        }
        _ => None,
    }));
    assert_eq!(installed, member.partitions.iter().copied().collect());
    let left = only(&records, |change| match change {
        MetadataChange::ConsumerGroupMemberLeft(body) => Some(body.clone()),
        _ => None,
    });
    assert_eq!(left.member_id, member.id);
    assert_eq!(left.members_count, 0);
    assert_eq!(left.reason, MetadataMemberLeftReason::Left);

    let user_created = only(&records, |change| match change {
        MetadataChange::UserCreated(body) => Some(body.clone()),
        _ => None,
    });
    assert_eq!(user_created.user_id, user.id);
    assert_eq!(user_created.username, USERNAME);
    assert_eq!(user_created.user_created_at, user.created_at.as_micros());
    assert_eq!(user_created.permissions, user.permissions);
    let user_updated = only(&records, |change| match change {
        MetadataChange::UserUpdated(body) => Some(body.clone()),
        _ => None,
    });
    assert_eq!(user_updated.username, RENAMED_USERNAME);
    let permission_updates: Vec<_> = records
        .iter()
        .filter_map(|record| match &record.change {
            MetadataChange::UserPermissionsUpdated(body) => {
                Some((body.reason, body.permissions.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        permission_updates,
        vec![
            (
                MetadataPermissionsUpdatedReason::Updated,
                Some(stream_grant(stream.id, true))
            ),
            (
                MetadataPermissionsUpdatedReason::ResourceDeleted,
                pruned.permissions
            ),
        ]
    );
    let token_deleted = only(&records, |change| match change {
        MetadataChange::PersonalAccessTokenDeleted(body) => Some(body.clone()),
        _ => None,
    });
    assert_eq!(token_deleted.user_id, ROOT_USER_ID);
    assert_eq!(token_deleted.name, TOKEN_NAME);
    assert_eq!(token_deleted.reason, MetadataTokenDeletedReason::Deleted);

    let registered = only(&records, |change| match change {
        MetadataChange::SessionRegistered(body) if body.client_id == writer_session.client_id => {
            Some(body.clone())
        }
        _ => None,
    });
    assert_eq!(registered.user_id, ROOT_USER_ID);
    assert_eq!(registered.session, writer_session.session);
    let closed = only(&records, |change| match change {
        MetadataChange::SessionClosed(body) if body.client_id == writer_session.client_id => {
            Some(body.clone())
        }
        _ => None,
    });
    assert_eq!(closed.session, writer_session.session);
    assert_eq!(closed.reason, MetadataSessionClosedReason::Logout);

    let next = records
        .last()
        .map(|record| cursor_of(start.log_id, record))
        .expect("records");
    let caught_up = reader
        .get_metadata_changes(&after(next, &[]))
        .await
        .expect("caught up page");
    assert_eq!(caught_up.status, MetadataChangeStatus::Ok);
    assert!(caught_up.records.is_empty());
    assert!(!caught_up.has_more);
    assert_eq!(caught_up.next, Some(next));
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn given_mixed_changes_when_filtering_should_return_exactly_the_matching_records(
    harness: &TestHarness,
) {
    let reader = harness.root_client().await.expect("reader");
    let writer = harness.tcp_root_client().await.expect("writer");
    seed_catalog(&writer).await;
    let everything = earliest(&reader, &[], None, None, MAX_METADATA_CHANGES_LIMIT).await;
    let present: BTreeSet<MetadataChangeGroup> = everything
        .iter()
        .filter_map(|record| record.change.group())
        .collect();
    assert_eq!(
        present,
        BTreeSet::from([
            MetadataChangeGroup::Streams,
            MetadataChangeGroup::Topics,
            MetadataChangeGroup::ConsumerGroups,
            MetadataChangeGroup::Users,
            MetadataChangeGroup::Sessions,
        ])
    );

    for group in MetadataChangeGroups::all().iter() {
        let filtered = earliest(&reader, &[group], None, None, MAX_METADATA_CHANGES_LIMIT).await;
        assert_eq!(filtered, matching(&everything, &[group]), "{group}");
    }
    let across_classes = [MetadataChangeGroup::Topics, MetadataChangeGroup::Sessions];
    let filtered = earliest(
        &reader,
        &across_classes,
        None,
        None,
        MAX_METADATA_CHANGES_LIMIT,
    )
    .await;
    assert!(!filtered.is_empty());
    assert_eq!(filtered, matching(&everything, &across_classes));
    let duplicated = earliest(
        &reader,
        &[MetadataChangeGroup::Users, MetadataChangeGroup::Users],
        None,
        None,
        MAX_METADATA_CHANGES_LIMIT,
    )
    .await;
    assert_eq!(
        duplicated,
        matching(&everything, &[MetadataChangeGroup::Users])
    );

    let one_by_one = earliest(&reader, &[], None, None, 1).await;
    assert_eq!(one_by_one, everything);

    let middle = everything.len() / 2;
    let since = everything[middle / 2].timestamp;
    let until = everything[middle].timestamp;
    let ranged = earliest(
        &reader,
        &[],
        Some(since.into()),
        Some(until.into()),
        MAX_METADATA_CHANGES_LIMIT,
    )
    .await;
    let expected: Vec<_> = everything
        .iter()
        .filter(|record| (since..=until).contains(&record.timestamp))
        .cloned()
        .collect();
    assert!(!expected.is_empty());
    assert_eq!(ranged, expected);

    let future = everything.last().expect("records").timestamp + 3_600_000_000;
    let empty = reader
        .get_metadata_changes(&MetadataChangesQuery {
            since: Some(future.into()),
            ..MetadataChangesQuery::default()
        })
        .await
        .expect("empty filtered page");
    assert_eq!(empty.status, MetadataChangeStatus::Ok);
    assert!(empty.records.is_empty());
    let continued = reader
        .get_metadata_changes(&after(empty.next.expect("continuation"), &[]))
        .await
        .expect("continue after an empty page");
    assert_eq!(continued.status, MetadataChangeStatus::Ok);

    for query in refused_queries() {
        let error = reader
            .get_metadata_changes(&query)
            .await
            .expect_err("the SDK refuses before sending");
        assert_eq!(
            error.as_code(),
            IggyError::InvalidCommand.as_code(),
            "{query:?}"
        );
        assert_refused_by_server(&reader, &query).await;
    }
    assert_raw_refusals(&reader).await;
}

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn given_foreign_future_or_mismatched_cursor_when_reading_should_answer_reset_or_behind(
    harness: &TestHarness,
) {
    let reader = harness.root_client().await.expect("reader");
    let writer = harness.tcp_root_client().await.expect("writer");
    seed_catalog(&writer).await;
    let head = reader
        .get_metadata_changes(&MetadataChangesQuery {
            start: MetadataChangeStart::Latest,
            ..MetadataChangesQuery::default()
        })
        .await
        .expect("latest");
    let cursor = head.next.expect("latest cursor");
    assert_ne!(cursor.op, 0);

    for (cursor, expected) in [
        (
            MetadataChangeCursor {
                log_id: cursor.log_id ^ 1,
                ..cursor
            },
            MetadataChangeStatus::Reset,
        ),
        (
            MetadataChangeCursor {
                checksum: cursor.checksum ^ 1,
                ..cursor
            },
            MetadataChangeStatus::Reset,
        ),
        (
            MetadataChangeCursor {
                op: head.head_op + 1_000,
                ..cursor
            },
            MetadataChangeStatus::Behind,
        ),
    ] {
        let answer = reader
            .get_metadata_changes(&after(cursor, &[]))
            .await
            .expect("position answer");
        assert_eq!(answer.status, expected, "{cursor:?}");
        assert!(answer.records.is_empty());
        let echoed = (expected == MetadataChangeStatus::Behind).then_some(cursor);
        assert_eq!(answer.next, echoed, "{cursor:?}");
        assert_eq!(answer.log_id, head.log_id);
    }

    let from_zero = reader
        .get_metadata_changes(&after(MetadataChangeCursor::zero(cursor.log_id), &[]))
        .await
        .expect("zero cursor");
    let from_earliest = reader
        .get_metadata_changes(&MetadataChangesQuery::default())
        .await
        .expect("earliest");
    assert_eq!(from_zero.status, MetadataChangeStatus::Ok);
    assert_eq!(from_zero.records, from_earliest.records);
    assert_eq!(from_zero.records[0].op, 1);
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(metadata.change_log_resource_capacity = "4")
)]
async fn given_evicted_records_when_following_should_expire_and_rebuild_from_current_state(
    harness: &TestHarness,
) {
    let groups = [MetadataChangeGroup::Streams];
    let reader = harness.root_client().await.expect("reader");
    let writer = harness.tcp_root_client().await.expect("writer");
    let start = latest(&reader, &groups).await;
    for index in 0..6 {
        writer
            .create_stream(
                &format!("{STREAM_NAME}-{index}"),
                &StreamCreateOptions::default(),
            )
            .await
            .expect("create stream");
    }

    let expired = reader
        .get_metadata_changes(&after(start, &groups))
        .await
        .expect("expired answer");
    assert_eq!(expired.status, MetadataChangeStatus::Expired);
    assert!(expired.records.is_empty());
    assert!(expired.oldest_op > start.op);
    let retained = earliest(&reader, &groups, None, None, MAX_METADATA_CHANGES_LIMIT).await;
    assert!(!retained.is_empty() && retained.len() <= 4, "{retained:?}");
    assert!(retained.iter().all(|record| record.op >= expired.oldest_op));

    let (items, anchor) = current_state(&reader, &groups, 1).await;
    assert_eq!(stream_items(&items), listed_streams(&reader).await);

    writer
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .expect("create stream after the rebuild");
    let caught_up = follow_until(&reader, &anchor, &groups, |records| !records.is_empty()).await;
    assert_eq!(caught_up.len(), 1);
    let MetadataChange::StreamCreated(created) = &caught_up[0].change else {
        panic!("expected a stream record, got {caught_up:?}");
    };
    assert_eq!(created.name, STREAM_NAME);
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn given_catalog_when_paging_current_state_should_match_list_routes_for_every_closed_subset(
    harness: &TestHarness,
) {
    let reader = harness.root_client().await.expect("reader");
    let writer = harness.tcp_root_client().await.expect("writer");
    let writer_session = session_identity(&writer).await;
    let stream_id = seed_catalog(&writer).await;
    let topic_id = Identifier::named(TOPIC_NAME).expect("topic id");
    let group_id = Identifier::named(GROUP_NAME).expect("group id");
    writer
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("join group");
    let assigned = wait_for_consumer_group_assignment(
        &writer,
        &stream_id,
        &topic_id,
        &group_id,
        1,
        FOLLOW_TIMEOUT,
    )
    .await;
    writer
        .create_personal_access_token(TOKEN_NAME, PersonalAccessTokenExpiry::NeverExpire)
        .await
        .expect("create token");

    let (items, anchor) = current_state(&reader, &[], 1).await;
    let (bulk, bulk_anchor) = current_state(&reader, &[], MAX_METADATA_CHANGES_LIMIT).await;
    assert_eq!(items, bulk);
    assert_eq!(anchor, bulk_anchor);

    assert_eq!(stream_items(&items), listed_streams(&reader).await);
    let topics: BTreeMap<u32, (String, u32)> = writer
        .get_topics(&stream_id)
        .await
        .expect("topics")
        .into_iter()
        .map(|topic| (topic.id, (topic.name, topic.partitions_count)))
        .collect();
    let projected_topics: BTreeMap<u32, (String, u32)> = items
        .iter()
        .filter_map(|item| match item {
            MetadataCurrentItem::Topic(topic) => Some((topic.topic_id, topic.name.clone())),
            _ => None,
        })
        .map(|(id, name)| {
            let count = items
                .iter()
                .find_map(|item| match item {
                    MetadataCurrentItem::Partitions(partitions) if partitions.topic_id == id => {
                        Some(partitions.partitions_count)
                    }
                    _ => None,
                })
                .expect("partitions item");
            (id, (name, count))
        })
        .collect();
    assert_eq!(projected_topics, topics);
    let groups: Vec<(u32, String)> = items
        .iter()
        .filter_map(|item| match item {
            MetadataCurrentItem::ConsumerGroup(group) => Some((group.group_id, group.name.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(groups, vec![(assigned.id, assigned.name.clone())]);
    let members: Vec<(u32, u128, Vec<u32>)> = items
        .iter()
        .filter_map(|item| match item {
            MetadataCurrentItem::ConsumerGroupMember(member) => Some((
                member.member_id,
                member.client_id,
                member.partition_ids.clone(),
            )),
            _ => None,
        })
        .collect();
    let mut partitions = assigned.members[0].partitions.clone();
    partitions.sort_unstable();
    assert_eq!(
        members,
        vec![(assigned.members[0].id, writer_session.client_id, partitions)]
    );
    let users: BTreeMap<u32, (String, UserStatus)> = reader
        .get_users()
        .await
        .expect("users")
        .into_iter()
        .map(|user| (user.id, (user.username, user.status)))
        .collect();
    let projected_users: BTreeMap<u32, (String, UserStatus)> = items
        .iter()
        .filter_map(|item| match item {
            MetadataCurrentItem::User(user) => {
                Some((user.user_id, (user.username.clone(), user.status)))
            }
            _ => None,
        })
        .collect();
    assert_eq!(projected_users, users);
    let tokens: Vec<(u32, String)> = items
        .iter()
        .filter_map(|item| match item {
            MetadataCurrentItem::PersonalAccessToken(token) => {
                Some((token.user_id, token.name.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(tokens, vec![(ROOT_USER_ID, TOKEN_NAME.to_owned())]);
    assert!(items.iter().any(|item| matches!(
        item,
        MetadataCurrentItem::Session(session)
            if session.client_id == writer_session.client_id
                && session.session == writer_session.session
    )));

    for mask in 1..=u8::MAX {
        let requested: Vec<MetadataChangeGroup> = MetadataChangeGroups::all()
            .iter()
            .filter(|group| mask & WireMetadataChangeGroup::from(*group).mask_bit() != 0)
            .collect();
        let query = MetadataChangesQuery {
            start: MetadataChangeStart::Current(None),
            groups: requested.clone(),
            limit: MAX_METADATA_CHANGES_LIMIT,
            ..MetadataChangesQuery::default()
        };
        if MetadataChangeGroups::requested(&requested)
            .missing_parent()
            .is_some()
        {
            assert_refused_by_server(&reader, &query).await;
            continue;
        }
        let (subset, subset_anchor) =
            current_state(&reader, &requested, MAX_METADATA_CHANGES_LIMIT).await;
        let expected: Vec<_> = items
            .iter()
            .filter(|item| item.group().is_some_and(|group| requested.contains(&group)))
            .cloned()
            .collect();
        assert_eq!(subset, expected, "{requested:?}");
        assert_eq!(subset_anchor.log_id, anchor.log_id);
    }

    let first = reader
        .get_metadata_changes(&MetadataChangesQuery {
            start: MetadataChangeStart::Current(None),
            limit: 1,
            ..MetadataChangesQuery::default()
        })
        .await
        .expect("first current page");
    let token = first.current_page_token.expect("more pages");
    let mismatched = MetadataChangesQuery {
        start: MetadataChangeStart::Current(Some(token)),
        groups: vec![MetadataChangeGroup::Users],
        limit: 1,
        ..MetadataChangesQuery::default()
    };
    let error = reader
        .get_metadata_changes(&mismatched)
        .await
        .expect_err("a token is bound to its groups");
    assert_eq!(error.as_code(), IggyError::InvalidCommand.as_code());
    assert_refused_by_server(&reader, &mismatched).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn given_keyed_streams_when_rotating_server_key_should_publish_fingerprints_without_key_material(
    harness: &mut TestHarness,
) {
    let groups = [MetadataChangeGroup::Streams];
    let own_key = StreamEncryptionKey::try_from([7u8; 32].as_slice()).expect("stream key");
    let start = latest(&harness.root_client().await.expect("reader"), &groups).await;
    let writer = harness.tcp_root_client().await.expect("writer");
    let created = [
        writer
            .create_stream(
                OWN_KEY_STREAM,
                &StreamCreateOptions {
                    encryption_key: Some(own_key.clone()),
                    ..StreamCreateOptions::default()
                },
            )
            .await
            .expect("create own key stream"),
        writer
            .create_stream(GENERATED_KEY_STREAM, &StreamCreateOptions::default())
            .await
            .expect("create generated key stream"),
    ];
    drop(writer);

    rotate_server_key(harness).await;
    let reader = harness.root_client().await.expect("reader after restart");
    let mut rotated = vec![
        kek_fingerprint(SERVER_KEY),
        kek_fingerprint(ROTATED_SERVER_KEY),
    ];
    rotated.sort();
    let fetched = wait_for_kek_fingerprints(&reader, &rotated).await;

    let rotated_options: Vec<MetadataOptions> = fetched
        .iter()
        .map(|details| MetadataOptions::Readable(details.options.clone()))
        .collect();
    let records = follow_until(&reader, &start, &groups, |records| {
        rotated_options.iter().all(|options| {
            records.iter().any(|record| {
                matches!(&record.change, MetadataChange::StreamUpdated(body) if &body.options == options)
            })
        })
    })
    .await;
    let (items, _) = current_state(&reader, &groups, MAX_METADATA_CHANGES_LIMIT).await;
    for record in &records {
        match &record.change {
            MetadataChange::StreamCreated(_) => {
                assert_eq!(record.actor_user_id, Some(ROOT_USER_ID));
            }
            MetadataChange::StreamUpdated(_) => assert_eq!(record.actor_user_id, None),
            other => panic!("unexpected record {other:?}"),
        }
    }

    let mut published: Vec<&ResourceOptions> =
        created.iter().map(|stream| &stream.options).collect();
    published.extend(fetched.iter().map(|stream| &stream.options));
    for change in records.iter().map(|record| &record.change) {
        if let MetadataChange::StreamCreated(body) | MetadataChange::StreamUpdated(body) = change {
            let MetadataOptions::Readable(options) = &body.options else {
                panic!("stream options must stay readable: {body:?}");
            };
            published.push(options);
        }
    }
    for item in &items {
        let MetadataCurrentItem::Stream(stream) = item else {
            panic!("unexpected item {item:?}");
        };
        let MetadataOptions::Readable(options) = &stream.options else {
            panic!("stream options must stay readable: {stream:?}");
        };
        let listed = fetched
            .iter()
            .find(|details| details.id == stream.stream_id)
            .expect("listed stream");
        assert_eq!(options, &listed.options);
        published.push(options);
    }
    let own_key_text = [text::as_base64(own_key.as_bytes()), hex(own_key.as_bytes())];
    for options in published {
        for hidden in [
            OptionKey::EncryptionKey,
            OptionKey::EncryptionKeyWrapped,
            OptionKey::CriticalKeys,
        ] {
            assert!(!options.contains_key(&HeaderKey::from(hidden)), "{hidden}");
        }
        let rendered = format!("{options:?}");
        for key_text in &own_key_text {
            assert!(!rendered.contains(key_text.as_str()));
        }
    }

    let http = harness.http_root_client().await.expect("HTTP client");
    for path in [
        "/streams".to_owned(),
        format!("/streams/{OWN_KEY_STREAM}"),
        format!("/streams/{GENERATED_KEY_STREAM}"),
        "/metadata/changes?from=earliest&groups=streams".to_owned(),
        "/metadata/changes?from=current&groups=streams".to_owned(),
    ] {
        let body = http
            .send_http_request(HttpMethod::Get, &path, None)
            .await
            .expect("HTTP read");
        let body = String::from_utf8_lossy(&body);
        for key_text in &own_key_text {
            assert!(!body.contains(key_text.as_str()), "{path}");
        }
        for hidden in [
            OptionKey::EncryptionKey,
            OptionKey::EncryptionKeyWrapped,
            OptionKey::CriticalKeys,
        ] {
            let name: &'static str = hidden.into();
            assert!(!body.contains(&format!("\"{name}\"")), "{path}: {name}");
        }
    }
    let own = fetched
        .iter()
        .find(|details| details.name == OWN_KEY_STREAM)
        .expect("own key stream");
    assert_eq!(
        own.encryption_key_fingerprint(),
        Some(own_key.fingerprint())
    );
    assert_eq!(
        own.encryption_key_source(),
        Some(StreamEncryptionSource::Stream)
    );
    let generated = fetched
        .iter()
        .find(|details| details.name == GENERATED_KEY_STREAM)
        .expect("generated key stream");
    assert_eq!(
        generated.encryption_key_source(),
        Some(StreamEncryptionSource::Server)
    );
}

/// Restart every node in turn under the rotated key, waiting for each to serve
/// before the next goes down.
async fn rotate_server_key(harness: &mut TestHarness) {
    let stream_id = Identifier::named(OWN_KEY_STREAM).expect("stream id");
    for node in 0..harness.cluster_size() {
        let server = harness.node_mut(node);
        server.add_env("IGGY_ENCRYPTION_KEY", ROTATED_SERVER_KEY);
        server.add_env(
            "IGGY_ENCRYPTION_PREVIOUS_KEYS",
            format!("[\"{SERVER_KEY}\"]"),
        );
        harness.restart_node(node).expect("rolling restart");
        let deadline = Instant::now() + FOLLOW_TIMEOUT;
        loop {
            if let Ok(client) = harness.root_client_for_node(node).await
                && matches!(client.get_stream(&stream_id).await, Ok(Some(_)))
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node {node} never served after its restart"
            );
            sleep(POLL_INTERVAL).await;
        }
    }
}

/// Streams, topics, a group and a user: records in every resource group.
async fn seed_catalog(writer: &IggyClient) -> Identifier {
    let stream = writer
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    let stream_id = Identifier::numeric(stream.id).expect("stream id");
    writer
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(3),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    writer
        .create_topic(
            &stream_id,
            DROPPED_TOPIC_NAME,
            &TopicCreateOptions::default(),
        )
        .await
        .expect("create second topic");
    writer
        .create_consumer_group(
            &stream_id,
            &Identifier::named(TOPIC_NAME).expect("topic id"),
            GROUP_NAME,
        )
        .await
        .expect("create group");
    writer
        .create_stream(RENAMED_STREAM_NAME, &StreamCreateOptions::default())
        .await
        .expect("create second stream");
    writer
        .create_user(
            USERNAME,
            PASSWORD,
            UserStatus::Active,
            Some(stream_grant(stream.id, false)),
            &UserCreateOptions::default(),
        )
        .await
        .expect("create user");
    stream_id
}

async fn latest(client: &IggyClient, groups: &[MetadataChangeGroup]) -> MetadataChangeCursor {
    let answer = client
        .get_metadata_changes(&MetadataChangesQuery {
            start: MetadataChangeStart::Latest,
            groups: groups.to_vec(),
            ..MetadataChangesQuery::default()
        })
        .await
        .expect("latest");
    assert_eq!(answer.status, MetadataChangeStatus::Ok);
    assert!(answer.records.is_empty());
    answer.next.expect("latest cursor")
}

fn after(cursor: MetadataChangeCursor, groups: &[MetadataChangeGroup]) -> MetadataChangesQuery {
    MetadataChangesQuery {
        start: MetadataChangeStart::After(cursor),
        groups: groups.to_vec(),
        limit: MAX_METADATA_CHANGES_LIMIT,
        ..MetadataChangesQuery::default()
    }
}

/// Every retained record of `groups` in the time range, paged by `limit`.
async fn earliest(
    client: &IggyClient,
    groups: &[MetadataChangeGroup],
    since: Option<IggyTimestamp>,
    until: Option<IggyTimestamp>,
    limit: u32,
) -> Vec<MetadataChangeRecord> {
    let mut query = MetadataChangesQuery {
        start: MetadataChangeStart::Earliest,
        groups: groups.to_vec(),
        since,
        until,
        limit,
    };
    let mut records = Vec::new();
    loop {
        let page = client
            .get_metadata_changes(&query)
            .await
            .expect("change page");
        assert_eq!(page.status, MetadataChangeStatus::Ok);
        assert!(page.records.len() <= limit as usize);
        records.extend(page.records);
        if !page.has_more {
            return records;
        }
        query.start = MetadataChangeStart::After(page.next.expect("continuation"));
    }
}

/// Follow from `start` until `done` holds for everything read so far.
async fn follow_until(
    client: &IggyClient,
    start: &MetadataChangeCursor,
    groups: &[MetadataChangeGroup],
    done: impl Fn(&[MetadataChangeRecord]) -> bool,
) -> Vec<MetadataChangeRecord> {
    let deadline = Instant::now() + FOLLOW_TIMEOUT;
    let mut cursor = *start;
    let mut records = Vec::new();
    loop {
        let page = client
            .get_metadata_changes(&after(cursor, groups))
            .await
            .expect("change page");
        assert_eq!(page.status, MetadataChangeStatus::Ok, "{page:?}");
        cursor = page.next.expect("continuation");
        records.extend(page.records);
        if page.has_more {
            continue;
        }
        if done(&records) {
            return records;
        }
        assert!(
            Instant::now() < deadline,
            "the followed records never reached the expected state: {records:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Every current state page of `groups`, checking the anchor stays fixed.
async fn current_state(
    client: &IggyClient,
    groups: &[MetadataChangeGroup],
    limit: u32,
) -> (Vec<MetadataCurrentItem>, MetadataChangeCursor) {
    let mut query = MetadataChangesQuery {
        start: MetadataChangeStart::Current(None),
        groups: groups.to_vec(),
        limit,
        ..MetadataChangesQuery::default()
    };
    let mut items = Vec::new();
    let mut anchor = None;
    loop {
        let page = client
            .get_metadata_changes(&query)
            .await
            .expect("current page");
        assert_eq!(page.status, MetadataChangeStatus::Ok, "{page:?}");
        assert_eq!(page.phase, MetadataChangePhase::Current);
        assert!(page.records.is_empty());
        assert!(page.items.len() <= limit as usize);
        let next = page.next.expect("anchor");
        assert_eq!(*anchor.get_or_insert(next), next);
        items.extend(page.items);
        match page.current_page_token {
            Some(token) => query.start = MetadataChangeStart::Current(Some(token)),
            None => return (items, next),
        }
    }
}

fn matching(
    records: &[MetadataChangeRecord],
    groups: &[MetadataChangeGroup],
) -> Vec<MetadataChangeRecord> {
    records
        .iter()
        .filter(|record| {
            record
                .change
                .group()
                .is_some_and(|group| groups.contains(&group))
        })
        .cloned()
        .collect()
}

fn only<T>(records: &[MetadataChangeRecord], select: impl Fn(&MetadataChange) -> Option<T>) -> T {
    let mut found: Vec<T> = records
        .iter()
        .filter_map(|record| select(&record.change))
        .collect();
    assert_eq!(found.len(), 1, "expected exactly one matching record");
    found.remove(0)
}

const fn cursor_of(log_id: u128, record: &MetadataChangeRecord) -> MetadataChangeCursor {
    MetadataChangeCursor {
        log_id,
        op: record.op,
        checksum: record.checksum,
        ordinal: record.ordinal,
    }
}

fn assert_positions_increase(start: &MetadataChangeCursor, records: &[MetadataChangeRecord]) {
    let mut previous = (start.op, start.ordinal);
    for record in records {
        let position = (record.op, record.ordinal);
        assert!(position > previous, "{previous:?} then {position:?}");
        previous = position;
    }
}

fn stream_items(items: &[MetadataCurrentItem]) -> BTreeMap<u32, (String, u64, ResourceOptions)> {
    items
        .iter()
        .filter_map(|item| match item {
            MetadataCurrentItem::Stream(stream) => match &stream.options {
                MetadataOptions::Readable(options) => Some((
                    stream.stream_id,
                    (stream.name.clone(), stream.created_at, options.clone()),
                )),
                MetadataOptions::Unreadable => panic!("unreadable stream {stream:?}"),
            },
            _ => None,
        })
        .collect()
}

async fn listed_streams(client: &IggyClient) -> BTreeMap<u32, (String, u64, ResourceOptions)> {
    client
        .get_streams()
        .await
        .expect("streams")
        .into_iter()
        .map(|stream| {
            (
                stream.id,
                (stream.name, stream.created_at.as_micros(), stream.options),
            )
        })
        .collect()
}

fn refused_queries() -> Vec<MetadataChangesQuery> {
    let cursor = MetadataChangeCursor {
        log_id: 7,
        op: 0,
        checksum: 1,
        ordinal: 0,
    };
    vec![
        MetadataChangesQuery {
            limit: 0,
            ..MetadataChangesQuery::default()
        },
        MetadataChangesQuery {
            limit: MAX_METADATA_CHANGES_LIMIT + 1,
            ..MetadataChangesQuery::default()
        },
        MetadataChangesQuery {
            since: Some(IggyTimestamp::from(5)),
            until: Some(IggyTimestamp::from(4)),
            ..MetadataChangesQuery::default()
        },
        MetadataChangesQuery {
            start: MetadataChangeStart::Latest,
            since: Some(IggyTimestamp::from(1)),
            ..MetadataChangesQuery::default()
        },
        MetadataChangesQuery {
            start: MetadataChangeStart::Current(None),
            until: Some(IggyTimestamp::from(1)),
            ..MetadataChangesQuery::default()
        },
        MetadataChangesQuery {
            start: MetadataChangeStart::Current(None),
            groups: vec![MetadataChangeGroup::Topics],
            ..MetadataChangesQuery::default()
        },
        MetadataChangesQuery {
            start: MetadataChangeStart::After(cursor),
            ..MetadataChangesQuery::default()
        },
    ]
}

/// Send `query` past the SDK's own checks and expect the server to refuse it.
async fn assert_refused_by_server(client: &IggyClient, query: &MetadataChangesQuery) {
    match client.get_connection_info().await.protocol {
        TransportProtocol::Http => {
            let pairs: Vec<String> = query
                .http_query_pairs()
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect();
            assert_http_refused(client, &pairs.join("&")).await;
        }
        _ => {
            assert_binary_refused(client, GetMetadataChangesRequest::from(query)).await;
        }
    }
}

/// Refusals only one transport can express: unknown group codes on the
/// binary request, malformed text on HTTP.
async fn assert_raw_refusals(client: &IggyClient) {
    match client.get_connection_info().await.protocol {
        TransportProtocol::Http => {
            for query in [
                "",
                "from=earliest&from=latest",
                "from=earliest&after=mc1_AAAA",
                "from=sometime",
                "from=earliest&unknown=1",
                "from=earliest&groups=streams,",
                "from=earliest&groups=queues",
                "from=earliest&since=01",
                "from=earliest&limit=ten",
                "after=mc1_not-a-cursor",
                "from=current&current_after=cs1_not-a-token",
            ] {
                assert_http_refused(client, query).await;
            }
        }
        _ => {
            let mut unknown_group =
                GetMetadataChangesRequest::from(&MetadataChangesQuery::default());
            unknown_group.groups.push(99);
            assert_binary_refused(client, unknown_group).await;
            let encoded =
                GetMetadataChangesRequest::from(&MetadataChangesQuery::default()).to_bytes();
            let error = client
                .send_binary_request(
                    GET_METADATA_CHANGES_CODE,
                    encoded.slice(..encoded.len() - 1),
                )
                .await
                .expect_err("a truncated body is refused");
            assert_eq!(error.as_code(), IggyError::InvalidCommand.as_code());
        }
    }
}

async fn assert_http_refused(client: &IggyClient, query: &str) {
    let error = client
        .send_http_request(HttpMethod::Get, &format!("/metadata/changes?{query}"), None)
        .await
        .expect_err(query);
    assert!(
        matches!(error, IggyError::HttpResponseError(400, _)),
        "{query}: {error:?}"
    );
}

async fn assert_binary_refused(client: &IggyClient, request: GetMetadataChangesRequest) {
    let error = client
        .send_binary_request(GET_METADATA_CHANGES_CODE, request.to_bytes())
        .await
        .expect_err("the server refuses the request");
    assert_eq!(
        error.as_code(),
        IggyError::InvalidCommand.as_code(),
        "{request:?}: {error:?}"
    );
}

async fn session_identity(client: &IggyClient) -> SessionIdentity {
    let handle = client.client();
    let transport = handle.read().await;
    let ClientWrapper::Tcp(tcp) = &*transport else {
        panic!("the writer uses the TCP transport");
    };
    tcp.session_identity().await.expect("bound session")
}

async fn wait_for_user(
    client: &IggyClient,
    user_id: &Identifier,
    done: impl Fn(&UserInfoDetails) -> bool,
) -> UserInfoDetails {
    let deadline = Instant::now() + FOLLOW_TIMEOUT;
    loop {
        let details = client
            .get_user(user_id)
            .await
            .expect("read user")
            .expect("user exists");
        if done(&details) {
            return details;
        }
        assert!(
            Instant::now() < deadline,
            "user never converged: {details:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_kek_fingerprints(client: &IggyClient, expected: &[String]) -> Vec<StreamDetails> {
    let deadline = Instant::now() + FOLLOW_TIMEOUT;
    loop {
        let mut fetched = Vec::with_capacity(2);
        for name in [OWN_KEY_STREAM, GENERATED_KEY_STREAM] {
            if let Ok(Some(details)) = client
                .get_stream(&Identifier::named(name).expect("stream id"))
                .await
            {
                fetched.push(details);
            }
        }
        if fetched.len() == 2
            && fetched
                .iter()
                .all(|details| details.encryption_kek_fingerprints() == expected)
        {
            return fetched;
        }
        assert!(
            Instant::now() < deadline,
            "streams never reported KEK fingerprints {expected:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

fn stream_grant(stream_id: u32, manage_stream: bool) -> Permissions {
    Permissions {
        global: GlobalPermissions::default(),
        streams: Some(BTreeMap::from([(
            stream_id as usize,
            StreamPermissions {
                manage_stream,
                read_stream: true,
                ..StreamPermissions::default()
            },
        )])),
    }
}

fn kek_fingerprint(key: &str) -> String {
    ServerKeyContext::from_config(key, &[], true)
        .expect("server key")
        .kek_fingerprint()
        .expect("fingerprint")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
