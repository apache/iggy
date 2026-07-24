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

use std::time::Duration;

use futures::StreamExt;
use iggy::prelude::*;
use integration::iggy_harness;
use tokio::time::timeout;

const STREAM_NAME: &str = "cg-membership-stream";
const TOPIC_NAME: &str = "cg-membership-topic";
const CONSUMER_GROUP_NAME: &str = "cg-membership-group";

// A member holding zero partitions polls empty forever, so a short wait is
// enough to let it sync (registering its membership) and then park.
const PARK_TIMEOUT: Duration = Duration::from_secs(2);
// Generous bound: on regression the poll hangs until this elapses.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(10);

// Legacy wire error codes for the join/leave failure ladder. Pinned as literals
// (not derived from `IggyError`) so renumbering the wire contract fails here
// loudly -- the same guarantee the non-Rust SDKs depend on.
const STREAM_ID_NOT_FOUND: u32 = 1009;
const TOPIC_ID_NOT_FOUND: u32 = 2010;
const CONSUMER_GROUP_ID_NOT_FOUND: u32 = 5000;
const CONSUMER_GROUP_MEMBER_NOT_FOUND: u32 = 5006;

// A consumer-group member holding zero partitions has the same empty client-side
// assignment as a non-member; only membership tells them apart. When the group
// is deleted under such a member, the poll must surface an error (driving a
// rejoin) rather than treating the empty assignment as "nothing to poll" and
// hanging forever.
#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_group_member_holds_no_partitions_when_group_deleted_should_surface_error_not_hang(
    harness: &TestHarness,
) {
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();

    // One connection administers the group and is the member that will hold the
    // single partition; the other backs the consumer under test.
    let mut clients = harness
        .root_clients(2)
        .await
        .expect("Failed to create root clients");
    let admin = clients.remove(0);
    let consumer_client = clients.remove(0);

    admin.create_stream(STREAM_NAME).await.unwrap();
    admin
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    admin
        .create_consumer_group(&stream_id, &topic_id, CONSUMER_GROUP_NAME)
        .await
        .unwrap();

    // The first member to join keeps the topic's only partition, leaving the
    // second member (the consumer under test) with none.
    admin
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();

    let mut consumer = consumer_client
        .consumer_group(CONSUMER_GROUP_NAME, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .batch_length(1)
        .poll_interval(IggyDuration::new(Duration::from_millis(100)))
        .auto_join_consumer_group()
        .do_not_create_consumer_group_if_not_exists()
        .build();
    consumer.init().await.unwrap();

    let group = admin
        .get_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap()
        .expect("Consumer group should exist");
    assert_eq!(group.members_count, 2);
    let partition_counts: Vec<u32> = group
        .members
        .iter()
        .map(|member| member.partitions_count)
        .collect();
    assert!(
        partition_counts.contains(&0) && partition_counts.contains(&1),
        "expected one member to hold the single partition and one to hold none, got {partition_counts:?}"
    );

    // Alive group: a zero-partition member is a legitimate member, so its poll
    // parks (yields nothing) instead of surfacing an error or churning.
    match timeout(PARK_TIMEOUT, consumer.next()).await {
        Err(_elapsed) => {}
        Ok(Some(Ok(_))) => {
            panic!("a zero-partition member must not receive a message while its group is alive")
        }
        Ok(Some(Err(error))) => panic!(
            "a zero-partition member must not surface an error while its group is alive, got {error:?}"
        ),
        Ok(None) => panic!("consumer stream closed unexpectedly while the group is alive"),
    }

    admin
        .delete_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();

    // Deleted group: the member is no longer registered, so the poll must fail
    // fast (the rejoin surfaces the missing group) rather than hang.
    let item = timeout(RESOLVE_TIMEOUT, consumer.next())
        .await
        .expect("poll must not hang after the group is deleted")
        .expect("consumer stream should remain open");
    match item {
        Err(IggyError::ConsumerGroupNameNotFound(..)) => {}
        Err(other) => {
            panic!("expected ConsumerGroupNameNotFound after group deletion, got error {other:?}")
        }
        Ok(_) => panic!("expected an error after group deletion, got a message"),
    }
}

// End-to-end wire pin for the consumer-group join/leave error ladder. The
// metadata STM unit tests pin the committed result codes; this pins that
// server-ng actually emits them over the wire, so a client observes the same
// codes the legacy server returns. The full ladder runs per binary transport,
// since the error response is encoded per transport.
#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_join_and_leave_failures_when_sent_over_the_wire_should_return_legacy_error_codes(
    harness: &TestHarness,
) {
    let root_client = harness.root_client().await.expect("root client");
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();
    // Stands in for whichever level is absent; its position in the call marks
    // the level under test.
    let missing = Identifier::named("cg-membership-nonexistent").unwrap();

    // Each level is created only after the case proving its absence is
    // rejected, so no assertion resolves against pre-existing state.

    // Nothing exists yet: the stream miss is the first rejection.
    assert_rejected(
        root_client
            .join_consumer_group(&missing, &topic_id, &group_id)
            .await,
        STREAM_ID_NOT_FOUND,
        "join with a missing stream",
    );

    root_client.create_stream(STREAM_NAME).await.unwrap();
    assert_rejected(
        root_client
            .join_consumer_group(&stream_id, &missing, &group_id)
            .await,
        TOPIC_ID_NOT_FOUND,
        "join with a missing topic",
    );

    root_client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    assert_rejected(
        root_client
            .join_consumer_group(&stream_id, &topic_id, &missing)
            .await,
        CONSUMER_GROUP_ID_NOT_FOUND,
        "join with a missing group",
    );

    root_client
        .create_consumer_group(&stream_id, &topic_id, CONSUMER_GROUP_NAME)
        .await
        .unwrap();
    // The group resolves now, but this client never joined it: the member check
    // is the loud rejection, distinct from the missing-group case above.
    assert_rejected(
        root_client
            .leave_consumer_group(&stream_id, &topic_id, &group_id)
            .await,
        CONSUMER_GROUP_MEMBER_NOT_FOUND,
        "leave a group the client never joined",
    );

    // Non-vacuous control: the same client joins the group whose leave just
    // returned member-not-found, proving the setup is live.
    root_client
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("join on an existing group must succeed");
}

fn assert_rejected(result: Result<(), IggyError>, expected_code: u32, context: &str) {
    let error = result.expect_err(context);
    assert_eq!(
        error.as_code(),
        expected_code,
        "{context}: expected wire error code {expected_code}, got {} ({error})",
        error.as_code(),
    );
}
