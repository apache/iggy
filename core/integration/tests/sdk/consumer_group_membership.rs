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
