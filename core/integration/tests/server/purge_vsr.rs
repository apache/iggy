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

//! Server purge durability: the applied purge generation survives a
//! restart (`purge.gen`), and purged journal-resident batches stay fenced
//! behind the purge floor instead of resurfacing through the shutdown flush.
//! Plus read-your-purge: the counters a purge acks are visible to the very
//! next read, without waiting for the reconciler's on-disk reset.

use crate::server::scenarios::purge_delete_scenario;
use iggy::prelude::*;
use integration::iggy_harness;

// Single node: the tests reason about ONE replica's on-disk state across a
// restart (`purge.gen` hydration, shutdown-flush fencing); a cluster would
// route polls to whichever node leads after the restart. Default fsync
// config on purpose: the restart is graceful, so segment bytes survive
// without fsync, and `purge.gen` is unconditionally synced by the purge.
#[iggy_harness(
    cluster_nodes = 1,
    server(partition.messages_required_to_save = "1")
)]
async fn given_post_purge_messages_when_server_restarts_should_retain_them(
    harness: &mut TestHarness,
) {
    purge_delete_scenario::run_purge_survives_restart(harness).await;
}

// The huge threshold keeps every batch journal-resident: nothing is ever
// flushed before the purge, so the purged bytes exist ONLY as consensus
// history the shutdown flush re-walks.
#[iggy_harness(
    cluster_nodes = 1,
    server(partition.messages_required_to_save = "10000")
)]
async fn given_journal_resident_messages_when_purged_should_not_resurface(
    harness: &mut TestHarness,
) {
    purge_delete_scenario::run_resident_purge_no_resurface(harness).await;
}

// No sleep, no poll: the purge acks on commit and the segment prune runs later
// on the reconciler, so the reset of the counters `get_topic` / `get_stream`
// read has to happen in the replicated apply. A retry loop here would pass
// against the pre-apply behavior too.
#[iggy_harness(
    test_client_transport = [Tcp],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
)]
async fn given_purged_topic_when_getting_topic_immediately_should_report_zero_stats(
    harness: &TestHarness,
) {
    const STREAM: &str = "purge-stats-stream";
    const TOPIC: &str = "purge-stats-topic";

    let client = harness.tcp_root_client().await.expect("tcp root client");
    client.create_stream(STREAM).await.expect("create stream");
    let stream_id = Identifier::from_str_value(STREAM).expect("stream identifier");
    let topic_id = Identifier::from_str_value(TOPIC).expect("topic identifier");
    client
        .create_topic(
            &stream_id,
            TOPIC,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("create topic");

    let mut messages: Vec<IggyMessage> = (0..10)
        .map(|index| {
            IggyMessage::builder()
                .payload(format!("message-{index}").into())
                .build()
                .expect("build message")
        })
        .collect();
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let before = client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("get topic before purge")
        .expect("topic exists before purge");
    assert_eq!(
        before.messages_count, 10,
        "the send must be counted before the purge, or the assert below proves nothing"
    );
    assert!(before.size.as_bytes_u64() > 0);

    client
        .purge_topic(&stream_id, &topic_id)
        .await
        .expect("purge topic");

    let topic = client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("get topic after purge")
        .expect("purge keeps the topic");
    assert_eq!(
        topic.messages_count, 0,
        "a read right after the purge ack must not report pre-purge messages"
    );
    assert_eq!(topic.size.as_bytes_u64(), 0);
    assert_eq!(
        topic.partitions.len(),
        1,
        "purge keeps the partition, it only empties it"
    );
    assert_eq!(topic.partitions[0].messages_count, 0);
    assert_eq!(topic.partitions[0].size.as_bytes_u64(), 0);
    assert_eq!(topic.partitions[0].current_offset, 0);

    let stream = client
        .get_stream(&stream_id)
        .await
        .expect("get stream after purge")
        .expect("purge keeps the stream");
    assert_eq!(
        stream.messages_count, 0,
        "the stream rollup must drop with its purged topic"
    );
    assert_eq!(stream.size.as_bytes_u64(), 0);
}
