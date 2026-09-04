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

use crate::server::raw_tcp;
use iggy::prelude::*;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::consensus::Operation;
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::{AckLevel, WireConsumer, WireIdentifier};
use integration::harness::{TestHarness, disk};
use integration::iggy_harness;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const STREAM_NAME: &str = "cluster-offset-quota-stream";
const TOPIC_NAME: &str = "cluster-offset-quota-topic";
const PARTITION_ID: u32 = 0;
const RAW_CLIENT_ID: u128 = 0xC0FF_EE02;
const WAIT: Duration = Duration::from_secs(20);

#[iggy_harness(
    cluster_nodes = 3,
    server(partition.consumer_offsets_max = "4")
)]
async fn given_replicated_partition_when_no_ack_offsets_mutate_should_converge_and_stay_bounded(
    harness: &mut TestHarness,
) {
    let leader = disk::leader_node_index(harness).await;
    let client = harness
        .root_client_for_node(leader)
        .await
        .expect("root client");
    let stream = Identifier::named(STREAM_NAME).expect("stream identifier");
    let topic = Identifier::named(TOPIC_NAME).expect("topic identifier");
    let stream_details = client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    let topic_details = client
        .create_topic(
            &stream,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    let mut messages = vec![
        IggyMessage::builder()
            .payload("cluster-offset-quota".into())
            .build()
            .expect("build message"),
    ];
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("seed non-empty partition");

    let address = harness.node(leader).tcp_addr().expect("leader TCP address");
    let mut raw = raw_tcp::connect_to(address).await;
    let session = raw_tcp::register_root(&mut raw, RAW_CLIENT_ID).await;
    let wire_stream = WireIdentifier::Numeric(stream_details.id);
    let wire_topic = WireIdentifier::Numeric(topic_details.id);
    let raw_consumer_id = 41;

    let store = StoreConsumerOffsetRequest {
        consumer: WireConsumer::consumer(WireIdentifier::Numeric(raw_consumer_id)),
        stream_id: wire_stream.clone(),
        topic_id: wire_topic.clone(),
        partition_id: Some(PARTITION_ID),
        offset: 0,
        ack: AckLevel::NoAck,
    }
    .to_bytes();
    let store_header = raw_tcp::request_header(
        Operation::StoreConsumerOffset,
        RAW_CLIENT_ID,
        session,
        1,
        store.len(),
    );
    let (reply, _) = raw_tcp::exchange(&mut raw, &store_header, &store).await;
    assert_eq!(raw_tcp::reply_status(&reply), 0);
    wait_for_file_state(
        harness,
        stream_details.id,
        topic_details.id,
        raw_consumer_id,
        true,
    )
    .await;

    let delete = DeleteConsumerOffsetRequest {
        consumer: WireConsumer::consumer(WireIdentifier::Numeric(raw_consumer_id)),
        stream_id: wire_stream,
        topic_id: wire_topic,
        partition_id: Some(PARTITION_ID),
        ack: AckLevel::NoAck,
    }
    .to_bytes();
    let delete_header = raw_tcp::request_header(
        Operation::DeleteConsumerOffset,
        RAW_CLIENT_ID,
        session,
        2,
        delete.len(),
    );
    let (reply, _) = raw_tcp::exchange(&mut raw, &delete_header, &delete).await;
    assert_eq!(raw_tcp::reply_status(&reply), 0);
    wait_for_file_state(
        harness,
        stream_details.id,
        topic_details.id,
        raw_consumer_id,
        false,
    )
    .await;

    for consumer_id in 1..=4 {
        client
            .store_consumer_offset(
                &Consumer::new(Identifier::numeric(consumer_id).expect("consumer identifier")),
                &stream,
                &topic,
                Some(PARTITION_ID),
                0,
            )
            .await
            .expect("store offset within limit");
    }
    let rejected = client
        .store_consumer_offset(
            &Consumer::new(Identifier::numeric(5).expect("consumer identifier")),
            &stream,
            &topic,
            Some(PARTITION_ID),
            0,
        )
        .await;
    assert!(matches!(rejected, Err(IggyError::TooManyConsumerOffsets)));
    wait_for_max_file_count(harness, stream_details.id, topic_details.id, 4).await;

    let backup = (0..harness.cluster_size())
        .find(|node| *node != leader)
        .expect("backup replica");
    harness
        .kill_node(backup)
        .expect("pause one replica during group churn");
    for generation in 0..6 {
        let name = format!("quota-group-{generation}");
        client
            .create_consumer_group(&stream, &topic, &name)
            .await
            .expect("create group");
        let group = Identifier::named(&name).expect("group identifier");
        client
            .join_consumer_group(&stream, &topic, &group)
            .await
            .expect("join group");
        client
            .store_consumer_offset(
                &Consumer::group(group.clone()),
                &stream,
                &topic,
                Some(PARTITION_ID),
                0,
            )
            .await
            .expect("store valid group offset");
        client
            .delete_consumer_group(&stream, &topic, &group)
            .await
            .expect("delete group metadata");
        let deadline = Instant::now() + WAIT;
        loop {
            let ids = disk::consumer_offset_file_ids(
                &harness.node(leader).data_path(),
                stream_details.id,
                topic_details.id,
                PARTITION_ID,
                ConsumerKind::ConsumerGroup,
            )
            .expect("group offset directory");
            if ids.is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "replicated cleanup did not delete {ids:?}"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }
    harness
        .restart_node(backup)
        .expect("restart lagging replica");
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("produce after rejoin");
    client
        .store_consumer_offset(
            &Consumer::new(Identifier::numeric(1).unwrap()),
            &stream,
            &topic,
            Some(PARTITION_ID),
            1,
        )
        .await
        .expect("store convergence marker");
    let deadline = Instant::now() + WAIT;
    loop {
        let caught_up = (0..harness.cluster_size()).all(|node| {
            std::fs::read(offset_file(
                harness,
                node,
                stream_details.id,
                topic_details.id,
                1,
            ))
            .ok()
            .and_then(|bytes| bytes.first_chunk::<8>().copied())
            .map(u64::from_le_bytes)
                == Some(1)
        });
        if caught_up {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replica never applied the post-cleanup marker"
        );
        sleep(Duration::from_millis(50)).await;
    }
    for node in 0..harness.cluster_size() {
        assert!(
            disk::consumer_offset_file_ids(
                &harness.node(node).data_path(),
                stream_details.id,
                topic_details.id,
                PARTITION_ID,
                ConsumerKind::ConsumerGroup
            )
            .expect("rejoined group directory")
            .is_empty(),
            "node {node} retained deleted group generations"
        );
    }
}

async fn wait_for_file_state(
    harness: &TestHarness,
    stream_id: u32,
    topic_id: u32,
    consumer_id: u32,
    expected: bool,
) {
    let deadline = Instant::now() + WAIT;
    loop {
        let states: Vec<bool> = (0..harness.cluster_size())
            .map(|node| offset_file(harness, node, stream_id, topic_id, consumer_id).exists())
            .collect();
        if states.iter().all(|state| *state == expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "consumer offset file state did not converge to {expected}: {states:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_max_file_count(harness: &TestHarness, stream_id: u32, topic_id: u32, max: usize) {
    let deadline = Instant::now() + WAIT;
    loop {
        let counts: Vec<usize> = (0..harness.cluster_size())
            .map(|node| {
                let dir = offset_file(harness, node, stream_id, topic_id, 0)
                    .parent()
                    .expect("offset file has parent")
                    .to_path_buf();
                std::fs::read_dir(dir)
                    .map(|entries| {
                        entries
                            .filter_map(Result::ok)
                            .filter(|entry| {
                                entry
                                    .file_name()
                                    .to_str()
                                    .is_some_and(|name| name.parse::<u32>().is_ok())
                            })
                            .count()
                    })
                    .unwrap_or_default()
            })
            .collect();
        if counts.iter().all(|count| *count == max) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "consumer offset files did not converge at {max}: {counts:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

fn offset_file(
    harness: &TestHarness,
    node: usize,
    stream_id: u32,
    topic_id: u32,
    consumer_id: u32,
) -> PathBuf {
    harness.node(node).data_path().join(format!(
        "streams/{stream_id}/topics/{topic_id}/partitions/{PARTITION_ID}/offsets/consumers/{consumer_id}"
    ))
}
