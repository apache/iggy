/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::streaming::common::test_setup::TestSetup;
use bytes::Bytes;
use iggy::prelude::*;
use server::configs::system::SystemConfig;
use server::streaming::polling_consumer::PollingConsumer;
use server::streaming::segments::IggyMessagesBatchMut;
use server::streaming::topics::topic::Topic;
use server::streaming::utils::hash;
use std::collections::HashMap;
use std::str::from_utf8;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};

#[tokio::test]
async fn assert_polling_messages() {
    let messages_count = 1000;
    let payload_size_bytes = 1000;
    let config = SystemConfig {
        ..Default::default()
    };
    let setup = TestSetup::init_with_config(config).await;
    let topic = init_topic(&setup, 1).await;
    let partition_id = 1;
    let partitioning = Partitioning::partition_id(partition_id);
    let messages = (0..messages_count)
        .map(|id| {
            IggyMessage::builder()
                .id(id as u128)
                .payload(Bytes::from(format!(
                    "{}:{}",
                    id + 1,
                    create_payload(payload_size_bytes)
                )))
                .build()
                .expect("Failed to create message")
        })
        .collect::<Vec<_>>();
    let mut sent_messages = Vec::new();
    for message in &messages {
        sent_messages.push(message);
    }
    let batch_size = messages
        .iter()
        .map(|m| m.get_size_bytes())
        .sum::<IggyByteSize>();
    let batch = IggyMessagesBatchMut::from_messages(&messages, batch_size.as_bytes_u32());
    topic.append_messages(&partitioning, batch).await.unwrap();

    let consumer = PollingConsumer::Consumer(1, partition_id);
    let (_, polled_messages) = topic
        .get_messages(
            consumer,
            partition_id,
            PollingStrategy::offset(0),
            messages_count,
        )
        .await
        .unwrap();
    assert_eq!(topic.get_messages_count(), messages_count as u64);
    assert_eq!(polled_messages.count(), messages_count);
    let mut cnt = 0;
    for batch in polled_messages.iter() {
        for polled_message in batch.iter() {
            let sent_message = sent_messages.get(cnt).unwrap();
            assert_eq!(sent_message.payload, polled_message.payload());
            let polled_payload_str = from_utf8(polled_message.payload()).unwrap();
            assert!(polled_payload_str.starts_with(&format!("{}:", cnt + 1)));
            cnt += 1;
        }
    }
}

#[tokio::test]
async fn given_key_none_messages_should_be_appended_to_the_next_partition_using_round_robin() {
    let setup = TestSetup::init().await;
    let partitions_count = 3;
    let messages_per_partition_count = 10;
    let topic = init_topic(&setup, partitions_count).await;
    let partitioning = Partitioning::balanced();
    for i in 1..=partitions_count * messages_per_partition_count {
        let payload = get_payload(i);
        let batch_size = IggyByteSize::from(16 + 4 + payload.len() as u64);
        let messages = IggyMessagesBatchMut::from_messages(
            &[IggyMessage::builder()
                .id(i as u128)
                .payload(Bytes::from(payload.to_owned()))
                .build()
                .expect("Failed to create message")],
            batch_size.as_bytes_u32(),
        );
        topic
            .append_messages(&partitioning, messages)
            .await
            .unwrap();
    }
    for i in 1..=partitions_count {
        assert_messages(&topic, i, messages_per_partition_count).await;
    }
}

#[tokio::test]
async fn given_key_partition_id_messages_should_be_appended_to_the_chosen_partition() {
    let setup = TestSetup::init().await;
    let partition_id = 1;
    let partitions_count = 3;
    let messages_per_partition_count = 10;
    let topic = init_topic(&setup, partitions_count).await;
    let partitioning = Partitioning::partition_id(partition_id);
    for i in 1..=partitions_count * messages_per_partition_count {
        let payload = get_payload(i);
        let batch_size = IggyByteSize::from(16 + 4 + payload.len() as u64);
        let messages = IggyMessagesBatchMut::from_messages(
            &[IggyMessage::builder()
                .id(i as u128)
                .payload(Bytes::from(payload.to_owned()))
                .build()
                .expect("Failed to create message")],
            batch_size.as_bytes_u32(),
        );
        topic
            .append_messages(&partitioning, messages)
            .await
            .unwrap();
    }

    for i in 1..=partitions_count {
        if i == partition_id {
            assert_messages(&topic, i, messages_per_partition_count * partitions_count).await;
            continue;
        }
        assert_messages(&topic, i, 0).await;
    }
}

#[tokio::test]
async fn given_key_messages_key_messages_should_be_appended_to_the_calculated_partition() {
    let setup = TestSetup::init().await;
    let partitions_count = 3;
    let messages_count = 10;
    let topic = init_topic(&setup, partitions_count).await;
    for entity_id in 1..=partitions_count * messages_count {
        let payload = get_payload(entity_id);
        let partitioning = Partitioning::messages_key_u32(entity_id);
        let batch_size = IggyByteSize::from(16 + 4 + payload.len() as u64);
        let messages = IggyMessagesBatchMut::from_messages(
            &[IggyMessage::builder()
                .id(entity_id as u128)
                .payload(Bytes::from(payload.to_owned()))
                .build()
                .expect("Failed to create message")],
            batch_size.as_bytes_u32(),
        );
        topic
            .append_messages(&partitioning, messages)
            .await
            .unwrap();
    }

    let mut messages_count_per_partition = HashMap::new();
    for entity_id in 1..=partitions_count * messages_count {
        let key = Partitioning::messages_key_u32(entity_id);
        let hash = hash::calculate_32(&key.value);
        let mut partition_id = hash % partitions_count;
        if partition_id == 0 {
            partition_id = partitions_count;
        }

        messages_count_per_partition
            .entry(partition_id)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    for (partition_id, expected_messages) in messages_count_per_partition {
        assert_messages(&topic, partition_id, expected_messages).await;
    }
}

fn get_payload(id: u32) -> String {
    format!("message-{id}")
}

async fn assert_messages(topic: &Topic, partition_id: u32, expected_messages: u32) {
    let consumer = PollingConsumer::Consumer(0, partition_id);
    let (_, polled_messages) = topic
        .get_messages(consumer, partition_id, PollingStrategy::offset(0), 1000)
        .await
        .unwrap();
    assert_eq!(polled_messages.count(), expected_messages);
}

async fn init_topic(setup: &TestSetup, partitions_count: u32) -> Topic {
    let stream_id = 1;
    setup.create_topics_directory(stream_id).await;
    let id = 2;
    let name = "test";
    let topic = Topic::create(
        stream_id,
        id,
        name,
        partitions_count,
        setup.config.clone(),
        setup.storage.clone(),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        IggyExpiry::NeverExpire,
        Default::default(),
        MaxTopicSize::ServerDefault,
        1,
    )
    .await
    .unwrap();
    topic.persist().await.unwrap();
    topic
}

fn create_payload(size: u32) -> String {
    let mut payload = String::with_capacity(size as usize);
    for i in 0..size {
        let char = (i % 26 + 97) as u8 as char;
        payload.push(char);
    }

    payload
}
