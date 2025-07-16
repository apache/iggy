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
use server::configs::system::{PartitionConfig, SystemConfig};
use server::state::system::PartitionState;
use server::streaming::partitions::partition::Partition;
use server::streaming::segments::IggyMessagesBatchMut;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};

#[compio::test]
async fn should_persist_messages_and_then_load_them_from_disk() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;
    let messages_count = 1000;
    let config = Arc::new(SystemConfig {
        path: setup.config.path.to_string(),
        partition: PartitionConfig {
            messages_required_to_save: messages_count,
            enforce_fsync: true,
            ..Default::default()
        },
        ..Default::default()
    });
    let mut partition = Partition::create(
        stream_id,
        topic_id,
        partition_id,
        true,
        config.clone(),
        setup.storage.clone(),
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        IggyTimestamp::now(),
    );
    partition.persist().await.unwrap();
    partition.open().await.unwrap();

    let mut messages = Vec::with_capacity(messages_count as usize);
    let mut appended_messages = Vec::with_capacity(messages_count as usize);
    for i in 1..=messages_count {
        let id = i as u128;
        let payload = Bytes::from(format!("message {i}"));
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("key_1").unwrap(),
            HeaderValue::from_str("Value 1").unwrap(),
        );
        headers.insert(
            HeaderKey::new("key 2").unwrap(),
            HeaderValue::from_bool(true).unwrap(),
        );
        headers.insert(
            HeaderKey::new("key-3").unwrap(),
            HeaderValue::from_uint64(123456).unwrap(),
        );

        let appended_message = IggyMessage::builder()
            .id(id)
            .payload(payload.clone())
            .user_headers(headers.clone())
            .build()
            .expect("Failed to create message with headers");
        let message = IggyMessage::builder()
            .id(id)
            .payload(payload)
            .user_headers(headers)
            .build()
            .expect("Failed to create message with headers");

        appended_messages.push(appended_message);
        messages.push(message);
    }

    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();
    let messages_size = messages
        .iter()
        .map(|msg| msg.get_size_bytes().as_bytes_u32())
        .sum::<u32>();
    let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);
    partition.append_messages(batch).await.unwrap();
    assert_eq!(
        partition.unsaved_messages_count, 0,
        "Expected unsaved messages count to be 0, but got {}",
        partition.unsaved_messages_count
    );

    let now = IggyTimestamp::now();
    let mut loaded_partition = Partition::create(
        stream_id,
        topic_id,
        partition.partition_id,
        false,
        config.clone(),
        setup.storage.clone(),
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        now,
    );
    let partition_state = PartitionState {
        id: partition.partition_id,
        created_at: now,
    };
    loaded_partition.load(partition_state).await.unwrap();
    let loaded_messages = loaded_partition
        .get_messages_by_offset(0, messages_count)
        .await
        .unwrap();
    assert_eq!(
        loaded_messages.count(),
        messages_count,
        "Unexpected loaded messages count "
    );
    (0..messages_count as usize).for_each(|i| {
        let loaded_message = &loaded_messages.get(i).unwrap();
        let appended_message = &appended_messages[i];
        assert_eq!(
            loaded_message.header().id(),
            appended_message.header.id,
            "Message ID mismatch at position {i}",
        );
        assert_eq!(
            loaded_message.payload(),
            appended_message.payload,
            "Payload mismatch at position {i}",
        );
        assert_eq!(
            loaded_message.user_headers_map().unwrap().unwrap(),
            appended_message.user_headers_map().unwrap().unwrap(),
            "Headers mismatch at position {i}",
        );
    });
}
