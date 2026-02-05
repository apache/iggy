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

use bytes::Bytes;
use iggy::prelude::*;
use integration::harness::{TestHarness, TestServerConfig};
use serial_test::parallel;
use std::collections::HashMap;
use test_case::test_matrix;

fn encryption_enabled() -> bool {
    true
}

fn encryption_disabled() -> bool {
    false
}

fn build_server_config(encryption: bool) -> TestServerConfig {
    let mut extra_envs = HashMap::new();

    if encryption {
        extra_envs.insert(
            "IGGY_SYSTEM_ENCRYPTION_ENABLED".to_string(),
            "true".to_string(),
        );
        extra_envs.insert(
            "IGGY_SYSTEM_ENCRYPTION_KEY".to_string(),
            "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=".to_string(),
        );
    }

    TestServerConfig::builder().extra_envs(extra_envs).build()
}

#[test_matrix(
    [encryption_enabled(), encryption_disabled()]
)]
#[tokio::test]
#[parallel]
async fn should_fill_data_with_headers_and_verify_after_restart_using_api(encryption: bool) {
    let mut harness = TestHarness::builder()
        .server(build_server_config(encryption))
        .build()
        .unwrap();

    harness.start().await.unwrap();

    let client = harness.tcp_root_client().await.unwrap();

    let stream_name = "test-stream-api";
    let topic_name = "test-topic-api";
    let partition_count = 1;

    client.create_stream(stream_name).await.unwrap();
    client
        .create_topic(
            &Identifier::named(stream_name).unwrap(),
            topic_name,
            partition_count,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let messages_per_batch = 1000;
    let mut messages_batch_1 = Vec::new();

    for i in 0..messages_per_batch {
        let mut headers = HashMap::new();
        headers.insert(HeaderKey::try_from("batch").unwrap(), 1u64.into());
        headers.insert(HeaderKey::try_from("index").unwrap(), i.into());
        headers.insert(
            HeaderKey::try_from("type").unwrap(),
            HeaderValue::try_from("test-message").unwrap(),
        );
        headers.insert(HeaderKey::try_from("encrypted").unwrap(), encryption.into());

        let message = IggyMessage::builder()
            .id((i + 1) as u128)
            .payload(Bytes::from(format!(
                "Message batch 1 index {i} with encryption {encryption}"
            )))
            .user_headers(headers)
            .build()
            .expect("Failed to create message");

        messages_batch_1.push(message);
    }

    client
        .send_messages(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            &Partitioning::partition_id(0),
            &mut messages_batch_1,
        )
        .await
        .unwrap();

    client
        .flush_unsaved_buffer(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            0,
            true,
        )
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let initial_stats = client.get_stats().await.unwrap();
    let initial_messages_count = initial_stats.messages_count;
    let initial_messages_size = initial_stats.messages_size_bytes;

    let consumer = Consumer::default();
    let polled = client
        .poll_messages(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            Some(0),
            &consumer,
            &PollingStrategy::offset(0),
            messages_per_batch.try_into().unwrap(),
            false,
        )
        .await
        .unwrap();

    let all_polled_messages_1 = polled.messages;

    eprintln!(
        "Polled {} messages, expected {}",
        all_polled_messages_1.len(),
        messages_per_batch
    );
    if !all_polled_messages_1.is_empty() {
        eprintln!(
            "First message offset: {}",
            all_polled_messages_1[0].header.offset
        );
        if all_polled_messages_1.len() > 1 {
            eprintln!(
                "Last message offset: {}",
                all_polled_messages_1.last().unwrap().header.offset
            );
        }
    }
    assert_eq!(all_polled_messages_1.len(), messages_per_batch as usize);
    for msg in all_polled_messages_1.iter() {
        assert!(msg.user_headers.is_some());
        let headers = msg.user_headers_map().unwrap().unwrap();
        assert_eq!(
            headers
                .get(&HeaderKey::try_from("batch").unwrap())
                .unwrap()
                .as_uint64()
                .unwrap(),
            1
        );
        assert_eq!(
            headers
                .get(&HeaderKey::try_from("type").unwrap())
                .unwrap()
                .as_str()
                .unwrap(),
            "test-message"
        );
        assert_eq!(
            headers
                .get(&HeaderKey::try_from("encrypted").unwrap())
                .unwrap()
                .as_bool()
                .unwrap(),
            encryption
        );
    }

    harness.restart_server().await.unwrap();

    let client = harness.tcp_root_client().await.unwrap();

    let mut messages_batch_2 = Vec::new();

    for i in 0..messages_per_batch {
        let mut headers = HashMap::new();
        headers.insert(HeaderKey::try_from("batch").unwrap(), 2u64.into());
        headers.insert(HeaderKey::try_from("index").unwrap(), i.into());
        headers.insert(
            HeaderKey::try_from("type").unwrap(),
            HeaderValue::try_from("test-message-after-restart").unwrap(),
        );
        headers.insert(HeaderKey::try_from("encrypted").unwrap(), encryption.into());

        let message = IggyMessage::builder()
            .id((messages_per_batch + i + 1) as u128)
            .payload(Bytes::from(format!(
                "Message batch 2 index {i} with encryption {encryption}"
            )))
            .user_headers(headers)
            .build()
            .expect("Failed to create message");

        messages_batch_2.push(message);
    }

    client
        .send_messages(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            &Partitioning::partition_id(0),
            &mut messages_batch_2,
        )
        .await
        .unwrap();

    client
        .flush_unsaved_buffer(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            0,
            true,
        )
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let polled = client
        .poll_messages(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            Some(0),
            &consumer,
            &PollingStrategy::offset(0),
            messages_per_batch as u32 * 2,
            false,
        )
        .await
        .unwrap();

    let all_polled_messages = polled.messages;

    assert_eq!(all_polled_messages.len(), (messages_per_batch * 2) as usize);

    let mut batch_1_count = 0;
    let mut batch_2_count = 0;

    for msg in &all_polled_messages {
        assert!(msg.user_headers.is_some());
        let headers = msg.user_headers_map().unwrap().unwrap();
        let batch_num = headers
            .get(&HeaderKey::try_from("batch").unwrap())
            .unwrap()
            .as_uint64()
            .unwrap();

        if batch_num == 1 {
            batch_1_count += 1;
            assert_eq!(
                headers
                    .get(&HeaderKey::try_from("type").unwrap())
                    .unwrap()
                    .as_str()
                    .unwrap(),
                "test-message"
            );
            assert_eq!(
                headers
                    .get(&HeaderKey::try_from("encrypted").unwrap())
                    .unwrap()
                    .as_bool()
                    .unwrap(),
                encryption
            );
        } else if batch_num == 2 {
            batch_2_count += 1;
            assert_eq!(
                headers
                    .get(&HeaderKey::try_from("type").unwrap())
                    .unwrap()
                    .as_str()
                    .unwrap(),
                "test-message-after-restart"
            );
        }
    }

    assert_eq!(batch_1_count, messages_per_batch as usize);
    assert_eq!(batch_2_count, messages_per_batch as usize);

    let final_stats = client.get_stats().await.unwrap();
    assert_eq!(final_stats.messages_count, initial_messages_count * 2);
    assert!(
        final_stats.messages_size_bytes.as_bytes_u64() >= initial_messages_size.as_bytes_u64() * 2,
        "Final message size ({}) should be at least 2x initial size ({})",
        final_stats.messages_size_bytes.as_bytes_u64(),
        initial_messages_size.as_bytes_u64()
    );
}
