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
use integration::{
    tcp_client::TcpClientFactory,
    test_server::{ClientFactory, IpAddrKind, SYSTEM_PATH_ENV_VAR, TestServer, login_root},
};
use serial_test::parallel;
use std::{collections::HashMap, str::FromStr};
use test_case::test_matrix;

fn encryption_enabled() -> bool {
    true
}

fn encryption_disabled() -> bool {
    false
}

#[test_matrix(
    [encryption_enabled(), encryption_disabled()]
)]
#[tokio::test]
#[parallel]
async fn should_fill_data_with_headers_and_verify_after_restart_using_api(encryption: bool) {
    // 1. Start server
    let mut env_vars = HashMap::from([(
        SYSTEM_PATH_ENV_VAR.to_owned(),
        TestServer::get_random_path(),
    )]);

    if encryption {
        env_vars.insert(
            "IGGY_SYSTEM_ENCRYPTION_ENABLED".to_string(),
            "true".to_string(),
        );
        env_vars.insert(
            "IGGY_SYSTEM_ENCRYPTION_KEY".to_string(),
            "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=".to_string(),
        );
    }

    let mut test_server = TestServer::new(Some(env_vars.clone()), false, None, IpAddrKind::V4);
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let local_data_path = test_server.get_local_data_path().to_owned();

    // 2. Connect and create initial client
    let client = TcpClientFactory {
        server_addr,
        ..Default::default()
    }
    .create_client()
    .await;
    let client = IggyClient::create(client, None, None);
    login_root(&client).await;

    // 3. Create test stream and topic
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

    // 4. Send messages with headers (first batch)
    let messages_per_batch = 1000;
    let mut messages_batch_1 = Vec::new();

    for i in 0..messages_per_batch {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("batch").unwrap(),
            HeaderValue::from_uint64(1).unwrap(),
        );
        headers.insert(
            HeaderKey::new("index").unwrap(),
            HeaderValue::from_uint64(i).unwrap(),
        );
        headers.insert(
            HeaderKey::new("type").unwrap(),
            HeaderValue::from_str("test-message").unwrap(),
        );
        headers.insert(
            HeaderKey::new("encrypted").unwrap(),
            HeaderValue::from_bool(encryption).unwrap(),
        );

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

    // 5. Flush and get initial stats
    client
        .flush_unsaved_buffer(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            0,
            true, // Force flush
        )
        .await
        .unwrap();

    // Give the server a moment to process encrypted messages
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let initial_stats = client.get_stats().await.unwrap();
    let initial_messages_count = initial_stats.messages_count;
    let initial_messages_size = initial_stats.messages_size_bytes;

    // 6. Poll messages to verify initial batch
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

    // Verify we got all messages with correct headers
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
                .get(&HeaderKey::new("batch").unwrap())
                .unwrap()
                .as_uint64()
                .unwrap(),
            1
        );
        assert_eq!(
            headers
                .get(&HeaderKey::new("type").unwrap())
                .unwrap()
                .as_str()
                .unwrap(),
            "test-message"
        );
        assert_eq!(
            headers
                .get(&HeaderKey::new("encrypted").unwrap())
                .unwrap()
                .as_bool()
                .unwrap(),
            encryption
        );
    }

    // 7. Stop and restart server
    test_server.stop();
    drop(test_server);

    let mut test_server = TestServer::new(Some(env_vars.clone()), false, None, IpAddrKind::V4);
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();

    // 8. Reconnect after restart
    let client = TcpClientFactory {
        server_addr,
        ..Default::default()
    }
    .create_client()
    .await;
    let client = IggyClient::create(client, None, None);
    login_root(&client).await;

    // 9. Send second batch of messages with different headers
    let mut messages_batch_2 = Vec::new();

    for i in 0..messages_per_batch {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("batch").unwrap(),
            HeaderValue::from_uint64(2).unwrap(),
        );
        headers.insert(
            HeaderKey::new("index").unwrap(),
            HeaderValue::from_uint64(i).unwrap(),
        );
        headers.insert(
            HeaderKey::new("type").unwrap(),
            HeaderValue::from_str("test-message-after-restart").unwrap(),
        );
        headers.insert(
            HeaderKey::new("encrypted").unwrap(),
            HeaderValue::from_bool(encryption).unwrap(),
        );

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
            &Partitioning::partition_id(0), // Use specific partition for testing
            &mut messages_batch_2,
        )
        .await
        .unwrap();

    // Flush the buffer after sending second batch
    client
        .flush_unsaved_buffer(
            &Identifier::named(stream_name).unwrap(),
            &Identifier::named(topic_name).unwrap(),
            0,
            true, // Force flush
        )
        .await
        .unwrap();

    // Give the server a moment to process encrypted messages
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 10. Poll all messages (both batches) and verify
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

    // Verify we have all messages from both batches
    assert_eq!(all_polled_messages.len(), (messages_per_batch * 2) as usize);

    // Count messages by batch
    let mut batch_1_count = 0;
    let mut batch_2_count = 0;

    for msg in &all_polled_messages {
        assert!(msg.user_headers.is_some());
        let headers = msg.user_headers_map().unwrap().unwrap();
        let batch_num = headers
            .get(&HeaderKey::new("batch").unwrap())
            .unwrap()
            .as_uint64()
            .unwrap();

        if batch_num == 1 {
            batch_1_count += 1;
            assert_eq!(
                headers
                    .get(&HeaderKey::new("type").unwrap())
                    .unwrap()
                    .as_str()
                    .unwrap(),
                "test-message"
            );
            assert_eq!(
                headers
                    .get(&HeaderKey::new("encrypted").unwrap())
                    .unwrap()
                    .as_bool()
                    .unwrap(),
                encryption
            );
        } else if batch_num == 2 {
            batch_2_count += 1;
            assert_eq!(
                headers
                    .get(&HeaderKey::new("type").unwrap())
                    .unwrap()
                    .as_str()
                    .unwrap(),
                "test-message-after-restart"
            );
        }
    }

    assert_eq!(batch_1_count, messages_per_batch as usize);
    assert_eq!(batch_2_count, messages_per_batch as usize);

    // 11. Verify final stats
    let final_stats = client.get_stats().await.unwrap();
    assert_eq!(final_stats.messages_count, initial_messages_count * 2);
    assert!(
        final_stats.messages_size_bytes.as_bytes_u64() >= initial_messages_size.as_bytes_u64() * 2,
        "Final message size ({}) should be at least 2x initial size ({})",
        final_stats.messages_size_bytes.as_bytes_u64(),
        initial_messages_size.as_bytes_u64()
    );

    // 12. Cleanup
    std::fs::remove_dir_all(local_data_path).unwrap();
}
