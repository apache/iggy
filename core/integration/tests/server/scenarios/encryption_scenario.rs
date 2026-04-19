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
use iggy_common::TransportProtocol;
use integration::harness::{TestHarness, TestServerConfig};
use serial_test::parallel;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use test_case::test_matrix;

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
        let mut headers = BTreeMap::new();
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

    // Verify on-disk encryption of headers and payload
    let data_path = harness.server().data_path();
    let log_files = find_log_files(&data_path);
    assert!(
        !log_files.is_empty(),
        "Expected at least one .log segment file on disk"
    );
    let all_raw_bytes: Vec<u8> = log_files
        .iter()
        .flat_map(|p| std::fs::read(p).unwrap())
        .collect();
    let contains_plaintext_header = all_raw_bytes
        .windows(b"test-message".len())
        .any(|w| w == b"test-message");
    let contains_plaintext_payload = all_raw_bytes
        .windows(b"Message batch 1".len())
        .any(|w| w == b"Message batch 1");
    if encryption {
        assert!(
            !contains_plaintext_header,
            "When encryption is enabled, header values must NOT appear as plaintext on disk"
        );
        assert!(
            !contains_plaintext_payload,
            "When encryption is enabled, payload must NOT appear as plaintext on disk"
        );
    } else {
        assert!(
            contains_plaintext_header,
            "When encryption is disabled, header values should appear as plaintext on disk"
        );
        assert!(
            contains_plaintext_payload,
            "When encryption is disabled, payload should appear as plaintext on disk"
        );
    }

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
        let mut headers = BTreeMap::new();
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

#[test_matrix(
    [TransportProtocol::Tcp, TransportProtocol::Http, TransportProtocol::Quic, TransportProtocol::WebSocket]
)]
#[tokio::test]
#[parallel]
async fn should_encrypt_and_decrypt_headers_with_client_side_encryption(
    transport: TransportProtocol,
) {
    let mut harness = TestHarness::builder()
        .server(TestServerConfig::default())
        .build()
        .unwrap();

    harness.start().await.unwrap();

    let setup_client = harness.tcp_root_client().await.unwrap();
    let stream_name = format!("client-enc-{transport}");
    let topic_name = "enc-topic";

    setup_client.create_stream(&stream_name).await.unwrap();
    setup_client
        .create_topic(
            &Identifier::named(&stream_name).unwrap(),
            topic_name,
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let encryptor = Arc::new(EncryptorKind::Aes256Gcm(
        Aes256GcmEncryptor::new(&[42u8; 32]).unwrap(),
    ));

    let encrypting_client = harness
        .client_builder_for(transport)
        .unwrap()
        .with_encryptor(encryptor)
        .with_root_login()
        .connect()
        .await
        .unwrap();

    let stream_id = Identifier::named(&stream_name).unwrap();
    let topic_id = Identifier::named(topic_name).unwrap();

    let mut messages = Vec::new();
    for i in 0..10i64 {
        let mut headers = BTreeMap::new();
        headers.insert(HeaderKey::try_from("index").unwrap(), HeaderValue::from(i));
        headers.insert(
            HeaderKey::try_from("transport").unwrap(),
            HeaderValue::try_from(transport.to_string().as_str()).unwrap(),
        );

        messages.push(
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(format!("client encrypted msg {i}")))
                .user_headers(headers)
                .build()
                .unwrap(),
        );
    }

    encrypting_client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    let polled = encrypting_client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            10,
            false,
        )
        .await
        .unwrap();

    assert_eq!(polled.messages.len(), 10);
    for (i, msg) in polled.messages.iter().enumerate() {
        assert_eq!(
            std::str::from_utf8(&msg.payload).unwrap(),
            format!("client encrypted msg {i}")
        );

        let headers = msg.user_headers_map().unwrap().unwrap();
        assert_eq!(
            headers
                .get(&HeaderKey::try_from("index").unwrap())
                .unwrap()
                .as_int64()
                .unwrap(),
            i as i64
        );
        assert_eq!(
            headers
                .get(&HeaderKey::try_from("transport").unwrap())
                .unwrap()
                .as_str()
                .unwrap(),
            transport.to_string().as_str()
        );
    }

    let client_without_encryptor = harness.root_client_for(transport).await.unwrap();
    let polled_without_decryption = client_without_encryptor
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            10,
            false,
        )
        .await
        .unwrap();

    assert_eq!(polled_without_decryption.messages.len(), 10);
    for msg in &polled_without_decryption.messages {
        let payload_text = std::str::from_utf8(&msg.payload).unwrap_or("");
        assert!(
            !payload_text.starts_with("client encrypted msg"),
            "Payload must not be readable without the encryptor"
        );

        let headers = msg.user_headers_map().unwrap();
        assert!(
            headers.is_none(),
            "Headers must not be parseable without the encryptor"
        );
    }
}

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

fn find_log_files(dir: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                result.extend(find_log_files(&path));
            } else if path.extension().and_then(|e| e.to_str()) == Some("log") {
                result.push(path);
            }
        }
    }
    result
}
