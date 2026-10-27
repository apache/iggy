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

use bytes::Bytes;
use iggy::prelude::*;
use iggy_common::{
    OptionKey, ServerKeyContext, StreamEncryptionKey, TransportProtocol, stream_key_fingerprint,
    text,
};
use integration::harness::{TestHarness, TestServerConfig};
use serial_test::parallel;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use test_case::test_matrix;

const CONFIG_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
const SECOND_ENCRYPTION_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";
const ALL_TRANSPORTS: [TransportProtocol; 4] = [
    TransportProtocol::Tcp,
    TransportProtocol::Quic,
    TransportProtocol::WebSocket,
    TransportProtocol::Http,
];
const DISK_WAIT: Duration = Duration::from_secs(10);
// The stored payload is the 12-byte nonce, the ciphertext and the 16-byte tag.
const AES_GCM_OVERHEAD: usize = 28;
const TOPIC_NAME: &str = "encrypted-topic";

#[test_matrix(
    [encryption_enabled(), encryption_disabled()]
)]
#[tokio::test]
#[parallel]
async fn should_fill_data_with_headers_and_verify_after_restart_using_api(encryption: bool) {
    // Restart scenarios run single-node: restarting a node in a multi-node
    // cluster trips a known partitions-plane view-change stall, tracked
    // separately.
    let mut harness = TestHarness::builder()
        .cluster_nodes(1)
        .server(build_server_config(encryption))
        .build()
        .unwrap();

    harness.start().await.unwrap();

    let client = harness.tcp_root_client().await.unwrap();

    let stream_name = "test-stream-api";
    let topic_name = "test-topic-api";
    let partition_count = 1;

    client
        .create_stream(stream_name, &StreamCreateOptions::default())
        .await
        .unwrap();
    client
        .create_topic(
            &Identifier::named(stream_name).unwrap(),
            topic_name,
            &TopicCreateOptions {
                partitions_count: Some(partition_count),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..eager_flush_options()
            },
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

    // No flush primitive exists; `eager_flush_options()` makes every committed
    // batch hit disk instead.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

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

    // No flush primitive exists; `eager_flush_options()` makes every committed
    // batch hit disk instead.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

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

    setup_client
        .create_stream(&stream_name, &StreamCreateOptions::default())
        .await
        .unwrap();
    setup_client
        .create_topic(
            &Identifier::named(&stream_name).unwrap(),
            topic_name,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..eager_flush_options()
            },
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

/// The server flushes on the journal thresholds (there is no flush primitive),
/// so force every committed batch straight to disk: the assertions below read
/// the segment files directly. Both knobs are topic creation options now.
#[tokio::test]
#[parallel]
async fn given_client_key_and_keyless_streams_when_polling_should_use_their_own_keys() {
    let mut harness = single_node(encryption_config(true, CONFIG_KEY)).await;
    let tcp = harness.tcp_root_client().await.unwrap();
    let http = harness
        .root_client_for(TransportProtocol::Http)
        .await
        .unwrap();
    let first_key = stream_key(11);
    let second_key = stream_key(22);
    create_stream_with_topic(&tcp, "own-key-tcp", Some(first_key.clone())).await;
    create_stream_with_topic(&http, "own-key-http", Some(second_key.clone())).await;
    create_stream_with_topic(&tcp, "generated-key", None).await;
    let streams = ["own-key-tcp", "own-key-http", "generated-key"];
    for name in streams {
        send_single_message(&tcp, &stream_id(name), &topic_id(), 1, &payload(name)).await;
    }

    for transport in ALL_TRANSPORTS {
        let client = harness.root_client_for(transport).await.unwrap();
        for name in streams {
            let polled = poll_single_partition(&client, &stream_id(name), &topic_id(), 0, 1)
                .await
                .unwrap_or_else(|error| panic!("{transport} poll of {name}: {error}"));
            assert_eq!(
                polled.messages[0].payload.as_ref(),
                payload(name).as_bytes()
            );
        }
    }
    let kek = kek_fingerprint(CONFIG_KEY);
    let projections = [
        (
            "own-key-tcp",
            StreamEncryptionSource::Stream,
            Some(first_key.fingerprint()),
        ),
        (
            "own-key-http",
            StreamEncryptionSource::Stream,
            Some(second_key.fingerprint()),
        ),
        ("generated-key", StreamEncryptionSource::Server, None),
    ];
    for (name, source, fingerprint) in projections {
        let details = tcp.get_stream(&stream_id(name)).await.unwrap().unwrap();
        assert_eq!(details.encryption_key_source(), Some(source), "{name}");
        if let Some(fingerprint) = fingerprint {
            assert_eq!(details.encryption_key_fingerprint(), Some(fingerprint));
        }
        assert_eq!(
            details.encryption_kek_fingerprints(),
            vec![kek.clone()],
            "{name}"
        );
    }
    for name in streams {
        assert_plaintext_on_disk(&harness, &payload(name), false).await;
    }
    let first_encryptor = first_key.encryptor().unwrap();
    let second_encryptor = second_key.encryptor().unwrap();
    let config_encryptor =
        EncryptorKind::Aes256Gcm(Aes256GcmEncryptor::from_base64_key(CONFIG_KEY).unwrap());
    assert_ciphertext_on_disk(&harness, &first_encryptor, &payload("own-key-tcp")).await;
    assert_ciphertext_on_disk(&harness, &second_encryptor, &payload("own-key-http")).await;
    let foreign_keys = [
        ("own-key-tcp", vec![&*second_encryptor, &config_encryptor]),
        ("own-key-http", vec![&*first_encryptor, &config_encryptor]),
        (
            "generated-key",
            vec![&*first_encryptor, &*second_encryptor, &config_encryptor],
        ),
    ];
    for (name, encryptors) in foreign_keys {
        for encryptor in encryptors {
            assert!(
                opened_on_disk(&harness, encryptor, payload(name).len()).is_empty(),
                "{name} ciphertext opened under a key that is not its own"
            );
        }
    }
    harness.restart_server().await.unwrap();
    let client = harness.tcp_root_client().await.unwrap();
    for name in streams {
        let polled = poll_single_partition(&client, &stream_id(name), &topic_id(), 0, 1)
            .await
            .expect("restart reopens every stream key");
        assert_eq!(
            polled.messages[0].payload.as_ref(),
            payload(name).as_bytes()
        );
    }
}

#[tokio::test]
#[parallel]
async fn given_keyless_stream_with_encryption_enabled_when_reading_should_report_generated_key() {
    let harness = single_node(encryption_config(true, CONFIG_KEY)).await;
    let client = harness.tcp_root_client().await.unwrap();
    for name in ["generated-first", "generated-second"] {
        create_stream_with_topic(&client, name, None).await;
        send_single_message(&client, &stream_id(name), &topic_id(), 1, &payload(name)).await;
    }
    let first = client
        .get_stream(&stream_id("generated-first"))
        .await
        .unwrap()
        .unwrap();
    let second = client
        .get_stream(&stream_id("generated-second"))
        .await
        .unwrap()
        .unwrap();
    for details in [&first, &second] {
        assert_eq!(
            details.encryption_key_source(),
            Some(StreamEncryptionSource::Server)
        );
        assert_eq!(
            details.encryption_kek_fingerprints(),
            vec![kek_fingerprint(CONFIG_KEY)]
        );
    }
    assert_ne!(
        first.encryption_key_fingerprint(),
        second.encryption_key_fingerprint()
    );
    assert!(first.encryption_key_fingerprint().is_some());
    for name in ["generated-first", "generated-second"] {
        assert_plaintext_on_disk(&harness, &payload(name), false).await;
        let polled = poll_single_partition(&client, &stream_id(name), &topic_id(), 0, 1)
            .await
            .unwrap();
        assert_eq!(
            polled.messages[0].payload.as_ref(),
            payload(name).as_bytes()
        );
    }
}

#[tokio::test]
#[parallel]
async fn given_disabled_encryption_with_config_key_when_creating_client_key_stream_should_encrypt_only_that_stream()
 {
    let harness = single_node(encryption_config(false, CONFIG_KEY)).await;
    for transport in [TransportProtocol::Tcp, TransportProtocol::Http] {
        let client = harness.root_client_for(transport).await.unwrap();
        let own = format!("disabled-own-{transport}");
        let plain = format!("disabled-plain-{transport}");
        create_stream_with_topic(&client, &own, Some(stream_key(33))).await;
        create_stream_with_topic(&client, &plain, None).await;
        send_single_message(&client, &stream_id(&own), &topic_id(), 1, &payload(&own)).await;
        send_single_message(
            &client,
            &stream_id(&plain),
            &topic_id(),
            1,
            &payload(&plain),
        )
        .await;
        assert_eq!(
            source_of(&client, &own).await,
            Some(StreamEncryptionSource::Stream)
        );
        assert_eq!(
            source_of(&client, &plain).await,
            Some(StreamEncryptionSource::None)
        );
        assert_plaintext_on_disk(&harness, &payload(&own), false).await;
        assert_plaintext_on_disk(&harness, &payload(&plain), true).await;
        for name in [&own, &plain] {
            let polled = poll_single_partition(&client, &stream_id(name), &topic_id(), 0, 1)
                .await
                .unwrap();
            assert_eq!(
                polled.messages[0].payload.as_ref(),
                payload(name).as_bytes()
            );
        }
    }
}

#[tokio::test]
#[parallel]
async fn given_stream_created_while_disabled_when_enabling_encryption_should_stay_plaintext() {
    let mut harness = single_node(encryption_config(false, CONFIG_KEY)).await;
    let client = harness.tcp_root_client().await.unwrap();
    create_stream_with_topic(&client, "before-enable", None).await;

    harness
        .server_mut()
        .add_env("IGGY_ENCRYPTION_ENABLED", "true");
    harness.restart_server().await.unwrap();
    let client = harness.tcp_root_client().await.unwrap();
    send_single_message(
        &client,
        &stream_id("before-enable"),
        &topic_id(),
        1,
        &payload("before-enable"),
    )
    .await;
    create_stream_with_topic(&client, "after-enable", None).await;

    assert_eq!(
        source_of(&client, "before-enable").await,
        Some(StreamEncryptionSource::None)
    );
    assert_eq!(
        source_of(&client, "after-enable").await,
        Some(StreamEncryptionSource::Server)
    );
    assert_plaintext_on_disk(&harness, &payload("before-enable"), true).await;
}

#[tokio::test]
#[parallel]
async fn given_custom_key_without_config_key_when_creating_should_reject_unavailable() {
    let harness = single_node(TestServerConfig::default()).await;
    for transport in ALL_TRANSPORTS {
        let client = harness.root_client_for(transport).await.unwrap();
        let name = format!("refused-{transport}");
        let error = client
            .create_stream(
                &name,
                &StreamCreateOptions {
                    encryption_key: Some(stream_key(44)),
                    ..StreamCreateOptions::default()
                },
            )
            .await
            .expect_err("a stream key needs a server key");
        assert!(
            is_error(&error, &IggyError::StreamEncryptionUnavailable),
            "{transport} returned {error}"
        );
        assert!(
            client
                .get_stream(&stream_id(&name))
                .await
                .unwrap()
                .is_none()
        );
    }
    let client = harness.tcp_root_client().await.unwrap();
    create_stream_with_topic(&client, "plain-without-key", None).await;
    assert_eq!(
        source_of(&client, "plain-without-key").await,
        Some(StreamEncryptionSource::None)
    );
}

#[tokio::test]
#[parallel]
async fn given_custom_key_when_reading_every_response_should_return_only_public_projections() {
    let harness = single_node(encryption_config(true, CONFIG_KEY)).await;
    let key = stream_key(55);
    let encoded = text::as_base64(key.as_bytes());
    let hidden = [
        OptionKey::EncryptionKey,
        OptionKey::EncryptionKeyWrapped,
        OptionKey::CriticalKeys,
    ];
    for transport in ALL_TRANSPORTS {
        let client = harness.root_client_for(transport).await.unwrap();
        let name = format!("projected-{transport}");
        let created = client
            .create_stream(
                &name,
                &StreamCreateOptions {
                    encryption_key: Some(key.clone()),
                    ..StreamCreateOptions::default()
                },
            )
            .await
            .unwrap();
        let fetched = client.get_stream(&stream_id(&name)).await.unwrap().unwrap();
        let listed = client
            .get_streams()
            .await
            .unwrap()
            .into_iter()
            .find(|stream| stream.name == name)
            .expect("listed");
        for options in [&created.options, &fetched.options, &listed.options] {
            for key in hidden {
                assert!(
                    !options.contains_key(&HeaderKey::from(key)),
                    "{transport}: {key}"
                );
            }
            let rendered = format!("{options:?}");
            assert!(!rendered.contains(&encoded), "{transport} rendered the key");
        }
        for details in [
            created.encryption_key_fingerprint(),
            fetched.encryption_key_fingerprint(),
        ] {
            assert_eq!(details, Some(key.fingerprint()), "{transport}");
        }
        assert_eq!(
            listed.encryption_key_fingerprint(),
            Some(stream_key_fingerprint(key.as_bytes()).unwrap())
        );
        assert_eq!(
            fetched.encryption_kek_fingerprints(),
            vec![kek_fingerprint(CONFIG_KEY)]
        );
    }
}

#[tokio::test]
#[parallel]
async fn given_changed_config_key_when_restarting_should_refuse_boot_naming_both_fingerprints() {
    let mut harness = single_node(encryption_config(true, CONFIG_KEY)).await;
    let client = harness.tcp_root_client().await.unwrap();
    create_stream_with_topic(&client, "kept-own", Some(stream_key(66))).await;
    create_stream_with_topic(&client, "kept-generated", None).await;
    for name in ["kept-own", "kept-generated"] {
        send_single_message(&client, &stream_id(name), &topic_id(), 1, &payload(name)).await;
    }

    harness
        .server_mut()
        .add_env("IGGY_ENCRYPTION_KEY", SECOND_ENCRYPTION_KEY);
    let error = harness
        .restart_server()
        .await
        .expect_err("a changed config key must refuse boot");
    if stderr_is_captured() {
        let diagnostics = error.to_string();
        for fingerprint in [
            kek_fingerprint(CONFIG_KEY),
            kek_fingerprint(SECOND_ENCRYPTION_KEY),
        ] {
            assert!(
                diagnostics.contains(&fingerprint),
                "the refusal must name {fingerprint}, got: {diagnostics}"
            );
        }
    }

    harness
        .server_mut()
        .add_env("IGGY_ENCRYPTION_ENABLED", "false");
    harness.server_mut().add_env("IGGY_ENCRYPTION_KEY", "");
    harness
        .restart_server()
        .await
        .expect_err("no config key must refuse boot over encrypted streams");

    harness
        .server_mut()
        .add_env("IGGY_ENCRYPTION_ENABLED", "true");
    harness
        .server_mut()
        .add_env("IGGY_ENCRYPTION_KEY", CONFIG_KEY);
    harness.restart_server().await.unwrap();
    let client = harness.tcp_root_client().await.unwrap();
    for name in ["kept-own", "kept-generated"] {
        let polled = poll_single_partition(&client, &stream_id(name), &topic_id(), 0, 1)
            .await
            .expect("the original key reads the stored messages");
        assert_eq!(
            polled.messages[0].payload.as_ref(),
            payload(name).as_bytes()
        );
    }
}

async fn single_node(config: TestServerConfig) -> TestHarness {
    let mut harness = TestHarness::builder()
        .cluster_nodes(1)
        .server(config)
        .build()
        .unwrap();
    harness.start().await.unwrap();
    harness
}

fn encryption_config(enabled: bool, key: &str) -> TestServerConfig {
    TestServerConfig::builder()
        .extra_envs(HashMap::from([
            ("IGGY_ENCRYPTION_ENABLED".to_string(), enabled.to_string()),
            ("IGGY_ENCRYPTION_KEY".to_string(), key.to_string()),
        ]))
        .build()
}

async fn create_stream_with_topic(
    client: &IggyClient,
    name: &str,
    encryption_key: Option<StreamEncryptionKey>,
) {
    client
        .create_stream(
            name,
            &StreamCreateOptions {
                encryption_key,
                ..StreamCreateOptions::default()
            },
        )
        .await
        .unwrap_or_else(|error| panic!("create stream {name}: {error}"));
    client
        .create_topic(
            &stream_id(name),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..eager_flush_options()
            },
        )
        .await
        .unwrap();
}

async fn source_of(client: &IggyClient, name: &str) -> Option<StreamEncryptionSource> {
    client
        .get_stream(&stream_id(name))
        .await
        .unwrap()
        .expect("stream exists")
        .encryption_key_source()
}

/// Wait until the stream data reaches disk, then check whether `payload`
/// appears there as plaintext.
async fn assert_plaintext_on_disk(harness: &TestHarness, payload: &str, expected: bool) {
    let deadline = tokio::time::Instant::now() + DISK_WAIT;
    loop {
        let bytes: Vec<u8> = find_log_files(&harness.server().data_path())
            .iter()
            .flat_map(|path| std::fs::read(path).unwrap_or_default())
            .collect();
        let found = bytes
            .windows(payload.len())
            .any(|window| window == payload.as_bytes());
        if found == expected && (found || bytes.len() >= payload.len()) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "payload {payload} plaintext on disk: expected {expected}, found {found}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until `encryptor` opens `payload` from a segment file.
async fn assert_ciphertext_on_disk(
    harness: &TestHarness,
    encryptor: &EncryptorKind,
    payload: &str,
) {
    let deadline = tokio::time::Instant::now() + DISK_WAIT;
    while !opened_on_disk(harness, encryptor, payload.len())
        .iter()
        .any(|plaintext| plaintext == payload.as_bytes())
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "payload {payload} is not stored under the expected key"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Every plaintext of `length` bytes that `encryptor` opens from any window of a segment file.
fn opened_on_disk(harness: &TestHarness, encryptor: &EncryptorKind, length: usize) -> Vec<Vec<u8>> {
    find_log_files(&harness.server().data_path())
        .iter()
        .flat_map(|path| {
            std::fs::read(path)
                .unwrap_or_default()
                .windows(length + AES_GCM_OVERHEAD)
                .filter_map(|window| encryptor.decrypt(window).ok())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn is_error(error: &IggyError, expected: &IggyError) -> bool {
    match error {
        IggyError::HttpResponseError(_, body) => {
            body.contains(&format!("\"id\":{}", expected.as_code()))
        }
        other => other.as_code() == expected.as_code(),
    }
}

fn kek_fingerprint(config_key: &str) -> String {
    ServerKeyContext::from_config(config_key, &[], true)
        .unwrap()
        .kek_fingerprint()
        .unwrap()
}

fn stream_key(seed: u8) -> StreamEncryptionKey {
    StreamEncryptionKey::try_from([seed; 32].as_slice()).unwrap()
}

fn stream_id(name: &str) -> Identifier {
    Identifier::named(name).unwrap()
}

fn topic_id() -> Identifier {
    Identifier::named(TOPIC_NAME).unwrap()
}

fn payload(stream: &str) -> String {
    format!("payload-of-{stream}")
}

fn stderr_is_captured() -> bool {
    std::env::var("IGGY_TEST_VERBOSE").is_err()
}

fn eager_flush_options() -> TopicCreateOptions {
    TopicCreateOptions {
        durability: iggy_common::Durability::Persisted,
        messages_required_to_save: Some(1),
        ..TopicCreateOptions::default()
    }
}

async fn send_single_message(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    id: u128,
    payload: &str,
) {
    let mut messages = vec![
        IggyMessage::builder()
            .id(id)
            .payload(Bytes::copy_from_slice(payload.as_bytes()))
            .build()
            .unwrap(),
    ];
    client
        .send_messages(
            stream_id,
            topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();
}

async fn poll_single_partition(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    offset: u64,
    count: u32,
) -> Result<PolledMessages, IggyError> {
    client
        .poll_messages(
            stream_id,
            topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(offset),
            count,
            false,
        )
        .await
}

fn build_server_config(encryption: bool) -> TestServerConfig {
    let mut extra_envs = HashMap::new();

    if encryption {
        extra_envs.insert("IGGY_ENCRYPTION_ENABLED".to_string(), "true".to_string());
        extra_envs.insert("IGGY_ENCRYPTION_KEY".to_string(), CONFIG_KEY.to_string());
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
