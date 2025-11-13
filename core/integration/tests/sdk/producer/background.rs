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

use crate::sdk::producer::{
    PARTITION_ID, STREAM_NAME, TOPIC_NAME, cleanup, create_message_payload, init_system,
};
use bytes::Bytes;
use iggy::clients::producer_config::BackpressureMode;
use iggy::prelude::*;
use iggy::{clients::client::IggyClient, prelude::TcpClient};
use iggy_common::TcpClientConfig;
use integration::test_server::{TestServer, login_root};
use serial_test::parallel;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};

#[tokio::test]
#[parallel]
async fn background_send_receive_ok() {
    let mut test_server = TestServer::default();
    test_server.start();

    let tcp_client_config = TcpClientConfig {
        server_address: test_server.get_raw_tcp_addr().unwrap(),
        ..TcpClientConfig::default()
    };
    let client = ClientWrapper::Tcp(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
    let client = IggyClient::create(client, None, None);

    client.connect().await.unwrap();
    assert!(client.ping().await.is_ok(), "Failed to ping server");

    login_root(&client).await;
    init_system(&client).await;

    client.connect().await.unwrap();
    assert!(client.ping().await.is_ok(), "Failed to ping server");

    let messages_count = 1000;

    let mut messages = Vec::new();
    for offset in 0..messages_count {
        let id = (offset + 1) as u128;
        let payload = create_message_payload(offset as u64);
        messages.push(
            IggyMessage::builder()
                .id(id)
                .payload(payload)
                .build()
                .expect("Failed to create message with headers"),
        );
    }

    let producer = client
        .producer(STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .partitioning(Partitioning::partition_id(PARTITION_ID))
        .background(BackgroundConfig::builder().build())
        .build();

    producer.send(messages).await.unwrap();
    sleep(Duration::from_millis(500)).await;
    producer.shutdown().await;

    let consumer = Consumer::default();
    let polled_messages = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            messages_count,
            false,
        )
        .await
        .unwrap();

    assert_eq!(polled_messages.messages.len() as u32, messages_count);
    cleanup(&client).await;
}

#[tokio::test]
#[parallel]
async fn background_buffer_overflow_immediate() {
    let mut test_server = TestServer::default();
    test_server.start();

    let tcp_client_config = TcpClientConfig {
        server_address: test_server.get_raw_tcp_addr().unwrap(),
        ..TcpClientConfig::default()
    };
    let client = ClientWrapper::Tcp(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
    let client = IggyClient::create(client, None, None);

    client.connect().await.unwrap();
    assert!(client.ping().await.is_ok(), "Failed to ping server");

    login_root(&client).await;
    init_system(&client).await;

    client.connect().await.unwrap();
    assert!(client.ping().await.is_ok(), "Failed to ping server");

    let cfg = BackgroundConfig::builder()
        .max_buffer_size(IggyByteSize::from(1024))
        .failure_mode(BackpressureMode::FailImmediately)
        .build();
    let producer = client
        .producer(STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .background(cfg)
        .build();

    let big = IggyMessage::builder()
        .id(1)
        .payload(Bytes::from(vec![0u8; 2048]))
        .build()
        .unwrap();
    let err = producer.send(vec![big]).await.unwrap_err();
    assert!(matches!(err, IggyError::BackgroundSendBufferOverflow));

    cleanup(&client).await;
}

/// Ensures that when backpressure mode is `BlockWithTimeout`, the producer
/// fails with `BackgroundSendTimeout` if buffer is full and the timeout is exceeded.
///
/// We configure:
/// - max_in_flight = 1: only one batch can be in-flight;
/// - batch_length = 0, batch_size = 0: disables trigger-based flushing;
/// - linger_time = 500ms: messages are flushed only after timeout;
/// - backpressure timeout = 100ms: sending should fail if not flushed within this window.
#[tokio::test]
#[parallel]
async fn background_block_with_timeout() {
    let mut test_server = TestServer::default();
    test_server.start();

    let tcp_client_config = TcpClientConfig {
        server_address: test_server.get_raw_tcp_addr().unwrap(),
        ..TcpClientConfig::default()
    };
    let client = ClientWrapper::Tcp(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
    let client = IggyClient::create(client, None, None);

    client.connect().await.unwrap();
    assert!(client.ping().await.is_ok(), "Failed to ping server");

    login_root(&client).await;
    init_system(&client).await;

    client.connect().await.unwrap();
    assert!(client.ping().await.is_ok(), "Failed to ping server");

    let big = IggyMessage::builder()
        .id(1)
        .payload(Bytes::from(vec![0u8; 2048]))
        .build()
        .unwrap();
    let cfg = BackgroundConfig::builder()
        .max_buffer_size(big.get_size_bytes() + 100.into())
        .max_in_flight(1)
        .batch_length(0)
        .batch_size(0)
        .linger_time(IggyDuration::from(500_000))
        .failure_mode(BackpressureMode::BlockWithTimeout(IggyDuration::from(
            100_000,
        )))
        .build();
    let producer = client
        .producer(STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .background(cfg)
        .build();

    producer.send(vec![big]).await.unwrap();

    let big = IggyMessage::builder()
        .id(1)
        .payload(Bytes::from(vec![0u8; 512]))
        .build()
        .unwrap();
    let t0 = Instant::now();
    let err = producer.send(vec![big]).await.unwrap_err();
    assert!(matches!(err, IggyError::BackgroundSendTimeout));
    assert!(t0.elapsed() >= Duration::from_millis(100));

    cleanup(&client).await;
}

#[tokio::test]
#[parallel]
async fn background_block_waits_then_succeeds() {
    let mut test_server = TestServer::default();
    test_server.start();

    let tcp_client_config = TcpClientConfig {
        server_address: test_server.get_raw_tcp_addr().unwrap(),
        ..TcpClientConfig::default()
    };
    let client = ClientWrapper::Tcp(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
    let client = IggyClient::create(client, None, None);

    client.connect().await.unwrap();
    login_root(&client).await;
    init_system(&client).await;

    let big_msg = IggyMessage::builder()
        .id(1)
        .payload(Bytes::from(vec![0u8; 2048]))
        .build()
        .unwrap();

    let cfg = BackgroundConfig::builder()
        .max_buffer_size(big_msg.get_size_bytes() + 100.into())
        .max_in_flight(1)
        .batch_length(0)
        .batch_size(0)
        .linger_time(IggyDuration::from(300_000))
        .failure_mode(BackpressureMode::Block)
        .build();

    let producer = client
        .producer(STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .background(cfg)
        .build();

    producer.send(vec![big_msg]).await.unwrap();

    let small_msg = IggyMessage::builder()
        .id(2)
        .payload(Bytes::from_static(b"x"))
        .build()
        .unwrap();

    let start = Instant::now();
    let res = producer.send(vec![small_msg]).await;
    let elapsed = start.elapsed();

    assert!(res.is_ok());
    assert!(elapsed >= Duration::from_millis(300));

    cleanup(&client).await;
}

#[tokio::test]
#[parallel]
async fn background_graceful_shutdown() {
    let mut test_server = TestServer::default();
    test_server.start();

    let tcp_client_config = TcpClientConfig {
        server_address: test_server.get_raw_tcp_addr().unwrap(),
        ..TcpClientConfig::default()
    };
    let client = ClientWrapper::Tcp(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
    let client = IggyClient::create(client, None, None);

    client.connect().await.unwrap();
    assert!(client.ping().await.is_ok(), "Failed to ping server");

    login_root(&client).await;
    init_system(&client).await;

    client.connect().await.unwrap();
    assert!(client.ping().await.is_ok(), "Failed to ping server");

    let cfg = BackgroundConfig::builder()
        .max_in_flight(1)
        .batch_length(0)
        .batch_size(0)
        .linger_time(IggyDuration::from(2_000_000)) // 2s – long enough not to flush automatically
        .build();
    let producer = client
        .producer(STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .background(cfg)
        .build();

    let msg = IggyMessage::builder()
        .id(1)
        .payload(Bytes::from(vec![0u8; 512]))
        .build()
        .unwrap();
    producer.send(vec![msg]).await.unwrap();

    sleep(Duration::from_millis(1000)).await;

    let consumer = Consumer::default();
    let polled_messages = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .unwrap();
    assert_eq!(polled_messages.messages.len() as u32, 0);

    producer.shutdown().await;
    let polled_messages = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .unwrap();
    assert_eq!(polled_messages.messages.len() as u32, 1);

    cleanup(&client).await;
}

#[tokio::test]
#[parallel]
async fn background_many_parallel_producers() {
    const PARALLEL_PRODUCERS: usize = 10;

    let mut test_server = TestServer::default();
    test_server.start();

    let tcp_client_config = TcpClientConfig {
        server_address: test_server.get_raw_tcp_addr().unwrap(),
        ..TcpClientConfig::default()
    };
    let client = ClientWrapper::Tcp(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
    let client = Arc::new(IggyClient::create(client, None, None));

    client.connect().await.unwrap();
    login_root(&client).await;
    init_system(&client).await;

    let mut handles = Vec::with_capacity(PARALLEL_PRODUCERS);

    for i in 0..PARALLEL_PRODUCERS {
        let client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let cfg = BackgroundConfig::builder().num_shards(1).build();
            let producer = client_clone
                .producer(STREAM_NAME, TOPIC_NAME)
                .unwrap()
                .partitioning(Partitioning::partition_id(PARTITION_ID))
                .background(cfg)
                .build();

            let _ = producer.init().await;

            let msg = IggyMessage::builder()
                .id(i as u128)
                .payload(Bytes::from(vec![0u8; 100]))
                .build()
                .unwrap();

            producer.send(vec![msg]).await.unwrap();
            sleep(Duration::from_millis(1000)).await;
            producer.shutdown().await;
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let consumer = Consumer::default();
    let polled_messages = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            10,
            false,
        )
        .await
        .unwrap();

    assert_eq!(polled_messages.messages.len() as u32, 10);
    cleanup(&client).await;
}
