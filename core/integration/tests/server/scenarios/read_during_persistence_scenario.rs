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
use std::collections::HashMap;
use std::time::{Duration, Instant};

const STREAM_NAME: &str = "eventual-consistency-stream";
const TOPIC_NAME: &str = "eventual-consistency-topic";
const TEST_DURATION_SECS: u64 = 10;

/// Test with two separate clients - one for sending, one for polling.
///
/// This should expose the race condition where messages are in-flight during
/// disk write and unavailable for polling.
#[tokio::test]
#[parallel]
async fn should_read_messages_immediately_after_send_with_aggressive_persistence() {
    let env_vars = HashMap::from([
        (
            SYSTEM_PATH_ENV_VAR.to_owned(),
            TestServer::get_random_path(),
        ),
        (
            "IGGY_SYSTEM_PARTITION_MESSAGES_REQUIRED_TO_SAVE".to_owned(),
            "32".to_owned(),
        ),
        (
            "IGGY_SYSTEM_PARTITION_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE".to_owned(),
            "512B".to_owned(),
        ),
        (
            "IGGY_SYSTEM_PARTITION_ENFORCE_FSYNC".to_owned(),
            "false".to_owned(),
        ),
    ]);

    let mut test_server = TestServer::new(Some(env_vars), true, None, IpAddrKind::V4);
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();

    let producer_client = TcpClientFactory {
        server_addr: server_addr.clone(),
        ..Default::default()
    }
    .create_client()
    .await;
    let producer = IggyClient::create(producer_client, None, None);
    login_root(&producer).await;

    producer.create_stream(STREAM_NAME).await.unwrap();
    producer
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let consumer_client = TcpClientFactory {
        server_addr,
        ..Default::default()
    }
    .create_client()
    .await;
    let consumer = IggyClient::create(consumer_client, None, None);
    login_root(&consumer).await;

    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let consumer_kind = Consumer::default();

    let test_duration = Duration::from_secs(TEST_DURATION_SECS);
    let messages_per_batch = 32u32;
    let payload = "X".repeat(1024);

    let start = Instant::now();
    let mut batches_sent = 0u64;
    let mut messages_sent = 0u64;

    println!(
        "Starting test: 1KB messages, {} msgs/batch, duration: {}s",
        messages_per_batch, TEST_DURATION_SECS
    );

    while start.elapsed() < test_duration {
        let base_offset = batches_sent * messages_per_batch as u64;

        let mut messages: Vec<IggyMessage> = (0..messages_per_batch)
            .map(|i| {
                IggyMessage::builder()
                    .id((base_offset + i as u64 + 1) as u128)
                    .payload(Bytes::from(format!(
                        "{} - batch {} msg {}",
                        payload, batches_sent, i
                    )))
                    .build()
                    .expect("Failed to create message")
            })
            .collect();

        println!("Sending batch {}", batches_sent);
        let send_result = producer
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await;
        match &send_result {
            Ok(_) => println!("Batch {} sent successfully", batches_sent),
            Err(e) => println!("Batch {} send error: {:?}", batches_sent, e),
        }
        send_result.unwrap();

        batches_sent += 1;
        messages_sent += messages_per_batch as u64;

        println!("Calling poll_messages after batch {}", batches_sent);
        let poll_result = consumer
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(0),
                &consumer_kind,
                &PollingStrategy::offset(0),
                messages_sent as u32,
                false,
            )
            .await;

        let polled_count = match &poll_result {
            Ok(polled) => polled.messages.len() as u64,
            Err(e) => {
                println!("Poll error: {:?}", e);
                0
            }
        };

        if polled_count < messages_sent {
            let missing = messages_sent - polled_count;
            let elapsed_ms = start.elapsed().as_millis();

            panic!(
                "RACE CONDITION DETECTED after {:.2}s/{}s ({} batches, {} messages), expected {} messages, got {}. Missing: {}",
                elapsed_ms as f64 / 1000.0,
                TEST_DURATION_SECS,
                batches_sent,
                messages_sent,
                messages_sent,
                polled_count,
                missing
            );
        }

        if batches_sent.is_multiple_of(1000) {
            println!(
                "Progress: {} batches, {} messages, elapsed: {:.2}s/{}s",
                batches_sent,
                messages_sent,
                start.elapsed().as_secs_f64(),
                TEST_DURATION_SECS
            );
        }
    }

    producer.delete_stream(&stream_id).await.unwrap();
}
