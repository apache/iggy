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

use futures::StreamExt;
use iggy::prelude::*;
use iggy_common::TransportProtocol;
use integration::harness::{TestBinary, TestHarness};
use std::str::FromStr;
use tokio::time::{Duration, sleep, timeout};

const STREAM_NAME: &str = "test-reconnect-stream";
const TOPIC_NAME: &str = "test-reconnect-topic";

pub async fn run_producer(harness: &mut TestHarness) {
    let client = create_client(harness);
    Client::connect(&client).await.expect("Failed to connect");

    let producer = client
        .producer(STREAM_NAME, TOPIC_NAME)
        .expect("Failed to create producer builder")
        .create_stream_if_not_exists()
        .create_topic_if_not_exists(
            1,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .send_retries(Some(10), Some(IggyDuration::from_str("2s").unwrap()))
        .build();

    producer
        .init()
        .await
        .expect("Failed to initialize producer");

    let msg = IggyMessage::from_str("before-restart").unwrap();
    producer
        .send(vec![msg])
        .await
        .expect("Initial send should succeed");

    // Stop the server, attempt a send while it is down, then restart.
    // The SDK should auto-reconnect and deliver the message.
    harness.server_mut().stop().expect("Failed to stop server");
    sleep(Duration::from_secs(2)).await;

    let send_handle = tokio::spawn(async move {
        let msg = IggyMessage::from_str("after-restart").unwrap();
        producer.send(vec![msg]).await
    });

    sleep(Duration::from_secs(1)).await;
    harness
        .server_mut()
        .start()
        .expect("Failed to start server");

    let send_result = timeout(Duration::from_secs(60), send_handle)
        .await
        .expect("Timed out waiting for send after server restart")
        .expect("Send task panicked");
    send_result.expect("Send after server restart should succeed");

    let poll_client = harness
        .root_client()
        .await
        .expect("Failed to create polling client");

    let polled = poll_client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            10,
            false,
        )
        .await
        .expect("Failed to poll messages after restart");

    assert!(
        !polled.messages.is_empty(),
        "Expected at least one message after server restart"
    );
}

pub async fn run_consumer(harness: &mut TestHarness) {
    let setup_client = harness
        .root_client()
        .await
        .expect("Failed to create setup client");

    setup_client
        .create_stream(STREAM_NAME)
        .await
        .expect("Failed to create stream");
    setup_client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            Default::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("Failed to create topic");

    send_messages(&setup_client, "pre-restart", 3).await;
    drop(setup_client);

    let client = create_client(harness);
    Client::connect(&client).await.expect("Failed to connect");

    let mut consumer = client
        .consumer("test-consumer", STREAM_NAME, TOPIC_NAME, 0)
        .expect("Failed to create consumer builder")
        .polling_strategy(PollingStrategy::next())
        .batch_length(10)
        .poll_interval(IggyDuration::from_str("100ms").unwrap())
        .polling_retry_interval(IggyDuration::from_str("500ms").unwrap())
        .build();

    consumer
        .init()
        .await
        .expect("Failed to initialize consumer");

    let pre_payloads = consume_messages_validated(&mut consumer, 3, Duration::from_secs(10)).await;
    assert_eq!(
        pre_payloads.len(),
        3,
        "Should consume all pre-restart messages"
    );
    for (i, payload) in pre_payloads.iter().enumerate() {
        assert_eq!(
            payload,
            &format!("pre-restart-{i}"),
            "Pre-restart message {i} has wrong payload"
        );
    }

    harness.server_mut().stop().expect("Failed to stop server");
    sleep(Duration::from_secs(2)).await;
    harness
        .server_mut()
        .start()
        .expect("Failed to start server");

    let post_client = harness
        .root_client()
        .await
        .expect("Failed to create post-restart client");
    send_messages(&post_client, "post-restart", 3).await;

    let post_payloads = consume_messages_validated(&mut consumer, 3, Duration::from_secs(30)).await;
    assert_eq!(
        post_payloads.len(),
        3,
        "Should consume all post-restart messages after reconnect"
    );
    for (i, payload) in post_payloads.iter().enumerate() {
        assert_eq!(
            payload,
            &format!("post-restart-{i}"),
            "Post-restart message {i} has wrong payload"
        );
    }
}

fn create_client(harness: &TestHarness) -> IggyClient {
    let transport = harness.transport().expect("No transport configured");
    let server = harness.server();

    let addr = match transport {
        TransportProtocol::Tcp => server.tcp_addr().expect("TCP address not available"),
        TransportProtocol::Quic => server.quic_addr().expect("QUIC address not available"),
        TransportProtocol::WebSocket => server
            .websocket_addr()
            .expect("WebSocket address not available"),
        TransportProtocol::Http => panic!("HTTP is stateless and does not support reconnect"),
    };

    let protocol_prefix = match transport {
        TransportProtocol::Tcp => "iggy",
        TransportProtocol::Quic => "iggy+quic",
        TransportProtocol::WebSocket => "iggy+ws",
        TransportProtocol::Http => unreachable!(),
    };

    let connection_string = format!("{protocol_prefix}://iggy:iggy@{addr}?heartbeat_interval=5min");
    IggyClient::from_connection_string(&connection_string)
        .expect("Failed to create client from connection string")
}

async fn send_messages(client: &IggyClient, prefix: &str, count: u32) {
    for i in 0..count {
        let msg = IggyMessage::from_str(&format!("{prefix}-{i}")).unwrap();
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(0),
                &mut [msg],
            )
            .await
            .expect("Failed to send message");
    }
}

/// Consumes up to `expected` messages, returning their payloads in order.
async fn consume_messages_validated(
    consumer: &mut IggyConsumer,
    expected: u32,
    max_wait: Duration,
) -> Vec<String> {
    let mut payloads = Vec::with_capacity(expected as usize);
    let deadline = tokio::time::Instant::now() + max_wait;
    while (payloads.len() as u32) < expected && tokio::time::Instant::now() < deadline {
        let poll_result: Option<Result<ReceivedMessage, IggyError>> =
            tokio::time::timeout(Duration::from_millis(500), consumer.next())
                .await
                .ok()
                .flatten();
        if let Some(Ok(msg)) = poll_result {
            let payload = String::from_utf8_lossy(&msg.message.payload).to_string();
            payloads.push(payload);
        }
    }
    payloads
}

/// Regression test: a partition with exactly one message at offset 0 must not
/// reassign offset 0 to the next message after server restart.
///
/// Root cause: `should_increment_offset = current_offset > 0` is false when
/// current_offset == 0, but a segment with 1 message at offset 0 has
/// end_offset = 0. The next append after restart reuses offset 0.
pub async fn run_single_message_offset_zero_restart(harness: &mut TestHarness) {
    const STREAM: &str = "offset-zero-restart-stream";
    const TOPIC: &str = "offset-zero-restart-topic";
    const CONSUMER_ID: u32 = 77;

    let client = harness
        .root_client()
        .await
        .expect("Failed to create client");

    client.create_stream(STREAM).await.unwrap();
    client
        .create_topic(
            &Identifier::named(STREAM).unwrap(),
            TOPIC,
            1,
            Default::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    // Send exactly 1 message (gets offset 0)
    let mut msg = [IggyMessage::from_str("single-msg-0").unwrap()];
    client
        .send_messages(
            &Identifier::named(STREAM).unwrap(),
            &Identifier::named(TOPIC).unwrap(),
            &Partitioning::partition_id(0),
            &mut msg,
        )
        .await
        .unwrap();

    // Consumer polls to commit offset 0
    let consumer = Consumer::new(Identifier::numeric(CONSUMER_ID).unwrap());
    let polled = client
        .poll_messages(
            &Identifier::named(STREAM).unwrap(),
            &Identifier::named(TOPIC).unwrap(),
            Some(0),
            &consumer,
            &PollingStrategy::next(),
            10,
            true,
        )
        .await
        .unwrap();
    assert_eq!(polled.messages.len(), 1, "Should get the single message");
    assert_eq!(polled.messages[0].header.offset, 0);

    // Wait for data to flush
    sleep(Duration::from_secs(1)).await;
    drop(client);

    // Restart
    harness.server_mut().stop().expect("Failed to stop server");
    sleep(Duration::from_secs(2)).await;
    harness
        .server_mut()
        .start()
        .expect("Failed to start server");

    let client = harness
        .root_client()
        .await
        .expect("Failed to create post-restart client");

    // Send a second message - it MUST get offset 1, not 0
    let mut msg = [IggyMessage::from_str("single-msg-1").unwrap()];
    client
        .send_messages(
            &Identifier::named(STREAM).unwrap(),
            &Identifier::named(TOPIC).unwrap(),
            &Partitioning::partition_id(0),
            &mut msg,
        )
        .await
        .unwrap();

    // Consumer polls with Next - should get offset 1
    let consumer = Consumer::new(Identifier::numeric(CONSUMER_ID).unwrap());
    let polled = client
        .poll_messages(
            &Identifier::named(STREAM).unwrap(),
            &Identifier::named(TOPIC).unwrap(),
            Some(0),
            &consumer,
            &PollingStrategy::next(),
            10,
            true,
        )
        .await
        .unwrap();

    assert!(
        !polled.messages.is_empty(),
        "BUG: consumer got empty result after restart - offset stuck"
    );
    assert_eq!(
        polled.messages[0].header.offset, 1,
        "BUG: second message got offset {} instead of 1. \
         should_increment_offset was incorrectly false after restart \
         with single message at offset 0",
        polled.messages[0].header.offset,
    );

    let payload = String::from_utf8_lossy(&polled.messages[0].payload);
    assert_eq!(payload, "single-msg-1");
}

/// Regression test: consumer offset persisted ahead of partition data (crash
/// simulation). After restart the consumer must not be permanently stuck.
///
/// Simulates: OOM/kill-9 where consumer auto_commit wrote offset to disk but
/// journal data was not flushed. On restart, consumer offset file says 999 but
/// partition only has 10 messages (offsets 0-9).
pub async fn run_consumer_offset_ahead_after_crash(harness: &mut TestHarness) {
    const STREAM: &str = "offset-ahead-crash-stream";
    const TOPIC: &str = "offset-ahead-crash-topic";
    const CONSUMER_ID: u32 = 88;

    let client = harness
        .root_client()
        .await
        .expect("Failed to create client");

    let stream = client.create_stream(STREAM).await.unwrap();
    let stream_id = stream.id;

    let topic = client
        .create_topic(
            &Identifier::named(STREAM).unwrap(),
            TOPIC,
            1,
            Default::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    let topic_id = topic.id;

    // Send 10 messages (offsets 0-9)
    for i in 0..10u32 {
        let mut msg = [IggyMessage::from_str(&format!("crash-msg-{i}")).unwrap()];
        client
            .send_messages(
                &Identifier::named(STREAM).unwrap(),
                &Identifier::named(TOPIC).unwrap(),
                &Partitioning::partition_id(0),
                &mut msg,
            )
            .await
            .unwrap();
    }

    // Consumer reads all 10 with auto_commit (stored offset = 9)
    let consumer = Consumer::new(Identifier::numeric(CONSUMER_ID).unwrap());
    let polled = client
        .poll_messages(
            &Identifier::named(STREAM).unwrap(),
            &Identifier::named(TOPIC).unwrap(),
            Some(0),
            &consumer,
            &PollingStrategy::next(),
            100,
            true,
        )
        .await
        .unwrap();
    assert_eq!(polled.messages.len(), 10);

    // Wait for flush
    sleep(Duration::from_secs(1)).await;
    drop(client);

    // Stop server
    harness.server_mut().stop().expect("Failed to stop server");
    sleep(Duration::from_secs(1)).await;

    // Tamper with consumer offset file to simulate crash scenario:
    // consumer offset persisted ahead of actual data
    let data_path = harness.server().data_path();
    let offset_file = data_path.join(format!(
        "streams/{stream_id}/topics/{topic_id}/partitions/0/offsets/consumers/{CONSUMER_ID}"
    ));
    assert!(
        offset_file.exists(),
        "Consumer offset file should exist at {}",
        offset_file.display()
    );
    std::fs::write(&offset_file, 999_u64.to_le_bytes()).expect("Failed to write offset file");

    // Verify the tampered value
    let bytes = std::fs::read(&offset_file).unwrap();
    let stored = u64::from_le_bytes(bytes.try_into().unwrap());
    assert_eq!(stored, 999, "Offset file should contain 999");

    // Restart server
    harness
        .server_mut()
        .start()
        .expect("Failed to start server");

    let client = harness
        .root_client()
        .await
        .expect("Failed to create post-restart client");

    // Send 5 more messages (should get offsets 10-14)
    for i in 0..5u32 {
        let mut msg = [IggyMessage::from_str(&format!("post-crash-{i}")).unwrap()];
        client
            .send_messages(
                &Identifier::named(STREAM).unwrap(),
                &Identifier::named(TOPIC).unwrap(),
                &Partitioning::partition_id(0),
                &mut msg,
            )
            .await
            .unwrap();
    }

    // Consumer polls with Next - must NOT be stuck at offset 1000 (999+1)
    let consumer = Consumer::new(Identifier::numeric(CONSUMER_ID).unwrap());
    let polled = client
        .poll_messages(
            &Identifier::named(STREAM).unwrap(),
            &Identifier::named(TOPIC).unwrap(),
            Some(0),
            &consumer,
            &PollingStrategy::next(),
            100,
            true,
        )
        .await
        .unwrap();

    assert!(
        !polled.messages.is_empty(),
        "BUG: consumer is permanently stuck - offset 999 is ahead of partition data. \
         After crash recovery, consumer offsets must be clamped to partition offset."
    );

    // Verify we got the new messages
    let first_offset = polled.messages[0].header.offset;
    assert!(
        first_offset <= 14,
        "BUG: consumer skipped to offset {first_offset}, expected messages in range 10-14"
    );
}
