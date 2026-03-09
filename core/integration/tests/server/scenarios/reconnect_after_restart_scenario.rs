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

    assert_eq!(
        consume_messages(&mut consumer, 3, Duration::from_secs(10)).await,
        3,
        "Should consume all pre-restart messages"
    );

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

    assert_eq!(
        consume_messages(&mut consumer, 3, Duration::from_secs(30)).await,
        3,
        "Should consume all post-restart messages after reconnect"
    );
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

async fn consume_messages(consumer: &mut IggyConsumer, expected: u32, max_wait: Duration) -> u32 {
    let mut consumed = 0u32;
    let deadline = tokio::time::Instant::now() + max_wait;
    while consumed < expected && tokio::time::Instant::now() < deadline {
        let poll_result: Option<Result<ReceivedMessage, IggyError>> =
            tokio::time::timeout(Duration::from_millis(500), consumer.next())
                .await
                .ok()
                .flatten();
        if let Some(Ok(_)) = poll_result {
            consumed += 1;
        }
    }
    consumed
}
