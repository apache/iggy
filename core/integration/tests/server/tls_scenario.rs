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
use integration::iggy_harness;

const STREAM_NAME: &str = "test-tls-stream";
const TOPIC_NAME: &str = "test-tls-topic";

/// Tests TLS with both self-signed and harness-generated certificates for TCP.
/// - SelfSigned: Server generates its own certs, client disables validation
/// - Generated: Harness generates certs, client validates using CA cert
#[iggy_harness(transport = [TcpTlsSelfSigned, TcpTlsGenerated])]
async fn tcp_tls_scenario(harness: &TestHarness) {
    let client = harness.tcp_root_client().await.unwrap();
    run_tls_test(&client).await;
}

/// Tests TLS with both self-signed and harness-generated certificates for WebSocket.
/// - SelfSigned: Server generates its own certs, client disables validation
/// - Generated: Harness generates certs, client validates using CA cert
#[iggy_harness(transport = [WebSocketTlsSelfSigned, WebSocketTlsGenerated])]
async fn websocket_tls_scenario(harness: &TestHarness) {
    let client = harness.websocket_root_client().await.unwrap();
    run_tls_test(&client).await;
}

async fn run_tls_test(client: &IggyClient) {
    client.create_stream(STREAM_NAME).await.unwrap();

    let stream = client
        .get_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stream.name, STREAM_NAME);

    client
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

    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from("Hello TLS!"))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    let polled_messages = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            true,
        )
        .await
        .unwrap();

    assert_eq!(polled_messages.messages.len(), 1);
    assert_eq!(polled_messages.messages[0].payload.as_ref(), b"Hello TLS!");

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}
