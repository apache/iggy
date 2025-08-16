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

mod background;

use std::net::SocketAddr;
use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use iggy::{clients::client::IggyClient, connection::tokio_tcp};
use iggy::prelude::*;
use integration::test_server::{login_root, TestServer};
use iggy::connection::{NewTcpClient, SFut, SocketFactory, TokioCompat, TokioRuntime};
use tokio::time::sleep;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;
const STREAM_NAME: &str = "test-stream-producer";
const TOPIC_NAME: &str = "test-topic-producer";
const PARTITIONS_COUNT: u32 = 3;

fn create_message_payload(offset: u64) -> Bytes {
    Bytes::from(format!("message {offset}"))
}

async fn init_system(client: &IggyClient) {
    // 1. Create the stream
    client
        .create_stream(STREAM_NAME, Some(STREAM_ID))
        .await
        .unwrap();

    // 2. Create the topic
    client
        .create_topic(
            &Identifier::numeric(STREAM_ID).unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            CompressionAlgorithm::default(),
            None,
            Some(TOPIC_ID),
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
}

async fn cleanup(system_client: &IggyClient) {
    system_client
        .delete_stream(&Identifier::numeric(STREAM_ID).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_async_send() {
    let mut test_server = TestServer::default();
    test_server.start();

    let tcp_client_config = TcpClientConfig {
        server_address: test_server.get_raw_tcp_addr().unwrap(),
        ..TcpClientConfig::default()
    };

    // let client = ClientWrapper::Tcp(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
    // let client = IggyClient::create(client, None, None);

    let factory: SocketFactory<TokioCompat> = Arc::new(move |addr: SocketAddr| -> SFut<TokioCompat> {
        Box::pin(tokio_tcp(addr))
    });
    let tcp_client = NewTcpClient::create(Arc::new(tcp_client_config), factory).unwrap();

    let client = IggyClient::create(iggy::prelude::ClientWrapper::New(tcp_client), None, None);

    client.connect().await.unwrap();
    sleep(Duration::from_secs(5)).await;
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
        .producer(&STREAM_ID.to_string(), &TOPIC_ID.to_string())
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
            &Identifier::numeric(STREAM_ID).unwrap(),
            &Identifier::numeric(TOPIC_ID).unwrap(),
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
