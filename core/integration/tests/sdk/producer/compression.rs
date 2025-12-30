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

use crate::sdk::producer::{cleanup, init_system};
use bytes::Bytes;
use futures::StreamExt;
use iggy::prelude::*;
use iggy::{clients::client::IggyClient, prelude::TcpClient};
use iggy_common::{ClientCompressionConfig, TcpClientConfig};
use integration::test_server::{TestServer, login_root};
use serial_test::parallel;
use std::sync::Arc;
use std::time::Duration;
use test_case::test_matrix;
use tokio::time::sleep;

const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";

fn none() -> CompressionAlgorithm {
    CompressionAlgorithm::None
}
fn gzip() -> CompressionAlgorithm {
    CompressionAlgorithm::Gzip
}
fn zstd() -> CompressionAlgorithm {
    CompressionAlgorithm::Zstd
}
fn lz4() -> CompressionAlgorithm {
    CompressionAlgorithm::Lz4
}
fn snappy() -> CompressionAlgorithm {
    CompressionAlgorithm::Snappy
}

fn compressible_payload(size: usize) -> (Bytes, String) {
    let sample = "Test payload for compression.";
    let n_reps = size.div_ceil(sample.len());
    let payload_str: String = sample.repeat(n_reps);
    let payload_str = payload_str[..size].to_string();
    (Bytes::from(payload_str.clone()), payload_str)
}

#[test_matrix([none(), gzip(), zstd(), lz4(), snappy()])]
#[tokio::test]
#[parallel]
async fn compression_send_receive_ok(algorithm: CompressionAlgorithm) {
    // setup
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

    // producer
    client.create_stream(STREAM_NAME).await.unwrap();
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let compression_config = ClientCompressionConfig {
        algorithm,
        min_size: 128,
    };

    let producer = client
        .producer(STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .compressor(compression_config)
        .build();

    producer.init().await.unwrap();

    // messages
    let (payload_bytes_one, payload_str_one) = compressible_payload(128);
    let payload_len_one = payload_bytes_one.len();
    let message_one = IggyMessage::builder()
        .id(1)
        .payload(payload_bytes_one)
        .build()
        .unwrap();

    let (payload_bytes_two, payload_str_two) = compressible_payload(256);
    let payload_len_two = payload_bytes_two.len();
    let message_two = IggyMessage::builder()
        .id(1)
        .payload(payload_bytes_two)
        .build()
        .unwrap();

    let (payload_bytes_eight, payload_str_eight) = compressible_payload(1024);
    let payload_len_eight = payload_bytes_eight.len();
    let message_eight = IggyMessage::builder()
        .id(1)
        .payload(payload_bytes_eight)
        .build()
        .unwrap();

    // send to server
    producer
        .send(vec![message_one, message_two, message_eight])
        .await
        .unwrap();
    sleep(Duration::from_millis(500)).await;
    producer.shutdown().await;

    // poll directly from server
    let consumer = Consumer::default();
    let polled = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(0),
            &consumer,
            &PollingStrategy::offset(0),
            3,
            false,
        )
        .await
        .unwrap();

    // dont compress min_size
    assert_eq!(
        polled.messages[0].header.payload_length, payload_len_one as u32,
        "128-byte message should not be compressed (at min_size threshold)"
    );

    if algorithm == CompressionAlgorithm::None {
        assert_eq!(
            polled.messages[1].header.payload_length, payload_len_two as u32,
            "256-byte message should not be compressed with None algorithm"
        );
        assert_eq!(
            polled.messages[2].header.payload_length, payload_len_eight as u32,
            "1024-byte message should not be compressed with None algorithm"
        );
    } else {
        // message on server should be smaller
        assert!(
            polled.messages[1].header.payload_length < payload_len_two as u32,
            "256-byte message should be compressed with {:?}",
            algorithm
        );
        assert!(
            polled.messages[2].header.payload_length < payload_len_eight as u32,
            "1024-byte message should be compressed with {:?}",
            algorithm
        );
    }

    let mut consumer = client
        .consumer("test-consumer", STREAM_NAME, TOPIC_NAME, 0)
        .unwrap()
        .polling_strategy(PollingStrategy::offset(0))
        .build();
    consumer.init().await.unwrap();

    // test consumer decompression
    let msg1 = consumer.next().await.unwrap().unwrap();
    assert_eq!(
        std::str::from_utf8(&msg1.message.payload).unwrap(),
        payload_str_one,
        "128-byte message payload mismatch after decompression"
    );
    let msg2 = consumer.next().await.unwrap().unwrap();
    assert_eq!(
        std::str::from_utf8(&msg2.message.payload).unwrap(),
        payload_str_two,
        "256-byte message payload mismatch after decompression"
    );
    let msg3 = consumer.next().await.unwrap().unwrap();
    assert_eq!(
        std::str::from_utf8(&msg3.message.payload).unwrap(),
        payload_str_eight,
        "1024-byte message payload mismatch after decompression"
    );

    // cleanup
    cleanup(&client).await;
}
