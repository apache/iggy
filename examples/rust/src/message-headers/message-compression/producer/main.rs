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

/// In the following couple of lines, we will write a small program, that connects and sends a couple of messages to an Iggy server.
/// The messages are then consumed by another small program for which the code sits in ` ../consumer/main.rs`.
///
/// The goal of this example is to illustrate how message user-headers can be used to implement additional special features or logic.
/// Specifically, we will compress our messages before sending to the server and `../consumer/main.rs` will decompress those messages
/// after reading them back from the server.
///
/// Troughout this example we will use the high-level API from the Iggy SDK.
///
/// Step-by-step guide
/// 1. Setup a client
/// 2. Connect to the iggy-server
/// 3. Generate payloads
/// 4. Compress payloads
/// 5. Transform payloads into Iggy messages to be send to the server
/// 6. Send Iggy messages to the server
// The compression and decompression utilities are shared between the producer and consumer compression examples.
// Hence, we import them here.
#[path = "../codec.rs"]
mod codec;

use bytes::Bytes;
use codec::{Codec, NUM_MESSAGES, STREAM_NAME, TOPIC_NAME};
use iggy::prelude::*;
use std::collections::HashMap;

/// Since we are communicating with a server, a lot of network I/O operations will happen.
/// We do not want to block ourselves while waiting for responses until we can do the next bit of work.
/// The Iggy SDK is mostly asynchronous by using tokio. Hence, we setup a tokio runtime for our main function.
#[tokio::main]
async fn main() -> Result<(), IggyError> {
    // Setup a server client to create a stream and topic to send messages to.
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address("127.0.0.1:8090".to_string())
        .build()?;
    client.connect().await?;

    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;

    client
        .create_stream(STREAM_NAME)
        .await
        .expect("Stream was NOT created! Start a fresh server to run this example.");

    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None, // NOTE: This configures the compression on the server, not the actual messages in transit!
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("Topic was NOT created! Start a fresh server to run this example.");

    // Uncompressed messages
    let codec = Codec::Lz4;
    let key = Codec::header_key();
    let value = codec.to_header_value();
    let compression_headers = HashMap::from([(key, value)]);

    let mut messages = Vec::new();
    for i in 0..NUM_MESSAGES {
        let payload = format!(
            r#"{{"ts": "2000-01-{:02}T{:02}:{:02}:{:02}Z", "level": "info", "trace":{}, "command": "command-{}", "status": 200, "latency_ms": {}}}"#,
            i % 28,
            i % 24,
            i % 60,
            i % 60,
            i,
            i % 1000,
            i % 120
        );
        let payload = Bytes::from(payload);
        let compressed_payload = codec
            .compress(&payload)
            .expect("Payload should be compressable.");
        let compressed_bytes = Bytes::from(compressed_payload);

        let msg = IggyMessage::builder()
            .payload(compressed_bytes)
            .user_headers(compression_headers.clone())
            .build()
            .expect("IggyMessage should be buildable.");
        messages.push(msg);
    }
    let producer = client.producer(STREAM_NAME, TOPIC_NAME)?.build();
    producer
        .send(messages)
        .await
        .expect("Message sending failed.");
    Ok(())
}
