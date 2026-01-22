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
use iggy::prelude::*;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::str::FromStr;

const STREAM: &str = "compression_stream";
const TOPIC: &str = "compression_topic";
const NUM_MESSAGES: u32 = 1000;
const COMPRESSION_HEADER: &str = "iggy-compression";

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
        .create_stream(STREAM)
        .await
        .expect("Stream was NOT created! Start a fresh server to run this example.");

    client
        .create_topic(
            &Identifier::named(STREAM).unwrap(),
            TOPIC,
            1,
            CompressionAlgorithm::None, // NOTE: This configures the compression on the server, not the actual messages in transit!
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("Topic was NOT created! Start a fresh server to run this example.");

    // Uncompressed messages
    let compressor = PayloadCompression::Lz4;
    let key =
        HeaderKey::from_str(COMPRESSION_HEADER).expect("Compression header should be parseable.");
    let value = HeaderValue::from_str(&compressor.to_string())
        .expect("Compression algorithm should be parseable.");
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
        let compressed_payload = compressor
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
    let producer = client.producer(STREAM, TOPIC)?.build();
    producer
        .send(messages)
        .await
        .expect("Message sending failed.");
    Ok(())
}

enum PayloadCompression {
    None,
    Lz4,
}

impl Display for PayloadCompression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PayloadCompression::None => write!(f, "none"),
            PayloadCompression::Lz4 => write!(f, "lz4"),
        }
    }
}

impl PayloadCompression {
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        match self {
            PayloadCompression::None => Ok(data.to_vec()),
            PayloadCompression::Lz4 => {
                let mut compressed_data = Vec::new();
                let mut encoder = FrameEncoder::new(&mut compressed_data);
                encoder
                    .write_all(data)
                    .expect("Cannot write into buffer using Lz4 compression.");
                encoder.finish().expect("Cannot finish Lz4 compression.");
                Ok(compressed_data)
            }
        }
    }

    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        match self {
            PayloadCompression::None => Ok(data.to_vec()),
            PayloadCompression::Lz4 => {
                let mut decoder = FrameDecoder::new(data);
                let mut decompressed_data = Vec::new();
                decoder
                    .read_to_end(&mut decompressed_data)
                    .expect("Cannot decode message payload using Lz4.");
                Ok(decompressed_data)
            }
        }
    }
}
