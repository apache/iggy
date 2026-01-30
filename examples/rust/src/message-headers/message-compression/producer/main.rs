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
use std::collections::HashMap;
// The compression and decompression utilities are shared between the producer and consumer compression examples.
// Hence, we import them here.
use iggy_examples::shared::codec::{Codec, NUM_MESSAGES, STREAM_NAME, TOPIC_NAME};

#[tokio::main]
async fn main() -> Result<(), IggyError> {
    // Setup a client to connect to the iggy-server via TCP.
    let client = IggyClientBuilder::new().with_tcp().build()?;
    client.connect().await?;

    // Login using default credentials.
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;

    // Create a Stream.
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("Stream was NOT created! Remove /local_data or start a fresh server with the --fresh flag to run this example.");

    // Create a Topic on that Stream.
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,                           // Number of partitions.
            CompressionAlgorithm::None, // NOTE: This configures the compression on the server, not the actual messages in transit!
            None,                       // Replication factor.
            IggyExpiry::NeverExpire,    // Time until messages expire on the server.
            MaxTopicSize::ServerDefault, // Defined in server/config.toml. Defaults to "unlimited".
        )
        .await
        .expect("Topic was NOT created! Start a fresh server to run this example.");

    // The Codec from ../compression.rs implements the compression and decompression utilities.
    let codec = Codec::Lz4;
    // NOTE: This is where the Codec is used to prepare the compression user-header for the IggyMessage.
    let key = Codec::header_key();
    let value = codec.to_header_value();
    let compression_headers = HashMap::from([(key, value)]);

    // Generate artificial example messages to send to the server.
    let mut messages = Vec::new();
    for i in 0..NUM_MESSAGES {
        // For illustration purposes a log-like pattern is resembled.
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
        let compressed_payload = codec.compress(&payload);
        let compressed_bytes = Bytes::from(compressed_payload);

        let msg = IggyMessage::builder()
            .payload(compressed_bytes)
            // NOTE: This is where the user_headers of IggyMessages are used to indicate, that a payload is compressed.
            .user_headers(compression_headers.clone())
            .build()
            .expect("IggyMessage should be buildable.");
        messages.push(msg);
    }

    // Send all compressed messages to the server.
    let producer = client.producer(STREAM_NAME, TOPIC_NAME)?.build();
    producer
        .send(messages)
        .await
        .expect("Message sending failed.");

    println!("All messages sent to server.");

    Ok(())
}
