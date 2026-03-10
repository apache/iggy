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

// TCP/TLS Consumer Example
//
// Demonstrates how to consume messages from an Iggy server over a TLS-encrypted
// TCP connection using custom certificates from core/certs/.
//
// Prerequisites:
//   Start the Iggy server with TLS enabled:
//     IGGY_TCP_TLS_ENABLED=true \
//     IGGY_TCP_TLS_CERT_FILE=core/certs/iggy_cert.pem \
//     IGGY_TCP_TLS_KEY_FILE=core/certs/iggy_key.pem \
//     cargo r --bin iggy-server
//
// Run this example (from repo root):
//   cargo run --example tcp-tls-consumer -p iggy_examples

use iggy::prelude::*;
use std::error::Error;
use std::str::FromStr;
use tokio::time::sleep;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

const STREAM_NAME: &str = "tls-stream";
const TOPIC_NAME: &str = "tls-topic";
const PARTITION_ID: u32 = 0;
const BATCHES_LIMIT: u32 = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .init();

    // Build a TCP client with TLS enabled.
    // with_tls_enabled(true)     activates TLS on the TCP transport
    // with_tls_domain(...)       sets the expected server hostname for certificate verification
    // with_tls_ca_file(...)      points to the CA certificate used to verify the server cert
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address("127.0.0.1:8090".to_string())
        .with_tls_enabled(true)
        .with_tls_domain("localhost".to_string())
        .with_tls_ca_file("core/certs/iggy_ca_cert.pem".to_string())
        .build()?;

    client.connect().await?;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;
    info!("Connected and logged in over TLS.");

    consume_messages(&client).await
}

async fn consume_messages(client: &dyn Client) -> Result<(), Box<dyn Error>> {
    let interval = IggyDuration::from_str("500ms")?;
    info!(
        "Messages will be consumed from stream: {}, topic: {}, partition: {} with interval {}.",
        STREAM_NAME,
        TOPIC_NAME,
        PARTITION_ID,
        interval.as_human_time_string()
    );

    let mut offset = 0;
    let messages_per_batch = 10;
    let mut consumed_batches = 0;
    let consumer = Consumer::default();
    loop {
        if consumed_batches == BATCHES_LIMIT {
            info!("Consumed {consumed_batches} batches of messages, exiting.");
            return Ok(());
        }

        let polled_messages = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                Some(PARTITION_ID),
                &consumer,
                &PollingStrategy::offset(offset),
                messages_per_batch,
                false,
            )
            .await?;

        if polled_messages.messages.is_empty() {
            info!("No messages found.");
            sleep(interval.get_duration()).await;
            continue;
        }

        offset += polled_messages.messages.len() as u64;
        for message in polled_messages.messages {
            handle_message(&message)?;
        }
        consumed_batches += 1;
        sleep(interval.get_duration()).await;
    }
}

fn handle_message(message: &IggyMessage) -> Result<(), Box<dyn Error>> {
    let payload = std::str::from_utf8(&message.payload)?;
    info!(
        "Handling message at offset: {}, payload: {}...",
        message.header.offset, payload
    );
    Ok(())
}
