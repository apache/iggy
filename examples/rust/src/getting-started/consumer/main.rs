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

use iggy::prelude::*;
use std::env;
use std::error::Error;
use std::str::FromStr;
use tokio::time::sleep;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

const STREAM_ID: u32 = 0;
const TOPIC_ID: u32 = 0;
const PARTITION_ID: u32 = 0;
const BATCHES_LIMIT: u32 = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .init();
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(get_tcp_server_addr())
        .build()?;

    // Or, instead of above lines, you can just use below code, which will create a Iggy
    // TCP client with default config (default server address for TCP is 127.0.0.1:8090):
    // let client = IggyClient::default();

    client.connect().await?;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;

    consume_messages(&client).await
}

async fn consume_messages(client: &dyn Client) -> Result<(), Box<dyn Error>> {
    let interval = IggyDuration::from_str("500ms")?;
    info!(
        "Messages will be consumed from stream: {}, topic: {}, partition: {} with interval {}.",
        STREAM_ID,
        TOPIC_ID,
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
                &STREAM_ID.try_into()?,
                &TOPIC_ID.try_into()?,
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
    // The payload can be of any type as it is a raw byte array. In this case it's a simple string.
    let payload = std::str::from_utf8(&message.payload)?;
    info!(
        "Handling message at offset: {}, payload: {}...",
        message.header.offset, payload
    );
    Ok(())
}

fn get_tcp_server_addr() -> String {
    let default_server_addr = "127.0.0.1:8090".to_string();
    let argument_name = env::args().nth(1);
    let tcp_server_addr = env::args().nth(2);

    if argument_name.is_none() && tcp_server_addr.is_none() {
        default_server_addr
    } else {
        let argument_name = argument_name.unwrap();
        if argument_name != "--tcp-server-address" {
            panic!(
                "Invalid argument {}! Usage: {} --tcp-server-address <server-address>",
                argument_name,
                env::args().next().unwrap()
            );
        }
        let tcp_server_addr = tcp_server_addr.unwrap();
        if tcp_server_addr.parse::<std::net::SocketAddr>().is_err() {
            panic!(
                "Invalid server address {}! Usage: {} --tcp-server-address <server-address>",
                tcp_server_addr,
                env::args().next().unwrap()
            );
        }
        info!("Using server address: {}", tcp_server_addr);
        tcp_server_addr
    }
}
