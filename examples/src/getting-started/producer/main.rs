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

use iggy::client::{Client, StreamClient, TopicClient, UserClient};
use iggy::clients::builder::IggyClientBuilder;
use iggy::clients::client::IggyClient;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::users::defaults::*;
use iggy::utils::duration::IggyDuration;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use std::env;
use std::error::Error;
use std::str::FromStr;
use tracing::{info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;
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
    init_system(&client).await;
    produce_messages(&client).await
}

async fn init_system(client: &IggyClient) {
    match client.create_stream("sample-stream", Some(STREAM_ID)).await {
        Ok(_) => info!("Stream was created."),
        Err(_) => warn!("Stream already exists and will not be created again."),
    }

    match client
        .create_topic(
            &STREAM_ID.try_into().unwrap(),
            "sample-topic",
            1,
            CompressionAlgorithm::default(),
            None,
            Some(TOPIC_ID),
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
    {
        Ok(_) => info!("Topic was created."),
        Err(_) => warn!("Topic already exists and will not be created again."),
    }
}

async fn produce_messages(client: &dyn Client) -> Result<(), Box<dyn Error>> {
    let duration = IggyDuration::from_str("500ms")?;
    let mut interval = tokio::time::interval(duration.get_duration());
    info!(
        "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {}.",
        STREAM_ID,
        TOPIC_ID,
        PARTITION_ID,
        duration.as_human_time_string()
    );

    let mut current_id = 0;
    let messages_per_batch = 10;
    let mut sent_batches = 0;
    let partitioning = Partitioning::partition_id(PARTITION_ID);
    loop {
        if sent_batches == BATCHES_LIMIT {
            info!("Sent {sent_batches} batches of messages, exiting.");
            return Ok(());
        }

        interval.tick().await;
        let mut messages = Vec::new();
        for _ in 0..messages_per_batch {
            current_id += 1;
            let payload = format!("message-{current_id}");
            let message = Message::from_str(&payload)?;
            messages.push(message);
        }
        client
            .send_messages(
                &STREAM_ID.try_into().unwrap(),
                &TOPIC_ID.try_into().unwrap(),
                &partitioning,
                &mut messages,
            )
            .await?;
        sent_batches += 1;
        info!("Sent {messages_per_batch} message(s).");
    }
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
