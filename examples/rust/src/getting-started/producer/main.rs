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
use tracing::{info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

const STREAM_NAME: &str = "sample-stream";
const TOPIC_NAME: &str = "sample-topic";
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
    let (stream_id, topic_id) = init_system(&client).await;
    produce_messages(&client, stream_id, topic_id).await
}

async fn init_system(client: &IggyClient) -> (u32, u32) {
    let stream = match client.create_stream(STREAM_NAME).await {
        Ok(stream) => {
            info!("Stream was created.");
            stream
        }
        Err(_) => {
            warn!("Stream already exists and will not be created again.");
            client
                .get_stream(&Identifier::named(STREAM_NAME).unwrap())
                .await
                .unwrap()
                .expect("Failed to get stream")
        }
    };

    let topic = match client
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
    {
        Ok(topic) => {
            info!("Topic was created.");
            topic
        }
        Err(_) => {
            warn!("Topic already exists and will not be created again.");
            client
                .get_topic(
                    &Identifier::named(STREAM_NAME).unwrap(),
                    &Identifier::named(TOPIC_NAME).unwrap(),
                )
                .await
                .unwrap()
                .expect("Failed to get topic")
        }
    };

    (stream.id, topic.id)
}

async fn produce_messages(
    client: &dyn Client,
    stream_id: u32,
    topic_id: u32,
) -> Result<(), Box<dyn Error>> {
    let duration = IggyDuration::from_str("500ms")?;
    let mut interval = tokio::time::interval(duration.get_duration());
    info!(
        "Messages will be sent to stream: {} ({}), topic: {} ({}), partition: {} with interval {}.",
        STREAM_NAME,
        stream_id,
        TOPIC_NAME,
        topic_id,
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
            let message = IggyMessage::from_str(&payload)?;
            messages.push(message);
        }
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
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
