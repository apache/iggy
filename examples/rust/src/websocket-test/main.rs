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
use std::sync::Arc;
use std::{error::Error, str::FromStr};
use tracing::info;
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .init();

    info!("Testing WebSocket client...");

    // For now, create a simple WebSocket client without auto-login
    // We'll connect and then login manually like other examples
    let websocket_config = WebSocketClientConfig {
        server_address: "127.0.0.1:8080".to_string(), // WebSocket port
        auto_login: AutoLogin::Disabled,              // Disable auto-login for now
        ..Default::default()
    };

    let websocket_client = WebSocketClient::create(Arc::new(websocket_config))?;
    let client = IggyClient::builder()
        .with_client(ClientWrapper::WebSocket(websocket_client))
        .build()?;

    // Test basic connectivity
    info!("Connecting to server...");
    client.connect().await?;
    info!("✓ Connected to server");

    // // Manual login like other examples do
    // info!("Logging in...");
    // client.login_user("iggy", "iggy").await?;
    // info!("✓ Login successful");

    // // Test ping/health check
    // info!("Testing ping...");
    // client.ping().await?;
    // info!("✓ Ping successful");

    // Test stream operations
    // let stream_name = "websocket-test-stream";
    // let topic_name = "websocket-test-topic";
    // let stream_id = Identifier::named(stream_name)?;
    // let topic_id = Identifier::named(topic_name)?;

    // info!("Creating test stream...");
    // match client.create_stream(stream_name, None).await {
    //     Ok(_) => info!("✓ Stream created"),
    //     Err(IggyError::StreamNameAlreadyExists(_)) => info!("✓ Stream already exists"),
    //     Err(e) => return Err(e.into()),
    // }

    // info!("Creating test topic...");
    // match client
    //     .create_topic(
    //         &stream_id,
    //         topic_name,
    //         1, // partitions
    //         CompressionAlgorithm::None,
    //         None,
    //         None,
    //         IggyExpiry::NeverExpire,
    //         MaxTopicSize::ServerDefault,
    //     )
    //     .await
    // {
    //     Ok(_) => info!("✓ Topic created"),
    //     Err(IggyError::TopicNameAlreadyExists(_, _)) => info!("✓ Topic already exists"),
    //     Err(e) => return Err(e.into()),
    // }

    // // Test message production
    // info!("Sending test messages...");
    // let test_messages = vec![
    //     IggyMessage::from_str("Hello WebSocket!")?,
    //     IggyMessage::from_str("Testing binary protocol over WebSocket")?,
    //     IggyMessage::from_str("Message 3 with some data: 12345")?,
    // ];

    // let mut messages = test_messages;
    // client
    //     .send_messages(
    //         &stream_id,
    //         &topic_id,
    //         &Partitioning::partition_id(1),
    //         &mut messages,
    //     )
    //     .await?;
    // info!("✓ Messages sent successfully");

    // // Test message consumption
    // info!("Polling messages...");
    // let polled_messages = client
    //     .poll_messages(
    //         &stream_id,
    //         &topic_id,
    //         Some(1),
    //         &Consumer::default(),
    //         &PollingStrategy::offset(0),
    //         3,
    //         false,
    //     )
    //     .await?;

    // info!("✓ Polled {} messages", polled_messages.messages.len());
    // for (i, message) in polled_messages.messages.iter().enumerate() {
    //     let payload = String::from_utf8_lossy(&message.payload);
    //     info!(
    //         "  Message {}: {} (offset: {})",
    //         i + 1,
    //         payload,
    //         message.header.offset
    //     );
    // }

    // // Test stats
    // info!("Getting server stats...");
    // let stats = client.get_stats().await?;
    // info!(
    //     "✓ Stats retrieved - {} streams, {} topics",
    //     stats.streams_count, stats.topics_count
    // );

    // // Cleanup
    // info!("Cleaning up test data...");
    // client.delete_stream(&stream_id).await.ok(); // Ignore errors during cleanup

    // info!("Disconnecting...");
    // client.disconnect().await?;

    // info!("✓ WebSocket client test completed successfully!");
    Ok(())
}
