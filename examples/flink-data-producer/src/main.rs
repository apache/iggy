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

use clap::Parser;
use iggy::prelude::*;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "flink-demo")]
    stream_id: String,

    #[arg(long, default_value = "sensor-data")]
    topic_id: String,

    #[arg(long, default_value = "1000")]
    interval_ms: u64,

    #[arg(long, default_value = "100")]
    batch_size: u32,

    #[arg(short = 'c', long)]
    continuous: bool,

    #[arg(short = 'n', long, default_value = "1000")]
    num_messages: u32,

    #[arg(long, default_value = DEFAULT_ROOT_USERNAME)]
    username: String,

    #[arg(long, default_value = DEFAULT_ROOT_PASSWORD)]
    password: String,

    #[arg(long, default_value = "tcp")]
    transport: String,

    #[arg(long, default_value = "127.0.0.1:8090")]
    tcp_server_address: String,

    #[arg(long, default_value = "http://localhost:3000")]
    http_api_url: String,
}

impl Args {
    fn to_sdk_args(&self) -> iggy::prelude::Args {
        iggy::prelude::Args {
            username: self.username.clone(),
            password: self.password.clone(),
            tcp_server_address: self.tcp_server_address.clone(),
            http_api_url: self.http_api_url.clone(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SensorData {
    id: Uuid,
    timestamp: i64,
    sensor_id: String,
    temperature: f64,
    humidity: f64,
    pressure: f64,
    location: Location,
}

#[derive(Debug, Serialize, Deserialize)]
struct Location {
    lat: f64,
    lon: f64,
    city: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("flink_data_producer=info".parse()?)
                .add_directive("iggy=info".parse()?),
        )
        .init();

    let args = Args::parse();
    info!("Starting Flink data producer");
    info!("Stream: {}, Topic: {}", args.stream_id, args.topic_id);

    // Create Iggy client using the pattern from examples
    let client = build_client(&args).await?;

    // Ensure stream and topic exist
    setup_stream_and_topic(&client, &args.stream_id, &args.topic_id).await?;

    // Start producing messages
    if args.continuous {
        info!("Running in continuous mode (press Ctrl+C to stop)");
        produce_continuous(&client, &args).await?;
    } else {
        info!("Producing {} messages", args.num_messages);
        produce_batch(&client, &args, args.num_messages).await?;
    }

    info!("Data producer finished");
    Ok(())
}

async fn build_client(args: &Args) -> Result<IggyClient, Box<dyn std::error::Error>> {
    use iggy::client_provider;
    use iggy::client_provider::ClientProviderConfig;
    use std::sync::Arc;

    let sdk_args = args.to_sdk_args();

    // Build client provider configuration
    let client_provider_config = Arc::new(
        ClientProviderConfig::from_args(sdk_args).expect("Failed to create client provider config"),
    );

    // Build client_provider
    let raw_client = client_provider::get_raw_client(client_provider_config, true)
        .await
        .expect("Failed to build client provider");

    // Build client
    let client = IggyClient::builder().with_client(raw_client).build()?;

    info!("Connected to Iggy server");
    Ok(client)
}

async fn setup_stream_and_topic(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if stream exists, create if not
    match client.get_streams().await {
        Ok(streams) => {
            let stream_exists = streams.iter().any(|s| s.name == stream_name);
            if !stream_exists {
                info!("Creating stream: {}", stream_name);
                match client.create_stream(stream_name, None).await {
                    Ok(_) => info!("Stream '{}' created successfully", stream_name),
                    Err(e) => {
                        error!("Failed to create stream '{}': {}", stream_name, e);
                        return Err(Box::new(e));
                    }
                }
            } else {
                info!("Stream '{}' already exists", stream_name);
            }
        }
        Err(e) => {
            error!("Failed to get streams: {}", e);
            return Err(Box::new(e));
        }
    }

    // Check if topic exists, create if not
    match client.get_topics(&stream_name.try_into()?).await {
        Ok(topics) => {
            let topic_exists = topics.iter().any(|t| t.name == topic_name);
            if !topic_exists {
                info!("Creating topic: {} with 1 partition", topic_name);
                match client
                    .create_topic(
                        &stream_name.try_into()?,
                        topic_name,
                        1,                          // partitions (1-indexed in Iggy)
                        CompressionAlgorithm::None, // compression algorithm
                        None,                       // replication factor
                        None,                       // topic id
                        IggyExpiry::NeverExpire,    // message expiry
                        MaxTopicSize::Unlimited,    // max topic size
                    )
                    .await
                {
                    Ok(_) => info!(
                        "Topic '{}' created successfully with 1 partition",
                        topic_name
                    ),
                    Err(e) => {
                        error!("Failed to create topic '{}': {}", topic_name, e);
                        return Err(Box::new(e));
                    }
                }
            } else {
                info!("Topic '{}' already exists", topic_name);
            }
        }
        Err(e) => {
            error!("Failed to get topics for stream '{}': {}", stream_name, e);
            return Err(Box::new(e));
        }
    }

    Ok(())
}

async fn produce_batch(
    client: &IggyClient,
    args: &Args,
    num_messages: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut messages_sent = 0u32;
    let mut batch = Vec::new();

    for i in 0..num_messages {
        let message = create_sensor_message(i)?;
        batch.push(message);

        if batch.len() as u32 >= args.batch_size || i == num_messages - 1 {
            send_batch(client, &args.stream_id, &args.topic_id, &mut batch).await?;
            messages_sent += batch.len() as u32;
            info!("Sent {} messages (total: {})", batch.len(), messages_sent);
            batch.clear();

            if i < num_messages - 1 {
                sleep(Duration::from_millis(args.interval_ms)).await;
            }
        }
    }

    Ok(())
}

async fn produce_continuous(
    client: &IggyClient,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut messages_sent = 0u32;
    let mut batch_number = 0u32;

    loop {
        let mut batch = Vec::new();

        for i in 0..args.batch_size {
            let message = create_sensor_message(batch_number * args.batch_size + i)?;
            batch.push(message);
        }

        if let Err(e) = send_batch(client, &args.stream_id, &args.topic_id, &mut batch).await {
            error!("Failed to send batch: {}", e);
            // Continue producing even if a batch fails
        } else {
            messages_sent += batch.len() as u32;
            info!(
                "Sent batch {} ({} messages, total: {})",
                batch_number,
                batch.len(),
                messages_sent
            );
        }

        batch_number += 1;
        sleep(Duration::from_millis(args.interval_ms)).await;
    }
}

fn create_sensor_message(index: u32) -> Result<IggyMessage, Box<dyn std::error::Error>> {
    let sensor_data = SensorData {
        id: Uuid::new_v4(),
        timestamp: chrono::Utc::now().timestamp_millis(),
        sensor_id: format!("sensor-{:04}", index % 100),
        temperature: 20.0 + (rand::random::<f64>() * 10.0),
        humidity: 40.0 + (rand::random::<f64>() * 30.0),
        pressure: 1000.0 + (rand::random::<f64>() * 50.0),
        location: Location {
            lat: 37.7749 + (rand::random::<f64>() * 0.1),
            lon: -122.4194 + (rand::random::<f64>() * 0.1),
            city: "San Francisco".to_string(),
        },
    };

    let payload = serde_json::to_string(&sensor_data)?;
    Ok(IggyMessage::from_str(&payload)?)
}

async fn send_batch(
    client: &IggyClient,
    stream: &str,
    topic: &str,
    messages: &mut [IggyMessage],
) -> Result<(), Box<dyn std::error::Error>> {
    client
        .send_messages(
            &stream.try_into()?,
            &topic.try_into()?,
            &Partitioning::partition_id(1),
            messages,
        )
        .await
        .map_err(|e| e.into())
}
