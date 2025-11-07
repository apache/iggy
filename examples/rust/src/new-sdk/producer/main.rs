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
use iggy_examples::shared::args::Args;
use iggy_examples::shared::messages_generator::MessagesGenerator;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn Error>> {
    let args = Args::parse_with_defaults("new-sdk-producer");
    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .init();
    info!(
        "New SDK producer has started, selected transport: {}",
        args.transport
    );
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args.to_sdk_args())?);
    let client = client_provider::get_raw_client(client_provider_config, false).await?;
    let client = IggyClient::builder().with_client(client).build()?;
    client.connect().await?;
    let interval = IggyDuration::from_str(&args.interval)?;
    let partitioning = if args.balanced_producer {
        Partitioning::balanced()
    } else {
        Partitioning::partition_id(args.partition_id)
    };
    let producer = client
        .producer(&args.stream_id, &args.topic_id)?
        .direct(
            DirectConfig::builder()
                .batch_length(args.messages_per_batch)
                .linger_time(interval)
                .build(),
        )
        .partitioning(partitioning)
        .create_topic_if_not_exists(
            args.partitions_count,
            None,
            IggyExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        )
        .build();
    producer.init().await?;
    produce_messages(&args, &producer).await?;
    Ok(())
}

async fn produce_messages(
    args: &Args,
    producer: &IggyProducer,
) -> anyhow::Result<(), Box<dyn Error>> {
    let interval = args.get_interval();
    info!(
        "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {}.",
        args.stream_id,
        args.topic_id,
        args.partition_id,
        interval.map_or("none".to_string(), |i| i.as_human_time_string())
    );
    let mut message_generator = MessagesGenerator::new();
    let mut sent_batches = 0;

    loop {
        if args.message_batches_limit > 0 && sent_batches == args.message_batches_limit {
            info!("Sent {sent_batches} batches of messages, exiting.");
            return Ok(());
        }

        let mut messages = Vec::new();
        for _ in 0..args.messages_per_batch {
            let serializable_message = message_generator.generate();
            let json_envelope = serializable_message.to_json_envelope();
            let message = IggyMessage::from_str(&json_envelope)?;
            messages.push(message);
        }
        producer.send(messages).await?;
        sent_batches += 1;
        info!(
            "Sent batch {sent_batches} of {} messages.",
            args.messages_per_batch
        );
    }
}
