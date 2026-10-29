// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use futures_util::StreamExt;
use iggy::prelude::*;
use iggy_examples::shared::args::Args;
use iggy_examples::shared::messages::{
    Envelope, ORDER_CONFIRMED_TYPE, ORDER_CREATED_TYPE, ORDER_REJECTED_TYPE, OrderConfirmed,
    OrderCreated, OrderRejected,
};
use std::error::Error;
use std::sync::Arc;
use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn Error>> {
    let args = Args::parse_with_defaults("new-sdk-consumer");
    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .init();
    info!(
        "New SDK consumer has started, selected transport: {}",
        args.transport
    );
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args.to_sdk_args())?);
    let client = client_provider::get_raw_client(client_provider_config, false).await?;
    let client = IggyClient::new(client);
    client.connect().await?;

    let name = "new-sdk-consumer";
    let mut consumer = match ConsumerKind::from_code(args.consumer_kind)? {
        ConsumerKind::Consumer => client
            .consumer(name, &args.stream_id, &args.topic_id)?
            .partitions([args.partition_id]),
        ConsumerKind::ConsumerGroup => {
            client.consumer_group(name, &args.stream_id, &args.topic_id)?
        }
        ConsumerKind::ExternalGroup => return Err("an external group cannot poll".into()),
    }
    .create_consumer_group_if_not_exists()
    .polling_strategy(PollingStrategy::next())
    .poll_options(PollOptions {
        count: args.messages_per_batch,
        server_commit: ServerCommitPolicy::OnRead,
        ..PollOptions::wait(IggyDuration::ONE_SECOND)
    })
    .build();

    consumer.init().await?;
    consume_messages(&args, &client, &mut consumer).await?;

    Ok(())
}

pub async fn consume_messages(
    args: &Args,
    client: &IggyClient,
    consumer: &mut IggyConsumer,
) -> Result<(), Box<dyn Error>> {
    let mut consumed_messages = 0;

    info!(
        "Messages will be polled by consumer: {} from stream: {}, topic: {}, partition: {} with long polling.",
        args.consumer_id, args.stream_id, args.topic_id, args.partition_id
    );

    let total_messages_to_consume = args.message_batches_limit * args.messages_per_batch as u64;

    while let Some(message) = consumer.next().await {
        if let Ok(message) = message {
            handle_message(&message)?;
            consumed_messages += 1;

            if args.message_batches_limit > 0 && consumed_messages >= total_messages_to_consume {
                info!(
                    "Consumed {} messages ({} batches of {} messages each), exiting.",
                    consumed_messages, args.message_batches_limit, args.messages_per_batch
                );
                return Ok(());
            }
        } else if let Err(error) = message {
            error!("Error while handling message: {error}");
            resume_partition(client, consumer, error).await?;
        }
    }

    info!(
        "No more messages to consume, consumed {} messages total.",
        consumed_messages
    );
    Ok(())
}

/// The consumer stops a partition after a read with an unknown outcome, a message that does not
/// decode, or a new partition history, and retries any other error by itself. Without a seek, a
/// run on one partition would stall.
async fn resume_partition(
    client: &IggyClient,
    consumer: &mut IggyConsumer,
    error: ReadError,
) -> Result<(), IggyError> {
    let Some(partition_id) = error.partition_id else {
        return Ok(());
    };
    let start = match (error.error, error.skip) {
        (
            IggyError::FetchProgressUncertain(operation)
            | IggyError::PartitionLaneBlocked(_, operation),
            _,
        ) => {
            match client.abandon_operation(operation).await {
                // A lost fetch stops each of its partitions, and the first abandon frees them all.
                Ok(()) | Err(IggyError::OperationNotRetained(_)) => {}
                Err(abandon_error) => return Err(abandon_error),
            }
            PollingStrategy::next()
        }
        (_, Some(skip)) => skip,
        (IggyError::HistoryUnavailable, None) => PollingStrategy::next(),
        _ => return Ok(()),
    };
    consumer.seek(partition_id, start).await
}

fn handle_message(message: &ReceivedMessage) -> anyhow::Result<(), Box<dyn Error>> {
    // The payload can be of any type as it is a raw byte array. In this case it's a JSON string.
    let json = std::str::from_utf8(&message.message.payload)?;
    // The message envelope can be used to send the different types of messages to the same topic.
    let envelope = serde_json::from_str::<Envelope>(json)?;
    info!(
        "Handling message type: {} at offset: {} in partition ID: {} with current offset: {}",
        envelope.message_type,
        message.message.header.offset,
        message.partition_id,
        message.current_offset,
    );
    match envelope.message_type.as_str() {
        ORDER_CREATED_TYPE => {
            let order_created = serde_json::from_str::<OrderCreated>(&envelope.payload)?;
            info!("{:#?}", order_created);
        }
        ORDER_CONFIRMED_TYPE => {
            let order_confirmed = serde_json::from_str::<OrderConfirmed>(&envelope.payload)?;
            info!("{:#?}", order_confirmed);
        }
        ORDER_REJECTED_TYPE => {
            let order_rejected = serde_json::from_str::<OrderRejected>(&envelope.payload)?;
            info!("{:#?}", order_rejected);
        }
        _ => {
            warn!("Received unknown message type: {}", envelope.message_type);
        }
    }
    Ok(())
}
