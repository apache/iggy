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

use crate::shared::args::Args;
use iggy::prelude::*;
use tracing::info;

type MessageHandler = dyn Fn(&IggyMessage) -> Result<(), Box<dyn std::error::Error>>;

pub async fn init_by_consumer(args: &Args, client: &dyn Client) {
    let (stream_id, topic_id, partition_id) = (
        args.stream_id.clone(),
        args.topic_id.clone(),
        args.partition_id,
    );
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    let stream_id = stream_id.try_into().unwrap();
    let topic_id = topic_id.try_into().unwrap();
    loop {
        interval.tick().await;
        info!("Validating if stream: {stream_id} exists..");
        let stream = client.get_stream(&stream_id).await;
        if stream.is_err() {
            continue;
        }

        let stream = stream.unwrap();
        if stream.is_none() {
            continue;
        }

        info!("Stream: {stream_id} was found.");
        break;
    }
    loop {
        interval.tick().await;
        info!("Validating if topic: {} exists..", topic_id);
        let topic = client.get_topic(&stream_id, &topic_id).await;
        if topic.is_err() {
            continue;
        }

        let topic = topic.unwrap();
        if topic.is_none() {
            continue;
        }

        let topic = topic.unwrap();
        info!("Topic: {} was found.", topic_id);
        if topic.partitions_count >= partition_id {
            break;
        }

        panic!(
            "Topic: {} has only {} partition(s), but partition: {} was requested.",
            topic_id, topic.partitions_count, partition_id
        );
    }
}

pub async fn init_by_producer(args: &Args, client: &dyn Client) -> Result<(), IggyError> {
    let stream_id = args.stream_id.clone().try_into()?;
    let topic_name = args.topic_id.clone();
    let stream = client.get_stream(&stream_id).await?;
    if stream.is_some() {
        return Ok(());
    }

    info!("Stream does not exist, creating...");
    client.create_stream(&args.stream_id).await?;
    client
        .create_topic(
            &stream_id,
            &topic_name,
            args.partitions_count,
            CompressionAlgorithm::from_code(args.compression_algorithm)?,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await?;
    Ok(())
}

pub async fn consume_messages(
    args: &Args,
    client: &dyn Client,
    handle_message: &MessageHandler,
) -> Result<(), Box<dyn std::error::Error>> {
    let interval = args.get_interval();
    info!(
        "Messages will be polled by consumer: {} from stream: {}, topic: {}, partition: {} with interval {}.",
        args.consumer_id,
        args.stream_id,
        args.topic_id,
        args.partition_id,
        interval.map_or("none".to_string(), |i| i.as_human_time_string())
    );

    let stream_id = args.stream_id.clone().try_into()?;
    let topic_id = args.topic_id.clone().try_into()?;
    let mut interval = interval.map(|interval| tokio::time::interval(interval.get_duration()));
    let mut consumed_batches = 0;
    let consumer = Consumer {
        kind: ConsumerKind::from_code(args.consumer_kind)?,
        id: Identifier::numeric(args.consumer_id).unwrap(),
    };

    loop {
        if args.message_batches_limit > 0 && consumed_batches == args.message_batches_limit {
            info!("Consumed {consumed_batches} batches of messages, exiting.");
            return Ok(());
        }

        if let Some(interval) = &mut interval {
            interval.tick().await;
        }

        let polled_messages = client
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(args.partition_id),
                &consumer,
                &PollingStrategy::next(),
                args.messages_per_batch,
                true,
            )
            .await?;
        if polled_messages.messages.is_empty() {
            info!("No messages found.");
            continue;
        }
        consumed_batches += 1;
        for message in polled_messages.messages {
            handle_message(&message)?;
        }
    }
}
