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
use rand::Rng;
use std::collections::HashMap;
use std::str::FromStr;

const PROD_STREAM_ID: u32 = 1;
const TEST_STREAM_ID: u32 = 2;
const DEV_STREAM_ID: u32 = 3;

pub async fn seed(client: &IggyClient) -> Result<(), IggyError> {
    create_streams(client).await?;
    create_topics(client).await?;
    send_messages(client).await?;
    Ok(())
}

async fn create_streams(client: &IggyClient) -> Result<(), IggyError> {
    client.create_stream("prod", Some(PROD_STREAM_ID)).await?;
    client.create_stream("test", Some(TEST_STREAM_ID)).await?;
    client.create_stream("dev", Some(DEV_STREAM_ID)).await?;
    Ok(())
}

async fn create_topics(client: &IggyClient) -> Result<(), IggyError> {
    let streams = [PROD_STREAM_ID, TEST_STREAM_ID, DEV_STREAM_ID];
    for stream_id in streams {
        let stream_id = stream_id.try_into()?;
        client
            .create_topic(
                &stream_id,
                "orders",
                1,
                Default::default(),
                None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await?;

        client
            .create_topic(
                &stream_id,
                "users",
                2,
                Default::default(),
                None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await?;

        client
            .create_topic(
                &stream_id,
                "notifications",
                3,
                Default::default(),
                None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await?;

        client
            .create_topic(
                &stream_id,
                "payments",
                2,
                Default::default(),
                None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await?;

        client
            .create_topic(
                &stream_id,
                "deliveries",
                1,
                Default::default(),
                None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await?;
    }
    Ok(())
}

async fn send_messages(client: &IggyClient) -> Result<(), IggyError> {
    let mut rng = rand::rng();
    let streams = [PROD_STREAM_ID, TEST_STREAM_ID, DEV_STREAM_ID];
    let partitioning = Partitioning::balanced();

    let message_batches_range = 100..=1000;
    let messages_per_batch_range = 10..=100;

    let mut total_messages_sent = 0;
    let mut total_batches_sent = 0;

    for (stream_idx, stream_id) in streams.iter().enumerate() {
        let stream_id_identifier = (*stream_id).try_into()?;
        let topics = client.get_topics(&stream_id_identifier).await?;
        tracing::info!(
            "Processing stream {} ({}/{})",
            stream_id,
            stream_idx + 1,
            streams.len()
        );

        for (topic_idx, topic) in topics.iter().enumerate() {
            let topic_id = topic.id.try_into()?;

            let message_batches = rng.random_range(message_batches_range.clone());
            tracing::info!(
                "Processing topic {} ({}/{}) - {} batches planned",
                topic.name,
                topic_idx + 1,
                topics.len(),
                message_batches
            );

            let mut message_id = 1;

            for batch_idx in 1..=message_batches {
                let messages_count = rng.random_range(messages_per_batch_range.clone());
                let mut messages = Vec::with_capacity(messages_count);

                for _ in 1..=messages_count {
                    let payload = format!("{}_data_{}", topic.name, message_id);
                    let headers = match rng.random_bool(0.5) {
                        false => None,
                        true => {
                            let mut headers = HashMap::new();
                            headers
                                .insert(HeaderKey::new("key 1")?, HeaderValue::from_str("value1")?);
                            headers.insert(HeaderKey::new("key-2")?, HeaderValue::from_bool(true)?);
                            headers.insert(
                                HeaderKey::new("key_3")?,
                                HeaderValue::from_uint64(123456)?,
                            );
                            Some(headers)
                        }
                    };

                    let message = if let Some(headers) = headers {
                        IggyMessage::builder()
                            .payload(payload.into())
                            .user_headers(headers)
                            .build()?
                    } else {
                        IggyMessage::builder().payload(payload.into()).build()?
                    };

                    messages.push(message);
                    message_id += 1;
                }

                client
                    .send_messages(
                        &stream_id_identifier,
                        &topic_id,
                        &partitioning,
                        &mut messages,
                    )
                    .await?;

                total_messages_sent += messages_count;
                total_batches_sent += 1;

                if batch_idx % 100 == 0 || batch_idx == message_batches {
                    tracing::info!(
                        "Sent {}/{} batches ({} messages total)...",
                        batch_idx,
                        message_batches,
                        total_messages_sent
                    );
                }
            }
        }
    }

    tracing::info!("Total messages sent: {}", total_messages_sent);
    tracing::info!("Total batches sent: {}", total_batches_sent);

    Ok(())
}
