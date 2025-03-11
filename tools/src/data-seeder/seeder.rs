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

use iggy::client::{MessageClient, StreamClient, TopicClient};
use iggy::clients::client::IggyClient;
use iggy::error::IggyError;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::models::header::{HeaderKey, HeaderValue};
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
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
    for stream_id in streams {
        let topics = client.get_topics(&stream_id.try_into()?).await?;

        let stream_id = stream_id.try_into()?;
        for topic in topics {
            let topic_id = topic.id.try_into()?;
            let mut messages = Vec::new();
            let message_batches = rng.random_range(100..=1000);
            let mut message_id = 1;
            for _ in 1..=message_batches {
                let messages_count = rng.random_range(10..=100);
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
                    let mut message = Message::from_str(&payload)?;
                    message.headers = headers;
                    messages.push(message);
                    message_id += 1;
                }
                client
                    .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                    .await?;
                messages = Vec::new();
            }
        }
    }
    Ok(())
}
