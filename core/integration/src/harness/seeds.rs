/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use iggy::prelude::{IggyClient, StreamClient, TopicClient};
use iggy_binary_protocol::{
    ConsumerGroupClient, ConsumerOffsetClient, MessageClient, PersonalAccessTokenClient, UserClient,
};
use iggy_common::{
    CompressionAlgorithm, Consumer, Identifier, IggyExpiry, IggyMessage, MaxTopicSize,
    Partitioning, PersonalAccessTokenExpiry, UserStatus,
};
use std::error::Error;

/// Seed error type for compatibility.
pub type SeedError = Box<dyn Error + Send + Sync>;

/// Standard names used by MCP seed functions.
pub mod names {
    pub use crate::harness::helpers::USER_PASSWORD;

    pub const STREAM: &str = "test_stream";
    pub const TOPIC: &str = "test_topic";
    pub const MESSAGE_PAYLOAD: &str = "test_message";
    pub const CONSUMER_GROUP: &str = "test_consumer_group";
    pub const CONSUMER: &str = "mcp";
    pub const USER: &str = "test_user";
    pub const PERSONAL_ACCESS_TOKEN: &str = "test_personal_access_token";
}

/// Creates a single stream named "test".
pub async fn stream_only(client: &IggyClient) -> Result<(), SeedError> {
    client.create_stream("test").await?;
    Ok(())
}

/// Creates a stream with a topic.
pub async fn stream_with_topic(client: &IggyClient) -> Result<(), SeedError> {
    client.create_stream("test_stream").await?;
    client
        .create_topic(
            &"test_stream".try_into()?,
            "test_topic",
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::Unlimited,
        )
        .await?;
    Ok(())
}

/// Seed for connector tests: creates stream and topic that connector will subscribe to.
pub async fn connector_stream(client: &IggyClient) -> Result<(), SeedError> {
    let stream_id: Identifier = names::STREAM.try_into()?;

    client.create_stream(names::STREAM).await?;

    client
        .create_topic(
            &stream_id,
            names::TOPIC,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        )
        .await?;

    Ok(())
}

/// Standard MCP test data: stream, topic, message, consumer group, consumer offset, user, PAT.
pub async fn mcp_standard(client: &IggyClient) -> Result<(), SeedError> {
    let stream_id: Identifier = names::STREAM.try_into()?;
    let topic_id: Identifier = names::TOPIC.try_into()?;

    client.create_stream(names::STREAM).await?;

    client
        .create_topic(
            &stream_id,
            names::TOPIC,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        )
        .await?;

    let mut messages = vec![
        IggyMessage::builder()
            .payload(names::MESSAGE_PAYLOAD.into())
            .build()?,
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await?;

    let consumer = Consumer::new(Identifier::named(names::CONSUMER)?);
    client
        .store_consumer_offset(&consumer, &stream_id, &topic_id, Some(0), 0)
        .await?;

    client
        .create_consumer_group(&stream_id, &topic_id, names::CONSUMER_GROUP)
        .await?;

    client
        .create_user(names::USER, names::USER_PASSWORD, UserStatus::Active, None)
        .await?;

    client
        .create_personal_access_token(
            names::PERSONAL_ACCESS_TOKEN,
            PersonalAccessTokenExpiry::NeverExpire,
        )
        .await?;

    Ok(())
}
