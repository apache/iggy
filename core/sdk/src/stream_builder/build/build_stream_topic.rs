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

use crate::prelude::{
    IdKind, Identifier, IggyClient, IggyError, StreamClient, TopicClient, TopicCreateOptions,
};

use crate::stream_builder::IggyConsumerConfig;
use tracing::{trace, warn};

/// Creates the stream and the topic of `config` when the server does not hold them yet.
///
/// A missing stream is created only when [`create_stream_if_not_exists()`] is set, and a missing
/// topic only when [`create_topic_if_not_exists()`] is set. When the switch is off, the function
/// logs a warning and returns `Ok(())`. So you can decide how a missing stream or topic should be handled.
///
/// A topic created here gets [`partitions_count()`] partitions, and takes the server
/// defaults for message expiry and maximum size.
///
/// # Errors
///
/// - Any error raised while reading the stream or the topic from the server.
/// - Any error raised while creating the stream or the topic.
/// - [`IggyError::InvalidIdentifier`] when the name cannot be read from the stream or topic
///   identifier of the configuration.
///
/// [`create_stream_if_not_exists()`]: crate::prelude::IggyConsumerConfig::create_stream_if_not_exists
/// [`create_topic_if_not_exists()`]: crate::prelude::IggyConsumerConfig::create_topic_if_not_exists
/// [`partitions_count()`]: crate::prelude::IggyConsumerConfig::partitions_count
pub(crate) async fn build_iggy_stream_topic_if_not_exists(
    client: &IggyClient,
    config: &IggyConsumerConfig,
) -> Result<(), IggyError> {
    let stream_id = config.stream_id();
    let stream_name = config.stream_name();
    let topic_id = config.topic_id();
    let topic_name = config.topic_name();

    trace!("Check if stream exists.");
    if client.get_stream(config.stream_id()).await?.is_none() {
        trace!("Check if stream should be created.");
        if !config.create_stream_if_not_exists() {
            warn!(
                "Stream {stream_name} does not exists and create stream is disabled. \
                If you want to create the stream automatically, please set create_stream_if_not_exists to true."
            );
            return Ok(());
        }

        let (name, _id) = extract_name_id_from_identifier(stream_id, stream_name)?;
        trace!("Creating stream: {name}");
        client.create_stream(&name).await?;
    }

    trace!("Check if topic exists.");
    if client
        .get_topic(config.stream_id(), config.topic_id())
        .await?
        .is_none()
    {
        trace!("Check if topic should be created.");
        if !config.create_topic_if_not_exists() {
            warn!(
                "Topic {topic_name} for stream {stream_name} does not exists and create topic is disabled.\
            If you want to create the topic automatically, please set create_topic_if_not_exists to true."
            );
            return Ok(());
        }

        let stream_id = config.stream_id();
        let stream_name = config.stream_name();
        let topic_partitions_count = config.partitions_count();

        let (name, _id) = extract_name_id_from_identifier(topic_id, topic_name)?;
        trace!("Create topic: {name} for stream: {}", stream_name);
        client
            .create_topic(
                stream_id,
                topic_name,
                &TopicCreateOptions {
                    partitions_count: Some(topic_partitions_count),
                    ..TopicCreateOptions::default()
                },
            )
            .await?;
    }

    Ok(())
}

fn extract_name_id_from_identifier(
    stream_id: &Identifier,
    stream_name: &str,
) -> Result<(String, Option<u32>), IggyError> {
    let (name, id) = match stream_id.kind {
        IdKind::Numeric => (stream_name.to_owned(), Some(stream_id.get_u32_value()?)),
        IdKind::String => (stream_id.get_string_value()?, None),
    };
    Ok((name, id))
}
