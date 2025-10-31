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
    CompressionAlgorithm, IdKind, Identifier, IggyClient, IggyError, IggyExpiry, MaxTopicSize,
    StreamClient, TopicClient,
};

use crate::stream_builder::IggyConsumerConfig;
use tracing::{trace, warn};

/// Builds an `IggyStream` and `IggyTopic` if any of them does not exists
/// and if the boolean flags to create them are set to true. In that case it will build
/// them using the given `IggyClient` and `IggyProducerConfig`.
///
/// If the boolean flags to create them are set to false, it will not build them and will return
/// and return Ok(()) since this is expected behavior.d
///
/// # Arguments
///
/// * `client` - The `IggyClient` to use.
/// * `config` - The `IggyProducerConfig` to use.
///
/// # Errors
///
/// * `IggyError` - If the iggy stream topic cannot be build.
///
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
        let topic_replication_factor = config.replication_factor();

        let (name, _id) = extract_name_id_from_identifier(topic_id, topic_name)?;
        trace!("Create topic: {name} for stream: {}", stream_name);
        client
            .create_topic(
                stream_id,
                topic_name,
                topic_partitions_count,
                CompressionAlgorithm::None,
                topic_replication_factor,
                IggyExpiry::ServerDefault,
                MaxTopicSize::ServerDefault,
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
