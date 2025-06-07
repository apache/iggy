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

use crate::clients::client::IggyClient;
use crate::clients::consumer::IggyConsumer;
use crate::prelude::{ConsumerKind, IggyError};
use crate::stream_builder::IggyConsumerConfig;
use tracing::{error, trace};

/// Builds an `IggyConsumer` from the given `IggyClient` and `IggyConsumerConfig`.
///
/// # Arguments
///
/// * `client` - The `IggyClient` to use.
/// * `config` - The `IggyConsumerConfig` to use.
///
/// # Errors
///
/// * `IggyError` - If the iggy consumer cannot be build.
///
/// # Details
///
/// This function will create a new `IggyConsumer` with the given `IggyClient` and `IggyConsumerConfig`.
/// The `IggyConsumerConfig` fields are used to configure the `IggyConsumer`.
///
pub(crate) async fn build_iggy_consumer(
    client: &IggyClient,
    config: &IggyConsumerConfig,
) -> Result<IggyConsumer, IggyError> {
    trace!("Extract config fields.");
    let stream = config.stream_name();
    let topic = config.topic_name();
    let auto_commit = config.auto_commit();
    let consumer_kind = config.consumer_kind();
    let consumer_name = config.consumer_name();
    let batch_length = config.batch_length();
    let polling_interval = config.polling_interval();
    let polling_strategy = config.polling_strategy();
    let partition = config.partitions_count();
    let polling_retry_interval = config.polling_retry_interval();

    trace!("Build iggy consumer");
    let mut builder = match consumer_kind {
        ConsumerKind::Consumer => client.consumer(consumer_name, stream, topic, partition)?,
        ConsumerKind::ConsumerGroup => client.consumer_group(consumer_name, stream, topic)?,
    }
    .auto_commit(auto_commit)
    .create_consumer_group_if_not_exists()
    .auto_join_consumer_group()
    .polling_strategy(polling_strategy)
    .poll_interval(polling_interval)
    .batch_length(batch_length)
    .polling_retry_interval(polling_retry_interval);

    if let Some(encryptor) = config.encryptor() {
        trace!("Set encryptor");
        builder = builder.encryptor(encryptor);
    }

    if let Some(init_retries) = config.init_retries() {
        let init_interval = config.init_interval();
        trace!("Set init_retries");
        builder = builder.init_retries(init_retries, init_interval);
    }

    trace!("Initialize consumer");
    let mut consumer = builder.build();

    consumer.init().await.map_err(|err| {
        error!("Failed to initialize consumer: {err}");
        err
    })?;

    Ok(consumer)
}
