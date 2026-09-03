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

use iggy::prelude::{
    Client, Identifier, IggyClient, IggyClientBuilder, IggyError, StreamClient, TopicClient,
    TopicCreateOptions,
};
use tracing::{debug, info};

use crate::bridge::config::IggyBridgeConfig;
use crate::bridge::error::BridgeError;

/// Owns one connected `IggyClient` and resolves Kafka topics against it.
///
/// Produce/Fetch handler wiring is a separate, later change (`#3535`/`#3536`) - this type is the
/// shared plumbing those handlers will call into, exercised standalone here via its own tests and
/// an integration test against a real `iggy-server`.
pub struct IggyBridge {
    client: IggyClient,
    config: IggyBridgeConfig,
}

impl IggyBridge {
    /// Connects to Iggy using `config` and authenticates.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidConfig`] if `config.address` is empty. Returns
    /// [`BridgeError::Iggy`] if the connection string is malformed, the TCP connection fails, or
    /// authentication is rejected - this is the boundary [`BridgeError::to_kafka_error_code`]
    /// exists for: a handler calling this must map the error to a wire response, never panic or
    /// unwrap, since an unreachable Iggy backend is an expected runtime condition, not a bug.
    pub async fn connect(config: IggyBridgeConfig) -> Result<Self, BridgeError> {
        if config.address.trim().is_empty() {
            return Err(BridgeError::InvalidConfig(
                "Iggy address must not be empty".to_string(),
            ));
        }

        let client = IggyClientBuilder::from_connection_string(&config.connection_string())
            .map_err(BridgeError::Iggy)?
            .build()
            .map_err(BridgeError::Iggy)?;
        client.connect().await.map_err(BridgeError::Iggy)?;
        info!("Iggy bridge connected to {}", config.address);

        Ok(Self { client, config })
    }

    /// Disconnects the underlying Iggy client.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Iggy`] if the client reports a disconnect failure.
    pub async fn close(&self) -> Result<(), BridgeError> {
        self.client.disconnect().await.map_err(BridgeError::Iggy)
    }

    /// Ensures the Iggy stream and topic backing `kafka_topic` exist, creating either or both if
    /// missing. Resolves `kafka_topic` through the configured [`TopicMapping`](crate::bridge::topic_map::TopicMapping).
    ///
    /// Idempotent: a `get` before each `create` means calling this twice for the same topic is a
    /// no-op the second time. A `NameAlreadyExists` race from a concurrent caller creating the
    /// same stream/topic between this call's `get` and `create` is treated as success, not an
    /// error - the desired end state (it exists) is what idempotency actually promises, not that
    /// this call was the one that created it.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Iggy`] for any Iggy failure other than the
    /// already-exists race described above (auth, connectivity, invalid name).
    pub async fn ensure_stream_and_topic(
        &self,
        kafka_topic: &str,
        partition_count: u32,
    ) -> Result<(), BridgeError> {
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        let stream_id = self.ensure_stream(&stream_name).await?;
        self.ensure_topic(&stream_id, &topic_name, partition_count)
            .await?;
        Ok(())
    }

    async fn ensure_stream(&self, stream_name: &str) -> Result<Identifier, BridgeError> {
        let identifier = Identifier::try_from(stream_name).map_err(BridgeError::Iggy)?;
        if let Some(existing) = self
            .client
            .get_stream(&identifier)
            .await
            .map_err(BridgeError::Iggy)?
        {
            debug!("Iggy stream '{stream_name}' already exists");
            return Identifier::numeric(existing.id).map_err(BridgeError::Iggy);
        }

        match self.client.create_stream(stream_name).await {
            Ok(created) => {
                info!("created Iggy stream '{stream_name}'");
                Identifier::numeric(created.id).map_err(BridgeError::Iggy)
            }
            Err(IggyError::StreamNameAlreadyExists(_)) => {
                // Lost a create race - the stream exists now regardless of who created it.
                let existing = self
                    .client
                    .get_stream(&identifier)
                    .await
                    .map_err(BridgeError::Iggy)?
                    .ok_or(IggyError::StreamIdNotFound(identifier))?;
                Identifier::numeric(existing.id).map_err(BridgeError::Iggy)
            }
            Err(err) => Err(BridgeError::Iggy(err)),
        }
    }

    async fn ensure_topic(
        &self,
        stream_id: &Identifier,
        topic_name: &str,
        partition_count: u32,
    ) -> Result<(), BridgeError> {
        let identifier = Identifier::try_from(topic_name).map_err(BridgeError::Iggy)?;
        if self
            .client
            .get_topic(stream_id, &identifier)
            .await
            .map_err(BridgeError::Iggy)?
            .is_some()
        {
            debug!("Iggy topic '{topic_name}' already exists");
            return Ok(());
        }

        let options = TopicCreateOptions {
            partitions_count: Some(partition_count),
            ..TopicCreateOptions::default()
        };
        match self
            .client
            .create_topic(stream_id, topic_name, &options)
            .await
        {
            Ok(_) => {
                info!("created Iggy topic '{topic_name}' with {partition_count} partitions");
                Ok(())
            }
            // Lost a create race - the topic exists now regardless of who created it.
            Err(IggyError::TopicNameAlreadyExists(_, _)) => Ok(()),
            Err(err) => Err(BridgeError::Iggy(err)),
        }
    }

    /// Returns the high watermark (offset of the next message to be written) for one partition of
    /// an Iggy topic.
    ///
    /// Maps directly from `Partition::current_offset` - Iggy partitions start a fresh partition's
    /// counter at `0` and advance it past each written message's own offset, the same "next free
    /// offset" convention Kafka's high watermark uses; there is no unit conversion needed for
    /// `ListOffsets` (`#3537`) to build on this.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Iggy`] if the stream/topic doesn't exist. Returns
    /// [`BridgeError::PartitionOutOfRange`] if `partition` is beyond the topic's partition count.
    pub async fn high_watermark(
        &self,
        stream: &str,
        topic: &str,
        partition: u32,
    ) -> Result<u64, BridgeError> {
        let stream_id = Identifier::try_from(stream).map_err(BridgeError::Iggy)?;
        let topic_id = Identifier::try_from(topic).map_err(BridgeError::Iggy)?;
        let details = self
            .client
            .get_topic(&stream_id, &topic_id)
            .await
            .map_err(BridgeError::Iggy)?
            .ok_or_else(|| BridgeError::Iggy(IggyError::TopicIdNotFound(topic_id, stream_id)))?;

        details
            .partitions
            .iter()
            .find(|p| p.id == partition)
            .map(|p| p.current_offset)
            .ok_or(BridgeError::PartitionOutOfRange {
                topic: topic.to_string(),
                partition,
                partitions_count: details.partitions_count,
            })
    }
}
