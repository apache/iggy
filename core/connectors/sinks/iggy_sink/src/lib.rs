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

use std::mem;

use async_trait::async_trait;
use bytes::Bytes;
use iggy::clients::client::IggyClient;
use iggy_common::{Client, Identifier, IggyMessage, MessageClient, Partitioning};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

sink_connector!(IggySink);

#[derive(Debug, Serialize, Deserialize)]
pub struct IggySinkConfig {
    pub server_address: String,
    pub stream_id: Option<String>,
    pub topic_id: Option<String>,
}

#[derive(Debug)]
pub struct IggySink {
    id: u32,
    config: IggySinkConfig,
    client: Option<IggyClient>,
}

impl IggySink {
    pub fn new(id: u32, config: IggySinkConfig) -> Self {
        IggySink {
            id,
            config,
            client: None,
        }
    }
}

#[async_trait]
impl Sink for IggySink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opened iggy_sink with ID: {}, connecting to: {}",
            self.id, self.config.server_address
        );

        let client = IggyClient::from_connection_string(&self.config.server_address)
            .map_err(|e| Error::InitError(format!("Failed to build client: {e}")))?;

        client
            .connect()
            .await
            .map_err(|e| Error::InitError(format!("Failed to connect: {e}")))?;

        info!("Successfully connected to downstream Iggy cluster");

        self.client = Some(client);
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::InitError("Client not initialized".to_string()))?;

        let stream = self
            .config
            .stream_id
            .as_ref()
            .unwrap_or(&topic_metadata.stream);
        let topic = self
            .config
            .topic_id
            .as_ref()
            .unwrap_or(&topic_metadata.topic);

        let stream_id: Identifier = stream
            .as_str()
            .try_into()
            .map_err(|_| Error::InvalidConfigValue(format!("Invalid stream: {stream}")))?;
        let topic_id: Identifier = topic
            .as_str()
            .try_into()
            .map_err(|_| Error::InvalidConfigValue(format!("Invalid topic: {topic}")))?;

        let partitioning = Partitioning::partition_id(messages_metadata.partition_id);
        let mut iggy_messages = Vec::with_capacity(messages.len());

        for mut msg in messages {
            let payload_bytes = match mem::replace(&mut msg.payload, Payload::Raw(vec![])) {
                Payload::Raw(bytes) => bytes,
                other => other
                    .try_to_bytes()
                    .map_err(|e| Error::InvalidRecordValue(e.to_string()))?,
            };

            // Preserving the incoming message ID ensures idempotent delivery on retries
            let iggy_msg = IggyMessage::builder()
                .id(msg.id)
                .payload(Bytes::from(payload_bytes))
                .maybe_user_headers(msg.headers)
                .build()
                .map_err(|e| Error::InvalidRecordValue(e.to_string()))?;

            iggy_messages.push(iggy_msg);
        }

        debug!(
            "Iggy sink {} sending {} messages to downstream stream '{stream}' topic '{topic}'",
            self.id,
            iggy_messages.len()
        );

        client
            .send_messages(&stream_id, &topic_id, &partitioning, &mut iggy_messages)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closed iggy_sink with ID: {}", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> IggySinkConfig {
        IggySinkConfig {
            server_address: "iggy://127.0.0.1:8090".to_string(),
            stream_id: None,
            topic_id: None,
        }
    }

    #[test]
    fn given_valid_config_should_instantiate_sink() {
        let sink = IggySink::new(1, test_config());
        assert_eq!(sink.id, 1);
        assert_eq!(sink.config.server_address, "iggy://127.0.0.1:8090");
        assert!(sink.client.is_none());
    }

    #[test]
    fn given_custom_stream_and_topic_should_store_overrides() {
        let config = IggySinkConfig {
            server_address: "iggy://127.0.0.1:8090".to_string(),
            stream_id: Some("target_stream".to_string()),
            topic_id: Some("target_topic".to_string()),
        };
        let sink = IggySink::new(2, config);
        assert_eq!(sink.config.stream_id.as_deref(), Some("target_stream"));
        assert_eq!(sink.config.topic_id.as_deref(), Some("target_topic"));
    }
}
