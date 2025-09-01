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

use async_trait::async_trait;
use iggy_connector_sdk::{
    sink_connector, ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

#[derive(Debug, Serialize, Deserialize)]
pub enum IcebergSinkTypes {
    REST,
    HDFS,
}

sink_connector!(IcebergSink);

#[derive(Debug)]
pub struct IcebergSink {
    id: u32,
    config: IcebergSinkConfig,
    client: reqwest::Client,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergSinkConfig {
    tables: Vec<String>,
    catalog_type: String,
    credential: String,
    warehouse: String,
    uri: String,
}

impl IcebergSink {
    pub fn new(id: u32, config: IcebergSinkConfig) -> Self {
        IcebergSink {
            id,
            config,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Sink for IcebergSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opened Quickwit sink connector with ID: {} for URL: {}",
            self.id, self.config.uri
        );
        Ok(())
    }

    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        info!(
            "Iceberg sink with ID: {} received: {} messages, format: {}",
            self.id,
            messages.len(),
            messages_metadata.schema
        );

        let mut json_payloads = Vec::with_capacity(messages.len());
        for message in messages {
            match message.payload {
                Payload::Json(value) => json_payloads.push(value),
                _ => {
                    warn!("Unsupported payload format: {}", messages_metadata.schema);
                }
            }
        }

        if json_payloads.is_empty() {
            return Ok(());
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Iceberg sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        assert_eq!(true, true);
    }
}
