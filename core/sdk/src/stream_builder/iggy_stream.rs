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
use crate::clients::producer::IggyProducer;
use crate::prelude::{IggyError, SystemClient};
use crate::stream_builder::{IggyStreamConfig, build};
use tracing::trace;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct IggyStream;

impl IggyStream {
    /// Build and connect iggy client, producer and consumer
    ///
    /// # Arguments
    ///
    /// * `client` - reference to the iggy client
    /// * `config` - configuration for the iggy stream
    ///
    /// # Errors
    ///
    /// If the builds fails, an `IggyError` is returned.
    ///
    pub async fn build(
        client: &IggyClient,
        config: &IggyStreamConfig,
    ) -> Result<(IggyProducer, IggyConsumer), IggyError> {
        trace!("Check if client is connected");
        if client.ping().await.is_err() {
            return Err(IggyError::NotConnected);
        }

        trace!("Build iggy producer");
        // The producer creates stream and topic if it doesn't exist
        let iggy_producer = build::build_iggy_producer(client, config.producer_config()).await?;

        trace!("Build iggy consumer");
        let iggy_consumer = build::build_iggy_consumer(client, config.consumer_config()).await?;

        Ok((iggy_producer, iggy_consumer))
    }

    /// Build and connect iggy client, producer and consumer from connection string
    ///
    /// # Arguments
    ///
    /// * `connection_string` - connection string for the iggy server
    /// * `config` - configuration for the iggy stream
    ///
    /// # Errors
    ///
    /// If the builds fails, an `IggyError` is returned.
    ///
    pub async fn with_client_from_connection_string(
        connection_string: &str,
        config: &IggyStreamConfig,
    ) -> Result<(IggyClient, IggyProducer, IggyConsumer), IggyError> {
        trace!("Build and connect iggy client");
        let client = build::build_iggy_client(connection_string).await?;

        trace!("Build iggy producer and consumer");
        let (iggy_producer, iggy_consumer) = Self::build(&client, config).await?;
        Ok((client, iggy_producer, iggy_consumer))
    }

    /// Builds an `IggyClient` from the given connection string.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - The connection string to use.
    ///
    /// # Errors
    ///
    /// * `IggyError` - If the connection string is invalid or the client cannot be initialized.
    ///
    /// # Details
    ///
    /// This function will create a new `IggyClient` with the given `connection_string`.
    /// It will then connect to the server using the provided connection string.
    /// If the connection string is invalid or the client cannot be initialized,
    /// an `IggyError` will be returned.
    ///
    pub async fn build_iggy_client(connection_string: &str) -> Result<IggyClient, IggyError> {
        trace!("Build and connect iggy client");
        let client = build::build_iggy_client(connection_string).await?;

        Ok(client)
    }
}
