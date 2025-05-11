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
use crate::clients::producer::IggyProducer;
use crate::prelude::{IggyError, SystemClient};
use crate::stream_builder::{IggyProducerConfig, build};
use tracing::trace;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct IggyStreamProducer;

impl IggyStreamProducer {
    /// Creates a new `IggyProducer` instance and its associated producer using the `client` and
    /// `config` parameters.
    ///
    /// # Arguments
    ///
    /// * `client`: The Iggy client to use to connect to the Iggy server.
    /// * `config`: The configuration for the producer.
    ///
    /// # Errors
    ///
    /// If the client is not connected or the producer cannot be built, an `IggyError` is returned.
    ///
    pub async fn build(
        client: &IggyClient,
        config: &IggyProducerConfig,
    ) -> Result<IggyProducer, IggyError> {
        trace!("Check if client is connected");
        if client.ping().await.is_err() {
            return Err(IggyError::NotConnected);
        }

        trace!("Build iggy producer");
        // The producer creates stream and topic if it doesn't exist
        let iggy_producer = build::build_iggy_producer(client, config).await?;

        Ok(iggy_producer)
    }

    /// Creates a new `IggyStreamProducer` instance and its associated client using the `connection_string`
    /// and `config` parameters.
    ///
    /// # Arguments
    ///
    /// * `connection_string`: The connection string to use to connect to the Iggy server.
    /// * `config`: The configuration for the producer.
    ///
    /// # Errors
    ///
    /// If the client cannot be connected or the producer cannot be built, an `IggyError` is returned.
    ///
    pub async fn with_client_from_url(
        connection_string: &str,
        config: &IggyProducerConfig,
    ) -> Result<(IggyClient, IggyProducer), IggyError> {
        trace!("Build and connect iggy client");
        let client = build::build_iggy_client::build_iggy_client(connection_string).await?;

        trace!("Build iggy producer");
        // The producer creates stream and topic if it doesn't exist
        let iggy_producer = build::build_iggy_producer(&client, config).await?;

        Ok((client, iggy_producer))
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
