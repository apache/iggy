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

use crate::clients::client_builder::IggyClientBuilder;
use iggy_common::locking::{IggySharedMut, IggySharedMutFn};

use crate::prelude::EncryptorKind;
use crate::prelude::IggyConsumerBuilder;
use crate::prelude::IggyError;
use crate::prelude::IggyProducerBuilder;
use crate::tcp::tcp_client::TcpClient;
use async_broadcast::Receiver;
use async_trait::async_trait;
use iggy_binary_protocol::Client;
use iggy_common::{Consumer, DiagnosticEvent, Partitioner};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::spawn;
use tokio::time::sleep;
use tracing::log::warn;
use tracing::{debug, error, info};

/// The main client struct which implements all the `Client` traits and wraps the underlying low-level client for the specific transport.
///
/// It also provides the additional builders for the standalone consumer, consumer group, and producer.
#[derive(Debug)]
#[allow(dead_code)]
pub struct IggyClient {
    pub(crate) client: IggySharedMut<Box<dyn Client>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    pub(crate) encryptor: Option<Arc<EncryptorKind>>,
}

impl Default for IggyClient {
    fn default() -> Self {
        IggyClient::new(Box::<TcpClient>::default())
    }
}

impl IggyClient {
    /// Creates a new `IggyClientBuilder`.
    pub fn builder() -> IggyClientBuilder {
        IggyClientBuilder::new()
    }

    /// Creates a new `IggyClientBuilder`.
    pub fn builder_from_connection_string(
        connection_string: &str,
    ) -> Result<IggyClientBuilder, IggyError> {
        IggyClientBuilder::from_connection_string(connection_string)
    }

    /// Creates a new `IggyClient` with the provided client implementation for the specific transport.
    pub fn new(client: Box<dyn Client>) -> Self {
        let client = IggySharedMut::new(client);
        IggyClient {
            client,
            partitioner: None,
            encryptor: None,
        }
    }

    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        let client = Box::new(TcpClient::from_connection_string(connection_string)?);
        Ok(IggyClient::new(client))
    }

    /// Creates a new `IggyClient` with the provided client implementation for the specific transport and the optional implementations for the `partitioner` and `encryptor`.
    pub fn create(
        client: Box<dyn Client>,
        partitioner: Option<Arc<dyn Partitioner>>,
        encryptor: Option<Arc<EncryptorKind>>,
    ) -> Self {
        if partitioner.is_some() {
            info!("Partitioner is enabled.");
        }
        if encryptor.is_some() {
            info!("Client-side encryption is enabled.");
        }

        let client = IggySharedMut::new(client);
        IggyClient {
            client,
            partitioner,
            encryptor,
        }
    }

    /// Returns the underlying client implementation for the specific transport.
    pub fn client(&self) -> IggySharedMut<Box<dyn Client>> {
        self.client.clone()
    }

    /// Returns the builder for the standalone consumer.
    pub fn consumer(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
        partition: u32,
    ) -> Result<IggyConsumerBuilder, IggyError> {
        Ok(IggyConsumerBuilder::new(
            self.client.clone(),
            name.to_owned(),
            Consumer::new(name.try_into()?),
            stream.try_into()?,
            topic.try_into()?,
            Some(partition),
            self.encryptor.clone(),
            None,
        ))
    }

    /// Returns the builder for the consumer group.
    pub fn consumer_group(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
    ) -> Result<IggyConsumerBuilder, IggyError> {
        Ok(IggyConsumerBuilder::new(
            self.client.clone(),
            name.to_owned(),
            Consumer::group(name.try_into()?),
            stream.try_into()?,
            topic.try_into()?,
            None,
            self.encryptor.clone(),
            None,
        ))
    }

    /// Returns the builder for the producer.
    pub fn producer(&self, stream: &str, topic: &str) -> Result<IggyProducerBuilder, IggyError> {
        Ok(IggyProducerBuilder::new(
            self.client.clone(),
            stream.try_into()?,
            stream.to_owned(),
            topic.try_into()?,
            topic.to_owned(),
            self.encryptor.clone(),
            None,
        ))
    }
}

#[async_trait]
impl Client for IggyClient {
    async fn connect(&self) -> Result<(), IggyError> {
        let heartbeat_interval;
        {
            let client = self.client.read().await;
            client.connect().await?;
            heartbeat_interval = client.heartbeat_interval().await;
        }

        let client = self.client.clone();
        spawn(async move {
            loop {
                debug!("Sending the heartbeat...");
                if let Err(error) = client.read().await.ping().await {
                    error!("There was an error when sending a heartbeat. {error}");
                    if error == IggyError::ClientShutdown {
                        warn!("The client has been shut down - stopping the heartbeat.");
                        return;
                    }
                } else {
                    debug!("Heartbeat was sent successfully.");
                }
                sleep(heartbeat_interval.get_duration()).await
            }
        });
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        self.client.read().await.disconnect().await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        self.client.read().await.shutdown().await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.client.read().await.subscribe_events().await
    }
}
