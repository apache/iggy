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

use crate::client::{
    Client, ConsumerGroupClient, ConsumerOffsetClient, MessageClient, PartitionClient,
    PersonalAccessTokenClient, StreamClient, SystemClient, TopicClient, UserClient,
};
use crate::clients::builder::IggyClientBuilder;
use crate::clients::consumer::IggyConsumerBuilder;
use crate::clients::producer::IggyProducerBuilder;
use crate::compression::compression_algorithm::CompressionAlgorithm;
use crate::consumer::Consumer;
use crate::diagnostic::DiagnosticEvent;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::locking::IggySharedMut;
use crate::locking::IggySharedMutFn;
use crate::messages::poll_messages::PollingStrategy;
use crate::messages::send_messages::{Message, Partitioning};
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::consumer_offset_info::ConsumerOffsetInfo;
use crate::models::identity_info::IdentityInfo;
use crate::models::messages::PolledMessages;
use crate::models::permissions::Permissions;
use crate::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::models::snapshot::Snapshot;
use crate::models::stats::Stats;
use crate::models::stream::{Stream, StreamDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::models::user_status::UserStatus;
use crate::partitioner::Partitioner;
use crate::snapshot::{SnapshotCompression, SystemSnapshotType};
use crate::tcp::client::TcpClient;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::crypto::EncryptorKind;
use crate::utils::duration::IggyDuration;
use crate::utils::expiry::IggyExpiry;
use crate::utils::personal_access_token_expiry::PersonalAccessTokenExpiry;
use crate::utils::topic_size::MaxTopicSize;
use async_broadcast::Receiver;
use async_dropper::AsyncDrop;
use async_trait::async_trait;
use bytes::Bytes;
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
    client: IggySharedMut<Box<dyn Client>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    encryptor: Option<Arc<EncryptorKind>>,
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

#[async_trait]
impl UserClient for IggyClient {
    async fn get_user(&self, user_id: &Identifier) -> Result<Option<UserInfoDetails>, IggyError> {
        self.client.read().await.get_user(user_id).await
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        self.client.read().await.get_users().await
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<UserInfoDetails, IggyError> {
        self.client
            .read()
            .await
            .create_user(username, password, status, permissions)
            .await
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        self.client.read().await.delete_user(user_id).await
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .update_user(user_id, username, status)
            .await
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .update_permissions(user_id, permissions)
            .await
    }

    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .change_password(user_id, current_password, new_password)
            .await
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        self.client
            .read()
            .await
            .login_user(username, password)
            .await
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        self.client.read().await.logout_user().await
    }
}

#[async_trait]
impl PersonalAccessTokenClient for IggyClient {
    async fn get_personal_access_tokens(&self) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
        self.client.read().await.get_personal_access_tokens().await
    }

    async fn create_personal_access_token(
        &self,
        name: &str,
        expiry: PersonalAccessTokenExpiry,
    ) -> Result<RawPersonalAccessToken, IggyError> {
        self.client
            .read()
            .await
            .create_personal_access_token(name, expiry)
            .await
    }

    async fn delete_personal_access_token(&self, name: &str) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_personal_access_token(name)
            .await
    }

    async fn login_with_personal_access_token(
        &self,
        token: &str,
    ) -> Result<IdentityInfo, IggyError> {
        self.client
            .read()
            .await
            .login_with_personal_access_token(token)
            .await
    }
}

#[async_trait]
impl SystemClient for IggyClient {
    async fn get_stats(&self) -> Result<Stats, IggyError> {
        self.client.read().await.get_stats().await
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        self.client.read().await.get_me().await
    }

    async fn get_client(&self, client_id: u32) -> Result<Option<ClientInfoDetails>, IggyError> {
        self.client.read().await.get_client(client_id).await
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        self.client.read().await.get_clients().await
    }

    async fn ping(&self) -> Result<(), IggyError> {
        self.client.read().await.ping().await
    }

    async fn heartbeat_interval(&self) -> IggyDuration {
        self.client.read().await.heartbeat_interval().await
    }

    async fn snapshot(
        &self,
        compression: SnapshotCompression,
        snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError> {
        self.client
            .read()
            .await
            .snapshot(compression, snapshot_types)
            .await
    }
}

#[async_trait]
impl StreamClient for IggyClient {
    async fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, IggyError> {
        self.client.read().await.get_stream(stream_id).await
    }

    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
        self.client.read().await.get_streams().await
    }

    async fn create_stream(
        &self,
        name: &str,
        stream_id: Option<u32>,
    ) -> Result<StreamDetails, IggyError> {
        self.client
            .read()
            .await
            .create_stream(name, stream_id)
            .await
    }

    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .update_stream(stream_id, name)
            .await
    }

    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        self.client.read().await.delete_stream(stream_id).await
    }

    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        self.client.read().await.purge_stream(stream_id).await
    }
}

#[async_trait]
impl TopicClient for IggyClient {
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<TopicDetails>, IggyError> {
        self.client
            .read()
            .await
            .get_topic(stream_id, topic_id)
            .await
    }

    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, IggyError> {
        self.client.read().await.get_topics(stream_id).await
    }

    async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions_count: u32,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        topic_id: Option<u32>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<TopicDetails, IggyError> {
        self.client
            .read()
            .await
            .create_topic(
                stream_id,
                name,
                partitions_count,
                compression_algorithm,
                replication_factor,
                topic_id,
                message_expiry,
                max_topic_size,
            )
            .await
    }

    async fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .update_topic(
                stream_id,
                topic_id,
                name,
                compression_algorithm,
                replication_factor,
                message_expiry,
                max_topic_size,
            )
            .await
    }

    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_topic(stream_id, topic_id)
            .await
    }

    async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .purge_topic(stream_id, topic_id)
            .await
    }
}

#[async_trait]
impl PartitionClient for IggyClient {
    async fn create_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .create_partitions(stream_id, topic_id, partitions_count)
            .await
    }

    async fn delete_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_partitions(stream_id, topic_id, partitions_count)
            .await
    }
}

#[async_trait]
impl MessageClient for IggyClient {
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<PolledMessages, IggyError> {
        if count == 0 {
            return Err(IggyError::InvalidMessagesCount);
        }

        let mut polled_messages = self
            .client
            .read()
            .await
            .poll_messages(
                stream_id,
                topic_id,
                partition_id,
                consumer,
                strategy,
                count,
                auto_commit,
            )
            .await?;

        if let Some(ref encryptor) = self.encryptor {
            for message in &mut polled_messages.messages {
                let payload = encryptor.decrypt(&message.payload)?;
                message.payload = Bytes::from(payload);
                message.length = IggyByteSize::from(message.payload.len() as u64);
            }
        }

        Ok(polled_messages)
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [Message],
    ) -> Result<(), IggyError> {
        if messages.is_empty() {
            return Err(IggyError::InvalidMessagesCount);
        }

        if let Some(encryptor) = &self.encryptor {
            for message in &mut *messages {
                message.payload = Bytes::from(encryptor.encrypt(&message.payload)?);
                message.length = message.payload.len() as u32;
            }
        }

        self.client
            .read()
            .await
            .send_messages(stream_id, topic_id, partitioning, messages)
            .await
    }

    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .flush_unsaved_buffer(stream_id, topic_id, partition_id, fsync)
            .await
    }
}

#[async_trait]
impl ConsumerOffsetClient for IggyClient {
    async fn store_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .store_consumer_offset(consumer, stream_id, topic_id, partition_id, offset)
            .await
    }

    async fn get_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        self.client
            .read()
            .await
            .get_consumer_offset(consumer, stream_id, topic_id, partition_id)
            .await
    }

    async fn delete_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_consumer_offset(consumer, stream_id, topic_id, partition_id)
            .await
    }
}

#[async_trait]
impl ConsumerGroupClient for IggyClient {
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<ConsumerGroupDetails>, IggyError> {
        self.client
            .read()
            .await
            .get_consumer_group(stream_id, topic_id, group_id)
            .await
    }

    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        self.client
            .read()
            .await
            .get_consumer_groups(stream_id, topic_id)
            .await
    }

    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        group_id: Option<u32>,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        self.client
            .read()
            .await
            .create_consumer_group(stream_id, topic_id, name, group_id)
            .await
    }

    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_consumer_group(stream_id, topic_id, group_id)
            .await
    }

    async fn join_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .join_consumer_group(stream_id, topic_id, group_id)
            .await
    }

    async fn leave_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .leave_consumer_group(stream_id, topic_id, group_id)
            .await
    }
}

#[async_trait]
impl AsyncDrop for IggyClient {
    async fn async_drop(&mut self) {
        let _ = self.client.read().await.logout_user().await;
    }
}
