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

use iggy::prelude::{
    ClusterClient, Consumer, ConsumerGroupClient, ConsumerOffsetClient, Identifier, IggyClient,
    IggyError, IggyMessage, IggyTimestamp, MessageClient, PartitionClient, Partitioning,
    PersonalAccessTokenClient, PollingKind, PollingStrategy, SegmentClient, StreamClient,
    SystemClient, SystemSnapshotType, TopicClient, UserClient, UserStatus,
};
use requests::*;
use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Content, ErrorData, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
};
use serde::Serialize;
use std::sync::Arc;
use tracing::error;

use crate::Permissions;
mod requests;

#[derive(Debug, Clone)]
pub struct IggyService {
    tool_router: ToolRouter<Self>,
    client: Arc<IggyClient>,
    consumer: Arc<Consumer>,
    permissions: Permissions,
}

#[tool_router]
impl IggyService {
    pub fn new(client: Arc<IggyClient>, consumer: Arc<Consumer>, permissions: Permissions) -> Self {
        Self {
            tool_router: Self::tool_router(),
            client,
            consumer,
            permissions,
        }
    }

    #[tool(description = "Ping")]
    pub async fn ping(&self) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.ping().await)
    }

    #[tool(description = "Get cluster metadata")]
    pub async fn get_cluster_metadata(&self) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.get_cluster_metadata().await)
    }

    #[tool(description = "Get stream")]
    pub async fn get_stream(
        &self,
        Parameters(GetStream { stream_id }): Parameters<GetStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.get_stream(&id(&stream_id)?).await)
    }

    #[tool(description = "Get streams")]
    pub async fn get_streams(&self) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.get_streams().await)
    }

    #[tool(description = "Create stream")]
    pub async fn create_stream(
        &self,
        Parameters(CreateStream { name }): Parameters<CreateStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_create()?;
        request(self.client.create_stream(&name).await)
    }

    #[tool(description = "Update stream")]
    pub async fn update_stream(
        &self,
        Parameters(UpdateStream { stream_id, name }): Parameters<UpdateStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_update()?;
        request(self.client.update_stream(&id(&stream_id)?, &name).await)
    }

    #[tool(description = "Delete stream")]
    pub async fn delete_stream(
        &self,
        Parameters(DeleteStream { stream_id }): Parameters<DeleteStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        request(self.client.delete_stream(&id(&stream_id)?).await)
    }

    #[tool(description = "Purge stream")]
    pub async fn purge_stream(
        &self,
        Parameters(PurgeStream { stream_id }): Parameters<PurgeStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        request(self.client.purge_stream(&id(&stream_id)?).await)
    }

    #[tool(description = "Get topics")]
    pub async fn get_topics(
        &self,
        Parameters(GetTopics { stream_id }): Parameters<GetTopics>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.get_topics(&id(&stream_id)?).await)
    }

    #[tool(description = "Get topic")]
    pub async fn get_topic(
        &self,
        Parameters(GetTopic {
            stream_id,
            topic_id,
        }): Parameters<GetTopic>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(
            self.client
                .get_topic(&id(&stream_id)?, &id(&topic_id)?)
                .await,
        )
    }

    #[tool(description = "Create topic")]
    pub async fn create_topic(
        &self,
        Parameters(CreateTopic {
            stream_id,
            name,
            partitions_count,
            compression_algorithm,
            replication_factor,
            message_expiry,
            max_size,
        }): Parameters<CreateTopic>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_create()?;
        let compression_algorithm = compression_algorithm
            .and_then(|ca| ca.parse().ok())
            .unwrap_or_default();
        let message_expiry = message_expiry
            .and_then(|me| me.parse().ok())
            .unwrap_or_default();
        let max_size = max_size.and_then(|ms| ms.parse().ok()).unwrap_or_default();
        request(
            self.client
                .create_topic(
                    &id(&stream_id)?,
                    &name,
                    partitions_count,
                    compression_algorithm,
                    replication_factor,
                    message_expiry,
                    max_size,
                )
                .await,
        )
    }

    #[tool(description = "Update topic")]
    pub async fn update_topic(
        &self,
        Parameters(UpdateTopic {
            stream_id,
            topic_id,
            name,
            compression_algorithm,
            replication_factor,
            message_expiry,
            max_size,
        }): Parameters<UpdateTopic>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_update()?;
        let compression_algorithm = compression_algorithm
            .and_then(|ca| ca.parse().ok())
            .unwrap_or_default();
        let message_expiry = message_expiry
            .and_then(|me| me.parse().ok())
            .unwrap_or_default();
        let max_size = max_size.and_then(|ms| ms.parse().ok()).unwrap_or_default();
        request(
            self.client
                .update_topic(
                    &id(&stream_id)?,
                    &id(&topic_id)?,
                    &name,
                    compression_algorithm,
                    replication_factor,
                    message_expiry,
                    max_size,
                )
                .await,
        )
    }

    #[tool(description = "Delete topic")]
    pub async fn delete_topic(
        &self,
        Parameters(DeleteTopic {
            stream_id,
            topic_id,
        }): Parameters<DeleteTopic>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        request(
            self.client
                .delete_topic(&id(&stream_id)?, &id(&topic_id)?)
                .await,
        )
    }

    #[tool(description = "Purge topic")]
    pub async fn purge_topic(
        &self,
        Parameters(PurgeTopic {
            stream_id,
            topic_id,
        }): Parameters<PurgeTopic>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        request(
            self.client
                .purge_topic(&id(&stream_id)?, &id(&topic_id)?)
                .await,
        )
    }

    #[tool(description = "Create partitions")]
    pub async fn create_partitions(
        &self,
        Parameters(CreatePartitions {
            stream_id,
            topic_id,
            partitions_count,
        }): Parameters<CreatePartitions>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_create()?;
        request(
            self.client
                .create_partitions(&id(&stream_id)?, &id(&topic_id)?, partitions_count)
                .await,
        )
    }

    #[tool(description = "Delete partitions")]
    pub async fn delete_partitions(
        &self,
        Parameters(DeletePartitions {
            stream_id,
            topic_id,
            partitions_count,
        }): Parameters<DeletePartitions>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        request(
            self.client
                .delete_partitions(&id(&stream_id)?, &id(&topic_id)?, partitions_count)
                .await,
        )
    }

    #[tool(description = "Delete segments")]
    pub async fn delete_segments(
        &self,
        Parameters(DeleteSegments {
            stream_id,
            topic_id,
            partition_id,
            segments_count,
        }): Parameters<DeleteSegments>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        request(
            self.client
                .delete_segments(
                    &id(&stream_id)?,
                    &id(&topic_id)?,
                    partition_id,
                    segments_count,
                )
                .await,
        )
    }

    #[tool(description = "Poll messages")]
    pub async fn poll_messages(
        &self,
        Parameters(PollMessages {
            stream_id,
            topic_id,
            partition_id,
            strategy,
            offset,
            timestamp,
            count,
            auto_commit,
        }): Parameters<PollMessages>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        let offset = offset.unwrap_or(0);
        let count = count.unwrap_or(10);
        let mut auto_commit = auto_commit.unwrap_or(false);
        let strategy = if let Some(strategy) = strategy {
            match strategy.as_str() {
                "offset" => PollingStrategy::offset(offset),
                "first" => PollingStrategy::first(),
                "last" => PollingStrategy::last(),
                "next" => PollingStrategy::next(),
                "timestamp" => PollingStrategy::timestamp(IggyTimestamp::from(
                    timestamp.unwrap_or(IggyTimestamp::now().as_micros()),
                )),
                _ => PollingStrategy::offset(offset),
            }
        } else {
            PollingStrategy::offset(offset)
        };
        if strategy.kind == PollingKind::Next {
            auto_commit = true;
        }

        request(
            self.client
                .poll_messages(
                    &id(&stream_id)?,
                    &id(&topic_id)?,
                    partition_id,
                    &self.consumer,
                    &strategy,
                    count,
                    auto_commit,
                )
                .await,
        )
    }

    #[tool(description = "Send messages")]
    pub async fn send_messages(
        &self,
        Parameters(SendMessages {
            stream_id,
            topic_id,
            partition_id,
            partitioning,
            messages_key,
            messages,
        }): Parameters<SendMessages>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_create()?;
        let partitioning = if let Some(partitioning) = partitioning {
            match partitioning.as_str() {
                "balanced" => Partitioning::balanced(),
                "key" => {
                    let key = messages_key.unwrap_or_default().as_bytes().to_vec();
                    if key.is_empty() {
                        return Err(ErrorData::invalid_request(
                            "Messages key cannot be empty",
                            None,
                        ));
                    }

                    Partitioning::messages_key(&key).map_err(|error| {
                        ErrorData::invalid_request(format!("Invalid messages key: {error}"), None)
                    })?
                }
                "partition" => Partitioning::partition_id(partition_id.unwrap_or(1)),
                _ => Partitioning::balanced(),
            }
        } else {
            Partitioning::balanced()
        };

        let mut messages = messages
            .into_iter()
            .flat_map(|message| {
                if let Some(id) = message.id {
                    return IggyMessage::builder()
                        .id(id)
                        .payload(message.payload.into())
                        .build();
                }

                IggyMessage::builder()
                    .payload(message.payload.into())
                    .build()
            })
            .collect::<Vec<_>>();

        request(
            self.client
                .send_messages(
                    &id(&stream_id)?,
                    &id(&topic_id)?,
                    &partitioning,
                    &mut messages,
                )
                .await,
        )
    }

    #[tool(description = "Get stats")]
    pub async fn get_stats(&self) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.get_stats().await)
    }

    #[tool(description = "Get me")]
    pub async fn get_me(&self) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.get_me().await)
    }

    #[tool(description = "Get client")]
    pub async fn get_client(
        &self,
        Parameters(GetClient { client_id }): Parameters<GetClient>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.get_client(client_id).await)
    }

    #[tool(description = "Get clients")]
    pub async fn get_clients(&self) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.get_clients().await)
    }

    #[tool(description = "Snapshot")]
    pub async fn snapshot(
        &self,
        Parameters(Snapshot { compression, types }): Parameters<Snapshot>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        let compression = compression.and_then(|c| c.parse().ok()).unwrap_or_default();
        let mut types = types
            .into_iter()
            .flatten()
            .flat_map(|t| t.parse().ok())
            .collect::<Vec<_>>();
        if types.is_empty() {
            types = SystemSnapshotType::all_snapshot_types();
        }

        request(self.client.snapshot(compression, types).await)
    }

    #[tool(description = "Get consumer groups")]
    pub async fn get_consumer_groups(
        &self,
        Parameters(GetConsumerGroups {
            stream_id,
            topic_id,
        }): Parameters<GetConsumerGroups>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(
            self.client
                .get_consumer_groups(&id(&stream_id)?, &id(&topic_id)?)
                .await,
        )
    }

    #[tool(description = "Get consumer group")]
    pub async fn get_consumer_group(
        &self,
        Parameters(GetConsumerGroup {
            stream_id,
            topic_id,
            group_id,
        }): Parameters<GetConsumerGroup>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(
            self.client
                .get_consumer_group(&id(&stream_id)?, &id(&topic_id)?, &id(&group_id)?)
                .await,
        )
    }

    #[tool(description = "Create consumer group")]
    pub async fn create_consumer_group(
        &self,
        Parameters(CreateConsumerGroup {
            stream_id,
            topic_id,
            name,
        }): Parameters<CreateConsumerGroup>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_create()?;
        request(
            self.client
                .create_consumer_group(&id(&stream_id)?, &id(&topic_id)?, &name)
                .await,
        )
    }

    #[tool(description = "Delete consumer group")]
    pub async fn delete_consumer_group(
        &self,
        Parameters(DeleteConsumerGroup {
            stream_id,
            topic_id,
            group_id,
        }): Parameters<DeleteConsumerGroup>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        request(
            self.client
                .delete_consumer_group(&id(&stream_id)?, &id(&topic_id)?, &id(&group_id)?)
                .await,
        )
    }

    #[tool(description = "Get consumer offset")]
    pub async fn get_consumer_offset(
        &self,
        Parameters(GetConsumerOffset {
            stream_id,
            topic_id,
            partition_id,
        }): Parameters<GetConsumerOffset>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(
            self.client
                .get_consumer_offset(
                    &self.consumer,
                    &id(&stream_id)?,
                    &id(&topic_id)?,
                    partition_id,
                )
                .await,
        )
    }

    #[tool(description = "Store consumer offset")]
    pub async fn store_consumer_offset(
        &self,
        Parameters(StoreConsumerOffset {
            stream_id,
            topic_id,
            partition_id,
            offset,
        }): Parameters<StoreConsumerOffset>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(
            self.client
                .store_consumer_offset(
                    &self.consumer,
                    &id(&stream_id)?,
                    &id(&topic_id)?,
                    partition_id,
                    offset,
                )
                .await,
        )
    }

    #[tool(description = "Delete consumer offset")]
    pub async fn delete_consumer_offset(
        &self,
        Parameters(DeleteConsumerOffset {
            stream_id,
            topic_id,
            partition_id,
        }): Parameters<DeleteConsumerOffset>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(
            self.client
                .delete_consumer_offset(
                    &self.consumer,
                    &id(&stream_id)?,
                    &id(&topic_id)?,
                    partition_id,
                )
                .await,
        )
    }

    #[tool(description = "Get personal access tokens")]
    pub async fn get_personal_access_tokens(&self) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.get_personal_access_tokens().await)
    }

    #[tool(description = "Create personal access token")]
    pub async fn create_personal_access_token(
        &self,
        Parameters(CreatePersonalAccessToken { name, expiry }): Parameters<
            CreatePersonalAccessToken,
        >,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        let expiry = expiry
            .and_then(|expiry| expiry.parse().ok())
            .unwrap_or_default();
        request(
            self.client
                .create_personal_access_token(&name, expiry)
                .await,
        )
    }

    #[tool(description = "Delete personal access token")]
    pub async fn delete_personal_access_token(
        &self,
        Parameters(DeletePersonalAccessToken { name }): Parameters<DeletePersonalAccessToken>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.delete_personal_access_token(&name).await)
    }

    #[tool(description = "Get users")]
    pub async fn get_users(&self) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.get_users().await)
    }

    #[tool(description = "Get user")]
    pub async fn get_user(
        &self,
        Parameters(GetUser { user_id }): Parameters<GetUser>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.client.get_user(&id(&user_id)?).await)
    }

    #[tool(description = "Create user")]
    pub async fn create_user(
        &self,
        Parameters(CreateUser {
            username,
            password,
            active,
            permissions,
        }): Parameters<CreateUser>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_create()?;
        let status = if active.unwrap_or_default() {
            UserStatus::Active
        } else {
            UserStatus::Inactive
        };
        let permissions = permissions.map(|p| p.into());
        request(
            self.client
                .create_user(&username, &password, status, permissions)
                .await,
        )
    }

    #[tool(description = "Update user")]
    pub async fn update_user(
        &self,
        Parameters(UpdateUser {
            user_id,
            username,
            active,
        }): Parameters<UpdateUser>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_update()?;
        let status = active.map(|active| {
            if active {
                UserStatus::Active
            } else {
                UserStatus::Inactive
            }
        });
        request(
            self.client
                .update_user(&id(&user_id)?, username.as_deref(), status)
                .await,
        )
    }

    #[tool(description = "Delete user")]
    pub async fn delete_user(
        &self,
        Parameters(DeleteUser { user_id }): Parameters<DeleteUser>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        request(self.client.delete_user(&id(&user_id)?).await)
    }

    #[tool(description = "Update permissions")]
    pub async fn update_permissions(
        &self,
        Parameters(UpdatePermissions {
            user_id,
            permissions,
        }): Parameters<UpdatePermissions>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_update()?;
        let permissions = permissions.map(|p| p.into());
        request(
            self.client
                .update_permissions(&id(&user_id)?, permissions)
                .await,
        )
    }

    #[tool(description = "Change password")]
    pub async fn change_password(
        &self,
        Parameters(ChangePassword {
            user_id,
            current_password,
            new_password,
        }): Parameters<ChangePassword>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_update()?;
        request(
            self.client
                .change_password(&id(&user_id)?, &current_password, &new_password)
                .await,
        )
    }
}

#[tool_handler]
impl ServerHandler for IggyService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some("Iggy service".into()),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}

fn id(id: &str) -> Result<Identifier, ErrorData> {
    Identifier::from_str_value(id).map_err(|e| {
        let message = format!("Failed to parse identifier. {e}");
        error!(message);
        ErrorData::invalid_request(message, None)
    })
}

fn request(result: Result<impl Sized + Serialize, IggyError>) -> Result<CallToolResult, ErrorData> {
    let result = result.map_err(|e| {
        let message = format!("There was an error when invoking the method. {e}");
        error!(message);
        ErrorData::invalid_request(message, None)
    })?;

    let content = Content::json(result).map_err(|error| {
        let message = format!("Failed to serialize result. {error}");
        error!(message);
        ErrorData::internal_error(message, None)
    })?;

    Ok(CallToolResult::success(vec![content]))
}
