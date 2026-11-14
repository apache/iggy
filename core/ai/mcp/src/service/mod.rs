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
    Client, ClusterClient, CommitPolicy, CompressionAlgorithm, Consumer, ConsumerGroupClient,
    ConsumerOffsetClient, Identifier, IggyClient, IggyError, IggyExpiry, IggyMessage,
    IggyTimestamp, MaxTopicSize, MessageClient, OptionKey, PartitionClient, Partitioning,
    PersonalAccessTokenClient, PollOptions, PollingKind, PollingStrategy, SegmentClient,
    StreamClient, StreamCreateOptions, StreamUpdateOptions, SystemClient, SystemSnapshotType,
    TopicClient, TopicCreateOptions, TopicUpdateOptions, UserClient, UserCreateOptions, UserStatus,
    UserUpdateOptions,
};
use requests::*;
use rmcp::{
    ServerHandler,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, ContentBlock, ErrorData, ServerCapabilities, ServerConfig},
    tool, tool_handler, tool_router,
};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::error;

use crate::Permissions;
mod requests;

const OPERATION_RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);
/// An omitted `partition_id` reads partition 0, as the server does for a standalone consumer.
const DEFAULT_PARTITION_ID: u32 = 0;

#[derive(Debug, Clone)]
pub struct IggyService {
    client: Arc<IggyClient>,
    consumer: Arc<Consumer>,
    permissions: Permissions,
}

/// Whether a failed call may have changed state, so an agent knows if it can retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    /// Refused or failed before anything was applied.
    Failed,
    /// Sent with an unknown outcome, so it may or may not have been applied.
    Uncertain,
    /// Not sent, because an earlier operation with an unknown outcome blocks it.
    Blocked,
}

/// The `data` of a tool error.
#[derive(Debug, Serialize)]
struct ToolErrorData {
    outcome: Outcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    operation_id: Option<String>,
}

#[tool_router]
impl IggyService {
    pub fn new(client: Arc<IggyClient>, consumer: Arc<Consumer>, permissions: Permissions) -> Self {
        Self {
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
        request(
            self.client
                .create_stream(&name, &StreamCreateOptions::default())
                .await,
        )
    }

    #[tool(description = "Update stream")]
    pub async fn update_stream(
        &self,
        Parameters(update): Parameters<UpdateStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_update()?;
        let stream_id = id(&update.stream_id)?;
        request(
            self.client
                .update_stream(&stream_id, &StreamUpdateOptions::from(update))
                .await,
        )
    }

    #[tool(description = "Delete a stream with all of its topics and messages")]
    pub async fn delete_stream(
        &self,
        Parameters(DeleteStream { stream_id }): Parameters<DeleteStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        request(self.client.delete_stream(&id(&stream_id)?).await)
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
        Parameters(create): Parameters<CreateTopic>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_create()?;
        let stream_id = id(&create.stream_id)?;
        let name = create.name.clone();
        let create_options = TopicCreateOptions::try_from(create)?;
        request(
            self.client
                .create_topic(&stream_id, &name, &create_options)
                .await,
        )
    }

    #[tool(description = "Update topic")]
    pub async fn update_topic(
        &self,
        Parameters(update): Parameters<UpdateTopic>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_update()?;
        let stream_id = id(&update.stream_id)?;
        let topic_id = id(&update.topic_id)?;
        let update_options = TopicUpdateOptions::try_from(update)?;
        request(
            self.client
                .update_topic(&stream_id, &topic_id, &update_options)
                .await,
        )
    }

    #[tool(description = "Delete a topic with all of its partitions and messages")]
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

    #[tool(
        description = "Delete the last partitions_count partitions of a topic with their messages"
    )]
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

    #[tool(
        description = "Delete the oldest segments_count sealed segments of a partition with their messages. The active segment is kept."
    )]
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

    #[tool(
        description = "Poll messages. Requires read permission, and auto_commit=true or strategy=next also requires update permission because it stores the consumer offset. Payloads in the reply are base64 encoded."
    )]
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
        let auto_commit = auto_commit.unwrap_or(false) || strategy.kind == PollingKind::Next;
        let commit = if auto_commit {
            self.permissions.ensure_update()?;
            CommitPolicy::OnRead
        } else {
            CommitPolicy::Manual
        };

        request(
            self.client
                .poll_messages(
                    &id(&stream_id)?,
                    &id(&topic_id)?,
                    partition_id.unwrap_or(DEFAULT_PARTITION_ID),
                    &self.consumer,
                    &strategy,
                    PollOptions {
                        count,
                        commit,
                        ..PollOptions::default()
                    },
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
                "partition" => Partitioning::partition_id(partition_id.unwrap_or(0)),
                _ => Partitioning::balanced(),
            }
        } else {
            Partitioning::balanced()
        };

        let mut messages = messages
            .into_iter()
            .enumerate()
            .map(|(index, message)| {
                IggyMessage::builder()
                    .maybe_id(message.id)
                    .payload(message.payload.into())
                    .build()
                    .map_err(|error| {
                        ErrorData::invalid_request(
                            format!("messages[{index}] is invalid, so nothing was sent. {error}"),
                            None,
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        request(
            self.client
                .send_messages(
                    &id(&stream_id)?,
                    &id(&topic_id)?,
                    &partitioning,
                    &mut messages,
                )
                .await
                // Wire responses carry no serde on purpose; the MCP reply stays
                // the unit acknowledgement it always was.
                .map(|_| ()),
        )
    }

    #[tool(
        description = "Resolve a retained write using operation_id from an uncertain or blocked tool error. Success proves the original write committed and releases its lane; it does not return the original response body. An uncertain result keeps the operation available for another resolve. A committing poll (auto_commit=true or strategy=next) cannot be resolved and stays uncertain; use abandon_operation for it. Requires write permission."
    )]
    pub async fn resolve_operation(
        &self,
        Parameters(RetainedOperation { operation_id }): Parameters<RetainedOperation>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_recovery()?;
        request(
            self.client
                .resolve_operation(
                    operation_id_value(&operation_id)?,
                    Instant::now() + OPERATION_RECOVERY_TIMEOUT,
                )
                .await,
        )
    }

    #[tool(
        description = "Explicitly abandon a retained write or a committing poll using operation_id from an uncertain or blocked tool error. A committing poll (auto_commit=true or strategy=next) with an unknown outcome, such as a lost reply, blocks later polls and offset writes of its partition until it is abandoned. Releases local recovery state without undoing the operation or establishing its outcome, so the stored offset of such a poll stays unknown. Requires write permission."
    )]
    pub async fn abandon_operation(
        &self,
        Parameters(RetainedOperation { operation_id }): Parameters<RetainedOperation>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_recovery()?;
        request(
            self.client
                .abandon_operation(operation_id_value(&operation_id)?)
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

    #[tool(
        description = "Collect a server diagnostics snapshot, returned as the bytes of a compressed archive. Requires the admin permission."
    )]
    pub async fn snapshot(
        &self,
        Parameters(Snapshot { compression, types }): Parameters<Snapshot>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        self.permissions.ensure_admin()?;
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
        self.permissions.ensure_update()?;
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
        self.permissions.ensure_delete()?;
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

    #[tool(description = "Get personal access tokens. Requires the admin permission.")]
    pub async fn get_personal_access_tokens(&self) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        self.permissions.ensure_admin()?;
        request(self.client.get_personal_access_tokens().await)
    }

    #[tool(
        description = "Create a personal access token for the connected Iggy user. The reply holds the token itself. Requires the admin permission."
    )]
    pub async fn create_personal_access_token(
        &self,
        Parameters(CreatePersonalAccessToken { name, expiry }): Parameters<
            CreatePersonalAccessToken,
        >,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_create()?;
        self.permissions.ensure_admin()?;
        let expiry = expiry
            .map(|expiry| parsed(&expiry, "personal access token expiry"))
            .transpose()?
            .unwrap_or_default();
        request(
            self.client
                .create_personal_access_token(&name, expiry)
                .await,
        )
    }

    #[tool(description = "Delete personal access token. Requires the admin permission.")]
    pub async fn delete_personal_access_token(
        &self,
        Parameters(DeletePersonalAccessToken { name }): Parameters<DeletePersonalAccessToken>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        self.permissions.ensure_admin()?;
        request(self.client.delete_personal_access_token(&name).await)
    }

    #[tool(description = "Get users. Requires the admin permission.")]
    pub async fn get_users(&self) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        self.permissions.ensure_admin()?;
        request(self.client.get_users().await)
    }

    #[tool(description = "Get user. Requires the admin permission.")]
    pub async fn get_user(
        &self,
        Parameters(GetUser { user_id }): Parameters<GetUser>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        self.permissions.ensure_admin()?;
        request(self.client.get_user(&id(&user_id)?).await)
    }

    #[tool(description = "Create user. Requires the admin permission.")]
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
        self.permissions.ensure_admin()?;
        let status = if active.unwrap_or(true) {
            UserStatus::Active
        } else {
            UserStatus::Inactive
        };
        let permissions = permissions.map(|p| p.into());
        request(
            self.client
                .create_user(
                    &username,
                    &password,
                    status,
                    permissions,
                    &UserCreateOptions::default(),
                )
                .await,
        )
    }

    #[tool(description = "Update user. Requires the admin permission.")]
    pub async fn update_user(
        &self,
        Parameters(update): Parameters<UpdateUser>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_update()?;
        self.permissions.ensure_admin()?;
        let user_id = id(&update.user_id)?;
        request(
            self.client
                .update_user(&user_id, &UserUpdateOptions::from(update))
                .await,
        )
    }

    #[tool(description = "Delete user. Requires the admin permission.")]
    pub async fn delete_user(
        &self,
        Parameters(DeleteUser { user_id }): Parameters<DeleteUser>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        self.permissions.ensure_admin()?;
        request(self.client.delete_user(&id(&user_id)?).await)
    }

    #[tool(
        description = "Replace every permission of a user. A flag left out becomes false, and leaving permissions out removes all of them. Requires the admin permission."
    )]
    pub async fn update_permissions(
        &self,
        Parameters(UpdatePermissions {
            user_id,
            permissions,
        }): Parameters<UpdatePermissions>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_update()?;
        self.permissions.ensure_admin()?;
        let permissions = permissions.map(|p| p.into());
        request(
            self.client
                .update_permissions(&id(&user_id)?, permissions)
                .await,
        )
    }

    #[tool(description = "Change password. Requires the admin permission.")]
    pub async fn change_password(
        &self,
        Parameters(ChangePassword {
            user_id,
            current_password,
            new_password,
        }): Parameters<ChangePassword>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_update()?;
        self.permissions.ensure_admin()?;
        request(
            self.client
                .change_password(&id(&user_id)?, &current_password, &new_password)
                .await,
        )
    }
}

#[tool_handler]
impl ServerHandler for IggyService {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions("Iggy service")
    }
}

impl From<&IggyError> for Outcome {
    fn from(error: &IggyError) -> Self {
        match error {
            IggyError::OperationUncertain(_)
            | IggyError::FetchProgressUncertain(_)
            | IggyError::TransientNotCommitted
            | IggyError::RequestTooOld => Self::Uncertain,
            IggyError::LaneBlocked(_) | IggyError::PartitionLaneBlocked(_, _) => Self::Blocked,
            _ => Self::Failed,
        }
    }
}

impl From<UpdateStream> for StreamUpdateOptions {
    fn from(update: UpdateStream) -> Self {
        let (reset, raw_reset) = split_reset(update.reset);
        StreamUpdateOptions {
            name: update.name,
            reset,
            raw_reset,
            ..StreamUpdateOptions::default()
        }
    }
}

impl TryFrom<CreateTopic> for TopicCreateOptions {
    type Error = ErrorData;

    fn try_from(create: CreateTopic) -> Result<Self, Self::Error> {
        let compression_algorithm = create
            .compression_algorithm
            .map(|value| parsed::<CompressionAlgorithm>(&value, "compression algorithm"))
            .transpose()?;
        let message_expiry = create
            .message_expiry
            .map(|value| parsed::<IggyExpiry>(&value, "message expiry"))
            .transpose()?;
        let max_topic_size = create
            .max_size
            .map(|value| parsed::<MaxTopicSize>(&value, "maximum size"))
            .transpose()?;
        Ok(TopicCreateOptions {
            partitions_count: Some(create.partitions_count),
            compression_algorithm: compression_algorithm
                .filter(|algorithm| *algorithm != CompressionAlgorithm::default()),
            message_expiry: message_expiry.filter(|expiry| *expiry != IggyExpiry::ServerDefault),
            max_topic_size: max_topic_size.filter(|size| *size != MaxTopicSize::ServerDefault),
            // Raw keys ride as strings and are parsed server-side by the same
            // rules a config value goes through, so a key added after this
            // build shipped is still reachable.
            raw: create.options,
            ..TopicCreateOptions::default()
        })
    }
}

impl TryFrom<UpdateTopic> for TopicUpdateOptions {
    type Error = ErrorData;

    fn try_from(update: UpdateTopic) -> Result<Self, Self::Error> {
        let (reset, raw_reset) = split_reset(update.reset);
        Ok(TopicUpdateOptions {
            name: update.name,
            message_expiry: update
                .message_expiry
                .map(|value| parsed(&value, "message expiry"))
                .transpose()?,
            max_topic_size: update
                .max_size
                .map(|value| parsed(&value, "maximum size"))
                .transpose()?,
            tiered_storage: None,
            local_retention_age: None,
            local_retention_size: None,
            reset,
            raw_reset,
            raw: update.options,
        })
    }
}

impl From<UpdateUser> for UserUpdateOptions {
    fn from(update: UpdateUser) -> Self {
        let (reset, raw_reset) = split_reset(update.reset);
        UserUpdateOptions {
            username: update.username,
            status: update.active.map(|active| {
                if active {
                    UserStatus::Active
                } else {
                    UserStatus::Inactive
                }
            }),
            reset,
            raw_reset,
            raw: update.options,
        }
    }
}

/// Split reset names into keys this build checks before sending and newer keys
/// the server decides on.
fn split_reset(names: Vec<String>) -> (BTreeSet<OptionKey>, BTreeSet<String>) {
    let mut known = BTreeSet::new();
    let mut newer = BTreeSet::new();
    for name in names {
        match name.parse::<OptionKey>() {
            Ok(key) => {
                known.insert(key);
            }
            Err(_) => {
                newer.insert(name);
            }
        }
    }
    (known, newer)
}

fn parsed<T: FromStr>(value: &str, what: &str) -> Result<T, ErrorData>
where
    T::Err: Display,
{
    value.parse().map_err(|error: T::Err| {
        let message = format!("Failed to parse {what}. {error}");
        error!(message);
        ErrorData::invalid_request(message, None)
    })
}

fn id(id: &str) -> Result<Identifier, ErrorData> {
    Identifier::from_str_value(id).map_err(|e| {
        let message = format!("Failed to parse identifier. {e}");
        error!(message);
        ErrorData::invalid_request(message, None)
    })
}

fn request(result: Result<impl Sized + Serialize, IggyError>) -> Result<CallToolResult, ErrorData> {
    let result = result.map_err(|error| {
        let message = match recovery(&error) {
            Some(recovery) => {
                format!("There was an error when invoking the method. {error}. {recovery}")
            }
            None => format!("There was an error when invoking the method. {error}"),
        };
        let data = serde_json::to_value(ToolErrorData {
            outcome: Outcome::from(&error),
            operation_id: error
                .operation_id()
                .map(|operation_id| operation_id.to_string()),
        })
        .ok();
        error!(message);
        ErrorData::invalid_request(message, data)
    })?;

    let content = ContentBlock::json(result).map_err(|error| {
        let message = format!("Failed to serialize result. {error}");
        error!(message);
        ErrorData::internal_error(message, None)
    })?;

    Ok(CallToolResult::success(vec![content]))
}

/// What an agent has to do before it retries a call that ended with `error`.
fn recovery(error: &IggyError) -> Option<String> {
    match error {
        IggyError::OperationUncertain(operation_id) => Some(format!(
            "The write may or may not have been applied. Call resolve_operation with operation_id {operation_id} to settle it before retrying."
        )),
        IggyError::FetchProgressUncertain(operation_id) => Some(format!(
            "The poll may or may not have stored the consumer offset. Call abandon_operation with operation_id {operation_id} before polling this partition again."
        )),
        IggyError::TransientNotCommitted | IggyError::RequestTooOld => Some(
            "The call may or may not have been applied. Read the state it changes before retrying."
                .to_owned(),
        ),
        IggyError::LaneBlocked(operation_id) | IggyError::PartitionLaneBlocked(_, operation_id) => {
            Some(format!(
                "This call was not sent. Settle operation {operation_id} with resolve_operation or abandon_operation first."
            ))
        }
        _ => None,
    }
}

fn operation_id_value(value: &str) -> Result<u64, ErrorData> {
    value.parse().map_err(|_| {
        ErrorData::invalid_request(
            "operation_id must be an unsigned 64-bit decimal string",
            None,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn given_uncertain_or_blocked_writes_when_reporting_errors_should_preserve_exact_operation_ids()
    {
        for error in [
            IggyError::OperationUncertain(u64::MAX),
            IggyError::LaneBlocked(u64::MAX),
        ] {
            let result = request(Err::<(), _>(error)).unwrap_err();
            let data = result.data.unwrap();
            let operation = data["operation_id"].as_str().unwrap();
            assert_eq!(operation_id_value(operation).unwrap(), u64::MAX);
        }
    }

    #[test]
    fn given_write_errors_when_reporting_should_say_whether_the_write_may_have_applied() {
        for (error, outcome, guidance) in [
            (
                IggyError::OperationUncertain(7),
                "uncertain",
                "may or may not have been applied",
            ),
            (
                IggyError::FetchProgressUncertain(7),
                "uncertain",
                "abandon_operation",
            ),
            (
                IggyError::TransientNotCommitted,
                "uncertain",
                "may or may not have been applied",
            ),
            (
                IggyError::RequestTooOld,
                "uncertain",
                "may or may not have been applied",
            ),
            (IggyError::LaneBlocked(7), "blocked", "was not sent"),
            (
                IggyError::PartitionLaneBlocked(0, 7),
                "blocked",
                "was not sent",
            ),
            (IggyError::InvalidCommand, "failed", "There was an error"),
        ] {
            let reported = request(Err::<(), _>(error)).unwrap_err();

            assert_eq!(
                reported.data.as_ref().map(|data| data["outcome"].clone()),
                Some(serde_json::Value::from(outcome)),
                "{}",
                reported.message
            );
            assert!(
                reported.message.contains(guidance),
                "{} must say {guidance}",
                reported.message
            );
        }
    }

    #[test]
    fn given_create_topic_values_when_converting_should_parse_them_into_typed_options() {
        let create: CreateTopic = serde_json::from_str(
            r#"{"stream_id":"orders","name":"events","partitions_count":2,"compression_algorithm":"gzip","message_expiry":"7days","max_size":"15GB","options":{"future_key":"1"}}"#,
        )
        .expect("valid create_topic body");

        let options = TopicCreateOptions::try_from(create).expect("valid values convert");

        assert_eq!(
            options,
            TopicCreateOptions {
                partitions_count: Some(2),
                compression_algorithm: Some(CompressionAlgorithm::Gzip),
                message_expiry: Some("7days".parse().expect("valid expiry")),
                max_topic_size: Some("15GB".parse().expect("valid size")),
                raw: BTreeMap::from([("future_key".to_owned(), "1".to_owned())]),
                ..TopicCreateOptions::default()
            }
        );
    }

    #[test]
    fn given_unparsable_create_topic_value_when_converting_should_refuse() {
        for field in [
            r#""compression_algorithm":"zstd""#,
            r#""message_expiry":"soon""#,
            r#""max_size":"big""#,
        ] {
            let create: CreateTopic = serde_json::from_str(&format!(
                r#"{{"stream_id":"orders","name":"events","partitions_count":1,{field}}}"#
            ))
            .expect("valid create_topic body");

            assert!(TopicCreateOptions::try_from(create).is_err(), "{field}");
        }
    }

    #[test]
    fn given_update_topic_values_when_converting_should_parse_them_into_typed_options() {
        let update = update_topic(
            r#"{"name":"events-v2","message_expiry":"7days","max_size":"15GB","options":{"future_key":"1"}}"#,
        );

        let options = TopicUpdateOptions::try_from(update).expect("valid values convert");

        assert_eq!(
            options,
            TopicUpdateOptions {
                name: Some("events-v2".to_owned()),
                message_expiry: Some("7days".parse().expect("valid expiry")),
                max_topic_size: Some("15GB".parse().expect("valid size")),
                raw: BTreeMap::from([("future_key".to_owned(), "1".to_owned())]),
                ..TopicUpdateOptions::default()
            }
        );
    }

    #[test]
    fn given_unparsable_update_topic_value_when_converting_should_refuse() {
        for body in [r#"{"message_expiry":"soon"}"#, r#"{"max_size":"big"}"#] {
            assert!(
                TopicUpdateOptions::try_from(update_topic(body)).is_err(),
                "{body}"
            );
        }
    }

    #[test]
    fn given_reset_names_when_converting_should_send_known_typed_and_newer_raw() {
        let update = update_topic(r#"{"reset":["message_expiry","future_key"]}"#);

        let options = TopicUpdateOptions::try_from(update).expect("reset names convert");

        assert_eq!(
            options,
            TopicUpdateOptions {
                reset: BTreeSet::from([OptionKey::MessageExpiry]),
                raw_reset: BTreeSet::from(["future_key".to_owned()]),
                ..TopicUpdateOptions::default()
            }
        );
    }

    #[test]
    fn given_stream_and_user_resets_when_converting_should_keep_the_name_optional() {
        let stream: UpdateStream =
            serde_json::from_str(r#"{"stream_id":"orders","reset":["future_key"]}"#)
                .expect("a reset without a name parses");
        let user: UpdateUser = serde_json::from_str(
            r#"{"user_id":"alice","reset":["future_key"],"options":{"newer_key":"x"}}"#,
        )
        .expect("a user patch parses");

        assert_eq!(
            StreamUpdateOptions::from(stream),
            StreamUpdateOptions {
                raw_reset: BTreeSet::from(["future_key".to_owned()]),
                ..StreamUpdateOptions::default()
            }
        );
        assert_eq!(
            UserUpdateOptions::from(user),
            UserUpdateOptions {
                raw_reset: BTreeSet::from(["future_key".to_owned()]),
                raw: BTreeMap::from([("newer_key".to_owned(), "x".to_owned())]),
                ..UserUpdateOptions::default()
            }
        );
    }

    fn update_topic(fields: &str) -> UpdateTopic {
        let ids = r#"{"stream_id":"orders","topic_id":"events","#;
        serde_json::from_str(&format!("{ids}{}", &fields[1..])).expect("valid update_topic body")
    }
}
