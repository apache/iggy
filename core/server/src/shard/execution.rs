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

use crate::streaming::users::user::User;
use crate::streaming::utils::crypto;
use crate::{
    shard::{
        IggyShard,
        transmission::{
            event::ShardEvent,
            frame::{ConsumerGroupResponseData, StreamResponseData, TopicResponseData},
            message::ResolvedTopic,
        },
    },
    state::{
        command::EntryCommand,
        models::{
            CreateConsumerGroupWithId, CreatePersonalAccessTokenWithHash, CreateStreamWithId,
            CreateTopicWithId, CreateUserWithId,
        },
    },
    streaming::polling_consumer::ConsumerGroupId,
};
use iggy_common::{
    Identifier, IggyError, PersonalAccessToken, change_password::ChangePassword,
    create_consumer_group::CreateConsumerGroup, create_partitions::CreatePartitions,
    create_personal_access_token::CreatePersonalAccessToken, create_stream::CreateStream,
    create_topic::CreateTopic, delete_consumer_group::DeleteConsumerGroup,
    delete_partitions::DeletePartitions, delete_personal_access_token::DeletePersonalAccessToken,
    delete_stream::DeleteStream, delete_topic::DeleteTopic, delete_user::DeleteUser,
    join_consumer_group::JoinConsumerGroup, leave_consumer_group::LeaveConsumerGroup,
    purge_stream::PurgeStream, purge_topic::PurgeTopic, update_permissions::UpdatePermissions,
    update_stream::UpdateStream, update_topic::UpdateTopic, update_user::UpdateUser,
};
pub struct DeleteStreamResult {
    pub stream_id: usize,
}

pub struct DeleteTopicResult {
    pub topic_id: usize,
}

pub struct CreatePartitionsResult {
    pub partition_ids: Vec<usize>,
}

pub struct DeletePartitionsResult {
    pub partition_ids: Vec<usize>,
}

pub async fn execute_create_stream(
    shard: &IggyShard,
    user_id: u32,
    command: CreateStream,
) -> Result<StreamResponseData, IggyError> {
    shard.metadata.perm_create_stream(user_id)?;

    let stream_id = shard.create_stream(command.name.clone()).await?;

    // Capture response data from metadata before state apply
    let response_data = shard.metadata.with_metadata(|m| {
        let stream = m
            .streams
            .get(stream_id)
            .expect("stream missing from metadata after creation");
        StreamResponseData {
            id: stream_id as u32,
            name: stream.name.clone(),
            created_at: stream.created_at,
        }
    });

    shard
        .state
        .apply(
            user_id,
            &EntryCommand::CreateStream(CreateStreamWithId {
                stream_id: stream_id as u32,
                command,
            }),
        )
        .await?;

    Ok(response_data)
}

pub async fn execute_update_stream(
    shard: &IggyShard,
    user_id: u32,
    command: UpdateStream,
) -> Result<(), IggyError> {
    let stream = shard.resolve_stream(&command.stream_id)?;
    shard.metadata.perm_update_stream(user_id, stream.id())?;

    shard.update_stream(stream, command.name.clone())?;

    shard
        .state
        .apply(user_id, &EntryCommand::UpdateStream(command))
        .await?;

    Ok(())
}

pub async fn execute_delete_stream(
    shard: &IggyShard,
    user_id: u32,
    command: DeleteStream,
) -> Result<DeleteStreamResult, IggyError> {
    let stream = shard.resolve_stream(&command.stream_id)?;
    shard.metadata.perm_delete_stream(user_id, stream.id())?;

    // Capture all topic/partition info BEFORE deletion for broadcast
    let topics_with_partitions: Vec<(usize, Vec<usize>)> = shard
        .metadata
        .get_topic_ids(stream.id())
        .into_iter()
        .map(|topic_id| {
            let partition_ids = shard.metadata.get_partition_ids(stream.id(), topic_id);
            (topic_id, partition_ids)
        })
        .collect();

    let stream_info = shard.delete_stream(stream).await?;

    shard
        .state
        .apply(user_id, &EntryCommand::DeleteStream(command))
        .await?;

    // Broadcast DeletedPartitions to all shards for each topic's partitions (best-effort)
    for (topic_id, partition_ids) in topics_with_partitions {
        if partition_ids.is_empty() {
            continue;
        }
        let event = ShardEvent::DeletedPartitions {
            stream_id: Identifier::numeric(stream.id() as u32)
                .expect("numeric identifier is always valid"),
            topic_id: Identifier::numeric(topic_id as u32)
                .expect("numeric identifier is always valid"),
            partitions_count: partition_ids.len() as u32,
            partition_ids,
        };
        if let Err(e) = shard.broadcast_event_to_all_shards(event).await {
            tracing::warn!("Broadcast failed: {e}. Shards will sync on restart.");
        }
    }

    Ok(DeleteStreamResult {
        stream_id: stream_info.id,
    })
}

pub async fn execute_purge_stream(
    shard: &IggyShard,
    user_id: u32,
    command: PurgeStream,
) -> Result<(), IggyError> {
    let stream = shard.resolve_stream(&command.stream_id)?;
    shard.metadata.perm_purge_stream(user_id, stream.id())?;

    shard.purge_stream(stream).await?;
    shard.purge_stream_local(stream).await?;

    shard
        .state
        .apply(user_id, &EntryCommand::PurgeStream(command))
        .await?;

    let event = ShardEvent::PurgedStream {
        stream_id: Identifier::numeric(stream.id() as u32)
            .expect("numeric identifier is always valid"),
    };
    if let Err(e) = shard.broadcast_event_to_all_shards(event).await {
        tracing::warn!("Broadcast failed: {e}. Shards will sync on restart.");
    }

    Ok(())
}

pub async fn execute_create_topic(
    shard: &IggyShard,
    user_id: u32,
    command: CreateTopic,
) -> Result<TopicResponseData, IggyError> {
    let stream = shard.resolve_stream(&command.stream_id)?;
    shard.metadata.perm_create_topic(user_id, stream.id())?;

    let topic_id = shard
        .create_topic(
            stream,
            command.name.clone(),
            command.message_expiry,
            command.compression_algorithm,
            command.max_topic_size,
            command.replication_factor,
        )
        .await?;

    let resolved_topic = ResolvedTopic {
        stream_id: stream.id(),
        topic_id,
    };
    let partition_infos = shard
        .create_partitions(resolved_topic, command.partitions_count)
        .await?;

    let response_data = shard.metadata.with_metadata(|m| {
        let topic = m
            .streams
            .get(stream.id())
            .and_then(|s| s.topics.get(topic_id))
            .expect("topic missing from metadata after creation");
        TopicResponseData {
            id: topic_id as u32,
            name: topic.name.clone(),
            created_at: topic.created_at,
            partitions: partition_infos.clone(),
            message_expiry: topic.message_expiry,
            compression_algorithm: topic.compression_algorithm,
            max_topic_size: topic.max_topic_size,
            replication_factor: topic.replication_factor,
        }
    });

    shard
        .state
        .apply(
            user_id,
            &EntryCommand::CreateTopic(CreateTopicWithId {
                topic_id: topic_id as u32,
                command,
            }),
        )
        .await?;

    let event = ShardEvent::CreatedPartitions {
        stream_id: Identifier::numeric(stream.id() as u32)
            .expect("numeric identifier is always valid"),
        topic_id: Identifier::numeric(topic_id as u32).expect("numeric identifier is always valid"),
        partitions: partition_infos,
    };
    if let Err(e) = shard.broadcast_event_to_all_shards(event).await {
        tracing::warn!("Broadcast failed: {e}. Shards will sync on restart.");
    }

    Ok(response_data)
}

pub async fn execute_update_topic(
    shard: &IggyShard,
    user_id: u32,
    command: UpdateTopic,
) -> Result<(), IggyError> {
    let topic = shard.resolve_topic(&command.stream_id, &command.topic_id)?;
    shard
        .metadata
        .perm_update_topic(user_id, topic.stream_id, topic.topic_id)?;

    shard.update_topic(
        topic,
        command.name.clone(),
        command.message_expiry,
        command.compression_algorithm,
        command.max_topic_size,
        command.replication_factor,
    )?;

    shard
        .state
        .apply(user_id, &EntryCommand::UpdateTopic(command))
        .await?;

    Ok(())
}

pub async fn execute_delete_topic(
    shard: &IggyShard,
    user_id: u32,
    command: DeleteTopic,
) -> Result<DeleteTopicResult, IggyError> {
    let topic = shard.resolve_topic(&command.stream_id, &command.topic_id)?;
    shard
        .metadata
        .perm_delete_topic(user_id, topic.stream_id, topic.topic_id)?;

    // Capture partition_ids BEFORE deletion for broadcast
    let partition_ids = shard
        .metadata
        .get_partition_ids(topic.stream_id, topic.topic_id);

    let topic_info = shard.delete_topic(topic).await?;

    shard
        .state
        .apply(user_id, &EntryCommand::DeleteTopic(command))
        .await?;

    // Broadcast to all shards to clean up their local_partitions entries (best-effort)
    let event = ShardEvent::DeletedPartitions {
        stream_id: Identifier::numeric(topic.stream_id as u32)
            .expect("numeric identifier is always valid"),
        topic_id: Identifier::numeric(topic.topic_id as u32)
            .expect("numeric identifier is always valid"),
        partitions_count: partition_ids.len() as u32,
        partition_ids,
    };
    if let Err(e) = shard.broadcast_event_to_all_shards(event).await {
        tracing::warn!("Broadcast failed: {e}. Shards will sync on restart.");
    }

    Ok(DeleteTopicResult {
        topic_id: topic_info.id,
    })
}

pub async fn execute_purge_topic(
    shard: &IggyShard,
    user_id: u32,
    command: PurgeTopic,
) -> Result<(), IggyError> {
    let topic = shard.resolve_topic(&command.stream_id, &command.topic_id)?;
    shard
        .metadata
        .perm_purge_topic(user_id, topic.stream_id, topic.topic_id)?;

    shard.purge_topic(topic).await?;
    shard.purge_topic_local(topic).await?;

    shard
        .state
        .apply(user_id, &EntryCommand::PurgeTopic(command))
        .await?;

    let event = ShardEvent::PurgedTopic {
        stream_id: Identifier::numeric(topic.stream_id as u32)
            .expect("numeric identifier is always valid"),
        topic_id: Identifier::numeric(topic.topic_id as u32)
            .expect("numeric identifier is always valid"),
    };
    if let Err(e) = shard.broadcast_event_to_all_shards(event).await {
        tracing::warn!("Broadcast failed: {e}. Shards will sync on restart.");
    }

    Ok(())
}

pub async fn execute_create_partitions(
    shard: &IggyShard,
    user_id: u32,
    command: CreatePartitions,
) -> Result<CreatePartitionsResult, IggyError> {
    let topic = shard.resolve_topic(&command.stream_id, &command.topic_id)?;
    shard
        .metadata
        .perm_create_partitions(user_id, topic.stream_id, topic.topic_id)?;

    let partition_infos = shard
        .create_partitions(topic, command.partitions_count)
        .await?;
    let partition_ids = partition_infos.iter().map(|p| p.id).collect::<Vec<_>>();

    let total_partition_count = shard
        .metadata
        .partitions_count(topic.stream_id, topic.topic_id) as u32;
    shard.writer().rebalance_consumer_groups_for_topic(
        topic.stream_id,
        topic.topic_id,
        total_partition_count,
    );

    shard
        .state
        .apply(user_id, &EntryCommand::CreatePartitions(command))
        .await?;

    let event = ShardEvent::CreatedPartitions {
        stream_id: Identifier::numeric(topic.stream_id as u32)
            .expect("numeric identifier is always valid"),
        topic_id: Identifier::numeric(topic.topic_id as u32)
            .expect("numeric identifier is always valid"),
        partitions: partition_infos,
    };
    if let Err(e) = shard.broadcast_event_to_all_shards(event).await {
        tracing::warn!("Broadcast failed: {e}. Shards will sync on restart.");
    }

    Ok(CreatePartitionsResult { partition_ids })
}

pub async fn execute_delete_partitions(
    shard: &IggyShard,
    user_id: u32,
    command: DeletePartitions,
) -> Result<DeletePartitionsResult, IggyError> {
    let topic = shard.resolve_topic(&command.stream_id, &command.topic_id)?;
    shard
        .metadata
        .perm_delete_partitions(user_id, topic.stream_id, topic.topic_id)?;

    let deleted_partition_ids = shard
        .delete_partitions(topic, command.partitions_count)
        .await?;

    let remaining_partition_count = shard
        .metadata
        .partitions_count(topic.stream_id, topic.topic_id)
        as u32;
    shard.writer().rebalance_consumer_groups_for_topic(
        topic.stream_id,
        topic.topic_id,
        remaining_partition_count,
    );

    shard
        .state
        .apply(user_id, &EntryCommand::DeletePartitions(command))
        .await?;

    let event = ShardEvent::DeletedPartitions {
        stream_id: Identifier::numeric(topic.stream_id as u32)
            .expect("numeric identifier is always valid"),
        topic_id: Identifier::numeric(topic.topic_id as u32)
            .expect("numeric identifier is always valid"),
        partitions_count: deleted_partition_ids.len() as u32,
        partition_ids: deleted_partition_ids.clone(),
    };
    if let Err(e) = shard.broadcast_event_to_all_shards(event).await {
        tracing::warn!("Broadcast failed: {e}. Shards will sync on restart.");
    }

    Ok(DeletePartitionsResult {
        partition_ids: deleted_partition_ids,
    })
}

pub async fn execute_create_consumer_group(
    shard: &IggyShard,
    user_id: u32,
    command: CreateConsumerGroup,
) -> Result<ConsumerGroupResponseData, IggyError> {
    let topic = shard.resolve_topic(&command.stream_id, &command.topic_id)?;
    shard
        .metadata
        .perm_create_consumer_group(user_id, topic.stream_id, topic.topic_id)?;

    let group_id = shard.create_consumer_group(topic, command.name.clone())?;

    let response_data = shard
        .metadata
        .get_consumer_group(topic.stream_id, topic.topic_id, group_id)
        .map(|cg| ConsumerGroupResponseData {
            id: group_id as u32,
            name: cg.name.clone(),
            partitions_count: cg.partitions.len() as u32,
        })
        .expect("consumer group missing from metadata after creation");

    shard
        .state
        .apply(
            user_id,
            &EntryCommand::CreateConsumerGroup(CreateConsumerGroupWithId {
                group_id: group_id as u32,
                command,
            }),
        )
        .await?;

    Ok(response_data)
}

pub async fn execute_delete_consumer_group(
    shard: &IggyShard,
    user_id: u32,
    command: DeleteConsumerGroup,
) -> Result<(), IggyError> {
    let group =
        shard.resolve_consumer_group(&command.stream_id, &command.topic_id, &command.group_id)?;
    shard
        .metadata
        .perm_delete_consumer_group(user_id, group.stream_id, group.topic_id)?;

    let deleted = shard.delete_consumer_group(group)?;

    let cg_id = ConsumerGroupId(deleted.group_id);
    shard
        .delete_consumer_group_offsets(
            cg_id,
            group.stream_id,
            group.topic_id,
            &deleted.partition_ids,
        )
        .await?;

    shard
        .state
        .apply(user_id, &EntryCommand::DeleteConsumerGroup(command))
        .await?;

    Ok(())
}

pub fn execute_join_consumer_group(
    shard: &IggyShard,
    user_id: u32,
    client_id: u32,
    command: JoinConsumerGroup,
) -> Result<(), IggyError> {
    let group =
        shard.resolve_consumer_group(&command.stream_id, &command.topic_id, &command.group_id)?;
    shard
        .metadata
        .perm_join_consumer_group(user_id, group.stream_id, group.topic_id)?;

    shard.join_consumer_group(client_id, group)?;

    Ok(())
}

pub fn execute_leave_consumer_group(
    shard: &IggyShard,
    user_id: u32,
    client_id: u32,
    command: LeaveConsumerGroup,
) -> Result<(), IggyError> {
    let group =
        shard.resolve_consumer_group(&command.stream_id, &command.topic_id, &command.group_id)?;
    shard
        .metadata
        .perm_leave_consumer_group(user_id, group.stream_id, group.topic_id)?;

    shard.leave_consumer_group(client_id, group)?;

    Ok(())
}

pub async fn execute_create_user(
    shard: &IggyShard,
    user_id: u32,
    command: iggy_common::create_user::CreateUser,
) -> Result<User, IggyError> {
    shard.metadata.perm_create_user(user_id)?;

    let user = shard.create_user(
        &command.username,
        &command.password,
        command.status,
        command.permissions.clone(),
    )?;

    shard
        .state
        .apply(
            user_id,
            &EntryCommand::CreateUser(CreateUserWithId {
                user_id: user.id,
                command: iggy_common::create_user::CreateUser {
                    password: crypto::hash_password(&command.password),
                    ..command
                },
            }),
        )
        .await?;

    Ok(user)
}

pub async fn execute_delete_user(
    shard: &IggyShard,
    user_id: u32,
    command: DeleteUser,
) -> Result<User, IggyError> {
    shard.metadata.perm_delete_user(user_id)?;

    let user = shard.delete_user(&command.user_id)?;

    shard
        .state
        .apply(user_id, &EntryCommand::DeleteUser(command))
        .await?;

    Ok(user)
}

pub async fn execute_update_user(
    shard: &IggyShard,
    user_id: u32,
    command: UpdateUser,
) -> Result<User, IggyError> {
    shard.metadata.perm_update_user(user_id)?;

    let user = shard.update_user(&command.user_id, command.username.clone(), command.status)?;

    shard
        .state
        .apply(user_id, &EntryCommand::UpdateUser(command))
        .await?;

    Ok(user)
}

pub async fn execute_change_password(
    shard: &IggyShard,
    user_id: u32,
    command: ChangePassword,
) -> Result<(), IggyError> {
    let target_user = shard.get_user(&command.user_id)?;
    if target_user.id != user_id {
        shard.metadata.perm_change_password(user_id)?;
    }

    shard.change_password(
        &command.user_id,
        &command.current_password,
        &command.new_password,
    )?;

    shard
        .state
        .apply(
            user_id,
            &EntryCommand::ChangePassword(ChangePassword {
                current_password: String::new(),
                new_password: crypto::hash_password(&command.new_password),
                ..command
            }),
        )
        .await?;

    Ok(())
}

pub async fn execute_update_permissions(
    shard: &IggyShard,
    user_id: u32,
    command: UpdatePermissions,
) -> Result<(), IggyError> {
    shard.metadata.perm_update_permissions(user_id)?;

    let target_user = shard.get_user(&command.user_id)?;
    if target_user.is_root() {
        return Err(IggyError::CannotChangePermissions(target_user.id));
    }

    shard.update_permissions(&command.user_id, command.permissions.clone())?;

    shard
        .state
        .apply(user_id, &EntryCommand::UpdatePermissions(command))
        .await?;

    Ok(())
}

pub async fn execute_create_personal_access_token(
    shard: &IggyShard,
    user_id: u32,
    command: CreatePersonalAccessToken,
) -> Result<(PersonalAccessToken, String), IggyError> {
    let (personal_access_token, token) =
        shard.create_personal_access_token(user_id, &command.name, command.expiry)?;

    shard
        .state
        .apply(
            user_id,
            &EntryCommand::CreatePersonalAccessToken(CreatePersonalAccessTokenWithHash {
                hash: personal_access_token.token.to_string(),
                command,
            }),
        )
        .await?;

    Ok((personal_access_token, token))
}

pub async fn execute_delete_personal_access_token(
    shard: &IggyShard,
    user_id: u32,
    command: DeletePersonalAccessToken,
) -> Result<(), IggyError> {
    shard.delete_personal_access_token(user_id, &command.name)?;

    shard
        .state
        .apply(user_id, &EntryCommand::DeletePersonalAccessToken(command))
        .await?;

    Ok(())
}
