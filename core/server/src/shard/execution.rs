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

use crate::binary::dispatch::wire_id_to_identifier;
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
use iggy_binary_protocol::requests::{
    consumer_groups::*, partitions::*, personal_access_tokens::*, streams::*, topics::*, users::*,
};
use iggy_common::wire_conversions::wire_permissions_to_permissions;
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyError, IggyExpiry, MaxTopicSize, PersonalAccessToken,
    UserStatus,
};
use secrecy::{ExposeSecret, SecretString};

pub async fn execute_create_stream(
    shard: &IggyShard,
    user_id: u32,
    wire: CreateStreamRequest,
) -> Result<StreamResponseData, IggyError> {
    shard.metadata.perm_create_stream(user_id)?;

    let stream_id = shard.create_stream(wire.name.to_string()).await?;

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
                command: wire,
            }),
        )
        .await?;

    Ok(response_data)
}

pub async fn execute_update_stream(
    shard: &IggyShard,
    user_id: u32,
    wire: UpdateStreamRequest,
) -> Result<(), IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let stream = shard.resolve_stream(&stream_id)?;
    shard.metadata.perm_update_stream(user_id, stream.id())?;

    shard.update_stream(stream, wire.name.to_string())?;

    shard
        .state
        .apply(user_id, &EntryCommand::UpdateStream(wire))
        .await?;

    Ok(())
}

pub async fn execute_delete_stream(
    shard: &IggyShard,
    user_id: u32,
    wire: DeleteStreamRequest,
) -> Result<(), IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let stream = shard.resolve_stream(&stream_id)?;
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

    shard.delete_stream(stream).await?;

    shard
        .state
        .apply(user_id, &EntryCommand::DeleteStream(wire))
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

    Ok(())
}

pub async fn execute_purge_stream(
    shard: &IggyShard,
    user_id: u32,
    wire: PurgeStreamRequest,
) -> Result<(), IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let stream = shard.resolve_stream(&stream_id)?;
    shard.metadata.perm_purge_stream(user_id, stream.id())?;

    shard.purge_stream(stream).await?;
    shard.purge_stream_local(stream).await?;

    shard
        .state
        .apply(user_id, &EntryCommand::PurgeStream(wire))
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
    wire: CreateTopicRequest,
) -> Result<TopicResponseData, IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let compression = CompressionAlgorithm::from_code(wire.compression_algorithm)?;
    let message_expiry = IggyExpiry::from(wire.message_expiry);
    let max_topic_size = MaxTopicSize::from(wire.max_topic_size);
    let replication_factor = if wire.replication_factor == 0 {
        None
    } else {
        Some(wire.replication_factor)
    };

    let stream = shard.resolve_stream(&stream_id)?;
    shard.metadata.perm_create_topic(user_id, stream.id())?;

    let topic_id = shard
        .create_topic(
            stream,
            wire.name.to_string(),
            message_expiry,
            compression,
            max_topic_size,
            replication_factor,
        )
        .await?;

    let resolved_topic = ResolvedTopic {
        stream_id: stream.id(),
        topic_id,
    };
    let partition_infos = shard
        .create_partitions(resolved_topic, wire.partitions_count)
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
                command: wire,
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
    wire: UpdateTopicRequest,
) -> Result<(), IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let topic_id = wire_id_to_identifier(&wire.topic_id)?;
    let compression = CompressionAlgorithm::from_code(wire.compression_algorithm)?;
    let message_expiry = IggyExpiry::from(wire.message_expiry);
    let max_topic_size = MaxTopicSize::from(wire.max_topic_size);
    let replication_factor = if wire.replication_factor == 0 {
        None
    } else {
        Some(wire.replication_factor)
    };

    let topic = shard.resolve_topic(&stream_id, &topic_id)?;
    shard
        .metadata
        .perm_update_topic(user_id, topic.stream_id, topic.topic_id)?;

    shard.update_topic(
        topic,
        wire.name.to_string(),
        message_expiry,
        compression,
        max_topic_size,
        replication_factor,
    )?;

    shard
        .state
        .apply(user_id, &EntryCommand::UpdateTopic(wire))
        .await?;

    Ok(())
}

pub async fn execute_delete_topic(
    shard: &IggyShard,
    user_id: u32,
    wire: DeleteTopicRequest,
) -> Result<(), IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let topic_id = wire_id_to_identifier(&wire.topic_id)?;
    let topic = shard.resolve_topic(&stream_id, &topic_id)?;
    shard
        .metadata
        .perm_delete_topic(user_id, topic.stream_id, topic.topic_id)?;

    // Capture partition_ids BEFORE deletion for broadcast
    let partition_ids = shard
        .metadata
        .get_partition_ids(topic.stream_id, topic.topic_id);

    shard.delete_topic(topic).await?;

    shard
        .state
        .apply(user_id, &EntryCommand::DeleteTopic(wire))
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

    Ok(())
}

pub async fn execute_purge_topic(
    shard: &IggyShard,
    user_id: u32,
    wire: PurgeTopicRequest,
) -> Result<(), IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let topic_id = wire_id_to_identifier(&wire.topic_id)?;
    let topic = shard.resolve_topic(&stream_id, &topic_id)?;
    shard
        .metadata
        .perm_purge_topic(user_id, topic.stream_id, topic.topic_id)?;

    shard.purge_topic(topic).await?;
    shard.purge_topic_local(topic).await?;

    shard
        .state
        .apply(user_id, &EntryCommand::PurgeTopic(wire))
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
    wire: CreatePartitionsRequest,
) -> Result<(), IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let topic_id = wire_id_to_identifier(&wire.topic_id)?;
    let topic = shard.resolve_topic(&stream_id, &topic_id)?;
    shard
        .metadata
        .perm_create_partitions(user_id, topic.stream_id, topic.topic_id)?;

    let partition_infos = shard
        .create_partitions(topic, wire.partitions_count)
        .await?;
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
        .apply(user_id, &EntryCommand::CreatePartitions(wire))
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

    Ok(())
}

pub async fn execute_delete_partitions(
    shard: &IggyShard,
    user_id: u32,
    wire: DeletePartitionsRequest,
) -> Result<(), IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let topic_id = wire_id_to_identifier(&wire.topic_id)?;
    let topic = shard.resolve_topic(&stream_id, &topic_id)?;
    shard
        .metadata
        .perm_delete_partitions(user_id, topic.stream_id, topic.topic_id)?;

    let deleted_partition_ids = shard
        .delete_partitions(topic, wire.partitions_count)
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
        .apply(user_id, &EntryCommand::DeletePartitions(wire))
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

    Ok(())
}

pub async fn execute_create_consumer_group(
    shard: &IggyShard,
    user_id: u32,
    wire: CreateConsumerGroupRequest,
) -> Result<ConsumerGroupResponseData, IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let topic_id = wire_id_to_identifier(&wire.topic_id)?;
    let topic = shard.resolve_topic(&stream_id, &topic_id)?;
    shard
        .metadata
        .perm_create_consumer_group(user_id, topic.stream_id, topic.topic_id)?;

    let group_id = shard.create_consumer_group(topic, wire.name.to_string())?;

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
                command: wire,
            }),
        )
        .await?;

    Ok(response_data)
}

pub async fn execute_delete_consumer_group(
    shard: &IggyShard,
    user_id: u32,
    wire: DeleteConsumerGroupRequest,
) -> Result<(), IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let topic_id = wire_id_to_identifier(&wire.topic_id)?;
    let group_id = wire_id_to_identifier(&wire.group_id)?;
    let group = shard.resolve_consumer_group(&stream_id, &topic_id, &group_id)?;
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
        .apply(user_id, &EntryCommand::DeleteConsumerGroup(wire))
        .await?;

    Ok(())
}

pub fn execute_join_consumer_group(
    shard: &IggyShard,
    user_id: u32,
    client_id: u32,
    wire: JoinConsumerGroupRequest,
) -> Result<(), IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let topic_id = wire_id_to_identifier(&wire.topic_id)?;
    let group_id = wire_id_to_identifier(&wire.group_id)?;
    let group = shard.resolve_consumer_group(&stream_id, &topic_id, &group_id)?;
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
    wire: LeaveConsumerGroupRequest,
) -> Result<(), IggyError> {
    let stream_id = wire_id_to_identifier(&wire.stream_id)?;
    let topic_id = wire_id_to_identifier(&wire.topic_id)?;
    let group_id = wire_id_to_identifier(&wire.group_id)?;
    let group = shard.resolve_consumer_group(&stream_id, &topic_id, &group_id)?;
    shard
        .metadata
        .perm_leave_consumer_group(user_id, group.stream_id, group.topic_id)?;

    shard.leave_consumer_group(client_id, group)?;

    Ok(())
}

pub async fn execute_create_user(
    shard: &IggyShard,
    user_id: u32,
    wire: CreateUserRequest,
) -> Result<User, IggyError> {
    shard.metadata.perm_create_user(user_id)?;

    let username = wire.username.to_string();
    let password = SecretString::from(wire.password.clone());
    let status = UserStatus::from_code(wire.status)?;
    let permissions = wire
        .permissions
        .as_ref()
        .map(wire_permissions_to_permissions);

    let user = shard.create_user(&username, password.expose_secret(), status, permissions)?;

    // Hash the password before persisting to WAL
    let mut wal_wire = wire;
    wal_wire.password = crypto::hash_password(password.expose_secret());

    shard
        .state
        .apply(
            user_id,
            &EntryCommand::CreateUser(CreateUserWithId {
                user_id: user.id,
                command: wal_wire,
            }),
        )
        .await?;

    Ok(user)
}

pub async fn execute_delete_user(
    shard: &IggyShard,
    user_id: u32,
    wire: DeleteUserRequest,
) -> Result<User, IggyError> {
    shard.metadata.perm_delete_user(user_id)?;

    let target_id = wire_id_to_identifier(&wire.user_id)?;
    let user = shard.delete_user(&target_id)?;

    shard
        .state
        .apply(user_id, &EntryCommand::DeleteUser(wire))
        .await?;

    Ok(user)
}

pub async fn execute_update_user(
    shard: &IggyShard,
    user_id: u32,
    wire: UpdateUserRequest,
) -> Result<User, IggyError> {
    shard.metadata.perm_update_user(user_id)?;

    let target_id = wire_id_to_identifier(&wire.user_id)?;
    let username = wire.username.as_ref().map(|n| n.to_string());
    let status = wire.status.map(UserStatus::from_code).transpose()?;
    let user = shard.update_user(&target_id, username, status)?;

    shard
        .state
        .apply(user_id, &EntryCommand::UpdateUser(wire))
        .await?;

    Ok(user)
}

pub async fn execute_change_password(
    shard: &IggyShard,
    user_id: u32,
    wire: ChangePasswordRequest,
) -> Result<(), IggyError> {
    let target_id = wire_id_to_identifier(&wire.user_id)?;
    let target_user = shard.get_user(&target_id)?;
    if target_user.id != user_id {
        shard.metadata.perm_change_password(user_id)?;
    }

    shard.change_password(&target_id, &wire.current_password, &wire.new_password)?;

    // Clear current password and hash new password before persisting to WAL
    let wal_wire = ChangePasswordRequest {
        user_id: wire.user_id,
        current_password: String::new(),
        new_password: crypto::hash_password(&wire.new_password),
    };

    shard
        .state
        .apply(user_id, &EntryCommand::ChangePassword(wal_wire))
        .await?;

    Ok(())
}

pub async fn execute_update_permissions(
    shard: &IggyShard,
    user_id: u32,
    wire: UpdatePermissionsRequest,
) -> Result<(), IggyError> {
    shard.metadata.perm_update_permissions(user_id)?;

    let target_id = wire_id_to_identifier(&wire.user_id)?;
    let target_user = shard.get_user(&target_id)?;
    if target_user.is_root() {
        return Err(IggyError::CannotChangePermissions(target_user.id));
    }

    let permissions = wire
        .permissions
        .as_ref()
        .map(wire_permissions_to_permissions);
    shard.update_permissions(&target_id, permissions)?;

    shard
        .state
        .apply(user_id, &EntryCommand::UpdatePermissions(wire))
        .await?;

    Ok(())
}

pub async fn execute_create_personal_access_token(
    shard: &IggyShard,
    user_id: u32,
    wire: CreatePersonalAccessTokenRequest,
) -> Result<(PersonalAccessToken, String), IggyError> {
    let name = wire.name.to_string();
    let expiry = IggyExpiry::from(wire.expiry);
    let (personal_access_token, token) =
        shard.create_personal_access_token(user_id, &name, expiry)?;

    shard
        .state
        .apply(
            user_id,
            &EntryCommand::CreatePersonalAccessToken(CreatePersonalAccessTokenWithHash {
                hash: personal_access_token.token.to_string(),
                command: wire,
            }),
        )
        .await?;

    Ok((personal_access_token, token))
}

pub async fn execute_delete_personal_access_token(
    shard: &IggyShard,
    user_id: u32,
    wire: DeletePersonalAccessTokenRequest,
) -> Result<(), IggyError> {
    shard.delete_personal_access_token(user_id, wire.name.as_str())?;

    shard
        .state
        .apply(user_id, &EntryCommand::DeletePersonalAccessToken(wire))
        .await?;

    Ok(())
}
