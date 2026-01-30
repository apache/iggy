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

use super::*;
use crate::{
    metadata::{PartitionId, TopicId},
    shard::{
        IggyShard,
        transmission::{
            event::ShardEvent,
            frame::ShardResponse,
            message::{ShardMessage, ShardRequest, ShardRequestPayload},
        },
    },
    streaming::utils::crypto,
    tcp::{
        connection_handler::{ConnectionAction, handle_connection, handle_error},
        tcp_listener::cleanup_connection,
    },
};
use compio::net::TcpStream;
use iggy_common::sharding::IggyNamespace;
use iggy_common::{Identifier, IggyError, SenderKind, TransportProtocol};
use nix::sys::stat::SFlag;
use std::os::fd::{FromRawFd, IntoRawFd};
use tracing::info;

pub(super) async fn handle_shard_message(
    shard: &Rc<IggyShard>,
    message: ShardMessage,
) -> Option<ShardResponse> {
    match message {
        ShardMessage::Request(request) => match handle_request(shard, request).await {
            Ok(response) => Some(response),
            Err(err) => Some(ShardResponse::ErrorResponse(err)),
        },
        ShardMessage::Event(event) => match handle_event(shard, event).await {
            Ok(_) => Some(ShardResponse::Event),
            Err(err) => Some(ShardResponse::ErrorResponse(err)),
        },
    }
}

async fn handle_request(
    shard: &Rc<IggyShard>,
    request: ShardRequest,
) -> Result<ShardResponse, IggyError> {
    let stream_id = request.stream_id;
    let topic_id = request.topic_id;
    let partition_id = request.partition_id;
    match request.payload {
        ShardRequestPayload::SendMessages { batch } => {
            let batch = shard.maybe_encrypt_messages(batch)?;
            let messages_count = batch.count();

            let (stream, topic) = shard.resolve_topic_id(&stream_id, &topic_id)?;
            let namespace = IggyNamespace::new(stream, topic, partition_id);

            shard.ensure_partition(&namespace).await?;

            shard
                .append_messages_to_local_partition(&namespace, batch, &shard.config.system)
                .await?;

            shard.metrics.increment_messages(messages_count as u64);
            Ok(ShardResponse::SendMessages)
        }
        ShardRequestPayload::PollMessages { args, consumer } => {
            let auto_commit = args.auto_commit;

            let (stream, topic) = shard.resolve_topic_id(&stream_id, &topic_id)?;
            let namespace = IggyNamespace::new(stream, topic, partition_id);

            shard.ensure_partition(&namespace).await?;

            let (poll_metadata, batches) = shard
                .poll_messages_from_local_partition(&namespace, consumer, args)
                .await?;

            if auto_commit && !batches.is_empty() {
                let offset = batches
                    .last_offset()
                    .expect("Batch set should have at least one batch");
                shard
                    .auto_commit_consumer_offset_from_local_partition(&namespace, consumer, offset)
                    .await?;
            }
            Ok(ShardResponse::PollMessages((poll_metadata, batches)))
        }
        ShardRequestPayload::FlushUnsavedBuffer { fsync } => {
            let (stream, topic) = shard.resolve_topic_id(&stream_id, &topic_id)?;
            shard
                .flush_unsaved_buffer_base(stream, topic, partition_id, fsync)
                .await?;
            Ok(ShardResponse::FlushUnsavedBuffer)
        }
        ShardRequestPayload::DeleteSegments { segments_count } => {
            let (stream, topic) = shard.resolve_topic_id(&stream_id, &topic_id)?;
            shard
                .delete_segments_base(stream, topic, partition_id, segments_count)
                .await?;
            Ok(ShardResponse::DeleteSegments)
        }
        ShardRequestPayload::CreatePartitions {
            user_id,
            stream_id,
            topic_id,
            partitions_count,
        } => {
            assert_eq!(
                shard.id, 0,
                "CreatePartitions should only be handled by shard0"
            );

            let _partition_guard = shard.fs_locks.partition_lock.lock().await;

            let partition_infos = shard
                .create_partitions(&stream_id, &topic_id, partitions_count)
                .await?;
            let partition_ids = partition_infos.iter().map(|p| p.id).collect::<Vec<_>>();

            let event = ShardEvent::CreatedPartitions {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partitions: partition_infos,
            };
            shard.broadcast_event_to_all_shards(event).await?;

            let (numeric_stream_id, numeric_topic_id) =
                shard.resolve_topic_id(&stream_id, &topic_id)?;
            let total_partition_count = shard
                .metadata
                .partitions_count(numeric_stream_id, numeric_topic_id)
                as u32;
            shard.writer().rebalance_consumer_groups_for_topic(
                numeric_stream_id,
                numeric_topic_id,
                total_partition_count,
            );

            let command = iggy_common::create_partitions::CreatePartitions {
                stream_id,
                topic_id,
                partitions_count,
            };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::CreatePartitions(command),
                )
                .await?;

            Ok(ShardResponse::CreatePartitionsResponse(partition_ids))
        }
        ShardRequestPayload::DeletePartitions {
            user_id,
            stream_id,
            topic_id,
            partitions_count,
        } => {
            assert_eq!(
                shard.id, 0,
                "DeletePartitions should only be handled by shard0"
            );

            let _partition_guard = shard.fs_locks.partition_lock.lock().await;

            let deleted_partition_ids = shard
                .delete_partitions(&stream_id, &topic_id, partitions_count)
                .await?;

            let event = ShardEvent::DeletedPartitions {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partitions_count,
                partition_ids: deleted_partition_ids.clone(),
            };
            shard.broadcast_event_to_all_shards(event).await?;

            let (numeric_stream_id, numeric_topic_id) =
                shard.resolve_topic_id(&stream_id, &topic_id)?;
            let remaining_partition_count = shard
                .metadata
                .partitions_count(numeric_stream_id, numeric_topic_id)
                as u32;
            shard.writer().rebalance_consumer_groups_for_topic(
                numeric_stream_id,
                numeric_topic_id,
                remaining_partition_count,
            );

            let command = iggy_common::delete_partitions::DeletePartitions {
                stream_id,
                topic_id,
                partitions_count,
            };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::DeletePartitions(command),
                )
                .await?;

            Ok(ShardResponse::DeletePartitionsResponse(
                deleted_partition_ids,
            ))
        }
        ShardRequestPayload::CreateStream { user_id, name } => {
            assert_eq!(shard.id, 0, "CreateStream should only be handled by shard0");

            // Acquire stream lock to serialize filesystem operations
            let _stream_guard = shard.fs_locks.stream_lock.lock().await;

            let created_stream_id = shard.create_stream(name.clone()).await?;

            let command = iggy_common::create_stream::CreateStream { name };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::CreateStream(
                        crate::state::models::CreateStreamWithId {
                            stream_id: created_stream_id as u32,
                            command,
                        },
                    ),
                )
                .await?;

            Ok(ShardResponse::CreateStreamResponse(created_stream_id))
        }
        ShardRequestPayload::CreateTopic {
            user_id,
            stream_id,
            name,
            partitions_count,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        } => {
            assert_eq!(shard.id, 0, "CreateTopic should only be handled by shard0");

            // Acquire topic lock to serialize filesystem operations
            let _topic_guard = shard.fs_locks.topic_lock.lock().await;

            let topic_id_num = shard
                .create_topic(
                    &stream_id,
                    name.clone(),
                    message_expiry,
                    compression_algorithm,
                    max_topic_size,
                    replication_factor,
                )
                .await?;

            let partition_infos = shard
                .create_partitions(
                    &stream_id,
                    &Identifier::numeric(topic_id_num as u32).unwrap(),
                    partitions_count,
                )
                .await?;

            let event = ShardEvent::CreatedPartitions {
                stream_id: stream_id.clone(),
                topic_id: Identifier::numeric(topic_id_num as u32).unwrap(),
                partitions: partition_infos,
            };
            shard.broadcast_event_to_all_shards(event).await?;

            let command = iggy_common::create_topic::CreateTopic {
                stream_id,
                partitions_count,
                compression_algorithm,
                message_expiry,
                max_topic_size,
                replication_factor,
                name,
            };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::CreateTopic(
                        crate::state::models::CreateTopicWithId {
                            topic_id: topic_id_num as u32,
                            command,
                        },
                    ),
                )
                .await?;

            Ok(ShardResponse::CreateTopicResponse(topic_id_num))
        }
        ShardRequestPayload::UpdateTopic {
            user_id,
            stream_id,
            topic_id,
            name,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        } => {
            assert_eq!(shard.id, 0, "UpdateTopic should only be handled by shard0");

            shard.update_topic(
                &stream_id,
                &topic_id,
                name.clone(),
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
            )?;

            let command = iggy_common::update_topic::UpdateTopic {
                stream_id,
                topic_id,
                compression_algorithm,
                message_expiry,
                max_topic_size,
                replication_factor,
                name,
            };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::UpdateTopic(command),
                )
                .await?;

            Ok(ShardResponse::UpdateTopicResponse)
        }
        ShardRequestPayload::DeleteTopic {
            user_id,
            stream_id,
            topic_id,
        } => {
            assert_eq!(shard.id, 0, "DeleteTopic should only be handled by shard0");

            // Capture numeric IDs and partition_ids BEFORE deletion for broadcast.
            let (numeric_stream_id, numeric_topic_id) =
                shard.resolve_topic_id(&stream_id, &topic_id)?;
            let partition_ids = shard
                .metadata
                .get_partition_ids(numeric_stream_id, numeric_topic_id);

            let _topic_guard = shard.fs_locks.topic_lock.lock().await;
            let topic_info = shard.delete_topic(&stream_id, &topic_id).await?;
            let topic_id_num = topic_info.id;

            // Broadcast to all shards to clean up their local_partitions entries.
            // Use numeric Identifiers since the topic is already deleted from metadata.
            let event = ShardEvent::DeletedPartitions {
                stream_id: Identifier::numeric(numeric_stream_id as u32).unwrap(),
                topic_id: Identifier::numeric(numeric_topic_id as u32).unwrap(),
                partitions_count: partition_ids.len() as u32,
                partition_ids,
            };
            shard.broadcast_event_to_all_shards(event).await?;

            let command = iggy_common::delete_topic::DeleteTopic {
                stream_id,
                topic_id,
            };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::DeleteTopic(command),
                )
                .await?;

            Ok(ShardResponse::DeleteTopicResponse(topic_id_num))
        }
        ShardRequestPayload::CreateUser {
            user_id: session_user_id,
            username,
            password,
            status,
            permissions,
        } => {
            assert_eq!(shard.id, 0, "CreateUser should only be handled by shard0");

            let _user_guard = shard.fs_locks.user_lock.lock().await;
            let user = shard.create_user(&username, &password, status, permissions.clone())?;

            let command = iggy_common::create_user::CreateUser {
                username,
                password: crypto::hash_password(&password),
                status,
                permissions,
            };
            shard
                .state
                .apply(
                    session_user_id,
                    &crate::state::command::EntryCommand::CreateUser(
                        crate::state::models::CreateUserWithId {
                            user_id: user.id,
                            command,
                        },
                    ),
                )
                .await?;

            Ok(ShardResponse::CreateUserResponse(user))
        }
        ShardRequestPayload::GetStats { .. } => {
            assert_eq!(shard.id, 0, "GetStats should only be handled by shard0");
            let stats = shard.get_stats().await?;
            Ok(ShardResponse::GetStatsResponse(stats))
        }
        ShardRequestPayload::DeleteUser {
            session_user_id,
            user_id,
        } => {
            assert_eq!(shard.id, 0, "DeleteUser should only be handled by shard0");

            let _user_guard = shard.fs_locks.user_lock.lock().await;
            let user = shard.delete_user(&user_id)?;

            let command = iggy_common::delete_user::DeleteUser { user_id };
            shard
                .state
                .apply(
                    session_user_id,
                    &crate::state::command::EntryCommand::DeleteUser(command),
                )
                .await?;

            Ok(ShardResponse::DeletedUser(user))
        }
        ShardRequestPayload::UpdateStream {
            user_id,
            stream_id,
            name,
        } => {
            assert_eq!(shard.id, 0, "UpdateStream should only be handled by shard0");

            shard.update_stream(&stream_id, name.clone())?;

            let command = iggy_common::update_stream::UpdateStream { stream_id, name };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::UpdateStream(command),
                )
                .await?;

            Ok(ShardResponse::UpdateStreamResponse)
        }
        ShardRequestPayload::DeleteStream { user_id, stream_id } => {
            assert_eq!(shard.id, 0, "DeleteStream should only be handled by shard0");

            // Capture numeric stream ID and all topic/partition info BEFORE deletion for broadcast.
            let numeric_stream_id = shard.resolve_stream_id(&stream_id)?;
            let topics_with_partitions: Vec<(TopicId, Vec<PartitionId>)> = shard
                .metadata
                .get_topic_ids(numeric_stream_id)
                .into_iter()
                .map(|topic_id| {
                    let partition_ids = shard
                        .metadata
                        .get_partition_ids(numeric_stream_id, topic_id);
                    (topic_id, partition_ids)
                })
                .collect();

            let _stream_guard = shard.fs_locks.stream_lock.lock().await;
            let stream_info = shard.delete_stream(&stream_id).await?;
            let stream_id_num = stream_info.id;

            // Broadcast DeletedPartitions to all shards for each topic's partitions.
            // Use numeric Identifiers since the stream is already deleted from metadata.
            for (topic_id, partition_ids) in topics_with_partitions {
                if partition_ids.is_empty() {
                    continue;
                }
                let event = ShardEvent::DeletedPartitions {
                    stream_id: Identifier::numeric(numeric_stream_id as u32).unwrap(),
                    topic_id: Identifier::numeric(topic_id as u32).unwrap(),
                    partitions_count: partition_ids.len() as u32,
                    partition_ids,
                };
                shard.broadcast_event_to_all_shards(event).await?;
            }

            let command = iggy_common::delete_stream::DeleteStream { stream_id };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::DeleteStream(command),
                )
                .await?;

            Ok(ShardResponse::DeleteStreamResponse(stream_id_num))
        }
        ShardRequestPayload::UpdatePermissions {
            session_user_id,
            user_id,
            permissions,
        } => {
            assert_eq!(
                shard.id, 0,
                "UpdatePermissions should only be handled by shard0"
            );

            let _user_guard = shard.fs_locks.user_lock.lock().await;
            shard.update_permissions(&user_id, permissions.clone())?;

            let command = iggy_common::update_permissions::UpdatePermissions {
                user_id,
                permissions,
            };
            shard
                .state
                .apply(
                    session_user_id,
                    &crate::state::command::EntryCommand::UpdatePermissions(command),
                )
                .await?;

            Ok(ShardResponse::UpdatePermissionsResponse)
        }
        ShardRequestPayload::ChangePassword {
            session_user_id,
            user_id,
            current_password,
            new_password,
        } => {
            assert_eq!(
                shard.id, 0,
                "ChangePassword should only be handled by shard0"
            );

            let _user_guard = shard.fs_locks.user_lock.lock().await;
            shard.change_password(&user_id, &current_password, &new_password)?;

            let command = iggy_common::change_password::ChangePassword {
                user_id,
                current_password: "".into(),
                new_password: crypto::hash_password(&new_password),
            };
            shard
                .state
                .apply(
                    session_user_id,
                    &crate::state::command::EntryCommand::ChangePassword(command),
                )
                .await?;

            Ok(ShardResponse::ChangePasswordResponse)
        }
        ShardRequestPayload::UpdateUser {
            session_user_id,
            user_id,
            username,
            status,
        } => {
            assert_eq!(shard.id, 0, "UpdateUser should only be handled by shard0");

            let _user_guard = shard.fs_locks.user_lock.lock().await;
            let user = shard.update_user(&user_id, username.clone(), status)?;

            let command = iggy_common::update_user::UpdateUser {
                user_id,
                username,
                status,
            };
            shard
                .state
                .apply(
                    session_user_id,
                    &crate::state::command::EntryCommand::UpdateUser(command),
                )
                .await?;

            Ok(ShardResponse::UpdateUserResponse(user))
        }
        ShardRequestPayload::CreateConsumerGroup {
            user_id,
            stream_id,
            topic_id,
            name,
        } => {
            assert_eq!(
                shard.id, 0,
                "CreateConsumerGroup should only be handled by shard0"
            );

            let cg_id = shard.create_consumer_group(&stream_id, &topic_id, name.clone())?;

            let command = iggy_common::create_consumer_group::CreateConsumerGroup {
                stream_id,
                topic_id,
                name,
            };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::CreateConsumerGroup(
                        crate::state::models::CreateConsumerGroupWithId {
                            group_id: cg_id as u32,
                            command,
                        },
                    ),
                )
                .await?;

            Ok(ShardResponse::CreateConsumerGroupResponse(cg_id))
        }
        ShardRequestPayload::JoinConsumerGroup {
            user_id: _,
            client_id,
            stream_id,
            topic_id,
            group_id,
        } => {
            assert_eq!(
                shard.id, 0,
                "JoinConsumerGroup should only be handled by shard0"
            );

            shard.join_consumer_group(client_id, &stream_id, &topic_id, &group_id)?;

            Ok(ShardResponse::JoinConsumerGroupResponse)
        }
        ShardRequestPayload::LeaveConsumerGroup {
            user_id: _,
            client_id,
            stream_id,
            topic_id,
            group_id,
        } => {
            assert_eq!(
                shard.id, 0,
                "LeaveConsumerGroup should only be handled by shard0"
            );

            shard.leave_consumer_group(client_id, &stream_id, &topic_id, &group_id)?;

            Ok(ShardResponse::LeaveConsumerGroupResponse)
        }
        ShardRequestPayload::DeleteConsumerGroup {
            user_id,
            stream_id,
            topic_id,
            group_id,
        } => {
            assert_eq!(
                shard.id, 0,
                "DeleteConsumerGroup should only be handled by shard0"
            );

            let cg_meta = shard.delete_consumer_group(&stream_id, &topic_id, &group_id)?;

            let cg_id = crate::streaming::polling_consumer::ConsumerGroupId(cg_meta.id);
            shard
                .delete_consumer_group_offsets(cg_id, &stream_id, &topic_id, &cg_meta.partitions)
                .await?;

            let command = iggy_common::delete_consumer_group::DeleteConsumerGroup {
                stream_id,
                topic_id,
                group_id,
            };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::DeleteConsumerGroup(command),
                )
                .await?;

            Ok(ShardResponse::DeleteConsumerGroupResponse)
        }
        ShardRequestPayload::CreatePersonalAccessToken {
            user_id,
            name,
            expiry,
        } => {
            assert_eq!(
                shard.id, 0,
                "CreatePersonalAccessToken should only be handled by shard0"
            );

            let (personal_access_token, token) =
                shard.create_personal_access_token(user_id, &name, expiry)?;

            let command = iggy_common::create_personal_access_token::CreatePersonalAccessToken {
                name,
                expiry,
            };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::CreatePersonalAccessToken(
                        crate::state::models::CreatePersonalAccessTokenWithHash {
                            hash: personal_access_token.token.to_string(),
                            command,
                        },
                    ),
                )
                .await?;

            Ok(ShardResponse::CreatePersonalAccessTokenResponse(
                personal_access_token,
                token,
            ))
        }
        ShardRequestPayload::DeletePersonalAccessToken { user_id, name } => {
            assert_eq!(
                shard.id, 0,
                "DeletePersonalAccessToken should only be handled by shard0"
            );

            shard.delete_personal_access_token(user_id, &name)?;

            let command =
                iggy_common::delete_personal_access_token::DeletePersonalAccessToken { name };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::DeletePersonalAccessToken(command),
                )
                .await?;

            Ok(ShardResponse::DeletePersonalAccessTokenResponse)
        }
        ShardRequestPayload::LeaveConsumerGroupMetadataOnly {
            stream_id,
            topic_id,
            group_id,
            client_id,
        } => {
            assert_eq!(
                shard.id, 0,
                "LeaveConsumerGroupMetadataOnly should only be handled by shard0"
            );

            shard
                .writer()
                .leave_consumer_group(stream_id, topic_id, group_id, client_id);

            Ok(ShardResponse::LeaveConsumerGroupMetadataOnlyResponse)
        }
        ShardRequestPayload::SocketTransfer {
            fd,
            from_shard,
            client_id,
            user_id,
            address,
            initial_data,
        } => {
            info!(
                "Received socket transfer msg, fd: {fd:?}, from_shard: {from_shard}, address: {address}"
            );

            // Safety: The fd already != 1.
            let stat = nix::sys::stat::fstat(&fd)
                .map_err(|e| IggyError::IoError(format!("Invalid fd: {}", e)))?;

            if !SFlag::from_bits_truncate(stat.st_mode).contains(SFlag::S_IFSOCK) {
                return Err(IggyError::IoError(format!("fd {:?} is not a socket", fd)));
            }

            // restore TcpStream from fd
            let tcp_stream = unsafe { TcpStream::from_raw_fd(fd.into_raw_fd()) };
            let session = shard.add_client(&address, TransportProtocol::Tcp);
            session.set_user_id(user_id);
            session.set_migrated();

            let mut sender = SenderKind::get_tcp_sender(tcp_stream);
            let conn_stop_receiver = shard.task_registry.add_connection(session.client_id);
            let shard_for_conn = shard.clone();
            let registry = shard.task_registry.clone();
            let registry_clone = registry.clone();

            let batch = shard.maybe_encrypt_messages(initial_data)?;
            let messages_count = batch.count();

            // Get numeric IDs for local_partitions lookup
            let (numeric_stream_id, numeric_topic_id) =
                shard.resolve_topic_id(&stream_id, &topic_id)?;

            let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
            shard.ensure_partition(&namespace).await?;

            shard
                .append_messages_to_local_partition(&namespace, batch, &shard.config.system)
                .await?;

            shard.metrics.increment_messages(messages_count as u64);

            sender.send_empty_ok_response().await?;

            registry.spawn_connection(async move {
                match handle_connection(&session, &mut sender, &shard_for_conn, conn_stop_receiver)
                    .await
                {
                    Ok(ConnectionAction::Migrated { to_shard }) => {
                        info!("Migrated to shard {to_shard}, ignore cleanup connection");
                    }
                    Ok(ConnectionAction::Finished) => {
                        cleanup_connection(
                            &mut sender,
                            client_id,
                            address,
                            &registry_clone,
                            &shard_for_conn,
                        )
                        .await;
                    }
                    Err(err) => {
                        handle_error(err);
                        cleanup_connection(
                            &mut sender,
                            client_id,
                            address,
                            &registry_clone,
                            &shard_for_conn,
                        )
                        .await;
                    }
                }
            });

            Ok(ShardResponse::SocketTransferResponse)
        }
        ShardRequestPayload::PurgeStream { user_id, stream_id } => {
            assert_eq!(shard.id, 0, "PurgeStream should only be handled by shard0");

            shard.purge_stream(&stream_id).await?;

            let event = ShardEvent::PurgedStream {
                stream_id: stream_id.clone(),
            };
            shard.broadcast_event_to_all_shards(event).await?;

            let command = iggy_common::purge_stream::PurgeStream { stream_id };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::PurgeStream(command),
                )
                .await?;

            Ok(ShardResponse::PurgeStreamResponse)
        }
        ShardRequestPayload::PurgeTopic {
            user_id,
            stream_id,
            topic_id,
        } => {
            assert_eq!(shard.id, 0, "PurgeTopic should only be handled by shard0");

            shard.purge_topic(&stream_id, &topic_id).await?;

            let event = ShardEvent::PurgedTopic {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
            };
            shard.broadcast_event_to_all_shards(event).await?;

            let command = iggy_common::purge_topic::PurgeTopic {
                stream_id,
                topic_id,
            };
            shard
                .state
                .apply(
                    user_id,
                    &crate::state::command::EntryCommand::PurgeTopic(command),
                )
                .await?;

            Ok(ShardResponse::PurgeTopicResponse)
        }
    }
}

pub async fn handle_event(shard: &Rc<IggyShard>, event: ShardEvent) -> Result<(), IggyError> {
    match event {
        ShardEvent::DeletedPartitions {
            stream_id,
            topic_id,
            partitions_count: _,
            partition_ids,
        } => {
            // SharedMetadata was already updated by the request handler before broadcasting.
            // Here we only need to clean up local local_partitions entries on all shards.
            //
            // For DeleteTopic, the topic is already removed from metadata, so we extract
            // numeric IDs directly from the Identifier (which must be numeric in that case).
            // For DeletePartitions, the topic still exists, so metadata lookup works.
            let numeric_stream_id = stream_id
                .get_u32_value()
                .map(|v| v as usize)
                .unwrap_or_else(|_| shard.metadata.get_stream_id(&stream_id).unwrap_or_default());
            let numeric_topic_id =
                topic_id
                    .get_u32_value()
                    .map(|v| v as usize)
                    .unwrap_or_else(|_| {
                        shard
                            .metadata
                            .get_topic_id(numeric_stream_id, &topic_id)
                            .unwrap_or_default()
                    });
            let mut partitions = shard.local_partitions.borrow_mut();
            for partition_id in partition_ids {
                let ns = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
                partitions.remove(&ns);
            }
            Ok(())
        }
        ShardEvent::PurgedStream { stream_id } => {
            shard.purge_stream_bypass_auth(&stream_id).await?;
            Ok(())
        }
        ShardEvent::PurgedTopic {
            stream_id,
            topic_id,
        } => {
            shard.purge_topic_bypass_auth(&stream_id, &topic_id).await?;
            Ok(())
        }
        ShardEvent::AddressBound { protocol, address } => {
            info!(
                "Received AddressBound event for {:?} with address: {}",
                protocol, address
            );
            match protocol {
                TransportProtocol::Tcp => {
                    shard.tcp_bound_address.set(Some(address));
                    let _ = shard.config_writer_notify.try_send(());
                }
                TransportProtocol::Quic => {
                    shard.quic_bound_address.set(Some(address));
                    let _ = shard.config_writer_notify.try_send(());
                }
                TransportProtocol::Http => {
                    shard.http_bound_address.set(Some(address));
                    let _ = shard.config_writer_notify.try_send(());
                }
                TransportProtocol::WebSocket => {
                    shard.websocket_bound_address.set(Some(address));
                    let _ = shard.config_writer_notify.try_send(());
                }
            }
            Ok(())
        }
        ShardEvent::CreatedPartitions {
            stream_id,
            topic_id,
            partitions,
        } => {
            let numeric_stream_id = match shard.metadata.get_stream_id(&stream_id) {
                Some(id) => id,
                None => {
                    tracing::warn!(
                        "CreatedPartitions: stream {:?} not found in SharedMetadata",
                        stream_id
                    );
                    return Ok(());
                }
            };
            let numeric_topic_id = match shard.metadata.get_topic_id(numeric_stream_id, &topic_id) {
                Some(id) => id,
                None => {
                    tracing::warn!(
                        "CreatedPartitions: topic {:?} not found in SharedMetadata for stream {}",
                        topic_id,
                        numeric_stream_id
                    );
                    return Ok(());
                }
            };

            let shards_count = shard.get_available_shards_count();
            for partition_info in partitions {
                let ns = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_info.id);
                let owner_shard_id = crate::shard::calculate_shard_assignment(&ns, shards_count);

                if shard.id == owner_shard_id as u16 {
                    shard.ensure_partition(&ns).await?;
                }
            }
            Ok(())
        }
        ShardEvent::FlushUnsavedBuffer {
            stream_id,
            topic_id,
            partition_id,
            fsync,
        } => {
            let numeric_stream_id = match shard.metadata.get_stream_id(&stream_id) {
                Some(id) => id,
                None => return Ok(()),
            };
            let numeric_topic_id = match shard.metadata.get_topic_id(numeric_stream_id, &topic_id) {
                Some(id) => id,
                None => return Ok(()),
            };

            let ns = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
            if shard.local_partitions.borrow().get(&ns).is_some() {
                shard
                    .flush_unsaved_buffer_from_local_partitions(&ns, fsync)
                    .await?;
            }
            Ok(())
        }
    }
}
