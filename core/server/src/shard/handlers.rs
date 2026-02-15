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
    shard::{
        IggyShard, execution,
        transmission::{
            event::ShardEvent,
            frame::ShardResponse,
            message::{ShardMessage, ShardRequest, ShardRequestPayload},
        },
    },
    tcp::{
        connection_handler::{ConnectionAction, handle_connection, handle_error},
        tcp_listener::cleanup_connection,
    },
};
use compio::net::TcpStream;
use iggy_common::{IggyError, SenderKind, TransportProtocol, sharding::IggyNamespace};
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
    // Data-plane operations extract namespace from routing
    let namespace = request.routing;
    match request.payload {
        ShardRequestPayload::SendMessages { batch } => {
            let batch = shard.maybe_encrypt_messages(batch)?;
            let messages_count = batch.count();

            let namespace = namespace.expect("SendMessages requires routing namespace");

            shard.ensure_partition(&namespace).await?;

            shard
                .append_messages_to_local_partition(&namespace, batch, &shard.config.system)
                .await?;

            shard.metrics.increment_messages(messages_count as u64);
            Ok(ShardResponse::SendMessages)
        }
        ShardRequestPayload::PollMessages { args, consumer } => {
            let namespace = namespace.expect("PollMessages requires routing namespace");

            if args.count == 0 {
                let current_offset = shard
                    .local_partitions
                    .borrow()
                    .get(&namespace)
                    .map(|p| p.offset.load(std::sync::atomic::Ordering::Relaxed))
                    .unwrap_or(0);
                return Ok(ShardResponse::PollMessages((
                    iggy_common::IggyPollMetadata::new(
                        namespace.partition_id() as u32,
                        current_offset,
                    ),
                    crate::streaming::segments::IggyMessagesBatchSet::empty(),
                )));
            }

            let auto_commit = args.auto_commit;

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
            let ns = namespace.expect("FlushUnsavedBuffer requires routing namespace");
            let flushed_count = shard
                .flush_unsaved_buffer_from_local_partitions(&ns, fsync)
                .await?;
            Ok(ShardResponse::FlushUnsavedBuffer { flushed_count })
        }
        ShardRequestPayload::DeleteSegments { segments_count } => {
            let ns = namespace.expect("DeleteSegments requires routing namespace");
            let (deleted_segments, deleted_messages) = shard
                .delete_oldest_segments(
                    ns.stream_id(),
                    ns.topic_id(),
                    ns.partition_id(),
                    segments_count,
                )
                .await?;
            Ok(ShardResponse::DeleteSegments {
                deleted_segments,
                deleted_messages,
            })
        }
        ShardRequestPayload::CleanTopicMessages {
            stream_id,
            topic_id,
            partition_ids,
        } => {
            let (deleted_segments, deleted_messages) = shard
                .clean_topic_messages(stream_id, topic_id, &partition_ids)
                .await?;
            Ok(ShardResponse::CleanTopicMessages {
                deleted_segments,
                deleted_messages,
            })
        }
        ShardRequestPayload::CreatePartitionsRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "CreatePartitionsRequest should only be handled by shard0"
            );

            let result = execution::execute_create_partitions(shard, user_id, command).await?;
            Ok(ShardResponse::CreatePartitionsResponse(
                result.partition_ids,
            ))
        }
        ShardRequestPayload::DeletePartitionsRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "DeletePartitionsRequest should only be handled by shard0"
            );

            let result = execution::execute_delete_partitions(shard, user_id, command).await?;
            Ok(ShardResponse::DeletePartitionsResponse(
                result.partition_ids,
            ))
        }
        ShardRequestPayload::CreateStreamRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "CreateStreamRequest should only be handled by shard0"
            );

            let result = execution::execute_create_stream(shard, user_id, command).await?;
            Ok(ShardResponse::CreateStreamResponse(result))
        }
        ShardRequestPayload::CreateTopicRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "CreateTopicRequest should only be handled by shard0"
            );

            let result = execution::execute_create_topic(shard, user_id, command).await?;
            Ok(ShardResponse::CreateTopicResponse(result))
        }
        ShardRequestPayload::UpdateTopicRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "UpdateTopicRequest should only be handled by shard0"
            );

            execution::execute_update_topic(shard, user_id, command).await?;
            Ok(ShardResponse::UpdateTopicResponse)
        }
        ShardRequestPayload::DeleteTopicRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "DeleteTopicRequest should only be handled by shard0"
            );

            let result = execution::execute_delete_topic(shard, user_id, command).await?;
            Ok(ShardResponse::DeleteTopicResponse(result.topic_id))
        }
        ShardRequestPayload::CreateUserRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "CreateUserRequest should only be handled by shard0"
            );
            let user = execution::execute_create_user(shard, user_id, command).await?;
            Ok(ShardResponse::CreateUserResponse(user))
        }
        ShardRequestPayload::GetStats { .. } => {
            assert_eq!(shard.id, 0, "GetStats should only be handled by shard0");
            let stats = shard.get_stats().await?;
            Ok(ShardResponse::GetStatsResponse(stats))
        }
        ShardRequestPayload::DeleteUserRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "DeleteUserRequest should only be handled by shard0"
            );
            let user = execution::execute_delete_user(shard, user_id, command).await?;
            Ok(ShardResponse::DeleteUserResponse(user))
        }
        ShardRequestPayload::UpdateStreamRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "UpdateStreamRequest should only be handled by shard0"
            );

            execution::execute_update_stream(shard, user_id, command).await?;
            Ok(ShardResponse::UpdateStreamResponse)
        }
        ShardRequestPayload::DeleteStreamRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "DeleteStreamRequest should only be handled by shard0"
            );

            let result = execution::execute_delete_stream(shard, user_id, command).await?;
            Ok(ShardResponse::DeleteStreamResponse(result.stream_id))
        }
        ShardRequestPayload::UpdatePermissionsRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "UpdatePermissionsRequest should only be handled by shard0"
            );
            execution::execute_update_permissions(shard, user_id, command).await?;
            Ok(ShardResponse::UpdatePermissionsResponse)
        }
        ShardRequestPayload::ChangePasswordRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "ChangePasswordRequest should only be handled by shard0"
            );
            execution::execute_change_password(shard, user_id, command).await?;
            Ok(ShardResponse::ChangePasswordResponse)
        }
        ShardRequestPayload::UpdateUserRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "UpdateUserRequest should only be handled by shard0"
            );
            let user = execution::execute_update_user(shard, user_id, command).await?;
            Ok(ShardResponse::UpdateUserResponse(user))
        }
        ShardRequestPayload::CreateConsumerGroupRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "CreateConsumerGroupRequest should only be handled by shard0"
            );

            let result = execution::execute_create_consumer_group(shard, user_id, command).await?;
            Ok(ShardResponse::CreateConsumerGroupResponse(result))
        }
        ShardRequestPayload::JoinConsumerGroupRequest {
            user_id,
            client_id,
            command,
        } => {
            assert_eq!(
                shard.id, 0,
                "JoinConsumerGroupRequest should only be handled by shard0"
            );

            execution::execute_join_consumer_group(shard, user_id, client_id, command)?;
            Ok(ShardResponse::JoinConsumerGroupResponse)
        }
        ShardRequestPayload::LeaveConsumerGroupRequest {
            user_id,
            client_id,
            command,
        } => {
            assert_eq!(
                shard.id, 0,
                "LeaveConsumerGroupRequest should only be handled by shard0"
            );

            execution::execute_leave_consumer_group(shard, user_id, client_id, command)?;
            Ok(ShardResponse::LeaveConsumerGroupResponse)
        }
        ShardRequestPayload::DeleteConsumerGroupRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "DeleteConsumerGroupRequest should only be handled by shard0"
            );

            execution::execute_delete_consumer_group(shard, user_id, command).await?;
            Ok(ShardResponse::DeleteConsumerGroupResponse)
        }
        ShardRequestPayload::CreatePersonalAccessTokenRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "CreatePersonalAccessTokenRequest should only be handled by shard0"
            );

            let (personal_access_token, token) =
                execution::execute_create_personal_access_token(shard, user_id, command).await?;

            Ok(ShardResponse::CreatePersonalAccessTokenResponse(
                personal_access_token,
                token,
            ))
        }
        ShardRequestPayload::DeletePersonalAccessTokenRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "DeletePersonalAccessTokenRequest should only be handled by shard0"
            );

            execution::execute_delete_personal_access_token(shard, user_id, command).await?;

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
        ShardRequestPayload::CompletePartitionRevocation {
            stream_id,
            topic_id,
            group_id,
            member_slab_id,
            member_id,
            partition_id,
            timed_out,
        } => {
            assert_eq!(
                shard.id, 0,
                "CompletePartitionRevocation should only be handled by shard0"
            );

            shard.writer().complete_partition_revocation(
                stream_id,
                topic_id,
                group_id,
                member_slab_id,
                member_id,
                partition_id,
                timed_out,
            );

            Ok(ShardResponse::CompletePartitionRevocationResponse)
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

            let ns = namespace.expect("SocketTransfer requires routing namespace");
            shard.ensure_partition(&ns).await?;

            shard
                .append_messages_to_local_partition(&ns, batch, &shard.config.system)
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
        ShardRequestPayload::PurgeStreamRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "PurgeStreamRequest should only be handled by shard0"
            );

            execution::execute_purge_stream(shard, user_id, command).await?;
            Ok(ShardResponse::PurgeStreamResponse)
        }
        ShardRequestPayload::PurgeTopicRequest { user_id, command } => {
            assert_eq!(
                shard.id, 0,
                "PurgeTopicRequest should only be handled by shard0"
            );

            execution::execute_purge_topic(shard, user_id, command).await?;
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
            let stream = shard.resolve_stream(&stream_id)?;
            shard.purge_stream_local(stream).await?;
            Ok(())
        }
        ShardEvent::PurgedTopic {
            stream_id,
            topic_id,
        } => {
            let topic = shard.resolve_topic(&stream_id, &topic_id)?;
            shard.purge_topic_local(topic).await?;
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
