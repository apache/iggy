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
        IggyShard,
        namespace::IggyNamespace,
        transmission::{
            event::ShardEvent,
            frame::ShardResponse,
            message::{ShardMessage, ShardRequest, ShardRequestPayload},
        },
    },
    streaming::session::Session,
    tcp::{
        connection_handler::{ConnectionAction, handle_connection, handle_error},
        tcp_listener::cleanup_connection,
    },
};
use compio_net::TcpStream;
use iggy_common::{Identifier, IggyError, IggyTimestamp, SenderKind, TransportProtocol};
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

            let metadata = shard.metadata.load();
            let partition_meta = metadata
                .streams
                .get(stream)
                .and_then(|s| s.topics.get(topic))
                .and_then(|t| t.partitions.get(partition_id));

            let needs_init = {
                let store = shard.partition_store.borrow();
                match (store.get(&namespace), partition_meta) {
                    (Some(data), Some(meta)) if data.revision_id == meta.revision_id => false,
                    (Some(_), _) => {
                        drop(store);
                        shard.partition_store.borrow_mut().remove(&namespace);
                        true
                    }
                    (None, _) => true,
                }
            };

            if needs_init {
                let created_at = partition_meta
                    .map(|m| m.created_at)
                    .unwrap_or_else(IggyTimestamp::now);

                shard
                    .init_partition_directly(stream, topic, partition_id, created_at)
                    .await?;
            }

            shard
                .append_messages_to_partition_store(&namespace, batch, &shard.config.system)
                .await?;

            shard.metrics.increment_messages(messages_count as u64);
            Ok(ShardResponse::SendMessages)
        }
        ShardRequestPayload::PollMessages { args, consumer } => {
            let auto_commit = args.auto_commit;

            let (stream, topic) = shard.resolve_topic_id(&stream_id, &topic_id)?;
            let namespace = IggyNamespace::new(stream, topic, partition_id);

            let metadata = shard.metadata.load();
            let partition_meta = metadata
                .streams
                .get(stream)
                .and_then(|s| s.topics.get(topic))
                .and_then(|t| t.partitions.get(partition_id));

            let needs_init = {
                let store = shard.partition_store.borrow();
                match (store.get(&namespace), partition_meta) {
                    (Some(data), Some(meta)) if data.revision_id == meta.revision_id => false,
                    (Some(_), _) => {
                        drop(store);
                        shard.partition_store.borrow_mut().remove(&namespace);
                        true
                    }
                    (None, _) => true,
                }
            };

            if needs_init {
                let created_at = partition_meta
                    .map(|m| m.created_at)
                    .unwrap_or_else(IggyTimestamp::now);

                shard
                    .init_partition_directly(stream, topic, partition_id, created_at)
                    .await?;
            }

            let (metadata, batches) = shard
                .poll_messages_from_partition_store(&namespace, consumer, args)
                .await?;

            if auto_commit && !batches.is_empty() {
                let offset = batches
                    .last_offset()
                    .expect("Batch set should have at least one batch");
                shard
                    .auto_commit_consumer_offset_from_partition_store(&namespace, consumer, offset)
                    .await?;
            }
            Ok(ShardResponse::PollMessages((metadata, batches)))
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

            let session = Session::stateless(
                user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );

            let _partition_guard = shard.fs_locks.partition_lock.lock().await;

            let partition_infos = shard
                .create_partitions(&session, &stream_id, &topic_id, partitions_count)
                .await?;
            let partition_ids = partition_infos.iter().map(|p| p.id).collect::<Vec<_>>();

            let event = ShardEvent::CreatedPartitions {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partitions: partition_infos,
            };
            shard.broadcast_event_to_all_shards(event).await?;

            // Rebalance consumer groups using SharedMetadata
            let numeric_stream_id = shard
                .metadata
                .get_stream_id(&stream_id)
                .expect("Stream must exist");
            let numeric_topic_id = shard
                .metadata
                .get_topic_id(numeric_stream_id, &topic_id)
                .expect("Topic must exist");
            shard.metadata.rebalance_consumer_groups_for_topic(
                numeric_stream_id,
                numeric_topic_id,
                &partition_ids,
            );

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

            let session = Session::stateless(
                user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );

            let _partition_guard = shard.fs_locks.partition_lock.lock().await;

            let deleted_partition_ids = shard
                .delete_partitions(&session, &stream_id, &topic_id, partitions_count)
                .await?;

            let event = ShardEvent::DeletedPartitions {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partitions_count,
                partition_ids: deleted_partition_ids.clone(),
            };
            shard.broadcast_event_to_all_shards(event).await?;

            // Rebalance consumer groups using SharedMetadata
            let numeric_stream_id = shard
                .metadata
                .get_stream_id(&stream_id)
                .expect("Stream must exist");
            let numeric_topic_id = shard
                .metadata
                .get_topic_id(numeric_stream_id, &topic_id)
                .expect("Topic must exist");
            let remaining_partition_ids: Vec<_> = {
                let metadata = shard.metadata.load();
                metadata
                    .streams
                    .get(numeric_stream_id)
                    .and_then(|s| s.topics.get(numeric_topic_id))
                    .map(|t| t.partitions.keys().collect())
                    .unwrap_or_default()
            };
            shard.metadata.rebalance_consumer_groups_for_topic(
                numeric_stream_id,
                numeric_topic_id,
                &remaining_partition_ids,
            );

            Ok(ShardResponse::DeletePartitionsResponse(
                deleted_partition_ids,
            ))
        }
        ShardRequestPayload::CreateStream { user_id, name } => {
            assert_eq!(shard.id, 0, "CreateStream should only be handled by shard0");

            let session = Session::stateless(
                user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );

            // Acquire stream lock to serialize filesystem operations
            let _stream_guard = shard.fs_locks.stream_lock.lock().await;

            let created_stream_id = shard.create_stream(&session, name.clone()).await?;

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

            let session = Session::stateless(
                user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );

            // Acquire topic lock to serialize filesystem operations
            let _topic_guard = shard.fs_locks.topic_lock.lock().await;

            let topic_id_num = shard
                .create_topic(
                    &session,
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
                    &session,
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

            let session = Session::stateless(
                user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );

            shard.update_topic(
                &session,
                &stream_id,
                &topic_id,
                name.clone(),
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
            )?;

            Ok(ShardResponse::UpdateTopicResponse)
        }
        ShardRequestPayload::DeleteTopic {
            user_id,
            stream_id,
            topic_id,
        } => {
            assert_eq!(shard.id, 0, "DeleteTopic should only be handled by shard0");

            let session = Session::stateless(
                user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );

            let _topic_guard = shard.fs_locks.topic_lock.lock().await;
            let topic_info = shard.delete_topic(&session, &stream_id, &topic_id).await?;
            let topic_id_num = topic_info.id;

            Ok(ShardResponse::DeleteTopicResponse(topic_id_num))
        }
        ShardRequestPayload::CreateUser {
            user_id,
            username,
            password,
            status,
            permissions,
        } => {
            assert_eq!(shard.id, 0, "CreateUser should only be handled by shard0");

            let session = Session::stateless(
                user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );
            let _user_guard = shard.fs_locks.user_lock.lock().await;
            let user =
                shard.create_user(&session, &username, &password, status, permissions.clone())?;

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
            assert_eq!(shard.id, 0, "CreateUser should only be handled by shard0");

            let session = Session::stateless(
                session_user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );
            let _user_guard = shard.fs_locks.user_lock.lock().await;
            let user = shard.delete_user(&session, &user_id)?;
            Ok(ShardResponse::DeletedUser(user))
        }
        ShardRequestPayload::UpdateStream {
            user_id,
            stream_id,
            name,
        } => {
            assert_eq!(shard.id, 0, "UpdateStream should only be handled by shard0");

            let session = Session::stateless(
                user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );

            shard.update_stream(&session, &stream_id, name.clone())?;

            Ok(ShardResponse::UpdateStreamResponse)
        }
        ShardRequestPayload::DeleteStream { user_id, stream_id } => {
            assert_eq!(shard.id, 0, "DeleteStream should only be handled by shard0");

            let session = Session::stateless(
                user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );
            let _stream_guard = shard.fs_locks.stream_lock.lock().await;
            let stream_info = shard.delete_stream(&session, &stream_id).await?;
            let stream_id_num = stream_info.id;

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

            let session = Session::stateless(
                session_user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );
            let _user_guard = shard.fs_locks.user_lock.lock().await;
            shard.update_permissions(&session, &user_id, permissions)?;

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

            let session = Session::stateless(
                session_user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );
            let _user_guard = shard.fs_locks.user_lock.lock().await;
            shard.change_password(&session, &user_id, &current_password, &new_password)?;

            Ok(ShardResponse::ChangePasswordResponse)
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

            // Get numeric IDs for partition_store lookup
            let numeric_stream_id = shard
                .metadata
                .get_stream_id(&stream_id)
                .expect("Stream must exist");
            let numeric_topic_id = shard
                .metadata
                .get_topic_id(numeric_stream_id, &topic_id)
                .expect("Topic must exist");

            // Use partition_store - initialize on-demand if not yet ready
            let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
            if !shard.partition_store.borrow().contains(&namespace) {
                // Partition not yet initialized (race with CreatedPartitions event)
                // Initialize it on-demand using SharedMetadata
                let created_at = {
                    let metadata = shard.metadata.load();
                    metadata
                        .streams
                        .get(numeric_stream_id)
                        .and_then(|s| s.topics.get(numeric_topic_id))
                        .and_then(|t| t.partitions.get(partition_id))
                        .map(|meta| meta.created_at)
                        .unwrap_or_else(IggyTimestamp::now)
                };

                shard
                    .init_partition_directly(
                        numeric_stream_id,
                        numeric_topic_id,
                        partition_id,
                        created_at,
                    )
                    .await?;
            }

            shard
                .append_messages_to_partition_store(&namespace, batch, &shard.config.system)
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
            // Here we only need to clean up local partition_store entries on all shards.
            let numeric_stream_id = shard.metadata.get_stream_id(&stream_id).unwrap_or_default();
            let numeric_topic_id = shard
                .metadata
                .get_topic_id(numeric_stream_id, &topic_id)
                .unwrap_or_default();
            let mut store = shard.partition_store.borrow_mut();
            for partition_id in partition_ids {
                let ns = crate::shard::namespace::IggyNamespace::new(
                    numeric_stream_id,
                    numeric_topic_id,
                    partition_id,
                );
                store.remove(&ns);
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

        // ========================================================================
        // PARTITION EVENTS - Need to initialize logs in partition_store
        // ========================================================================
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
                let ns = crate::shard::namespace::IggyNamespace::new(
                    numeric_stream_id,
                    numeric_topic_id,
                    partition_info.id,
                );
                let owner_shard_id = crate::shard::calculate_shard_assignment(&ns, shards_count);

                if shard.id == owner_shard_id as u16 {
                    shard
                        .init_partition_directly(
                            numeric_stream_id,
                            numeric_topic_id,
                            partition_info.id,
                            partition_info.created_at,
                        )
                        .await?;
                }
            }
            Ok(())
        }

        // ========================================================================
        // OPERATIONAL EVENTS - Work directly with partition_store
        // ========================================================================
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

            let ns = crate::shard::namespace::IggyNamespace::new(
                numeric_stream_id,
                numeric_topic_id,
                partition_id,
            );
            if shard.partition_store.borrow().get(&ns).is_some() {
                shard
                    .flush_unsaved_buffer_from_partition_store(&ns, fsync)
                    .await?;
            }
            Ok(())
        }
    }
}
