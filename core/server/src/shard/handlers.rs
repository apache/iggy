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
use iggy_common::sharding::IggyNamespace;
use iggy_common::{Identifier, IggyError, SenderKind, TransportProtocol};
use nix::sys::stat::SFlag;
use std::os::fd::{FromRawFd, IntoRawFd};
use tracing::{info, trace};

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
            // Build namespace for partition_store lookup
            let numeric_stream_id = shard
                .shared_metadata
                .get_stream_id(&stream_id)
                .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
            let numeric_topic_id = shard
                .shared_metadata
                .get_topic_id(numeric_stream_id, &topic_id)
                .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

            let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);

            // Ensure partition exists in partition_store (lazy init if needed)
            if !shard.partition_store.borrow().contains(&namespace) {
                shard
                    .ensure_local_partition(&stream_id, &topic_id, partition_id)
                    .await?;
            }

            trace!("SendMessages: Using partition_store for {:?}", namespace);
            let batch = shard.maybe_encrypt_messages(batch)?;
            let messages_count = batch.count();
            shard.append_to_partition_store(&namespace, batch).await?;
            shard.metrics.increment_messages(messages_count as u64);
            Ok(ShardResponse::SendMessages)
        }
        ShardRequestPayload::PollMessages { args, consumer } => {
            // Build namespace for partition_store lookup
            let numeric_stream_id = shard
                .shared_metadata
                .get_stream_id(&stream_id)
                .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
            let numeric_topic_id = shard
                .shared_metadata
                .get_topic_id(numeric_stream_id, &topic_id)
                .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

            let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);

            // Ensure partition exists in partition_store (lazy init if needed)
            if !shard.partition_store.borrow().contains(&namespace) {
                shard
                    .ensure_local_partition(&stream_id, &topic_id, partition_id)
                    .await?;
            }

            // Clone the Rc synchronously
            let rc_partition = shard.partition_store.borrow().get_rc(&namespace).ok_or(
                IggyError::PartitionNotFound(partition_id, topic_id.clone(), stream_id.clone()),
            )?;

            trace!("PollMessages: Using partition_store for {:?}", namespace);
            let auto_commit = args.auto_commit;

            let (poll_metadata, batches) =
                poll_from_partition_store(&rc_partition, partition_id, consumer, args).await?;

            // Handle auto_commit via partition_store
            if auto_commit && !batches.is_empty() {
                let offset = batches
                    .last_offset()
                    .expect("Batch set should have at least one batch");
                shard
                    .store_consumer_offset_in_partition_store(&namespace, consumer, offset)
                    .await?;
            }
            Ok(ShardResponse::PollMessages((poll_metadata, batches)))
        }
        ShardRequestPayload::FlushUnsavedBuffer { fsync } => {
            // Lazy init: ensure partition exists locally
            shard
                .ensure_local_partition(&stream_id, &topic_id, partition_id)
                .await?;

            shard
                .flush_unsaved_buffer_base(&stream_id, &topic_id, partition_id, fsync)
                .await?;
            Ok(ShardResponse::FlushUnsavedBuffer)
        }
        ShardRequestPayload::DeleteSegments { segments_count } => {
            // Lazy init: ensure partition exists locally
            shard
                .ensure_local_partition(&stream_id, &topic_id, partition_id)
                .await?;

            shard
                .delete_segments_base(&stream_id, &topic_id, partition_id, segments_count)
                .await?;
            Ok(ShardResponse::DeleteSegments)
        }
        ShardRequestPayload::CreateStream { user_id, name } => {
            assert_eq!(shard.id, 0, "CreateStream should only be handled by shard0");

            let session = Session::stateless(
                user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );

            // Acquire stream lock to serialize filesystem operations
            let _stream_guard = shard.fs_locks.stream_lock.lock().await;

            let stream = shard.create_stream(&session, name.clone()).await?;

            Ok(ShardResponse::CreateStreamResponse(stream))
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

            let topic = shard
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

            let topic_id = topic.id;

            shard
                .create_partitions(
                    &session,
                    &stream_id,
                    &Identifier::numeric(topic_id as u32).unwrap(),
                    partitions_count,
                )
                .await?;

            Ok(ShardResponse::CreateTopicResponse(topic))
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
            let topic = shard.delete_topic(&session, &stream_id, &topic_id).await?;

            Ok(ShardResponse::DeleteTopicResponse(topic))
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
        ShardRequestPayload::DeleteStream { user_id, stream_id } => {
            assert_eq!(shard.id, 0, "DeleteStream should only be handled by shard0");

            let session = Session::stateless(
                user_id,
                std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            );
            let _stream_guard = shard.fs_locks.stream_lock.lock().await;
            let stream = shard.delete_stream(&session, &stream_id).await?;
            Ok(ShardResponse::DeleteStreamResponse(stream))
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

            // Build namespace for partition_store lookup
            let numeric_stream_id = shard
                .shared_metadata
                .get_stream_id(&stream_id)
                .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
            let numeric_topic_id = shard
                .shared_metadata
                .get_topic_id(numeric_stream_id, &topic_id)
                .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

            let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);

            // Ensure partition exists in partition_store (lazy init if needed)
            if !shard.partition_store.borrow().contains(&namespace) {
                shard
                    .ensure_local_partition(&stream_id, &topic_id, partition_id)
                    .await?;
            }

            trace!("SocketTransfer: Using partition_store for {:?}", namespace);
            let batch = shard.maybe_encrypt_messages(initial_data)?;
            let messages_count = batch.count();
            shard.append_to_partition_store(&namespace, batch).await?;
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
        ShardEvent::FlushUnsavedBuffer {
            stream_id,
            topic_id,
            partition_id,
            fsync,
        } => {
            shard
                .flush_unsaved_buffer_base(&stream_id, &topic_id, partition_id, fsync)
                .await?;
            Ok(())
        }
    }
}

/// Helper function to poll messages from partition_store.
///
/// This function is separated to allow the `#[allow(clippy::await_holding_refcell_ref)]`
/// attribute to be applied at the function level.
///
/// # Safety
/// Single-threaded runtime (compio) ensures no concurrent access. The RefCell
/// borrow is held across await but no other code can access the partition_store
/// while this task is suspended.
#[allow(clippy::await_holding_refcell_ref)]
async fn poll_from_partition_store(
    rc_partition: &crate::partition_store::RcPartitionData,
    partition_id: usize,
    consumer: crate::streaming::polling_consumer::PollingConsumer,
    args: crate::shard::system::messages::PollingArgs,
) -> Result<
    (
        crate::binary::handlers::messages::poll_messages_handler::IggyPollMetadata,
        crate::streaming::segments::IggyMessagesBatchSet,
    ),
    IggyError,
> {
    let partition_data = rc_partition.borrow();
    partition_data
        .poll_messages(partition_id, consumer, args)
        .await
}
