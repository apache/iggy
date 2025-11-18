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
        namespace::IggyFullNamespace,
        transmission::{
            event::ShardEvent,
            frame::ShardResponse,
            message::{ShardMessage, ShardRequest, ShardRequestPayload},
        },
    },
    streaming::{session::Session, traits::MainOps},
};
use iggy_common::{Identifier, IggyError, TransportProtocol};
use tracing::info;

pub(super) async fn handle_shard_message(
    shard: &IggyShard,
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
    shard: &IggyShard,
    request: ShardRequest,
) -> Result<ShardResponse, IggyError> {
    let stream_id = request.stream_id;
    let topic_id = request.topic_id;
    let partition_id = request.partition_id;
    match request.payload {
        ShardRequestPayload::SendMessages { batch } => {
            let ns = IggyFullNamespace::new(stream_id, topic_id, partition_id);
            let batch = shard.maybe_encrypt_messages(batch)?;
            let messages_count = batch.count();
            shard
                .streams
                .append_messages(&shard.config.system, &shard.task_registry, &ns, batch)
                .await?;
            shard.metrics.increment_messages(messages_count as u64);
            Ok(ShardResponse::SendMessages)
        }
        ShardRequestPayload::PollMessages { args, consumer } => {
            let auto_commit = args.auto_commit;
            let ns = IggyFullNamespace::new(stream_id, topic_id, partition_id);
            let (metadata, batches) = shard.streams.poll_messages(&ns, consumer, args).await?;

            if auto_commit && !batches.is_empty() {
                let offset = batches
                    .last_offset()
                    .expect("Batch set should have at least one batch");
                shard
                    .streams
                    .auto_commit_consumer_offset(
                        &shard.config.system,
                        ns.stream_id(),
                        ns.topic_id(),
                        partition_id,
                        consumer,
                        offset,
                    )
                    .await?;
            }
            Ok(ShardResponse::PollMessages((metadata, batches)))
        }
        ShardRequestPayload::FlushUnsavedBuffer { fsync } => {
            shard
                .flush_unsaved_buffer_base(&stream_id, &topic_id, partition_id, fsync)
                .await?;
            Ok(ShardResponse::FlushUnsavedBuffer)
        }
        ShardRequestPayload::DeleteSegments { segments_count } => {
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
            let created_stream_id = stream.id();

            let event = ShardEvent::CreatedStream {
                id: created_stream_id,
                stream: stream.clone(),
            };

            shard.broadcast_event_to_all_shards(event).await?;

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

            let topic_id = topic.id();
            let event = ShardEvent::CreatedTopic {
                stream_id: stream_id.clone(),
                topic: topic.clone(),
            };
            shard.broadcast_event_to_all_shards(event).await?;
            let partitions = shard
                .create_partitions(
                    &session,
                    &stream_id,
                    &Identifier::numeric(topic_id as u32).unwrap(),
                    partitions_count,
                )
                .await?;

            let event = ShardEvent::CreatedPartitions {
                stream_id: stream_id.clone(),
                topic_id: Identifier::numeric(topic_id as u32).unwrap(),
                partitions,
            };
            shard.broadcast_event_to_all_shards(event).await?;

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

            let event = ShardEvent::UpdatedTopic {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                name,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
            };
            shard.broadcast_event_to_all_shards(event).await?;

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
            let topic_id_num = topic.root().id();

            let event = ShardEvent::DeletedTopic {
                id: topic_id_num,
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
            };
            shard.broadcast_event_to_all_shards(event).await?;

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

            let created_user_id = user.id;

            let event = ShardEvent::CreatedUser {
                user_id: created_user_id,
                username: username.clone(),
                password: password.clone(),
                status,
                permissions: permissions.clone(),
            };
            shard.broadcast_event_to_all_shards(event).await?;
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
            let event = ShardEvent::DeletedUser { user_id };
            shard.broadcast_event_to_all_shards(event).await?;
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
            let event = ShardEvent::DeletedStream {
                id: stream.id(),
                stream_id,
            };
            shard.broadcast_event_to_all_shards(event).await?;
            Ok(ShardResponse::DeleteStreamResponse(stream))
        }
    }
}

pub(crate) async fn handle_event(shard: &IggyShard, event: ShardEvent) -> Result<(), IggyError> {
    match event {
        ShardEvent::DeletedPartitions {
            stream_id,
            topic_id,
            partitions_count,
            partition_ids,
        } => {
            shard.delete_partitions_bypass_auth(
                &stream_id,
                &topic_id,
                partitions_count,
                partition_ids,
            )?;
            Ok(())
        }
        ShardEvent::UpdatedStream { stream_id, name } => {
            shard.update_stream_bypass_auth(&stream_id, &name)?;
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
        ShardEvent::CreatedUser {
            user_id,
            username,
            password,
            status,
            permissions,
        } => {
            shard.create_user_bypass_auth(
                user_id,
                &username,
                &password,
                status,
                permissions.clone(),
            )?;
            Ok(())
        }
        ShardEvent::DeletedUser { user_id } => {
            shard.delete_user_bypass_auth(&user_id)?;
            Ok(())
        }
        ShardEvent::ChangedPassword {
            user_id,
            current_password,
            new_password,
        } => {
            shard.change_password_bypass_auth(&user_id, &current_password, &new_password)?;
            Ok(())
        }
        ShardEvent::CreatedPersonalAccessToken {
            personal_access_token,
        } => {
            shard.create_personal_access_token_bypass_auth(personal_access_token.to_owned())?;
            Ok(())
        }
        ShardEvent::DeletedPersonalAccessToken { user_id, name } => {
            shard.delete_personal_access_token_bypass_auth(user_id, &name)?;
            Ok(())
        }
        ShardEvent::UpdatedUser {
            user_id,
            username,
            status,
        } => {
            shard.update_user_bypass_auth(&user_id, username.to_owned(), status)?;
            Ok(())
        }
        ShardEvent::UpdatedPermissions {
            user_id,
            permissions,
        } => {
            shard.update_permissions_bypass_auth(&user_id, permissions.to_owned())?;
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
        ShardEvent::CreatedStream { id, stream } => {
            let stream_id = shard.create_stream_bypass_auth(stream);
            assert_eq!(stream_id, id);
            Ok(())
        }
        ShardEvent::DeletedStream { id, stream_id } => {
            let stream = shard.delete_stream_bypass_auth(&stream_id);
            assert_eq!(stream.id(), id);

            Ok(())
        }
        ShardEvent::CreatedTopic { stream_id, topic } => {
            let topic_id_from_event = topic.id();
            let topic_id = shard.create_topic_bypass_auth(&stream_id, topic.clone());
            assert_eq!(topic_id, topic_id_from_event);
            Ok(())
        }
        ShardEvent::CreatedPartitions {
            stream_id,
            topic_id,
            partitions,
        } => {
            shard
                .create_partitions_bypass_auth(&stream_id, &topic_id, partitions)
                .await?;
            Ok(())
        }
        ShardEvent::DeletedTopic {
            id,
            stream_id,
            topic_id,
        } => {
            let topic = shard.delete_topic_bypass_auth(&stream_id, &topic_id);
            assert_eq!(topic.id(), id);
            Ok(())
        }
        ShardEvent::UpdatedTopic {
            stream_id,
            topic_id,
            name,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        } => {
            shard.update_topic_bypass_auth(
                &stream_id,
                &topic_id,
                name.clone(),
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
            )?;
            Ok(())
        }
        ShardEvent::CreatedConsumerGroup {
            stream_id,
            topic_id,
            cg,
        } => {
            let cg_id = cg.id();
            let id = shard.create_consumer_group_bypass_auth(&stream_id, &topic_id, cg);
            assert_eq!(id, cg_id);
            Ok(())
        }
        ShardEvent::DeletedConsumerGroup {
            id,
            stream_id,
            topic_id,
            group_id,
        } => {
            let cg = shard.delete_consumer_group_bypass_auth(&stream_id, &topic_id, &group_id);
            assert_eq!(cg.id(), id);

            Ok(())
        }
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
