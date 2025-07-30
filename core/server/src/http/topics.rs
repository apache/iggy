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

use crate::http::COMPONENT;
use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::mapper;
use crate::http::shared::AppState;
use crate::state::command::EntryCommand;
use crate::state::models::CreateTopicWithId;
use crate::streaming::session::Session;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Extension, Json, Router, debug_handler};
use error_set::ErrContext;
use iggy_common::Validatable;
use iggy_common::create_topic::CreateTopic;
use iggy_common::delete_topic::DeleteTopic;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::purge_topic::PurgeTopic;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::{Identifier, Sizeable};
use iggy_common::{Topic, TopicDetails};
use send_wrapper::SendWrapper;
use std::sync::Arc;
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/streams/{stream_id}/topics",
            get(get_topics).post(create_topic),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}",
            get(get_topic).put(update_topic).delete(delete_topic),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/purge",
            delete(purge_topic),
        )
        .with_state(state)
}

#[debug_handler]
async fn get_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<Json<TopicDetails>, CustomError> {
    let identity_stream_id = Identifier::from_str_value(&stream_id)?;
    let identity_topic_id = Identifier::from_str_value(&topic_id)?;

    // Extract all the data we need synchronously first
    let (topic_data, partition_futures) = {
        let stream = state
            .shard
            .shard()
            .get_stream(&identity_stream_id)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;

        let Ok(topic) = state.shard.shard().try_find_topic(
            &Session::stateless(identity.user_id, identity.ip_address),
            &stream,
            &identity_topic_id,
        ) else {
            return Err(CustomError::ResourceNotFound);
        };

        let Some(topic) = topic else {
            return Err(CustomError::ResourceNotFound);
        };

        // Extract basic topic data synchronously
        let topic_data = (
            topic.topic_id,
            topic.created_at,
            topic.name.clone(),
            topic.get_size_bytes(),
            topic.get_messages_count(),
            topic.get_partitions().len() as u32,
            topic.message_expiry,
            topic.compression_algorithm,
            topic.max_topic_size,
            topic.replication_factor,
        );

        // Get all partition references and create futures for them
        let partition_futures: Vec<_> = topic
            .get_partitions()
            .iter()
            .map(|partition| {
                let partition_clone = partition.clone();
                SendWrapper::new(async move {
                    let partition_guard = partition_clone.read().await;
                    iggy_common::Partition {
                        id: partition_guard.partition_id,
                        created_at: partition_guard.created_at,
                        segments_count: partition_guard.get_segments().len() as u32,
                        current_offset: partition_guard.current_offset,
                        size: partition_guard.get_size_bytes(),
                        messages_count: partition_guard.get_messages_count(),
                    }
                })
            })
            .collect();

        (topic_data, partition_futures)
    };

    let mut partitions = Vec::new();
    for partition_future in partition_futures {
        partitions.push(partition_future.await);
    }
    partitions.sort_by(|a, b| a.id.cmp(&b.id));

    let topic_details = TopicDetails {
        id: topic_data.0,
        created_at: topic_data.1,
        name: topic_data.2,
        size: topic_data.3,
        messages_count: topic_data.4,
        partitions_count: topic_data.5,
        partitions,
        message_expiry: topic_data.6,
        compression_algorithm: topic_data.7,
        max_topic_size: topic_data.8,
        replication_factor: topic_data.9,
    };

    Ok(Json(topic_details))
}

#[debug_handler]
async fn get_topics(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<Json<Vec<Topic>>, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;

    let stream = state
        .shard
        .shard()
        .get_stream(&stream_id)
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;

    let topics = state.shard.shard()
        .find_topics(
            &Session::stateless(identity.user_id, identity.ip_address),
            &stream,
        )
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to find topics for stream with ID: {stream_id}"
            )
        })?;
    let topics = mapper::map_topics(&topics);
    Ok(Json(topics))
}

#[debug_handler]
#[instrument(skip_all, name = "trace_create_topic", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id))]
async fn create_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
    Json(mut command): Json<CreateTopic>,
) -> Result<Json<TopicDetails>, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.validate()?;

    let session = SendWrapper::new(Session::stateless(identity.user_id, identity.ip_address));

    let (topic_id, partition_ids) = {
        let future = SendWrapper::new(state.shard.shard().create_topic(
            &session,
            &command.stream_id,
            command.topic_id,
            &command.name,
            command.partitions_count,
            command.message_expiry,
            command.compression_algorithm,
            command.max_topic_size,
            command.replication_factor,
        ));
        future.await
    }
    .with_error_context(|error| {
        format!("{COMPONENT} (error: {error}) - failed to create topic, stream ID: {stream_id}")
    })?;

    let broadcast_future = SendWrapper::new(async {
        use crate::shard::transmission::event::ShardEvent;

        let shard = state.shard.shard();

        let event = ShardEvent::CreatedTopic {
            stream_id: command.stream_id.clone(),
            topic_id: topic_id.clone(),
            name: command.name.clone(),
            partitions_count: command.partitions_count,
            message_expiry: command.message_expiry,
            compression_algorithm: command.compression_algorithm,
            max_topic_size: command.max_topic_size,
            replication_factor: command.replication_factor,
        };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;

        let stream = shard.get_stream(&command.stream_id)?;
        let topic = stream.get_topic(&topic_id)?;
        let numeric_stream_id = stream.stream_id;
        let numeric_topic_id = topic.topic_id;

        let records = shard
            .create_shard_table_records(&partition_ids, numeric_stream_id, numeric_topic_id)
            .collect::<Vec<_>>();

        for (ns, shard_info) in records.iter() {
            let partition = topic.get_partition(ns.partition_id)?;
            let mut partition = partition.write().await;
            partition.persist().await?;
            if shard_info.id() == state.shard.shard().id {
                let partition_id = ns.partition_id;
                partition.open().await.with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to open partition with ID: {partition_id} in topic with ID: {topic_id} for stream with ID: {stream_id}"
                    )
                })?;
            }
        }

        shard.insert_shard_table_records(records);

        let event = ShardEvent::CreatedShardTableRecords {
            stream_id: numeric_stream_id,
            topic_id: numeric_topic_id,
            partition_ids: partition_ids.clone(),
        };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;

        Ok::<(), CustomError>(())
    });

    broadcast_future.await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to broadcast topic events, stream ID: {stream_id}"
            )
        })?;

    let (topic_data, partition_futures) = {
        let stream = state
            .shard
            .shard()
            .get_stream(&command.stream_id)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
        let topic = state.shard.shard().find_topic(&session, &stream, &topic_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {stream_id}")
        })?;

        command.message_expiry = topic.message_expiry;
        command.max_topic_size = topic.max_topic_size;

        let topic_data = (
            topic.topic_id,
            topic.created_at,
            topic.name.clone(),
            topic.get_size_bytes(),
            topic.get_messages_count(),
            topic.get_partitions().len() as u32,
            topic.message_expiry,
            topic.compression_algorithm,
            topic.max_topic_size,
            topic.replication_factor,
        );

        let partition_futures: Vec<_> = topic
            .get_partitions()
            .iter()
            .map(|partition| {
                let partition_clone = partition.clone();
                SendWrapper::new(async move {
                    let partition_guard = partition_clone.read().await;
                    iggy_common::Partition {
                        id: partition_guard.partition_id,
                        created_at: partition_guard.created_at,
                        segments_count: partition_guard.get_segments().len() as u32,
                        current_offset: partition_guard.current_offset,
                        size: partition_guard.get_size_bytes(),
                        messages_count: partition_guard.get_messages_count(),
                    }
                })
            })
            .collect();

        (topic_data, partition_futures)
    };

    let mut partitions = Vec::new();
    for partition_future in partition_futures {
        partitions.push(partition_future.await);
    }
    partitions.sort_by(|a, b| a.id.cmp(&b.id));

    let topic_details = TopicDetails {
        id: topic_data.0,
        created_at: topic_data.1,
        name: topic_data.2,
        size: topic_data.3,
        messages_count: topic_data.4,
        partitions_count: topic_data.5,
        partitions,
        message_expiry: topic_data.6,
        compression_algorithm: topic_data.7,
        max_topic_size: topic_data.8,
        replication_factor: topic_data.9,
    };

    let response = Json(topic_details);

    {
        let entry_command = EntryCommand::CreateTopic(CreateTopicWithId {
            topic_id: topic_data.0,
            command,
        });
        let future = SendWrapper::new(
            state
                .shard
                .shard()
                .state
                .apply(identity.user_id, &entry_command),
        );
        future.await
    }
    .with_error_context(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to apply create topic, stream ID: {stream_id}",
        )
    })?;

    Ok(response)
}

#[debug_handler]
#[instrument(skip_all, name = "trace_update_topic", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn update_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<UpdateTopic>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;

    let session = SendWrapper::new(Session::stateless(identity.user_id, identity.ip_address));

    {
        let future = SendWrapper::new(state.shard.shard().update_topic(
            &session,
            &command.stream_id,
            &command.topic_id,
            &command.name,
            command.message_expiry,
            command.compression_algorithm,
            command.max_topic_size,
            command.replication_factor,
        ));
        future.await
    }.with_error_context(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to update topic, stream ID: {stream_id}, topic ID: {topic_id}"
        )
    })?;

    let (message_expiry, max_topic_size) = {
        let stream = state
            .shard
            .shard()
            .get_stream(&command.stream_id)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
        let topic = state.shard.shard().find_topic(&session, &stream, &command.topic_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {stream_id}")
        })?;

        (topic.message_expiry, topic.max_topic_size)
    };

    command.message_expiry = message_expiry;
    command.max_topic_size = max_topic_size;

    {
        let entry_command = EntryCommand::UpdateTopic(command);
        let future = SendWrapper::new(state.shard.shard().state
            .apply(identity.user_id, &entry_command));
        future.await
    }.with_error_context(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to apply update topic, stream ID: {stream_id}, topic ID: {topic_id}"
        )
    })?;

    Ok(StatusCode::NO_CONTENT)
}

#[debug_handler]
#[instrument(skip_all, name = "trace_delete_topic", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn delete_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<StatusCode, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;
    let identifier_topic_id = Identifier::from_str_value(&topic_id)?;

    let session = SendWrapper::new(Session::stateless(identity.user_id, identity.ip_address));

    {
        let future = SendWrapper::new(state.shard.shard().delete_topic(
            &session,
            &identifier_stream_id,
            &identifier_topic_id,
        ));
        future.await
    }.with_error_context(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to delete topic with ID: {topic_id} in stream with ID: {stream_id}",
        )
    })?;

    {
        let entry_command = EntryCommand::DeleteTopic(DeleteTopic {
            stream_id: identifier_stream_id,
            topic_id: identifier_topic_id,
        });
        let future = SendWrapper::new(state.shard.shard().state
            .apply(
                identity.user_id,
                &entry_command,
            ));
        future.await
    }.with_error_context(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to apply delete topic, stream ID: {stream_id}, topic ID: {topic_id}"
        )
    })?;

    Ok(StatusCode::NO_CONTENT)
}

#[debug_handler]
#[instrument(skip_all, name = "trace_purge_topic", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn purge_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<StatusCode, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;
    let identifier_topic_id = Identifier::from_str_value(&topic_id)?;

    let session = SendWrapper::new(Session::stateless(identity.user_id, identity.ip_address));

    {
        let future = SendWrapper::new(state.shard.shard().purge_topic(
            &session,
            &identifier_stream_id,
            &identifier_topic_id,
        ));
        future.await
    }.with_error_context(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to purge topic, stream ID: {stream_id}, topic ID: {topic_id}"
        )
    })?;

    {
        let entry_command = EntryCommand::PurgeTopic(PurgeTopic {
            stream_id: identifier_stream_id,
            topic_id: identifier_topic_id,
        });
        let future = SendWrapper::new(state.shard.shard().state
            .apply(
                identity.user_id,
                &entry_command,
            ));
        future.await
    }.with_error_context(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to apply purge topic, stream ID: {stream_id}, topic ID: {topic_id}"
        )
    })?;

    Ok(StatusCode::NO_CONTENT)
}
