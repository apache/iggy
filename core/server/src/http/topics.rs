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
use crate::http::shared::AppState;
use crate::slab::traits_ext::{EntityComponentSystem, EntityMarker, IntoComponents};
use crate::state::command::EntryCommand;
use crate::state::models::CreateTopicWithId;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Extension, Json, Router, debug_handler};
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::Validatable;
use iggy_common::create_topic::CreateTopic;
use iggy_common::delete_topic::DeleteTopic;
use iggy_common::purge_topic::PurgeTopic;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::{IggyError, Topic, TopicDetails};
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

    let (numeric_stream_id, numeric_topic_id) = state
        .shard
        .shard()
        .resolve_topic_id(&identity_stream_id, &identity_topic_id)?;
    state
        .shard
        .shard()
        .permissioner
        .borrow()
        .get_topic(identity.user_id, numeric_stream_id, numeric_topic_id)
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - permission denied to get topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                identity.user_id,
            )
        })?;

    // Get topic details using the new API
    let topic_details = state.shard.shard().streams.with_topic_by_id(
        &identity_stream_id,
        &identity_topic_id,
        |(root, _, stats)| crate::http::mapper::map_topic_details(&root, &stats),
    );

    Ok(Json(topic_details))
}

#[debug_handler]
async fn get_topics(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<Json<Vec<Topic>>, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;

    let numeric_stream_id = state.shard.shard().resolve_stream_id(&stream_id)?;
    state
        .shard
        .shard()
        .permissioner
        .borrow()
        .get_topics(identity.user_id, numeric_stream_id)
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - permission denied to get topics in stream with ID: {stream_id} for user with ID: {}",
                identity.user_id,
            )
        })?;

    // Get topics using the new API
    let topics = state
        .shard
        .shard()
        .streams
        .with_topics(&stream_id, |topics| {
            topics.with_components(|topics| {
                let (roots, _, stats) = topics.into_components();
                crate::http::mapper::map_topics_from_components(&roots, &stats)
            })
        });

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

    let numeric_stream_id = state.shard.shard().resolve_stream_id(&command.stream_id)?;
    state
        .shard
        .shard()
        .permissioner
        .borrow()
        .create_topic(identity.user_id, numeric_stream_id)?;

    let _topic_guard = state.shard.shard().fs_locks.topic_lock.lock().await;
    let topic = {
        let future = SendWrapper::new(state.shard.shard().create_topic(
            &command.stream_id,
            command.name.clone(),
            command.message_expiry,
            command.compression_algorithm,
            command.max_topic_size,
            command.replication_factor,
        ));
        future.await
    }
    .error(|e: &IggyError| {
        format!("{COMPONENT} (error: {e}) - failed to create topic, stream ID: {stream_id}")
    })?;

    // Update command with actual values from created topic
    command.message_expiry = topic.root().message_expiry();
    command.max_topic_size = topic.root().max_topic_size();

    let topic_id = topic.id();

    // Send events for topic creation
    let broadcast_future = SendWrapper::new(async {
        use crate::shard::transmission::event::ShardEvent;

        let shard = state.shard.shard();

        let event = ShardEvent::CreatedTopic {
            stream_id: command.stream_id.clone(),
            topic,
        };
        let _responses = shard.broadcast_event_to_all_shards(event).await;

        // Create partitions
        let partitions = shard
            .create_partitions(
                &command.stream_id,
                &Identifier::numeric(topic_id as u32).unwrap(),
                command.partitions_count,
            )
            .await?;

        let event = ShardEvent::CreatedPartitions {
            stream_id: command.stream_id.clone(),
            topic_id: Identifier::numeric(topic_id as u32).unwrap(),
            partitions,
        };
        let _responses = shard.broadcast_event_to_all_shards(event).await;

        Ok::<(), CustomError>(())
    });

    broadcast_future.await.error(|e: &CustomError| {
        format!(
            "{COMPONENT} (error: {e}) - failed to broadcast topic events, stream ID: {stream_id}"
        )
    })?;

    // Create response using the same approach as binary handler
    let response = {
        let topic_identifier = Identifier::numeric(topic_id as u32).unwrap();
        let topic_response = state.shard.shard().streams.with_topic_by_id(
            &command.stream_id,
            &topic_identifier,
            |(root, _, stats)| crate::http::mapper::map_topic_details(&root, &stats),
        );
        Json(topic_response)
    };

    // Apply state change like in binary handler
    {
        let entry_command = EntryCommand::CreateTopic(CreateTopicWithId {
            topic_id: topic_id as u32,
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
    .error(|e: &IggyError| {
        format!("{COMPONENT} (error: {e}) - failed to apply create topic, stream ID: {stream_id}",)
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

    let (numeric_stream_id, numeric_topic_id) = state
        .shard
        .shard()
        .resolve_topic_id(&command.stream_id, &command.topic_id)?;
    state.shard.shard().permissioner.borrow().update_topic(
        identity.user_id,
        numeric_stream_id,
        numeric_topic_id,
    )?;

    let name_changed = !command.name.is_empty();
    state.shard.shard().update_topic(
        &command.stream_id,
        &command.topic_id,
        command.name.clone(),
        command.message_expiry,
        command.compression_algorithm,
        command.max_topic_size,
        command.replication_factor,
    ).error(|e: &IggyError| {
        format!(
            "{COMPONENT} (error: {e}) - failed to update topic, stream ID: {stream_id}, topic ID: {topic_id}"
        )
    })?;

    // TODO: Tech debt.
    let topic_id = if name_changed {
        Identifier::named(&command.name.clone()).unwrap()
    } else {
        command.topic_id.clone()
    };

    // Get the updated values from the topic
    let (message_expiry, max_topic_size) = state.shard.shard().streams.with_topic_by_id(
        &command.stream_id,
        &topic_id,
        |(root, _, _)| (root.message_expiry(), root.max_topic_size()),
    );

    // Send event for topic update
    {
        let broadcast_future = SendWrapper::new(async {
            use crate::shard::transmission::event::ShardEvent;
            let event = ShardEvent::UpdatedTopic {
                stream_id: command.stream_id.clone(),
                topic_id: command.topic_id.clone(),
                name: command.name.clone(),
                message_expiry: command.message_expiry,
                compression_algorithm: command.compression_algorithm,
                max_topic_size: command.max_topic_size,
                replication_factor: command.replication_factor,
            };
            let _responses = state
                .shard
                .shard()
                .broadcast_event_to_all_shards(event)
                .await;
        });
        broadcast_future.await;
    }

    command.message_expiry = message_expiry;
    command.max_topic_size = max_topic_size;

    {
        let entry_command = EntryCommand::UpdateTopic(command);
        let future = SendWrapper::new(state.shard.shard().state
            .apply(identity.user_id, &entry_command));
        future.await
    }.error(|e: &IggyError| {
        format!(
            "{COMPONENT} (error: {e}) - failed to apply update topic, stream ID: {stream_id}, topic ID: {topic_id}"
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

    let (numeric_stream_id, numeric_topic_id) = state
        .shard
        .shard()
        .resolve_topic_id(&identifier_stream_id, &identifier_topic_id)?;
    state.shard.shard().permissioner.borrow().delete_topic(
        identity.user_id,
        numeric_stream_id,
        numeric_topic_id,
    )?;
    let _topic_guard = state.shard.shard().fs_locks.topic_lock.lock().await;

    let topic = {
        let future = SendWrapper::new(state.shard.shard().delete_topic(
            &identifier_stream_id,
            &identifier_topic_id,
        ));
        future.await
    }.error(|e: &IggyError| {
        format!(
            "{COMPONENT} (error: {e}) - failed to delete topic with ID: {topic_id} in stream with ID: {stream_id}",
        )
    })?;

    let topic_id_numeric = topic.root().id();

    // Send event for topic deletion
    {
        let broadcast_future = SendWrapper::new(async {
            use crate::shard::transmission::event::ShardEvent;
            let event = ShardEvent::DeletedTopic {
                id: topic_id_numeric,
                stream_id: identifier_stream_id.clone(),
                topic_id: identifier_topic_id.clone(),
            };
            let _responses = state
                .shard
                .shard()
                .broadcast_event_to_all_shards(event)
                .await;
        });
        broadcast_future.await;
    }

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
    }.error(|e: &IggyError| {
        format!(
            "{COMPONENT} (error: {e}) - failed to apply delete topic, stream ID: {stream_id}, topic ID: {topic_id}"
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

    let (numeric_stream_id, numeric_topic_id) = state
        .shard
        .shard()
        .resolve_topic_id(&identifier_stream_id, &identifier_topic_id)?;
    state.shard.shard().permissioner.borrow().purge_topic(
        identity.user_id,
        numeric_stream_id,
        numeric_topic_id,
    )?;

    {
        let future = SendWrapper::new(state.shard.shard().purge_topic(
            &identifier_stream_id,
            &identifier_topic_id,
        ));
        future.await
    }.error(|e: &IggyError| {
        format!(
            "{COMPONENT} (error: {e}) - failed to purge topic, stream ID: {stream_id}, topic ID: {topic_id}"
        )
    })?;

    // Send event for topic purge
    {
        let broadcast_future = SendWrapper::new(async {
            use crate::shard::transmission::event::ShardEvent;
            let event = ShardEvent::PurgedTopic {
                stream_id: identifier_stream_id.clone(),
                topic_id: identifier_topic_id.clone(),
            };
            let _responses = state
                .shard
                .shard()
                .broadcast_event_to_all_shards(event)
                .await;
        });
        broadcast_future.await;
    }

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
    }.error(|e: &IggyError| {
        format!(
            "{COMPONENT} (error: {e}) - failed to apply purge topic, stream ID: {stream_id}, topic ID: {topic_id}"
        )
    })?;

    Ok(StatusCode::NO_CONTENT)
}
