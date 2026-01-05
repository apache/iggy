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

    let session = Session::stateless(identity.user_id, identity.ip_address);

    // Check permissions and stream existence
    state.shard.shard().ensure_authenticated(&session)?;
    let stream_exists = state
        .shard
        .shard()
        .ensure_stream_exists(&identity_stream_id)
        .is_ok();
    if !stream_exists {
        return Err(CustomError::ResourceNotFound);
    }

    let topic_exists = state
        .shard
        .shard()
        .ensure_topic_exists(&identity_stream_id, &identity_topic_id)
        .is_ok();
    if !topic_exists {
        return Err(CustomError::ResourceNotFound);
    }

    let numeric_stream_id = state
        .shard
        .shard()
        .shared_metadata
        .get_stream_id(&identity_stream_id)
        .ok_or(CustomError::ResourceNotFound)?;
    let numeric_topic_id = state
        .shard
        .shard()
        .shared_metadata
        .get_topic_id(numeric_stream_id, &identity_topic_id)
        .ok_or(CustomError::ResourceNotFound)?;

    state.shard.shard()
        .permissioner
        .get_topic(session.get_user_id(), numeric_stream_id, numeric_topic_id)
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - permission denied to get topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                session.get_user_id(),
            )
        })?;

    // Get topic from SharedMetadata
    let snapshot = state.shard.shard().shared_metadata.load();
    let stream_meta = snapshot
        .streams
        .get(&numeric_stream_id)
        .ok_or(CustomError::ResourceNotFound)?;
    let topic_meta = stream_meta
        .get_topic(numeric_topic_id)
        .ok_or(CustomError::ResourceNotFound)?;

    // Get topic stats
    let topic_stats = state
        .shard
        .shard()
        .shared_stats
        .get_topic_stats(numeric_stream_id, numeric_topic_id);

    // Get partition stats
    let partition_stats: Vec<_> = topic_meta
        .partitions
        .values()
        .map(|p| {
            let p_stats = state.shard.shard().shared_stats.get_partition_stats(
                numeric_stream_id,
                numeric_topic_id,
                p.id,
            );
            (p, p_stats)
        })
        .collect();

    let partition_refs: Vec<_> = partition_stats
        .iter()
        .map(|(p, s)| (*p, s.as_ref().map(|arc| arc.as_ref())))
        .collect();

    let topic_details = mapper::map_topic_details_from_metadata(
        topic_meta,
        topic_stats.as_ref().map(|arc| arc.as_ref()),
        &partition_refs,
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
    let session = Session::stateless(identity.user_id, identity.ip_address);

    // Check permissions and stream existence
    state.shard.shard().ensure_authenticated(&session)?;
    state.shard.shard().ensure_stream_exists(&stream_id)?;

    let numeric_stream_id = state
        .shard
        .shard()
        .shared_metadata
        .get_stream_id(&stream_id)
        .ok_or(CustomError::ResourceNotFound)?;

    state.shard.shard()
        .permissioner
        .get_topics(session.get_user_id(), numeric_stream_id)
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - permission denied to get topics in stream with ID: {stream_id} for user with ID: {}",
                session.get_user_id(),
            )
        })?;

    // Get topics from SharedMetadata
    let snapshot = state.shard.shard().shared_metadata.load();
    let stream_meta = snapshot
        .streams
        .get(&numeric_stream_id)
        .ok_or(CustomError::ResourceNotFound)?;

    let topics_with_stats: Vec<_> = stream_meta
        .topics
        .values()
        .map(|t| {
            let stats = state
                .shard
                .shard()
                .shared_stats
                .get_topic_stats(numeric_stream_id, t.id);
            (t, stats)
        })
        .collect();

    let topics_refs: Vec<_> = topics_with_stats
        .iter()
        .map(|(t, s)| (*t, s.as_ref().map(|arc| arc.as_ref())))
        .collect();

    let topics = mapper::map_topics_from_metadata(&topics_refs);

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

    let _topic_guard = state.shard.shard().fs_locks.topic_lock.lock().await;
    let topic = {
        let future = SendWrapper::new(state.shard.shard().create_topic(
            &session,
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
    command.message_expiry = topic.message_expiry;
    command.max_topic_size = topic.max_topic_size;

    let topic_id = topic.id;

    // Create partitions
    {
        let shard = state.shard.shard();
        let topic_identifier = Identifier::numeric(topic_id as u32).unwrap();
        let future = SendWrapper::new(shard.create_partitions(
            &session,
            &command.stream_id,
            &topic_identifier,
            command.partitions_count,
        ));
        future.await
    }
    .error(|e: &IggyError| {
        format!("{COMPONENT} (error: {e}) - failed to create partitions, stream ID: {stream_id}")
    })?;

    // Get numeric stream ID
    let numeric_stream_id = state
        .shard
        .shard()
        .shared_metadata
        .get_stream_id(&command.stream_id)
        .ok_or(CustomError::ResourceNotFound)?;

    // Get topic from SharedMetadata for response
    let snapshot = state.shard.shard().shared_metadata.load();
    let stream_meta = snapshot
        .streams
        .get(&numeric_stream_id)
        .ok_or(CustomError::ResourceNotFound)?;
    let topic_meta = stream_meta
        .get_topic(topic_id)
        .ok_or(CustomError::ResourceNotFound)?;

    // Get partition stats for the response
    let partition_stats: Vec<_> = topic_meta
        .partitions
        .values()
        .map(|p| (p, None::<&crate::streaming::stats::PartitionStats>))
        .collect();

    let topic_response =
        mapper::map_topic_details_from_metadata(topic_meta, None, &partition_stats);
    drop(snapshot);

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

    Ok(Json(topic_response))
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

    let session = Session::stateless(identity.user_id, identity.ip_address);

    let name_changed = !command.name.is_empty();
    state.shard.shard().update_topic(
        &session,
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

    let lookup_topic_id = if name_changed {
        Identifier::named(&command.name).unwrap()
    } else {
        command.topic_id.clone()
    };

    // Get the updated values from SharedMetadata
    let numeric_stream_id = state
        .shard
        .shard()
        .shared_metadata
        .get_stream_id(&command.stream_id)
        .ok_or(CustomError::ResourceNotFound)?;
    let numeric_topic_id = state
        .shard
        .shard()
        .shared_metadata
        .get_topic_id(numeric_stream_id, &lookup_topic_id)
        .ok_or(CustomError::ResourceNotFound)?;

    let snapshot = state.shard.shard().shared_metadata.load();
    let stream_meta = snapshot
        .streams
        .get(&numeric_stream_id)
        .ok_or(CustomError::ResourceNotFound)?;
    let topic_meta = stream_meta
        .get_topic(numeric_topic_id)
        .ok_or(CustomError::ResourceNotFound)?;

    command.message_expiry = topic_meta.message_expiry;
    command.max_topic_size = topic_meta.max_topic_size;
    drop(snapshot);

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

    let session = Session::stateless(identity.user_id, identity.ip_address);
    let _topic_guard = state.shard.shard().fs_locks.topic_lock.lock().await;

    {
        let future = SendWrapper::new(state.shard.shard().delete_topic(
            &session,
            &identifier_stream_id,
            &identifier_topic_id,
        ));
        future.await
    }.error(|e: &IggyError| {
        format!(
            "{COMPONENT} (error: {e}) - failed to delete topic with ID: {topic_id} in stream with ID: {stream_id}",
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

    let session = Session::stateless(identity.user_id, identity.ip_address);

    {
        let future = SendWrapper::new(state.shard.shard().purge_topic(
            &session,
            &identifier_stream_id,
            &identifier_topic_id,
        ));
        future.await
    }.error(|e: &IggyError| {
        format!(
            "{COMPONENT} (error: {e}) - failed to purge topic, stream ID: {stream_id}, topic ID: {topic_id}"
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
    }.error(|e: &IggyError| {
        format!(
            "{COMPONENT} (error: {e}) - failed to apply purge topic, stream ID: {stream_id}, topic ID: {topic_id}"
        )
    })?;

    Ok(StatusCode::NO_CONTENT)
}
