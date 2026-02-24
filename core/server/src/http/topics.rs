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
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardRequest, ShardRequestPayload};
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

    let shard = state.shard.shard();

    let numeric_stream_id = shard
        .metadata
        .get_stream_id(&identity_stream_id)
        .ok_or(CustomError::ResourceNotFound)?;

    let numeric_topic_id = shard
        .metadata
        .get_topic_id(numeric_stream_id, &identity_topic_id)
        .ok_or(CustomError::ResourceNotFound)?;

    shard
        .metadata
        .perm_get_topic(identity.user_id, numeric_stream_id, numeric_topic_id)
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - permission denied to get topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                identity.user_id,
            )
        })?;

    let topic_meta = shard
        .metadata
        .get_topic(numeric_stream_id, numeric_topic_id)
        .ok_or(CustomError::ResourceNotFound)?;

    let topic_details = crate::http::mapper::map_topic_details_from_metadata(&topic_meta);

    Ok(Json(topic_details))
}

#[debug_handler]
async fn get_topics(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<Json<Vec<Topic>>, CustomError> {
    let stream_id_ident = Identifier::from_str_value(&stream_id)?;
    let shard = state.shard.shard();

    let numeric_stream_id = shard
        .metadata
        .get_stream_id(&stream_id_ident)
        .ok_or(CustomError::ResourceNotFound)?;

    shard
        .metadata
        .perm_get_topics(identity.user_id, numeric_stream_id)
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - permission denied to get topics in stream with ID: {stream_id} for user with ID: {}",
                identity.user_id,
            )
        })?;

    let stream_meta = shard
        .metadata
        .get_stream(numeric_stream_id)
        .ok_or(CustomError::ResourceNotFound)?;
    let topics = crate::http::mapper::map_topics_from_metadata(&stream_meta);

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

    let numeric_stream_id = state
        .shard
        .shard()
        .metadata
        .get_stream_id(&command.stream_id)
        .ok_or(CustomError::ResourceNotFound)?;

    let request = ShardRequest::control_plane(ShardRequestPayload::CreateTopicRequest {
        user_id: identity.user_id,
        command,
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::CreateTopicResponse(data) => {
            let topic_meta = state
                .shard
                .shard()
                .metadata
                .get_topic(numeric_stream_id, data.id as usize)
                .expect("Topic must exist after creation");
            let response = crate::http::mapper::map_topic_details_from_metadata(&topic_meta);
            Ok(Json(response))
        }
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected CreateTopicResponse"),
    }
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

    let request = ShardRequest::control_plane(ShardRequestPayload::UpdateTopicRequest {
        user_id: identity.user_id,
        command,
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::UpdateTopicResponse => Ok(StatusCode::NO_CONTENT),
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected UpdateTopicResponse"),
    }
}

#[debug_handler]
#[instrument(skip_all, name = "trace_delete_topic", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn delete_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;

    let request = ShardRequest::control_plane(ShardRequestPayload::DeleteTopicRequest {
        user_id: identity.user_id,
        command: DeleteTopic {
            stream_id,
            topic_id,
        },
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::DeleteTopicResponse(_) => Ok(StatusCode::NO_CONTENT),
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected DeleteTopicResponse"),
    }
}

#[debug_handler]
#[instrument(skip_all, name = "trace_purge_topic", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn purge_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;

    let request = ShardRequest::control_plane(ShardRequestPayload::PurgeTopicRequest {
        user_id: identity.user_id,
        command: PurgeTopic {
            stream_id,
            topic_id,
        },
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::PurgeTopicResponse => Ok(StatusCode::NO_CONTENT),
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected PurgeTopicResponse"),
    }
}
