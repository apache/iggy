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

use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::shared::AppState;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardRequest, ShardRequestPayload};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Extension, Json, Router, debug_handler};
use iggy_common::Identifier;
use iggy_common::Validatable;
use iggy_common::create_stream::CreateStream;
use iggy_common::delete_stream::DeleteStream;
use iggy_common::purge_stream::PurgeStream;
use iggy_common::update_stream::UpdateStream;
use iggy_common::{Stream, StreamDetails};
use std::sync::Arc;
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/streams", get(get_streams).post(create_stream))
        .route(
            "/streams/{stream_id}",
            get(get_stream).put(update_stream).delete(delete_stream),
        )
        .route("/streams/{stream_id}/purge", delete(purge_stream))
        .with_state(state)
}

#[debug_handler]
async fn get_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<Json<StreamDetails>, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;

    let shard = state.shard.shard();
    let numeric_stream_id = shard
        .metadata
        .get_stream_id(&stream_id)
        .ok_or(CustomError::ResourceNotFound)?;

    shard
        .metadata
        .perm_get_stream(identity.user_id, numeric_stream_id)?;

    let stream_meta = shard
        .metadata
        .get_stream(numeric_stream_id)
        .ok_or(CustomError::ResourceNotFound)?;

    let stream_details = crate::http::mapper::map_stream_details_from_metadata(&stream_meta);

    Ok(Json(stream_details))
}

#[debug_handler]
async fn get_streams(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<Stream>>, CustomError> {
    let shard = state.shard.shard();

    shard.metadata.perm_get_streams(identity.user_id)?;

    let streams = shard
        .metadata
        .with_metadata(crate::http::mapper::map_streams_from_metadata);

    Ok(Json(streams))
}

#[debug_handler]
#[instrument(skip_all, name = "trace_create_stream", fields(iggy_user_id = identity.user_id))]
async fn create_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<CreateStream>,
) -> Result<Json<StreamDetails>, CustomError> {
    command.validate()?;

    let request = ShardRequest::control_plane(ShardRequestPayload::CreateStreamRequest {
        user_id: identity.user_id,
        command,
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::CreateStreamResponse(data) => {
            let stream_meta = state
                .shard
                .shard()
                .metadata
                .get_stream(data.id as usize)
                .expect("Stream must exist after creation");
            let response = crate::http::mapper::map_stream_details_from_metadata(&stream_meta);
            Ok(Json(response))
        }
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected CreateStreamResponse"),
    }
}

#[debug_handler]
#[instrument(skip_all, name = "trace_update_stream", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id))]
async fn update_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
    Json(mut command): Json<UpdateStream>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.validate()?;

    let request = ShardRequest::control_plane(ShardRequestPayload::UpdateStreamRequest {
        user_id: identity.user_id,
        command,
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::UpdateStreamResponse => Ok(StatusCode::NO_CONTENT),
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected UpdateStreamResponse"),
    }
}

#[debug_handler]
#[instrument(skip_all, name = "trace_delete_stream", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id))]
async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;

    let request = ShardRequest::control_plane(ShardRequestPayload::DeleteStreamRequest {
        user_id: identity.user_id,
        command: DeleteStream { stream_id },
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::DeleteStreamResponse(_) => Ok(StatusCode::NO_CONTENT),
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected DeleteStreamResponse"),
    }
}

#[debug_handler]
#[instrument(skip_all, name = "trace_purge_stream", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id))]
async fn purge_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;

    let request = ShardRequest::control_plane(ShardRequestPayload::PurgeStreamRequest {
        user_id: identity.user_id,
        command: PurgeStream { stream_id },
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::PurgeStreamResponse => Ok(StatusCode::NO_CONTENT),
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected PurgeStreamResponse"),
    }
}
