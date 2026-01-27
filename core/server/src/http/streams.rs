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
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Extension, Json, Router, debug_handler};
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::Validatable;
use iggy_common::create_stream::CreateStream;
use iggy_common::delete_stream::DeleteStream;
use iggy_common::purge_stream::PurgeStream;
use iggy_common::update_stream::UpdateStream;
use iggy_common::{IggyError, Stream, StreamDetails};
use send_wrapper::SendWrapper;

use crate::state::command::EntryCommand;
use crate::state::models::CreateStreamWithId;
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
    state
        .shard
        .shard()
        .metadata
        .perm_create_stream(identity.user_id)?;

    let result = SendWrapper::new(async move {
        let _stream_guard = state.shard.shard().fs_locks.stream_lock.lock().await;
        // Create stream using wrapper method
        let created_stream_id = state
            .shard
            .create_stream(command.name.clone())
            .await
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to create stream with name: {}",
                    command.name
                )
            })?;

        let entry_command = EntryCommand::CreateStream(CreateStreamWithId {
            stream_id: created_stream_id as u32,
            command,
        });

        state
            .shard
            .apply_state(identity.user_id, &entry_command)
            .await
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to apply create stream for id: {:?}",
                    created_stream_id
                )
            })?;

        let shard = state.shard.shard();
        let stream_meta = shard
            .metadata
            .get_stream(created_stream_id)
            .expect("Stream must exist after creation");
        let response = crate::http::mapper::map_stream_details_from_metadata(&stream_meta);

        Ok::<Json<StreamDetails>, CustomError>(Json(response))
    });

    result.await
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

    let stream_id_numeric = state.shard.shard().resolve_stream_id(&command.stream_id)?;
    state
        .shard
        .shard()
        .metadata
        .perm_update_stream(identity.user_id, stream_id_numeric)?;

    let result = SendWrapper::new(async move {
        // Update stream using wrapper method
        state
            .shard
            .update_stream(&command.stream_id, command.name.clone())
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to update stream, stream ID: {stream_id}"
                )
            })?;

        let entry_command = EntryCommand::UpdateStream(command);
        state.shard.apply_state(identity.user_id, &entry_command).await.error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to apply update stream, stream ID: {stream_id}"
            )
        })?;
        Ok::<StatusCode, CustomError>(StatusCode::NO_CONTENT)
    });

    result.await
}

#[debug_handler]
#[instrument(skip_all, name = "trace_delete_stream", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id))]
async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;

    let stream_id_numeric = state
        .shard
        .shard()
        .resolve_stream_id(&identifier_stream_id)?;
    state
        .shard
        .shard()
        .metadata
        .perm_delete_stream(identity.user_id, stream_id_numeric)?;

    let result =
        SendWrapper::new(async move {
            let _stream_guard = state.shard.shard().fs_locks.stream_lock.lock().await;
            {
                let future =
                    SendWrapper::new(state.shard.shard().delete_stream(&identifier_stream_id));
                future.await
            }
            .error(|e: &IggyError| {
                format!("{COMPONENT} (error: {e}) - failed to delete stream with ID: {stream_id}",)
            })?;

            let entry_command = EntryCommand::DeleteStream(DeleteStream {
                stream_id: identifier_stream_id,
            });
            state.shard.apply_state(identity.user_id, &entry_command).await
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to apply delete stream with ID: {stream_id}",
                )
            })?;
            Ok::<StatusCode, CustomError>(StatusCode::NO_CONTENT)
        });

    result.await
}

#[debug_handler]
#[instrument(skip_all, name = "trace_purge_stream", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id))]
async fn purge_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;

    let stream_id_numeric = state
        .shard
        .shard()
        .resolve_stream_id(&identifier_stream_id)?;
    state
        .shard
        .shard()
        .metadata
        .perm_purge_stream(identity.user_id, stream_id_numeric)?;

    let result = SendWrapper::new(async move {
        // Purge stream using wrapper method
        state
            .shard
            .purge_stream(&identifier_stream_id)
            .await
            .error(|e: &IggyError| {
                format!("{COMPONENT} (error: {e}) - failed to purge stream, stream ID: {stream_id}")
            })?;

        // Send event for stream purge
        {
            let broadcast_future = SendWrapper::new(async {
                use crate::shard::transmission::event::ShardEvent;
                let event = ShardEvent::PurgedStream {
                    stream_id: identifier_stream_id.clone(),
                };
                let _responses = state
                    .shard
                    .shard()
                    .broadcast_event_to_all_shards(event)
                    .await;
            });
            broadcast_future.await;
        }

        let entry_command = EntryCommand::PurgeStream(PurgeStream {
            stream_id: identifier_stream_id,
        });
        state.shard.apply_state(identity.user_id, &entry_command).await
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to apply purge stream, stream ID: {stream_id}"
                )
            })?;
        Ok::<StatusCode, CustomError>(StatusCode::NO_CONTENT)
    });

    result.await
}
