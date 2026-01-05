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
use crate::streaming::session::Session;
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
    Path(stream_id): Path<String>,
) -> Result<Json<StreamDetails>, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;

    // Get numeric stream ID
    let numeric_stream_id = state
        .shard
        .shard()
        .shared_metadata
        .get_stream_id(&stream_id)
        .ok_or(CustomError::ResourceNotFound)?;

    // Use SharedMetadata for stream lookup
    let snapshot = state.shard.shard().shared_metadata.load();
    let stream_meta = snapshot
        .streams
        .get(&numeric_stream_id)
        .ok_or(CustomError::ResourceNotFound)?;

    // Get stream stats
    let stream_stats = state
        .shard
        .shard()
        .shared_stats
        .get_stream_stats(numeric_stream_id);

    // Get topics with their stats
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

    let stream_details = crate::http::mapper::map_stream_details_from_metadata(
        stream_meta,
        stream_stats.as_ref().map(|arc| arc.as_ref()),
        &topics_refs,
    );
    Ok(Json(stream_details))
}

#[debug_handler]
async fn get_streams(State(state): State<Arc<AppState>>) -> Result<Json<Vec<Stream>>, CustomError> {
    // Use SharedMetadata for streams retrieval
    let streams_meta = state.shard.shard().shared_metadata.get_streams();
    let streams = crate::http::mapper::map_streams_from_metadata(&streams_meta);
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

    let result = SendWrapper::new(async move {
        let session = Session::stateless(identity.user_id, identity.ip_address);

        let _stream_guard = state.shard.shard().fs_locks.stream_lock.lock().await;
        // Create stream using wrapper method
        let stream = state
            .shard
            .create_stream(&session, command.name.clone())
            .await
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to create stream with name: {}",
                    command.name
                )
            })?;

        let created_stream_id = stream.id;

        // Apply state change using wrapper method
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

        // Get the created stream details from SharedMetadata
        let snapshot = state.shard.shard().shared_metadata.load();
        let stream_meta = snapshot
            .streams
            .get(&(created_stream_id as usize))
            .ok_or_else(|| {
                IggyError::StreamIdNotFound(Identifier::numeric(created_stream_id as u32).unwrap())
            })?;

        // New stream has no stats yet, pass empty
        let response =
            crate::http::mapper::map_stream_details_from_metadata(stream_meta, None, &[]);

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

    let result = SendWrapper::new(async move {
        let session = Session::stateless(identity.user_id, identity.ip_address);

        // Update stream using wrapper method
        state
            .shard
            .update_stream(&session, &command.stream_id, command.name.clone())
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to update stream, stream ID: {stream_id}"
                )
            })?;

        // Apply state change using wrapper method
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

    let result =
        SendWrapper::new(async move {
            let session = Session::stateless(identity.user_id, identity.ip_address);

            let _stream_guard = state.shard.shard().fs_locks.stream_lock.lock().await;
            // Delete stream
            {
                let future = SendWrapper::new(
                    state
                        .shard
                        .shard()
                        .delete_stream(&session, &identifier_stream_id),
                );
                future.await
            }
            .error(|e: &IggyError| {
                format!("{COMPONENT} (error: {e}) - failed to delete stream with ID: {stream_id}",)
            })?;

            // Apply state change using wrapper method
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

    let result = SendWrapper::new(async move {
        let session = Session::stateless(identity.user_id, identity.ip_address);

        // Purge stream using wrapper method
        state
            .shard
            .purge_stream(&session, &identifier_stream_id)
            .await
            .error(|e: &IggyError| {
                format!("{COMPONENT} (error: {e}) - failed to purge stream, stream ID: {stream_id}")
            })?;

        // Apply state change using wrapper method
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
