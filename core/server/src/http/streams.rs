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
use crate::streaming::session::Session;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Extension, Json, Router, debug_handler};
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::Validatable;
use iggy_common::create_stream::CreateStream;
use iggy_common::delete_stream::DeleteStream;
use iggy_common::purge_stream::PurgeStream;
use iggy_common::update_stream::UpdateStream;
use iggy_common::{Stream, StreamDetails};
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
    let Ok(stream) = state.shard.shard().try_find_stream(
        &Session::stateless(identity.user_id, identity.ip_address),
        &stream_id,
    ) else {
        return Err(CustomError::ResourceNotFound);
    };
    let Some(stream) = stream else {
        return Err(CustomError::ResourceNotFound);
    };

    let stream = mapper::map_stream(&SendWrapper::new(stream));
    Ok(Json(stream))
}

#[debug_handler]
async fn get_streams(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<Stream>>, CustomError> {
    let streams: Vec<std::cell::Ref<'_, crate::streaming::streams::stream::Stream>> = state
        .shard
        .shard()
        .find_streams(&Session::stateless(identity.user_id, identity.ip_address))
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to find streams, user ID: {}",
                identity.user_id
            )
        })?;
    let stream_refs = {
        let refs: Vec<&crate::streaming::streams::stream::Stream> =
            streams.iter().map(|ref_guard| &**ref_guard).collect();
        refs
    };
    let streams = mapper::map_streams(&stream_refs);
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

    let session = Session::stateless(identity.user_id, identity.ip_address);
    let create_stream_future = SendWrapper::new(state.shard.shard().create_stream(
        &session,
        command.stream_id,
        &command.name,
    ));

    let stream_identifier = create_stream_future.await.with_error_context(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to create stream, stream ID: {:?}",
            command.stream_id
        )
    })?;

    let broadcast_future = SendWrapper::new(async {
        use crate::shard::transmission::event::ShardEvent;

        let shard = state.shard.shard();

        let event = ShardEvent::CreatedStream {
            stream_id: command.stream_id,
            name: command.name.clone(),
        };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;

        Ok::<(), CustomError>(())
    });

    broadcast_future.await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to broadcast stream creation event, stream ID: {stream_identifier}"
            )
        })?;

    let stream = state
        .shard
        .shard()
        .find_stream(&Session::stateless(identity.user_id, identity.ip_address), &stream_identifier)
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to find created stream, stream ID: {stream_identifier}"
            )
        })?;

    let response = Json(mapper::map_stream(&SendWrapper::new(stream)));

    let entry_command = EntryCommand::CreateStream(CreateStreamWithId {
        stream_id: command.stream_id.unwrap(), // TODO: handle unwrap
        command,
    });
    let state_future = SendWrapper::new(
        state
            .shard
            .shard()
            .state
            .apply(identity.user_id, &entry_command),
    );

    state_future
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply create stream, stream ID: {stream_identifier}",
            )
        })?;
    Ok(response)
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

    let session = Session::stateless(identity.user_id, identity.ip_address);
    let update_stream_future = SendWrapper::new(state.shard.shard().update_stream(
        &session,
        &command.stream_id,
        &command.name,
    ));

    update_stream_future.await.with_error_context(|error| {
        format!("{COMPONENT} (error: {error}) - failed to update stream, stream ID: {stream_id}")
    })?;

    let entry_command = EntryCommand::UpdateStream(command);
    let state_future = SendWrapper::new(
        state
            .shard
            .shard()
            .state
            .apply(identity.user_id, &entry_command),
    );

    state_future.await.with_error_context(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to apply update stream, stream ID: {stream_id}"
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

#[debug_handler]
#[instrument(skip_all, name = "trace_delete_stream", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id))]
async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;

    state
        .shard
        .shard()
        .delete_stream(
            &Session::stateless(identity.user_id, identity.ip_address),
            &identifier_stream_id,
        )
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to delete stream with ID: {stream_id}",)
        })?;

    let entry_command = EntryCommand::DeleteStream(DeleteStream {
        stream_id: identifier_stream_id,
    });
    let state_future = SendWrapper::new(
        state
            .shard
            .shard()
            .state
            .apply(identity.user_id, &entry_command),
    );

    state_future.await.with_error_context(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to apply delete stream with ID: {stream_id}",
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

#[instrument(skip_all, name = "trace_purge_stream", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id))]
async fn purge_stream(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;
    let session = Session::stateless(identity.user_id, identity.ip_address);
    let purge_stream_future = SendWrapper::new(
        state
            .shard
            .shard()
            .purge_stream(&session, &identifier_stream_id),
    );

    purge_stream_future.await.with_error_context(|error| {
        format!("{COMPONENT} (error: {error}) - failed to purge stream, stream ID: {stream_id}")
    })?;

    let entry_command = EntryCommand::PurgeStream(PurgeStream {
        stream_id: identifier_stream_id,
    });
    let state_future = SendWrapper::new(
        state
            .shard
            .shard()
            .state
            .apply(identity.user_id, &entry_command),
    );

    state_future.await.with_error_context(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to apply purge stream, stream ID: {stream_id}"
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}
