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
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::delete;
use axum::{Extension, Router, debug_handler};
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::Validatable;
use iggy_common::delete_segments::DeleteSegments;
use iggy_common::sharding::IggyNamespace;
use send_wrapper::SendWrapper;
use std::sync::Arc;
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/streams/{stream_id}/topics/{topic_id}/partitions/{partition_id}",
            delete(delete_segments),
        )
        .with_state(state)
}

#[debug_handler]
#[instrument(skip_all, name = "trace_delete_segments", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn delete_segments(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id, partition_id)): Path<(String, String, u32)>,
    mut query: Query<DeleteSegments>,
) -> Result<StatusCode, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.partition_id = partition_id;
    query.validate()?;
    let segments_count = query.segments_count;

    let partition = state.shard.shard().resolve_partition_for_delete_segments(
        identity.user_id,
        &query.stream_id,
        &query.topic_id,
        partition_id as usize,
    )?;

    let namespace = IggyNamespace::new(
        partition.stream_id,
        partition.topic_id,
        partition.partition_id,
    );
    let request = ShardRequest::data_plane(
        namespace,
        ShardRequestPayload::DeleteSegments { segments_count },
    );

    let delete_future = SendWrapper::new(state.shard.shard().send_to_data_plane(request));
    match delete_future.await? {
        ShardResponse::DeleteSegments {
            deleted_segments,
            deleted_messages,
        } => {
            state
                .shard
                .shard()
                .metrics
                .decrement_segments(deleted_segments as u32);
            state
                .shard
                .shard()
                .metrics
                .decrement_messages(deleted_messages);
        }
        ShardResponse::ErrorResponse(err) => return Err(err.into()),
        _ => unreachable!("Expected DeleteSegments"),
    }

    let command = DeleteSegments {
        stream_id: query.stream_id.clone(),
        topic_id: query.topic_id.clone(),
        partition_id: query.partition_id,
        segments_count,
    };
    let entry_command = crate::state::command::EntryCommand::DeleteSegments(command);
    let state_future = SendWrapper::new(
        state
            .shard
            .shard()
            .state
            .apply(identity.user_id, &entry_command),
    );
    state_future.await.error(|e: &iggy_common::IggyError| {
        format!(
            "{COMPONENT} (error: {e}) - failed to apply delete segments, stream ID: {stream_id}, topic ID: {topic_id}"
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}
