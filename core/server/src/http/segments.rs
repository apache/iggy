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
use crate::shard::transmission::event::ShardEvent;
use crate::state::command::EntryCommand;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::delete;
use axum::{Extension, Router, debug_handler};
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::Validatable;
use iggy_common::delete_segments::DeleteSegments;
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
    Query((segments_count,)): Query<(u32,)>,
    mut query: Query<DeleteSegments>,
) -> Result<StatusCode, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.partition_id = partition_id;
    query.segments_count = segments_count;
    query.validate()?;

    let (numeric_stream_id, numeric_topic_id) = state
        .shard
        .shard()
        .resolve_topic_id(&query.stream_id, &query.topic_id)?;
    state.shard.shard().metadata.perm_delete_segments(
        identity.user_id,
        numeric_stream_id,
        numeric_topic_id,
    )?;
    let _deleted_segment_ids = {
        let delete_future = SendWrapper::new(state.shard.shard().delete_segments_base(
            numeric_stream_id,
            numeric_topic_id,
            partition_id as usize,
            query.segments_count,
        ));

        delete_future.await.error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to delete segments for topic with ID: {topic_id} in stream with ID: {stream_id}"
            )
        })?
    };

    // Send event for deleted segments
    {
        let broadcast_future = SendWrapper::new(async {
            let event = ShardEvent::DeletedSegments {
                stream_id: query.stream_id.clone(),
                topic_id: query.topic_id.clone(),
                segments_count: query.segments_count,
                partition_id: query.partition_id,
            };
            let _responses = state
                .shard
                .shard()
                .broadcast_event_to_all_shards(event)
                .await;
        });
        broadcast_future.await;
    }

    let command = EntryCommand::DeleteSegments(DeleteSegments {
        stream_id: query.stream_id.clone(),
        topic_id: query.topic_id.clone(),
        partition_id: query.partition_id,
        segments_count: query.segments_count,
    });
    let state_future =
        SendWrapper::new(state.shard.shard().state.apply(identity.user_id, &command));

    state_future.await
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to apply delete segments, stream ID: {stream_id}, topic ID: {topic_id}"
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}
