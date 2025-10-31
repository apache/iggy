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
use crate::streaming::session::Session;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router, debug_handler};
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::Validatable;
use iggy_common::create_partitions::CreatePartitions;
use iggy_common::delete_partitions::DeletePartitions;
use send_wrapper::SendWrapper;
use std::sync::Arc;
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/streams/{stream_id}/topics/{topic_id}/partitions",
            post(create_partitions).delete(delete_partitions),
        )
        .with_state(state)
}

#[debug_handler]
#[instrument(skip_all, name = "trace_create_partitions", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn create_partitions(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<CreatePartitions>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;

    let _parititon_guard = state.shard.shard().fs_locks.partition_lock.lock().await;
    let session = Session::stateless(identity.user_id, identity.ip_address);
    let partitions = SendWrapper::new(state.shard.shard().create_partitions(
        &session,
        &command.stream_id,
        &command.topic_id,
        command.partitions_count,
    ))
    .await?;

    let broadcast_future = SendWrapper::new(async {
        let shard = state.shard.shard();

        let event = ShardEvent::CreatedPartitions {
            stream_id: command.stream_id.clone(),
            topic_id: command.topic_id.clone(),
            partitions,
        };
        let _responses = shard.broadcast_event_to_all_shards(event).await;
        Ok::<(), CustomError>(())
    });

    broadcast_future.await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to broadcast partition events, stream ID: {stream_id}, topic ID: {topic_id}"
                )
            })?;
    let command = EntryCommand::CreatePartitions(command);
    let state_future =
        SendWrapper::new(state.shard.shard().state.apply(identity.user_id, &command));

    state_future.await
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply create partitions, stream ID: {stream_id}, topic ID: {topic_id}"
            )
        })?;

    Ok(StatusCode::CREATED)
}

#[debug_handler]
#[instrument(skip_all, name = "trace_delete_partitions", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn delete_partitions(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<DeletePartitions>,
) -> Result<StatusCode, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;

    let session = Session::stateless(identity.user_id, identity.ip_address);
    let deleted_partition_ids = {
        let delete_future = SendWrapper::new(state.shard.shard().delete_partitions(
            &session,
            &query.stream_id,
            &query.topic_id,
            query.partitions_count,
        ));

        delete_future.await.with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to delete partitions for topic with ID: {topic_id} in stream with ID: {stream_id}"
            )
        })?
    };

    // Send event for partition deletion
    {
        let broadcast_future = SendWrapper::new(async {
            let event = ShardEvent::DeletedPartitions {
                stream_id: query.stream_id.clone(),
                topic_id: query.topic_id.clone(),
                partitions_count: query.partitions_count,
                partition_ids: deleted_partition_ids,
            };
            let _responses = state
                .shard
                .shard()
                .broadcast_event_to_all_shards(event)
                .await;
        });
        broadcast_future.await;
    }

    let command = EntryCommand::DeletePartitions(DeletePartitions {
        stream_id: query.stream_id.clone(),
        topic_id: query.topic_id.clone(),
        partitions_count: query.partitions_count,
    });
    let state_future =
        SendWrapper::new(state.shard.shard().state.apply(identity.user_id, &command));

    state_future.await
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply delete partitions, stream ID: {stream_id}, topic ID: {topic_id}"
            )
        })?;

    Ok(StatusCode::NO_CONTENT)
}
