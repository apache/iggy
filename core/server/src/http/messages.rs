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
use crate::shard::system::messages::PollingArgs;
use crate::streaming::segments::{IggyIndexesMut, IggyMessagesBatchMut};
use crate::streaming::session::Session;
use crate::streaming::utils::PooledBuffer;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router, debug_handler};
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::Validatable;
use iggy_common::{Consumer, PollMessages, SendMessages};
use iggy_common::{IggyMessagesBatch, PolledMessages};
use send_wrapper::SendWrapper;
use std::sync::Arc;
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/streams/{stream_id}/topics/{topic_id}/messages",
            get(poll_messages).post(send_messages),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/messages/flush/{partition_id}/{fsync}",
            get(flush_unsaved_buffer),
        )
        .with_state(state)
}

#[debug_handler]
async fn poll_messages(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<PollMessages>,
) -> Result<Json<PolledMessages>, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;

    let consumer = Consumer::new(query.0.consumer.id);

    let session = Session::stateless(identity.user_id, identity.ip_address);

    let poll_future = SendWrapper::new(state.shard.poll_messages(
        session.client_id,
        session.get_user_id(),
        query.0.stream_id,
        query.0.topic_id,
        consumer,
        query.0.partition_id,
        PollingArgs::new(query.0.strategy, query.0.count, query.0.auto_commit),
    ));

    let (metadata, messages)  = poll_future.await
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to poll messages, stream ID: {}, topic ID: {}, partition ID: {:?}",
                stream_id, topic_id, query.0.partition_id
            )
        })?;
    let polled_messages = messages.into_polled_messages(metadata);

    Ok(Json(polled_messages))
}

#[debug_handler]
async fn send_messages(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<SendMessages>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.partitioning.length = command.partitioning.value.len() as u8;
    command.validate()?;

    let batch = make_mutable(command.batch);
    let command_stream_id = command.stream_id;
    let command_topic_id = command.topic_id;
    let partitioning = command.partitioning;

    let append_future = SendWrapper::new(state.shard.append_messages(
        identity.user_id,
        command_stream_id,
        command_topic_id,
        &partitioning,
        batch,
    ));

    append_future.await
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to append messages, stream ID: {stream_id}, topic ID: {topic_id}"
            )
        })?;

    Ok(StatusCode::CREATED)
}

#[debug_handler]
#[instrument(skip_all, name = "trace_flush_unsaved_buffer", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id, iggy_partition_id = partition_id, iggy_fsync = fsync))]
async fn flush_unsaved_buffer(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id, partition_id, fsync)): Path<(String, String, u32, bool)>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let partition_id = partition_id as usize;

    let flush_future = SendWrapper::new(state.shard.shard().flush_unsaved_buffer(
        identity.user_id,
        stream_id,
        topic_id,
        partition_id,
        fsync,
    ));
    flush_future.await?;
    Ok(StatusCode::OK)
}

fn make_mutable(batch: IggyMessagesBatch) -> IggyMessagesBatchMut {
    let (_, indexes, messages) = batch.decompose();
    let (_, indexes_buffer) = indexes.decompose();
    let indexes_buffer_mut = PooledBuffer::from_existing(indexes_buffer.into());
    let indexes_mut = IggyIndexesMut::from_bytes(indexes_buffer_mut, 0);
    let count = indexes_mut.count();
    let messages_buffer_mut = PooledBuffer::from_existing(messages.into());
    IggyMessagesBatchMut::from_indexes_and_messages(count, indexes_mut, messages_buffer_mut)
}
