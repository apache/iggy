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
use axum::debug_handler;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Extension, Json, Router};
use err_trail::ErrContext;
use iggy_common::Consumer;
use iggy_common::ConsumerOffsetInfo;
use iggy_common::Identifier;
use iggy_common::Validatable;
use iggy_common::delete_consumer_offset::DeleteConsumerOffset;
use iggy_common::get_consumer_offset::GetConsumerOffset;
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use send_wrapper::SendWrapper;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-offsets",
            get(get_consumer_offset).put(store_consumer_offset),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-offsets/{consumer_id}",
            delete(delete_consumer_offset),
        )
        .with_state(state)
}

#[debug_handler]
async fn get_consumer_offset(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    query: Query<GetConsumerOffset>,
) -> Result<Json<ConsumerOffsetInfo>, CustomError> {
    let mut query = query;
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;
    let consumer = Consumer::new(query.0.consumer.id);
    let session = SendWrapper::new(Session::stateless(identity.user_id, identity.ip_address));
    let Ok(offset) = state
        .shard
        .get_consumer_offset(
            &session,
            consumer,
            &query.0.stream_id,
            &query.0.topic_id,
            query.0.partition_id,
        )
        .await
    else {
        return Err(CustomError::ResourceNotFound);
    };

    let Some(offset) = offset else {
        return Err(CustomError::ResourceNotFound);
    };

    Ok(Json(offset))
}

#[debug_handler]
async fn store_consumer_offset(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut command: Json<StoreConsumerOffset>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;
    let session = SendWrapper::new(Session::stateless(identity.user_id, identity.ip_address));
    let consumer = Consumer::new(command.0.consumer.id);
    state.shard
        .store_consumer_offset(
            &session,
            consumer,
            &command.0.stream_id,
            &command.0.topic_id,
            command.0.partition_id,
            command.0.offset,
        )
        .await
        .with_error(|error| format!("{COMPONENT} (error: {error}) - failed to store consumer offset, stream ID: {}, topic ID: {}, partition ID: {:?}", stream_id, topic_id, command.0.partition_id))?;
    Ok(StatusCode::NO_CONTENT)
}

#[debug_handler]
async fn delete_consumer_offset(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id, consumer_id)): Path<(String, String, String)>,
    query: Query<DeleteConsumerOffset>,
) -> Result<StatusCode, CustomError> {
    let consumer = Consumer::new(consumer_id.try_into()?);
    let session = SendWrapper::new(Session::stateless(identity.user_id, identity.ip_address));
    state
        .shard
        .delete_consumer_offset(
            &session,
            consumer,
            &query.stream_id,
            &query.topic_id,
            query.partition_id,
        )
        .await
        .with_error(|error| format!("{COMPONENT} (error: {error}) - failed to delete consumer offset, stream ID: {}, topic ID: {}, partition ID: {:?}", stream_id, topic_id, query.partition_id))?;
    Ok(StatusCode::NO_CONTENT)
}
