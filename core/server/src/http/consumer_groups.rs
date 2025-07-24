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
use crate::state::command::EntryCommand;
use crate::state::models::CreateConsumerGroupWithId;
use crate::streaming::session::Session;
use axum::debug_handler;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::Validatable;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::delete_consumer_group::DeleteConsumerGroup;
use iggy_common::{ConsumerGroup, ConsumerGroupDetails};
use send_wrapper::SendWrapper;
use std::sync::Arc;
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-groups",
            get(get_consumer_groups).post(create_consumer_group),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-groups/{group_id}",
            get(get_consumer_group).delete(delete_consumer_group),
        )
        .with_state(state)
}

async fn get_consumer_group(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id, group_id)): Path<(String, String, String)>,
) -> Result<Json<ConsumerGroupDetails>, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;
    let identifier_topic_id = Identifier::from_str_value(&topic_id)?;
    let identifier_group_id = Identifier::from_str_value(&group_id)?;
    let stream = state
        .shard
        .get_stream(&identifier_stream_id)
        .map_err(|_| CustomError::ResourceNotFound)?;
    let Ok(consumer_group) = state.shard.get_consumer_group(
        &Session::stateless(identity.user_id, identity.ip_address),
        &stream,
        &identifier_topic_id,
        &identifier_group_id,
    ) else {
        return Err(CustomError::ResourceNotFound);
    };
    let Some(consumer_group) = consumer_group else {
        return Err(CustomError::ResourceNotFound);
    };

    let consumer_group = mapper::map_consumer_group(&consumer_group);
    Ok(Json(consumer_group))
}

async fn get_consumer_groups(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<Json<Vec<ConsumerGroup>>, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;

    let consumer_groups = state.shard.get_consumer_groups(
        &Session::stateless(identity.user_id, identity.ip_address),
        &stream_id,
        &topic_id,
    )?;
    let consumer_groups = mapper::map_consumer_groups(&consumer_groups);
    Ok(Json(consumer_groups))
}

#[debug_handler]
#[instrument(skip_all, name = "trace_create_consumer_group", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id))]
async fn create_consumer_group(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<CreateConsumerGroup>,
) -> Result<(StatusCode, Json<ConsumerGroupDetails>), CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;

    let group_id_identifier = state.shard
        .create_consumer_group(
            &Session::stateless(identity.user_id, identity.ip_address),
            &command.stream_id,
            &command.topic_id,
            command.group_id,
            &command.name,
        )
        .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to create consumer group, stream ID: {}, topic ID: {}, group_id: {:?}", stream_id, topic_id, command.group_id))?;

    let group_id = group_id_identifier.get_u32_value().unwrap_or_default();

    let consumer_group_details = {
        let stream = state
            .shard
            .get_stream(&command.stream_id)
            .map_err(|_| CustomError::ResourceNotFound)?;

        let Ok(consumer_group) = state.shard.get_consumer_group(
            &Session::stateless(identity.user_id, identity.ip_address),
            &stream,
            &command.topic_id,
            &group_id_identifier,
        ) else {
            return Err(CustomError::ResourceNotFound);
        };

        let Some(consumer_group) = consumer_group else {
            return Err(CustomError::ResourceNotFound);
        };

        mapper::map_consumer_group(&consumer_group)
    };

    let entry_command =
        EntryCommand::CreateConsumerGroup(CreateConsumerGroupWithId { group_id, command });
    let state_future = SendWrapper::new(
        state
            .shard
            .shard()
            .state
            .apply(identity.user_id, &entry_command),
    );

    state_future.await?;

    Ok((StatusCode::CREATED, Json(consumer_group_details)))
}

#[debug_handler]
#[instrument(skip_all, name = "trace_delete_consumer_group", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id, iggy_group_id = group_id))]
async fn delete_consumer_group(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id, group_id)): Path<(String, String, String)>,
) -> Result<StatusCode, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;
    let identifier_topic_id = Identifier::from_str_value(&topic_id)?;
    let identifier_group_id = Identifier::from_str_value(&group_id)?;

    let session = Session::stateless(identity.user_id, identity.ip_address);
    let delete_future = SendWrapper::new(state.shard.delete_consumer_group(
        &session,
        &identifier_stream_id,
        &identifier_topic_id,
        &identifier_group_id,
    ));

    delete_future.await
        .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to delete consumer group with ID: {group_id} for topic with ID: {topic_id} in stream with ID: {stream_id}"))?;

    let entry_command = EntryCommand::DeleteConsumerGroup(DeleteConsumerGroup {
        stream_id: identifier_stream_id,
        topic_id: identifier_topic_id,
        group_id: identifier_group_id,
    });
    let state_future = SendWrapper::new(
        state
            .shard
            .shard()
            .state
            .apply(identity.user_id, &entry_command),
    );

    state_future.await?;

    Ok(StatusCode::NO_CONTENT)
}
