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
use crate::streaming::polling_consumer::ConsumerGroupId;
use crate::streaming::session::Session;
use axum::debug_handler;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::Validatable;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::delete_consumer_group::DeleteConsumerGroup;
use iggy_common::{ConsumerGroup, ConsumerGroupDetails, IggyError};
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

    let session = Session::stateless(identity.user_id, identity.ip_address);

    // Check permissions and existence
    state.shard.shard().ensure_authenticated(&session)?;
    let exists = state
        .shard
        .shard()
        .ensure_consumer_group_exists(
            &identifier_stream_id,
            &identifier_topic_id,
            &identifier_group_id,
        )
        .is_ok();
    if !exists {
        return Err(CustomError::ResourceNotFound);
    }

    let numeric_stream_id = state
        .shard
        .shard()
        .shared_metadata
        .get_stream_id(&identifier_stream_id)
        .ok_or(CustomError::ResourceNotFound)?;
    let numeric_topic_id = state
        .shard
        .shard()
        .shared_metadata
        .get_topic_id(numeric_stream_id, &identifier_topic_id)
        .ok_or(CustomError::ResourceNotFound)?;

    state.shard.shard().permissioner.get_consumer_group(
        session.get_user_id(),
        numeric_stream_id,
        numeric_topic_id,
    )?;

    // Get consumer group from SharedMetadata
    let consumer_group = state
        .shard
        .shard()
        .shared_metadata
        .get_consumer_group(
            &identifier_stream_id,
            &identifier_topic_id,
            &identifier_group_id,
        )
        .ok_or(CustomError::ResourceNotFound)?;

    let details = mapper::map_consumer_group_details_from_metadata(&consumer_group);
    Ok(Json(details))
}

async fn get_consumer_groups(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<Json<Vec<ConsumerGroup>>, CustomError> {
    let identifier_stream_id = Identifier::from_str_value(&stream_id)?;
    let identifier_topic_id = Identifier::from_str_value(&topic_id)?;

    let session = Session::stateless(identity.user_id, identity.ip_address);

    // Check permissions and existence
    state.shard.shard().ensure_authenticated(&session)?;
    state
        .shard
        .shard()
        .ensure_topic_exists(&identifier_stream_id, &identifier_topic_id)?;

    let numeric_stream_id = state
        .shard
        .shard()
        .shared_metadata
        .get_stream_id(&identifier_stream_id)
        .ok_or(CustomError::ResourceNotFound)?;
    let numeric_topic_id = state
        .shard
        .shard()
        .shared_metadata
        .get_topic_id(numeric_stream_id, &identifier_topic_id)
        .ok_or(CustomError::ResourceNotFound)?;

    state.shard.shard().permissioner.get_consumer_groups(
        session.get_user_id(),
        numeric_stream_id,
        numeric_topic_id,
    )?;

    // Get consumer groups from SharedMetadata
    let snapshot = state.shard.shard().shared_metadata.load();
    let stream_meta = snapshot
        .streams
        .get(&numeric_stream_id)
        .ok_or(CustomError::ResourceNotFound)?;
    let topic_meta = stream_meta
        .get_topic(numeric_topic_id)
        .ok_or(CustomError::ResourceNotFound)?;

    let groups: Vec<_> = topic_meta.consumer_groups.values().cloned().collect();
    drop(snapshot);

    let consumer_groups = mapper::map_consumer_groups_from_metadata(&groups);
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

    let session = Session::stateless(identity.user_id, identity.ip_address);

    // Create consumer group using the new API
    let consumer_group = state.shard.shard().create_consumer_group(
        &session,
        &command.stream_id,
        &command.topic_id,
        command.name.clone(),
    )
    .error(|e: &IggyError| format!("{COMPONENT} (error: {e}) - failed to create consumer group, stream ID: {}, topic ID: {}, name: {}", stream_id, topic_id, command.name))?;

    let group_id = consumer_group.id;

    // Map the created consumer group
    let consumer_group_details = mapper::map_consumer_group_details_from_metadata(&consumer_group);

    // Apply state change
    let entry_command = EntryCommand::CreateConsumerGroup(CreateConsumerGroupWithId {
        group_id: group_id as u32,
        command,
    });
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

    let result = SendWrapper::new(async move {
        let session = Session::stateless(identity.user_id, identity.ip_address);

        // Get stream and topic IDs
        let stream_id_usize = state
            .shard
            .shard()
            .shared_metadata
            .get_stream_id(&identifier_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(identifier_stream_id.clone()))?;
        let topic_id_usize = state
            .shard
            .shard()
            .shared_metadata
            .get_topic_id(stream_id_usize, &identifier_topic_id)
            .ok_or_else(|| {
                IggyError::TopicIdNotFound(
                    identifier_topic_id.clone(),
                    identifier_stream_id.clone(),
                )
            })?;

        // Get consumer group metadata before deletion for cleanup
        let consumer_group = state
            .shard
            .shard()
            .shared_metadata
            .get_consumer_group(
                &identifier_stream_id,
                &identifier_topic_id,
                &identifier_group_id,
            )
            .ok_or_else(|| {
                IggyError::ConsumerGroupIdNotFound(
                    identifier_group_id.clone(),
                    identifier_topic_id.clone(),
                )
            })?;

        let cg_id = consumer_group.id;
        let partition_ids: Vec<usize> = consumer_group.partitions.clone();
        let member_client_ids: Vec<u32> = consumer_group.members.keys().copied().collect();

        // Delete using the new API
        state.shard.shard().delete_consumer_group(
            &session,
            &identifier_stream_id,
            &identifier_topic_id,
            &identifier_group_id
        )
        .error(|e: &IggyError| format!("{COMPONENT} (error: {e}) - failed to delete consumer group with ID: {group_id} for topic with ID: {topic_id} in stream with ID: {stream_id}"))?;

        // Remove all consumer group members from ClientManager
        for client_id in member_client_ids {
            if let Err(err) = state.shard.shard().client_manager.leave_consumer_group(
                client_id,
                stream_id_usize,
                topic_id_usize,
                cg_id,
            ) {
                tracing::warn!(
                    "{COMPONENT} (error: {err}) - failed to make client leave consumer group for client ID: {}, group ID: {}",
                    client_id,
                    cg_id
                );
            }
        }

        let cg_id_spez = ConsumerGroupId(cg_id);
        // Clean up consumer group offsets from all partitions
        state.shard.shard().delete_consumer_group_offsets(
            cg_id_spez,
            &identifier_stream_id,
            &identifier_topic_id,
            &partition_ids,
        ).await.error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to delete consumer group offsets for group ID: {} in stream: {}, topic: {}",
                cg_id_spez,
                identifier_stream_id,
                identifier_topic_id
            )
        })?;

        // Apply state change
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

        Ok::<StatusCode, CustomError>(StatusCode::NO_CONTENT)
    });

    result.await
}
