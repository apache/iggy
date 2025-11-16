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
use crate::slab::traits_ext::{EntityComponentSystem, EntityMarker, IntoComponents};
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

    let numeric_topic_id = state.shard.shard().streams.with_topic_by_id(
        &identifier_stream_id,
        &identifier_topic_id,
        crate::streaming::topics::helpers::get_topic_id(),
    );
    let numeric_stream_id = state.shard.shard().streams.with_stream_by_id(
        &identifier_stream_id,
        crate::streaming::streams::helpers::get_stream_id(),
    );

    state
        .shard
        .shard()
        .permissioner
        .borrow()
        .get_consumer_group(session.get_user_id(), numeric_stream_id, numeric_topic_id)?;

    let consumer_group = state.shard.shard().streams.with_consumer_group_by_id(
        &identifier_stream_id,
        &identifier_topic_id,
        &identifier_group_id,
        |(root, members)| mapper::map_consumer_group(root, members),
    );

    Ok(Json(consumer_group))
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

    let numeric_topic_id = state.shard.shard().streams.with_topic_by_id(
        &identifier_stream_id,
        &identifier_topic_id,
        crate::streaming::topics::helpers::get_topic_id(),
    );
    let numeric_stream_id = state.shard.shard().streams.with_stream_by_id(
        &identifier_stream_id,
        crate::streaming::streams::helpers::get_stream_id(),
    );

    state
        .shard
        .shard()
        .permissioner
        .borrow()
        .get_consumer_groups(session.get_user_id(), numeric_stream_id, numeric_topic_id)?;

    let consumer_groups = state.shard.shard().streams.with_consumer_groups(
        &identifier_stream_id,
        &identifier_topic_id,
        |cgs| {
            cgs.with_components(|cgs| {
                let (roots, members) = cgs.into_components();
                mapper::map_consumer_groups(roots, members)
            })
        },
    );

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
    .with_error(|error| format!("{COMPONENT} (error: {error}) - failed to create consumer group, stream ID: {}, topic ID: {}, name: {}", stream_id, topic_id, command.name))?;

    let group_id = consumer_group.id();

    // Send event for consumer group creation
    {
        let broadcast_future = SendWrapper::new(async {
            use crate::shard::transmission::event::ShardEvent;
            let event = ShardEvent::CreatedConsumerGroup {
                stream_id: command.stream_id.clone(),
                topic_id: command.topic_id.clone(),
                cg: consumer_group.clone(),
            };
            let _responses = state
                .shard
                .shard()
                .broadcast_event_to_all_shards(event)
                .await;
        });
        broadcast_future.await;
    }

    // Get the created consumer group details
    let group_id_identifier = Identifier::numeric(group_id as u32).unwrap();
    let consumer_group_details = state.shard.shard().streams.with_consumer_group_by_id(
        &command.stream_id,
        &command.topic_id,
        &group_id_identifier,
        |(root, members)| mapper::map_consumer_group(root, members),
    );

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

        // Delete using the new API
        let consumer_group = state.shard.shard().delete_consumer_group(
            &session,
            &identifier_stream_id,
            &identifier_topic_id,
            &identifier_group_id
        )
        .with_error(|error| format!("{COMPONENT} (error: {error}) - failed to delete consumer group with ID: {group_id} for topic with ID: {topic_id} in stream with ID: {stream_id}"))?;

        let cg_id = consumer_group.id();

        // Remove all consumer group members from ClientManager
        let stream_id_usize = state.shard.shard().streams.with_stream_by_id(
            &identifier_stream_id,
            crate::streaming::streams::helpers::get_stream_id(),
        );
        let topic_id_usize = state.shard.shard().streams.with_topic_by_id(
            &identifier_stream_id,
            &identifier_topic_id,
            crate::streaming::topics::helpers::get_topic_id(),
        );

        // TODO: Tech debt, repeated code from `delete_consumer_group_handler.rs`
        // Get members from the deleted consumer group and make them leave
        let slab = consumer_group.members().inner().shared_get();
        for (_, member) in slab.iter() {
            if let Err(err) = state.shard.shard().client_manager.leave_consumer_group(
                member.client_id,
                stream_id_usize,
                topic_id_usize,
                cg_id,
            ) {
                tracing::warn!(
                    "{COMPONENT} (error: {err}) - failed to make client leave consumer group for client ID: {}, group ID: {}",
                    member.client_id,
                    cg_id
                );
            }
        }

        let cg_id_spez = ConsumerGroupId(cg_id);
        // Clean up consumer group offsets from all partitions using the specialized method
        let partition_ids = consumer_group.partitions();
        state.shard.shard().delete_consumer_group_offsets(
            cg_id_spez,
            &identifier_stream_id,
            &identifier_topic_id,
            partition_ids,
        ).await.with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to delete consumer group offsets for group ID: {} in stream: {}, topic: {}",
                cg_id_spez,
                identifier_stream_id,
                identifier_topic_id
            )
        })?;

        // Send event for consumer group deletion
        {
            let broadcast_future = SendWrapper::new(async {
                use crate::shard::transmission::event::ShardEvent;
                let event = ShardEvent::DeletedConsumerGroup {
                    id: cg_id,
                    stream_id: identifier_stream_id.clone(),
                    topic_id: identifier_topic_id.clone(),
                    group_id: identifier_group_id.clone(),
                };
                let _responses = state
                    .shard
                    .shard()
                    .broadcast_event_to_all_shards(event)
                    .await;
            });
            broadcast_future.await;
        }

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
