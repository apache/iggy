use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::shared::AppState;
use crate::http::COMPONENT;
use crate::streaming::session::Session;
use crate::streaming::systems::messages::PollingArgs;
use crate::streaming::utils::random_id;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use error_set::ErrContext;
use iggy::consumer::Consumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollMessages;
use iggy::messages::send_messages::SendMessages;
use iggy::validatable::Validatable;
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

async fn poll_messages(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<PollMessages>,
) -> Result<Json<()>, CustomError> {
    /*
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;

    let consumer = Consumer::new(query.0.consumer.id);
    let system = state.system.read().await;
    let polled_messages = system
        .poll_messages(
            &Session::stateless(identity.user_id, identity.ip_address),
            &consumer,
            &query.0.stream_id,
            &query.0.topic_id,
            query.0.partition_id,
            PollingArgs::new(query.0.strategy, query.0.count, query.0.auto_commit),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to poll messages, stream ID: {}, topic ID: {}, partition ID: {:?}",
                stream_id, topic_id, query.0.partition_id
            )
        })?;
    Ok(Json(polled_messages))
    */
    todo!()
}

async fn send_messages(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<SendMessages>,
) -> Result<StatusCode, CustomError> {
    //TODO: Fix me
    /*
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.partitioning.length = command.partitioning.value.len() as u8;
    command.messages.iter_mut().for_each(|msg| {
        if msg.id == 0 {
            msg.id = random_id::get_uuid();
        }
    });
    command.validate()?;

    let messages = command.messages;
    let command_stream_id = command.stream_id;
    let command_topic_id = command.topic_id;
    let partitioning = command.partitioning;
    let system = state.system.read().await;
    // TODO(haze): Add confirmation level after testing is complete
    system
        .append_messages(
            &Session::stateless(identity.user_id, identity.ip_address),
            command_stream_id,
            command_topic_id,
            partitioning,
            messages,
            None,
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to append messages, stream ID: {}, topic ID: {}",
                stream_id, topic_id
            )
        })?;
    Ok(StatusCode::CREATED)
    */
    todo!()
}

#[instrument(skip_all, name = "trace_flush_unsaved_buffer", fields(iggy_user_id = identity.user_id, iggy_stream_id = stream_id, iggy_topic_id = topic_id, iggy_partition_id = partition_id, iggy_fsync = fsync))]
async fn flush_unsaved_buffer(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id, partition_id, fsync)): Path<(String, String, u32, bool)>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let system = state.system.read().await;
    system
        .flush_unsaved_buffer(
            &Session::stateless(identity.user_id, identity.ip_address),
            stream_id,
            topic_id,
            partition_id,
            fsync,
        )
        .await?;
    Ok(StatusCode::OK)
}
