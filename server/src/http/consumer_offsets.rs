use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::shared::AppState;
use crate::http::COMPONENT;
use crate::streaming::session::Session;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Extension, Json, Router};
use error_set::ErrContext;
use iggy::consumer::Consumer;
use iggy::consumer_offsets::delete_consumer_offset::DeleteConsumerOffset;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::identifier::Identifier;
use iggy::models::consumer_offset_info::ConsumerOffsetInfo;
use iggy::validatable::Validatable;
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

async fn get_consumer_offset(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut query: Query<GetConsumerOffset>,
) -> Result<Json<ConsumerOffsetInfo>, CustomError> {
    query.stream_id = Identifier::from_str_value(&stream_id)?;
    query.topic_id = Identifier::from_str_value(&topic_id)?;
    query.validate()?;
    let consumer = Consumer::new(query.0.consumer.id);
    let system = state.system.read().await;
    let offset = system
        .get_consumer_offset(
            &Session::stateless(identity.user_id, identity.ip_address),
            &consumer,
            &query.0.stream_id,
            &query.0.topic_id,
            query.0.partition_id,
        )
        .await
        .with_error_context(|_| format!("{COMPONENT} - failed to get consumer offset, stream ID: {}, topic ID: {}, patition ID: {:?}", stream_id, topic_id, query.0.partition_id));
    if offset.is_err() {
        return Err(CustomError::ResourceNotFound);
    }

    let Some(offset) = offset? else {
        return Err(CustomError::ResourceNotFound);
    };

    Ok(Json(offset))
}

async fn store_consumer_offset(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    mut command: Json<StoreConsumerOffset>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;
    let consumer = Consumer::new(command.0.consumer.id);
    let system = state.system.read().await;
    system
        .store_consumer_offset(
            &Session::stateless(identity.user_id, identity.ip_address),
            consumer,
            &command.0.stream_id,
            &command.0.topic_id,
            command.0.partition_id,
            command.0.offset,
        )
        .await
        .with_error_context(|_| format!("{COMPONENT} - failed to store consumer offset, stream ID: {}, topic ID: {}, partition ID: {:?}", stream_id, topic_id, command.0.partition_id))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_consumer_offset(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id, consumer_id)): Path<(String, String, String)>,
    query: Query<DeleteConsumerOffset>,
) -> Result<StatusCode, CustomError> {
    let consumer = Consumer::new(consumer_id.try_into()?);
    let system = state.system.read().await;
    system
        .delete_consumer_offset(
            &Session::stateless(identity.user_id, identity.ip_address),
            consumer,
            &query.stream_id,
            &query.topic_id,
            query.partition_id,
        )
        .await
        .with_error_context(|_| format!("{COMPONENT} - failed to delete consumer offset, stream ID: {}, topic ID: {}, partition ID: {:?}", stream_id, topic_id, query.partition_id))?;
    Ok(StatusCode::NO_CONTENT)
}
