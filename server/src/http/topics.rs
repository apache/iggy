use crate::http::error::CustomError;
use crate::http::jwt::Identity;
use crate::http::mapper;
use crate::http::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use iggy::identifier::Identifier;
use iggy::models::topic::{Topic, TopicDetails};
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::update_topic::UpdateTopic;
use iggy::validatable::Validatable;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(get_topics).post(create_topic))
        .route(
            "/:topic_id",
            get(get_topic).put(update_topic).delete(delete_topic),
        )
        .with_state(state)
}

async fn get_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<Json<TopicDetails>, CustomError> {
    let system = state.system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    let stream = system.get_stream(&stream_id)?;
    let topic = stream.get_topic(&topic_id)?;
    system
        .permissioner
        .get_topic(identity.user_id, stream.stream_id, topic.topic_id)?;
    let topic = mapper::map_topic(topic).await;
    Ok(Json(topic))
}

async fn get_topics(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
) -> Result<Json<Vec<Topic>>, CustomError> {
    let system = state.system.read().await;
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let stream = system.get_stream(&stream_id)?;
    system
        .permissioner
        .get_topics(identity.user_id, stream.stream_id)?;
    let topics = stream.get_topics();
    let topics = mapper::map_topics(&topics).await;
    Ok(Json(topics))
}

async fn create_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(stream_id): Path<String>,
    Json(mut command): Json<CreateTopic>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.validate()?;
    {
        let system = state.system.read().await;
        let stream = system.get_stream(&command.stream_id)?;
        system
            .permissioner
            .create_topic(identity.user_id, stream.stream_id)?;
    }

    let mut system = state.system.write().await;
    system
        .create_topic(
            &command.stream_id,
            command.topic_id,
            &command.name,
            command.partitions_count,
            command.message_expiry,
        )
        .await?;
    Ok(StatusCode::CREATED)
}

async fn update_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(mut command): Json<UpdateTopic>,
) -> Result<StatusCode, CustomError> {
    command.stream_id = Identifier::from_str_value(&stream_id)?;
    command.topic_id = Identifier::from_str_value(&topic_id)?;
    command.validate()?;
    {
        let system = state.system.read().await;
        let stream = system.get_stream(&command.stream_id)?;
        let topic = stream.get_topic(&command.topic_id)?;
        system
            .permissioner
            .update_topic(identity.user_id, stream.stream_id, topic.topic_id)?;
    }

    let mut system = state.system.write().await;
    system
        .get_stream_mut(&command.stream_id)?
        .update_topic(&command.topic_id, &command.name, command.message_expiry)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_topic(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<StatusCode, CustomError> {
    let stream_id = Identifier::from_str_value(&stream_id)?;
    let topic_id = Identifier::from_str_value(&topic_id)?;
    {
        let system = state.system.read().await;
        let stream = system.get_stream(&stream_id)?;
        let topic = stream.get_topic(&topic_id)?;
        system
            .permissioner
            .delete_topic(identity.user_id, stream.stream_id, topic.topic_id)?;
    }

    let mut system = state.system.write().await;
    system.delete_topic(&stream_id, &topic_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
