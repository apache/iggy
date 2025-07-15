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

use super::{
    config::map_connector_config,
    error::ApiError,
    models::{SinkDetailsResponse, SinkInfoResponse, TransformResponse},
};
use crate::{configs::ConfigFormat, context::RuntimeContext, error::RuntimeError};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use serde::Deserialize;
use std::sync::Arc;

pub fn router(state: Arc<RuntimeContext>) -> Router {
    Router::new()
        .route("/sinks", get(get_sinks))
        .route("/sinks/{key}", get(get_sink))
        .route("/sinks/{key}/config", get(get_sink_config))
        .route("/sinks/{key}/transforms", get(get_sink_transforms))
        .with_state(state)
}

async fn get_sinks(
    State(context): State<Arc<RuntimeContext>>,
) -> Result<Json<Vec<SinkInfoResponse>>, ApiError> {
    let sinks = context
        .sinks
        .get_all()
        .await
        .into_iter()
        .map(|sink| sink.into())
        .collect::<Vec<_>>();
    Ok(Json(sinks))
}

async fn get_sink(
    State(context): State<Arc<RuntimeContext>>,
    Path(key): Path<String>,
) -> Result<Json<SinkDetailsResponse>, ApiError> {
    let Some(sink) = context.sinks.get(&key).await else {
        return Err(ApiError::Error(RuntimeError::SinkNotFound(key)));
    };
    let sink = sink.lock().await;
    Ok(Json(SinkDetailsResponse {
        info: sink.info.clone().into(),
        streams: sink.streams.to_vec(),
    }))
}

async fn get_sink_config(
    State(context): State<Arc<RuntimeContext>>,
    Path(key): Path<String>,
    Query(query): Query<GetSinkConfig>,
) -> Result<impl IntoResponse, ApiError> {
    let Some(sink) = context.sinks.get(&key).await else {
        return Err(ApiError::Error(RuntimeError::SinkNotFound(key)));
    };
    let sink = sink.lock().await;
    let Some(config) = sink.config.as_ref() else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let format = query
        .format
        .unwrap_or(sink.info.config_format.unwrap_or_default());
    let (content_type, config) = map_connector_config(config, format)?;
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, content_type);
    Ok((headers, config).into_response())
}

#[derive(Debug, Deserialize)]
struct GetSinkConfig {
    format: Option<ConfigFormat>,
}

async fn get_sink_transforms(
    State(context): State<Arc<RuntimeContext>>,
    Path(key): Path<String>,
) -> Result<Json<Vec<TransformResponse>>, ApiError> {
    let Some(sink) = context.sinks.get(&key).await else {
        return Err(ApiError::Error(RuntimeError::SinkNotFound(key)));
    };
    let sink = sink.lock().await;
    let Some(transforms) = sink.transforms.as_ref() else {
        return Ok(Json(vec![]));
    };

    Ok(Json(
        transforms
            .transforms
            .iter()
            .map(|(r#type, config)| TransformResponse {
                r#type: *r#type,
                config: config.clone(),
            })
            .collect(),
    ))
}
