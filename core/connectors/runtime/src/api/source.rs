/*
 * Licensed to the Apache Software Foundation (ASF) under one
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
    models::{SourceDetailsResponse, SourceInfoResponse, TransformResponse},
};
use crate::configs::connectors::ConfigFormat;
use crate::{context::RuntimeContext, error::RuntimeError};
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
        .route("/sources", get(get_sources))
        .route("/sources/{key}", get(get_source))
        .route("/sources/{key}/config", get(get_source_config))
        .route("/sources/{key}/transforms", get(get_source_transforms))
        .with_state(state)
}

async fn get_sources(
    State(context): State<Arc<RuntimeContext>>,
) -> Result<Json<Vec<SourceInfoResponse>>, ApiError> {
    let sources = context
        .sources
        .get_all()
        .await
        .into_iter()
        .map(|source| source.into())
        .collect::<Vec<_>>();
    Ok(Json(sources))
}

async fn get_source(
    State(context): State<Arc<RuntimeContext>>,
    Path(key): Path<String>,
) -> Result<Json<SourceDetailsResponse>, ApiError> {
    let Some(source) = context.sources.get(&key).await else {
        return Err(ApiError::Error(RuntimeError::SourceNotFound(key)));
    };
    let source = source.lock().await;
    Ok(Json(SourceDetailsResponse {
        info: source.info.clone().into(),
        streams: source.streams.to_vec(),
    }))
}

async fn get_source_config(
    State(context): State<Arc<RuntimeContext>>,
    Path(key): Path<String>,
    Query(query): Query<GetSourceConfig>,
) -> Result<impl IntoResponse, ApiError> {
    let Some(source) = context.sources.get(&key).await else {
        return Err(ApiError::Error(RuntimeError::SourceNotFound(key)));
    };
    let source = source.lock().await;
    let Some(config) = source.config.as_ref() else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let format = query
        .format
        .unwrap_or(source.info.config_format.unwrap_or_default());
    let (content_type, config) = map_connector_config(config, format)?;
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, content_type);
    Ok((headers, config).into_response())
}

#[derive(Debug, Deserialize)]
struct GetSourceConfig {
    format: Option<ConfigFormat>,
}

async fn get_source_transforms(
    State(context): State<Arc<RuntimeContext>>,
    Path(key): Path<String>,
) -> Result<Json<Vec<TransformResponse>>, ApiError> {
    let Some(source) = context.sources.get(&key).await else {
        return Err(ApiError::Error(RuntimeError::SourceNotFound(key)));
    };
    let source = source.lock().await;
    let Some(transforms) = source.transforms.as_ref() else {
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
