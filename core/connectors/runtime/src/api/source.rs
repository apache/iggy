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
use crate::api::models::SourceConfigResponse;
use crate::configs::connectors::{ConfigFormat, CreateSourceConfig};
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
        .route("/sources/{key}/transforms", get(get_source_transforms))
        .route(
            "/sources/{key}/configs",
            get(get_source_configs)
                .post(create_source_config)
                .delete(delete_source_config),
        )
        .route("/sources/{key}/configs/{version}", get(get_source_config))
        .route(
            "/sources/{key}/configs/plugin",
            get(get_source_plugin_config),
        )
        .route(
            "/sources/{key}/configs/active",
            get(get_source_active_config).put(update_source_active_config),
        )
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
        streams: source.config.streams.to_vec(),
    }))
}

async fn get_source_plugin_config(
    State(context): State<Arc<RuntimeContext>>,
    Path(key): Path<String>,
    Query(query): Query<GetSourceConfig>,
) -> Result<impl IntoResponse, ApiError> {
    let Some(source) = context.sources.get(&key).await else {
        return Err(ApiError::Error(RuntimeError::SourceNotFound(key)));
    };
    let source = source.lock().await;
    let Some(config) = source.config.plugin_config.as_ref() else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let format = query
        .format
        .unwrap_or(source.info.plugin_config_format.unwrap_or_default());
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
    let Some(transforms) = source.config.transforms.as_ref() else {
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

async fn get_source_configs(
    State(context): State<Arc<RuntimeContext>>,
    Path((key,)): Path<(String,)>,
) -> Result<Json<Vec<SourceConfigResponse>>, ApiError> {
    let active_config = context
        .sources
        .get_config(&key)
        .await
        .ok_or(ApiError::Error(RuntimeError::SourceNotFound(key.clone())))?;
    let configs = context.config_provider.get_source_configs(&key).await?;
    let configs = configs
        .into_iter()
        .map(|config| {
            let active = config.version == active_config.version;
            SourceConfigResponse { config, active }
        })
        .collect();
    Ok(Json(configs))
}

async fn create_source_config(
    State(context): State<Arc<RuntimeContext>>,
    Path((key,)): Path<(String,)>,
    Json(config): Json<CreateSourceConfig>,
) -> Result<Json<SourceConfigResponse>, ApiError> {
    let created_config = context
        .config_provider
        .create_source_config(&key, config.clone())
        .await?;

    Ok(Json(SourceConfigResponse {
        config: created_config,
        active: false,
    }))
}

async fn get_source_config(
    State(context): State<Arc<RuntimeContext>>,
    Path((key, version)): Path<(String, u64)>,
) -> Result<Json<SourceConfigResponse>, ApiError> {
    let active_config = context
        .sources
        .get_config(&key)
        .await
        .ok_or(ApiError::Error(RuntimeError::SourceNotFound(key.clone())))?;

    let config = context
        .config_provider
        .get_source_config(&key, Some(version))
        .await?;

    match config {
        Some(source_config) => {
            let active = source_config.version == active_config.version;
            Ok(Json(SourceConfigResponse {
                config: source_config,
                active,
            }))
        }
        None => Err(ApiError::Error(RuntimeError::SourceConfigNotFound(
            key, version,
        ))),
    }
}

async fn get_source_active_config(
    State(context): State<Arc<RuntimeContext>>,
    Path((key,)): Path<(String,)>,
) -> Result<Json<SourceConfigResponse>, ApiError> {
    let config = context
        .sources
        .get_config(&key)
        .await
        .ok_or(ApiError::Error(RuntimeError::SourceNotFound(key)))?;
    Ok(Json(SourceConfigResponse {
        config,
        active: true,
    }))
}

#[derive(Debug, Deserialize)]
struct UpdateSourceActiveConfig {
    version: u64,
}

async fn update_source_active_config(
    State(context): State<Arc<RuntimeContext>>,
    Path((key,)): Path<(String,)>,
    Json(update): Json<UpdateSourceActiveConfig>,
) -> Result<StatusCode, ApiError> {
    context
        .config_provider
        .set_active_source_version(&key, update.version)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct DeleteSourceConfig {
    version: Option<u64>,
}

async fn delete_source_config(
    State(context): State<Arc<RuntimeContext>>,
    Path((key,)): Path<(String,)>,
    Query(query): Query<DeleteSourceConfig>,
) -> Result<StatusCode, ApiError> {
    context
        .config_provider
        .delete_source_config(&key, query.version)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
