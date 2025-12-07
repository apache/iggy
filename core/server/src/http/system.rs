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

use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    body::Body,
    debug_handler,
    extract::{Path, State},
    http::{HeaderMap, header},
    response::IntoResponse,
    routing::{get, post},
};
use bytes::Bytes;
use chrono::Local;
use err_trail::ErrContext;
use iggy_common::{
    ClientInfo, ClientInfoDetails, ClusterMetadata, Stats, Validatable, get_snapshot::GetSnapshot,
};
use send_wrapper::SendWrapper;

use crate::{
    configs::http::HttpMetricsConfig,
    http::{
        COMPONENT, error::CustomError, jwt::json_web_token::Identity, mapper, shared::AppState,
    },
    streaming::session::Session,
};

const NAME: &str = "Iggy API";
const PONG: &str = "pong";

pub fn router(state: Arc<AppState>, metrics_config: &HttpMetricsConfig) -> Router {
    let mut router = Router::new()
        .route("/", get(|| async { NAME }))
        .route("/ping", get(|| async { PONG }))
        .route("/stats", get(get_stats))
        .route("/cluster/metadata", get(get_cluster_metadata))
        .route("/clients", get(get_clients))
        .route("/clients/{client_id}", get(get_client))
        .route("/snapshot", post(get_snapshot));
    if metrics_config.enabled {
        router = router.route(&metrics_config.endpoint, get(get_metrics));
    }

    router.with_state(state)
}

#[debug_handler]
async fn get_metrics(State(state): State<Arc<AppState>>) -> Result<String, CustomError> {
    let metrics_formatted_output = state.shard.shard().metrics.get_formatted_output();
    Ok(metrics_formatted_output)
}

#[debug_handler]
async fn get_stats(State(state): State<Arc<AppState>>) -> Result<Json<Stats>, CustomError> {
    let stats_future = SendWrapper::new(state.shard.shard().get_stats());
    let stats = stats_future
        .await
        .with_error(|error| format!("{COMPONENT} (error: {error}) - failed to get stats"))?;
    Ok(Json(stats))
}

async fn get_cluster_metadata(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<ClusterMetadata>, CustomError> {
    let shard = state.shard.shard();
    let session = Session::stateless(identity.user_id, identity.ip_address);
    let cluster_metadata = shard.get_cluster_metadata(&session).with_error(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to get cluster metadata, user ID: {}",
            identity.user_id
        )
    })?;
    Ok(Json(cluster_metadata))
}

async fn get_client(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(client_id): Path<u32>,
) -> Result<Json<ClientInfoDetails>, CustomError> {
    let Ok(client) = state
        .shard
        .shard()
        .get_client(
            &Session::stateless(identity.user_id, identity.ip_address),
            client_id,
        )
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to get client, user ID: {}",
                identity.user_id
            )
        })
    else {
        return Err(CustomError::ResourceNotFound);
    };
    let Some(client) = client else {
        return Err(CustomError::ResourceNotFound);
    };

    let client = mapper::map_client(&client);
    Ok(Json(client))
}

#[debug_handler]
async fn get_clients(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<ClientInfo>>, CustomError> {
    let clients = state
        .shard
        .shard()
        .get_clients(&Session::stateless(identity.user_id, identity.ip_address))
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to get clients, user ID: {}",
                identity.user_id
            )
        })?;
    let clients = mapper::map_clients(&clients);
    Ok(Json(clients))
}

#[debug_handler]
async fn get_snapshot(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<GetSnapshot>,
) -> Result<impl IntoResponse, CustomError> {
    command.validate()?;

    let session = Session::stateless(identity.user_id, identity.ip_address);
    let snapshot_future = SendWrapper::new(state.shard.shard().get_snapshot(
        &session,
        command.compression,
        &command.snapshot_types,
    ));

    let snapshot = snapshot_future
        .await
        .with_error(|error| format!("{COMPONENT} (error: {error}) - failed to get snapshot"))?;

    let zip_data = Bytes::from(snapshot.0);
    let filename = format!("iggy_snapshot_{}.zip", Local::now().format("%Y%m%d_%H%M%S"));

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/zip"),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        header::HeaderValue::from_str(&format!("attachment; filename=\"{filename}\"")).unwrap(),
    );
    Ok((headers, Body::from(zip_data)))
}
