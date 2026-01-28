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
use crate::http::mapper::map_generated_access_token_to_identity_info;
use crate::http::shared::AppState;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardRequest, ShardRequestPayload};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router, debug_handler};
use err_trail::ErrContext;
use iggy_common::IdentityInfo;
use iggy_common::PersonalAccessTokenInfo;
use iggy_common::Validatable;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use iggy_common::delete_personal_access_token::DeletePersonalAccessToken;
use iggy_common::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy_common::{IggyError, RawPersonalAccessToken};
use std::sync::Arc;
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/personal-access-tokens",
            get(get_personal_access_tokens).post(create_personal_access_token),
        )
        .route(
            "/personal-access-tokens/{name}",
            delete(delete_personal_access_token),
        )
        .route(
            "/personal-access-tokens/login",
            post(login_with_personal_access_token),
        )
        .with_state(state)
}

#[debug_handler]
async fn get_personal_access_tokens(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<PersonalAccessTokenInfo>>, CustomError> {
    let personal_access_tokens = state
        .shard
        .shard()
        .get_personal_access_tokens(identity.user_id)
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to get personal access tokens, user ID: {}",
                identity.user_id
            )
        })?;
    let personal_access_tokens = mapper::map_personal_access_tokens(&personal_access_tokens);
    Ok(Json(personal_access_tokens))
}

#[debug_handler]
#[instrument(skip_all, name = "trace_create_personal_access_token", fields(iggy_user_id = identity.user_id))]
async fn create_personal_access_token(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<CreatePersonalAccessToken>,
) -> Result<Json<RawPersonalAccessToken>, CustomError> {
    command.validate()?;

    let request =
        ShardRequest::control_plane(ShardRequestPayload::CreatePersonalAccessTokenRequest {
            user_id: identity.user_id,
            command,
        });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::CreatePersonalAccessTokenResponse(_, token) => {
            Ok(Json(RawPersonalAccessToken { token }))
        }
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected CreatePersonalAccessTokenResponse"),
    }
}

#[debug_handler]
#[instrument(skip_all, name = "trace_delete_personal_access_token", fields(iggy_user_id = identity.user_id))]
async fn delete_personal_access_token(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<StatusCode, CustomError> {
    let command = DeletePersonalAccessToken { name };
    let request =
        ShardRequest::control_plane(ShardRequestPayload::DeletePersonalAccessTokenRequest {
            user_id: identity.user_id,
            command,
        });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::DeletePersonalAccessTokenResponse => Ok(StatusCode::NO_CONTENT),
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected DeletePersonalAccessTokenResponse"),
    }
}

#[instrument(skip_all, name = "trace_login_with_personal_access_token")]
async fn login_with_personal_access_token(
    State(state): State<Arc<AppState>>,
    Json(command): Json<LoginWithPersonalAccessToken>,
) -> Result<Json<IdentityInfo>, CustomError> {
    command.validate()?;
    let user = state
        .shard
        .shard()
        .login_with_personal_access_token(&command.token, None)
        .error(|e: &IggyError| {
            format!("{COMPONENT} (error: {e}) - failed to login with personal access token")
        })?;
    let tokens = state.jwt_manager.generate(user.id)?;
    Ok(Json(map_generated_access_token_to_identity_info(tokens)))
}
