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
use crate::state::command::EntryCommand;
use crate::state::models::CreatePersonalAccessTokenWithHash;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::session::Session;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router, debug_handler};
use err_trail::ErrContext;
use iggy_common::IdentityInfo;
use iggy_common::Validatable;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use iggy_common::delete_personal_access_token::DeletePersonalAccessToken;
use iggy_common::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy_common::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use send_wrapper::SendWrapper;
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
        .get_personal_access_tokens(&Session::stateless(identity.user_id, identity.ip_address))
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to get personal access tokens, user ID: {}",
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
    let (_personal_access_token, token) = state.shard
            .create_personal_access_token(
                &Session::stateless(identity.user_id, identity.ip_address),
                &command.name,
                command.expiry,
            )
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to create personal access token, user ID: {}",
                    identity.user_id
                )
            })?;

    let token_hash = PersonalAccessToken::hash_token(&token);
    let command = EntryCommand::CreatePersonalAccessToken(CreatePersonalAccessTokenWithHash {
        command,
        hash: token_hash,
    });
    let state_future =
        SendWrapper::new(state.shard.shard().state.apply(identity.user_id, &command));

    state_future.await
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply create personal access token with hash, user ID: {}",
                identity.user_id
            )
        })?;
    Ok(Json(RawPersonalAccessToken { token }))
}

#[debug_handler]
#[instrument(skip_all, name = "trace_delete_personal_access_token", fields(iggy_user_id = identity.user_id))]
async fn delete_personal_access_token(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<StatusCode, CustomError> {
    state.shard
        .delete_personal_access_token(
            &Session::stateless(identity.user_id, identity.ip_address),
            &name,
        )
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to delete personal access token, user ID: {}",
                identity.user_id
            )
        })?;

    let command = EntryCommand::DeletePersonalAccessToken(DeletePersonalAccessToken { name });
    let state_future =
        SendWrapper::new(state.shard.shard().state.apply(identity.user_id, &command));

    state_future.await
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply delete personal access token, user ID: {}",
                identity.user_id
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
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
        .with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to login with personal access token")
        })?;
    let tokens = state.jwt_manager.generate(user.id)?;
    Ok(Json(map_generated_access_token_to_identity_info(tokens)))
}
