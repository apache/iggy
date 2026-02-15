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
use crate::streaming::session::Session;
use crate::streaming::users::user::User;
use ::iggy_common::change_password::ChangePassword;
use ::iggy_common::create_user::CreateUser;
use ::iggy_common::delete_user::DeleteUser;
use ::iggy_common::login_user::LoginUser;
use ::iggy_common::update_permissions::UpdatePermissions;
use ::iggy_common::update_user::UpdateUser;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post, put};
use axum::{Extension, Json, Router, debug_handler};
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::IdentityInfo;
use iggy_common::Validatable;
use iggy_common::{IggyError, UserInfo, UserInfoDetails};
use send_wrapper::SendWrapper;
use serde::Deserialize;
use std::sync::Arc;
use tracing::instrument;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/users", get(get_users).post(create_user))
        .route(
            "/users/{user_id}",
            get(get_user).put(update_user).delete(delete_user),
        )
        .route("/users/{user_id}/permissions", put(update_permissions))
        .route("/users/{user_id}/password", put(change_password))
        .route("/users/login", post(login_user))
        .route("/users/logout", delete(logout_user))
        .route("/users/refresh-token", post(refresh_token))
        .with_state(state)
}

#[debug_handler]
async fn get_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
) -> Result<Json<UserInfoDetails>, CustomError> {
    let identifier_user_id = Identifier::from_str_value(&user_id)?;
    let Ok(user) = state.shard.shard().find_user(&identifier_user_id) else {
        return Err(CustomError::ResourceNotFound);
    };
    let Some(user) = user else {
        return Err(CustomError::ResourceNotFound);
    };

    if user.id != identity.user_id {
        state
            .shard
            .shard()
            .metadata
            .perm_get_user(identity.user_id)?;
    }

    let user = mapper::map_user(&user);
    Ok(Json(user))
}

#[debug_handler]
async fn get_users(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<UserInfo>>, CustomError> {
    state
        .shard
        .shard()
        .metadata
        .perm_get_users(identity.user_id)?;

    let users = state.shard.shard().get_users();
    let user_refs: Vec<&User> = users.iter().collect();
    let users = mapper::map_users(&user_refs);
    Ok(Json(users))
}

#[debug_handler]
#[instrument(skip_all, name = "trace_create_user", fields(iggy_user_id = identity.user_id))]
async fn create_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<CreateUser>,
) -> Result<Json<UserInfoDetails>, CustomError> {
    command.validate()?;

    let request = ShardRequest::control_plane(ShardRequestPayload::CreateUserRequest {
        user_id: identity.user_id,
        command,
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::CreateUserResponse(user) => {
            let response = mapper::map_user(&user);
            Ok(Json(response))
        }
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected CreateUserResponse"),
    }
}

#[debug_handler]
#[instrument(skip_all, name = "trace_update_user", fields(iggy_user_id = identity.user_id, iggy_updated_user_id = user_id))]
async fn update_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
    Json(mut command): Json<UpdateUser>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;

    let request = ShardRequest::control_plane(ShardRequestPayload::UpdateUserRequest {
        user_id: identity.user_id,
        command,
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::UpdateUserResponse(_) => Ok(StatusCode::NO_CONTENT),
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected UpdateUserResponse"),
    }
}

#[debug_handler]
#[instrument(skip_all, name = "trace_update_permissions", fields(iggy_user_id = identity.user_id, iggy_updated_user_id = user_id))]
async fn update_permissions(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
    Json(mut command): Json<UpdatePermissions>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;

    let request = ShardRequest::control_plane(ShardRequestPayload::UpdatePermissionsRequest {
        user_id: identity.user_id,
        command,
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::UpdatePermissionsResponse => Ok(StatusCode::NO_CONTENT),
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected UpdatePermissionsResponse"),
    }
}

#[debug_handler]
#[instrument(skip_all, name = "trace_change_password", fields(iggy_user_id = identity.user_id, iggy_updated_user_id = user_id))]
async fn change_password(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
    Json(mut command): Json<ChangePassword>,
) -> Result<StatusCode, CustomError> {
    command.user_id = Identifier::from_str_value(&user_id)?;
    command.validate()?;

    let request = ShardRequest::control_plane(ShardRequestPayload::ChangePasswordRequest {
        user_id: identity.user_id,
        command,
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::ChangePasswordResponse => Ok(StatusCode::NO_CONTENT),
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected ChangePasswordResponse"),
    }
}

#[debug_handler]
#[instrument(skip_all, name = "trace_delete_user", fields(iggy_user_id = identity.user_id, iggy_deleted_user_id = user_id))]
async fn delete_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let user_id = Identifier::from_str_value(&user_id)?;

    let command = DeleteUser { user_id };
    let request = ShardRequest::control_plane(ShardRequestPayload::DeleteUserRequest {
        user_id: identity.user_id,
        command,
    });

    match state.shard.send_to_control_plane(request).await? {
        ShardResponse::DeleteUserResponse(_) => Ok(StatusCode::NO_CONTENT),
        ShardResponse::ErrorResponse(err) => Err(err.into()),
        _ => unreachable!("Expected DeleteUserResponse"),
    }
}

#[debug_handler]
#[instrument(skip_all, name = "trace_login_user")]
async fn login_user(
    State(state): State<Arc<AppState>>,
    Json(command): Json<LoginUser>,
) -> Result<Json<IdentityInfo>, CustomError> {
    command.validate()?;
    let user = state
        .shard
        .shard()
        .login_user(&command.username, &command.password, None)
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to login, username: {}",
                command.username
            )
        })?;
    let tokens = state.jwt_manager.generate(user.id)?;
    Ok(Json(map_generated_access_token_to_identity_info(tokens)))
}

#[debug_handler]
#[instrument(skip_all, name = "trace_logout_user", fields(iggy_user_id = identity.user_id))]
async fn logout_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<StatusCode, CustomError> {
    let session = Session::stateless(identity.user_id, identity.ip_address);
    state
        .shard
        .shard()
        .logout_user(&session)
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to logout, user ID: {}",
                identity.user_id
            )
        })?;

    {
        let revoke_token_future = SendWrapper::new(
            state
                .jwt_manager
                .revoke_token(&identity.token_id, identity.token_expiry),
        );

        revoke_token_future.await.error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to revoke token, user ID: {}",
                identity.user_id
            )
        })?;
    }

    Ok(StatusCode::NO_CONTENT)
}

#[debug_handler]
async fn refresh_token(
    State(state): State<Arc<AppState>>,
    Json(command): Json<RefreshToken>,
) -> Result<Json<IdentityInfo>, CustomError> {
    let token = {
        let refresh_token_future =
            SendWrapper::new(state.jwt_manager.refresh_token(&command.token));

        refresh_token_future
            .await
            .error(|e: &IggyError| format!("{COMPONENT} (error: {e}) - failed to refresh token"))?
    };

    Ok(Json(map_generated_access_token_to_identity_info(token)))
}

#[derive(Debug, Deserialize)]
struct RefreshToken {
    token: String,
}
