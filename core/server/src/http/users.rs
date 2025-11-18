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
use crate::state::models::CreateUserWithId;
use crate::streaming::session::Session;
use crate::streaming::users::user::User;
use crate::streaming::utils::crypto;
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
use iggy_common::{UserInfo, UserInfoDetails};
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
    let Ok(user) = state.shard.shard().find_user(
        &Session::stateless(identity.user_id, identity.ip_address),
        &identifier_user_id,
    ) else {
        return Err(CustomError::ResourceNotFound);
    };
    let Some(user) = user else {
        return Err(CustomError::ResourceNotFound);
    };

    let user = mapper::map_user(&user);
    Ok(Json(user))
}

#[debug_handler]
async fn get_users(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<UserInfo>>, CustomError> {
    let session = SendWrapper::new(Session::stateless(identity.user_id, identity.ip_address));

    let users = {
        let future = SendWrapper::new(state.shard.shard().get_users(&session));
        future.await
    }
    .with_error(|error| {
        format!(
            "{COMPONENT} (error: {error}) - failed to get users, user ID: {}",
            identity.user_id
        )
    })?;

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

    let session = SendWrapper::new(Session::stateless(identity.user_id, identity.ip_address));

    let user = state
        .shard
        .shard()
        .create_user(
            &session,
            &command.username,
            &command.password,
            command.status,
            command.permissions.clone(),
        )
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to create user, username: {}",
                command.username
            )
        })?;

    let user_id = user.id;
    let response = Json(mapper::map_user(&user));

    // Send event for user creation
    {
        let broadcast_future = SendWrapper::new(async {
            use crate::shard::transmission::event::ShardEvent;
            let event = ShardEvent::CreatedUser {
                user_id,
                username: command.username.to_owned(),
                password: command.password.to_owned(),
                status: command.status,
                permissions: command.permissions.clone(),
            };
            let _responses = state
                .shard
                .shard()
                .broadcast_event_to_all_shards(event)
                .await;
        });
        broadcast_future.await;
    }

    {
        let username = command.username.clone();
        let entry_command = EntryCommand::CreateUser(CreateUserWithId {
            user_id,
            command: CreateUser {
                username: command.username.to_owned(),
                password: crypto::hash_password(&command.password),
                status: command.status,
                permissions: command.permissions.clone(),
            },
        });
        let future = SendWrapper::new(
            state
                .shard
                .shard()
                .state
                .apply(identity.user_id, &entry_command),
        );
        future.await.with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply create user, username: {}",
                username
            )
        })?;
    }

    Ok(response)
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

    let session = Session::stateless(identity.user_id, identity.ip_address);

    state
        .shard
        .shard()
        .update_user(
            &session,
            &command.user_id,
            command.username.clone(),
            command.status,
        )
        .with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to update user, user ID: {user_id}")
        })?;

    // Send event for user update
    {
        let broadcast_future = SendWrapper::new(async {
            use crate::shard::transmission::event::ShardEvent;
            let event = ShardEvent::UpdatedUser {
                user_id: command.user_id.clone(),
                username: command.username.clone(),
                status: command.status,
            };
            let _responses = state
                .shard
                .shard()
                .broadcast_event_to_all_shards(event)
                .await;
        });
        broadcast_future.await;
    }

    {
        let username = command.username.clone();
        let entry_command = EntryCommand::UpdateUser(command);
        let future = SendWrapper::new(
            state
                .shard
                .shard()
                .state
                .apply(identity.user_id, &entry_command),
        );
        future.await.with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply update user, username: {}",
                username.unwrap()
            )
        })?;
    }

    Ok(StatusCode::NO_CONTENT)
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

    let session = Session::stateless(identity.user_id, identity.ip_address);
    state
        .shard
        .shard()
        .update_permissions(&session, &command.user_id, command.permissions.clone())
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to update permissions, user ID: {user_id}"
            )
        })?;

    // Send event for permissions update
    {
        let broadcast_future = SendWrapper::new(async {
            use crate::shard::transmission::event::ShardEvent;
            let event = ShardEvent::UpdatedPermissions {
                user_id: command.user_id.clone(),
                permissions: command.permissions.clone(),
            };
            let _responses = state
                .shard
                .shard()
                .broadcast_event_to_all_shards(event)
                .await;
        });
        broadcast_future.await;
    }

    {
        let entry_command = EntryCommand::UpdatePermissions(command);
        let future = SendWrapper::new(
            state
                .shard
                .shard()
                .state
                .apply(identity.user_id, &entry_command),
        );
        future.await.with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply update permissions, user ID: {user_id}"
            )
        })?;
    }

    Ok(StatusCode::NO_CONTENT)
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

    let session = Session::stateless(identity.user_id, identity.ip_address);
    state
        .shard
        .shard()
        .change_password(
            &session,
            &command.user_id,
            &command.current_password,
            &command.new_password,
        )
        .with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to change password, user ID: {user_id}")
        })?;

    // Send event for password change
    {
        let broadcast_future = SendWrapper::new(async {
            use crate::shard::transmission::event::ShardEvent;
            let event = ShardEvent::ChangedPassword {
                user_id: command.user_id.clone(),
                current_password: command.current_password.clone(),
                new_password: command.new_password.clone(),
            };
            let _responses = state
                .shard
                .shard()
                .broadcast_event_to_all_shards(event)
                .await;
        });
        broadcast_future.await;
    }

    {
        let entry_command = EntryCommand::ChangePassword(command);
        let future = SendWrapper::new(
            state
                .shard
                .shard()
                .state
                .apply(identity.user_id, &entry_command),
        );
        future.await.with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply change password, user ID: {user_id}"
            )
        })?;
    }

    Ok(StatusCode::NO_CONTENT)
}

#[debug_handler]
#[instrument(skip_all, name = "trace_delete_user", fields(iggy_user_id = identity.user_id, iggy_deleted_user_id = user_id))]
async fn delete_user(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(user_id): Path<String>,
) -> Result<StatusCode, CustomError> {
    let identifier_user_id = Identifier::from_str_value(&user_id)?;

    let session = Session::stateless(identity.user_id, identity.ip_address);

    state
        .shard
        .shard()
        .delete_user(&session, &identifier_user_id)
        .with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to delete user with ID: {user_id}")
        })?;

    // Send event for user deletion
    {
        let broadcast_future = SendWrapper::new(async {
            use crate::shard::transmission::event::ShardEvent;
            let event = ShardEvent::DeletedUser {
                user_id: identifier_user_id.clone(),
            };
            let _responses = state
                .shard
                .shard()
                .broadcast_event_to_all_shards(event)
                .await;
        });
        broadcast_future.await;
    }

    {
        let entry_command = EntryCommand::DeleteUser(DeleteUser {
            user_id: identifier_user_id,
        });
        let future = SendWrapper::new(
            state
                .shard
                .shard()
                .state
                .apply(identity.user_id, &entry_command),
        );
        future.await.with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to apply delete user with ID: {user_id}")
        })?;
    }

    Ok(StatusCode::NO_CONTENT)
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
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to login, username: {}",
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
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to logout, user ID: {}",
                identity.user_id
            )
        })?;

    {
        let revoke_token_future = SendWrapper::new(
            state
                .jwt_manager
                .revoke_token(&identity.token_id, identity.token_expiry),
        );

        revoke_token_future.await.with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to revoke token, user ID: {}",
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
            .with_error(|error| format!("{COMPONENT} (error: {error}) - failed to refresh token"))?
    };

    Ok(Json(map_generated_access_token_to_identity_info(token)))
}

#[derive(Debug, Deserialize)]
struct RefreshToken {
    token: String,
}
