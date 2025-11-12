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

use super::COMPONENT;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use crate::streaming::users::user::User;
use crate::streaming::utils::crypto;
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::Permissions;
use iggy_common::UserStatus;
use tracing::{error, warn};

const MAX_USERS: usize = u32::MAX as usize;

impl IggyShard {
    pub fn find_user(
        &self,
        session: &Session,
        user_id: &Identifier,
    ) -> Result<Option<User>, IggyError> {
        self.ensure_authenticated(session)?;
        let Some(user) = self.try_get_user(user_id)? else {
            return Ok(None);
        };

        let session_user_id = session.get_user_id();
        if user.id != session_user_id {
            self.permissioner.borrow().get_user(session_user_id).with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get user with ID: {user_id} for current user with ID: {session_user_id}"
                )
            })?;
        }

        Ok(Some(user))
    }

    pub fn get_user(&self, user_id: &Identifier) -> Result<User, IggyError> {
        self.try_get_user(user_id)?
            .ok_or(IggyError::ResourceNotFound(user_id.to_string()))
    }

    pub fn try_get_user(&self, user_id: &Identifier) -> Result<Option<User>, IggyError> {
        self.users.get_by_identifier(user_id)
    }

    pub async fn get_users(&self, session: &Session) -> Result<Vec<User>, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
        .borrow()
            .get_users(session.get_user_id())
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get users for user with id: {}",
                    session.get_user_id()
                )
            })?;
        Ok(self.users.values())
    }

    pub fn create_user(
        &self,
        session: &Session,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<User, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .borrow()
            .create_user(session.get_user_id())
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to create user for user with id: {}",
                    session.get_user_id()
                )
            })?;

        if self.users.username_exists(username) {
            error!("User: {username} already exists.");
            return Err(IggyError::UserAlreadyExists);
        }

        if self.users.len() >= MAX_USERS {
            error!("Available users limit reached.");
            return Err(IggyError::UsersLimitReached);
        }

        let user_id = self.create_user_base(username, password, status, permissions)?;
        self.get_user(&(user_id as u32).try_into()?)
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get user with id: {user_id}")
            })
    }

    pub fn create_user_bypass_auth(
        &self,
        expected_user_id: u32,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        let assigned_user_id = self.create_user_base(username, password, status, permissions)?;

        assert_eq!(
            assigned_user_id as u32, expected_user_id,
            "User ID mismatch: expected {}, got {}. This indicates shards are out of sync.",
            expected_user_id, assigned_user_id
        );

        Ok(())
    }

    fn create_user_base(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<usize, IggyError> {
        let user = User::new(0, username, password, status, permissions.clone());
        let user_id = self.users.insert(user);
        self.permissioner
            .borrow_mut()
            .init_permissions_for_user(user_id as u32, permissions);
        self.metrics.increment_users(1);
        Ok(user_id)
    }

    pub fn delete_user(&self, session: &Session, user_id: &Identifier) -> Result<User, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .borrow()
            .delete_user(session.get_user_id())
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to delete user for user with id: {}",
                    session.get_user_id()
                )
            })?;

        self.delete_user_base(user_id)
    }

    pub fn delete_user_bypass_auth(&self, user_id: &Identifier) -> Result<User, IggyError> {
        self.delete_user_base(user_id)
    }

    fn delete_user_base(&self, user_id: &Identifier) -> Result<User, IggyError> {
        let user = self.get_user(user_id).with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get user with id: {user_id}")
        })?;

        if user.is_root() {
            error!("Cannot delete the root user.");
            return Err(IggyError::CannotDeleteUser(user.id));
        }

        let user_slab_id = user.id as usize;
        let user_u32_id = user.id;

        let user = self
            .users
            .remove(user_slab_id)
            .ok_or(IggyError::ResourceNotFound(user_id.to_string()))?;
        self.permissioner
            .borrow_mut()
            .delete_permissions_for_user(user_u32_id);
        self.client_manager
            .delete_clients_for_user(user_u32_id)
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to delete clients for user with ID: {user_u32_id}"
                )
            })?;
        self.metrics.decrement_users(1);
        Ok(user)
    }

    pub fn update_user(
        &self,
        session: &Session,
        user_id: &Identifier,
        username: Option<String>,
        status: Option<UserStatus>,
    ) -> Result<User, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
        .borrow()
            .update_user(session.get_user_id())
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to update user for user with id: {}",
                    session.get_user_id()
                )
            })?;

        self.update_user_base(user_id, username, status)
    }

    pub fn update_user_bypass_auth(
        &self,
        user_id: &Identifier,
        username: Option<String>,
        status: Option<UserStatus>,
    ) -> Result<User, IggyError> {
        self.update_user_base(user_id, username, status)
    }

    fn update_user_base(
        &self,
        user_id: &Identifier,
        username: Option<String>,
        status: Option<UserStatus>,
    ) -> Result<User, IggyError> {
        let user = self.get_user(user_id)?;
        let numeric_user_id = Identifier::numeric(user.id).unwrap();

        if let Some(ref new_username) = username {
            let existing_user = self.get_user(&new_username.to_owned().try_into()?);
            if existing_user.is_ok() && existing_user.unwrap().id != user.id {
                error!("User: {new_username} already exists.");
                return Err(IggyError::UserAlreadyExists);
            }

            self.users.update_username(user_id, new_username.clone())?;
        }

        if let Some(status) = status {
            self.users.with_user_mut(&numeric_user_id, |user| {
                user.status = status;
            }).with_error(|error| {
                format!("{COMPONENT} update user (error: {error}) - failed to update user with id: {user_id}")
            })?;
        }

        self.get_user(&numeric_user_id)
            .with_error(|error| {
                format!("{COMPONENT} update user (error: {error}) - failed to get updated user with id: {user_id}")
            })
    }

    pub fn update_permissions(
        &self,
        session: &Session,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;

        {
            self.permissioner
            .borrow()
                .update_permissions(session.get_user_id())
                .with_error(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to update permissions for user with id: {}", session.get_user_id()
                    )
                })?;
            let user: User = self.get_user(user_id).with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get user with id: {user_id}")
            })?;
            if user.is_root() {
                error!("Cannot change the root user permissions.");
                return Err(IggyError::CannotChangePermissions(user.id));
            }
        }

        self.update_permissions_base(user_id, permissions)
    }

    pub fn update_permissions_bypass_auth(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.update_permissions_base(user_id, permissions)
    }

    fn update_permissions_base(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        let user: User = self.get_user(user_id).with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get user with id: {user_id}")
        })?;

        self.permissioner
            .borrow_mut()
            .update_permissions_for_user(user.id, permissions.clone());

        self.users.with_user_mut(user_id, |user| {
            user.permissions = permissions;
        }).with_error(|error| {
            format!(
                "{COMPONENT} update user permissions (error: {error}) - failed to update permissions for user with id: {user_id}"
            )
        })
    }

    pub fn change_password(
        &self,
        session: &Session,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;

        {
            let user = self.get_user(user_id).with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get user with id: {user_id}")
            })?;
            let session_user_id = session.get_user_id();
            if user.id != session_user_id {
                self.permissioner
                    .borrow()
                    .change_password(session_user_id)?;
            }
        }

        self.change_password_base(user_id, current_password, new_password)
    }

    pub fn change_password_bypass_auth(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        self.change_password_base(user_id, current_password, new_password)
    }

    fn change_password_base(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        self.users.with_user_mut(user_id, |user| {
            if !crypto::verify_password(current_password, &user.password) {
                error!(
                    "Invalid current password for user: {} with ID: {user_id}.",
                    user.username
                );
                return Err(IggyError::InvalidCredentials);
            }
            user.password = crypto::hash_password(new_password);
            Ok(())
        }).with_error(|error| {
            format!("{COMPONENT} change password (error: {error}) - failed to change password for user with id: {user_id}")
        })?
    }

    pub fn login_user(
        &self,
        username: &str,
        password: &str,
        session: Option<&Session>,
    ) -> Result<User, IggyError> {
        self.login_user_with_credentials(username, Some(password), session)
    }

    pub fn login_user_with_credentials(
        &self,
        username: &str,
        password: Option<&str>,
        session: Option<&Session>,
    ) -> Result<User, IggyError> {
        let user = match self.get_user(&username.try_into()?) {
            Ok(user) => user,
            Err(_) => {
                error!("Cannot login user: {username} (not found).");
                return Err(IggyError::InvalidCredentials);
            }
        };

        if !user.is_active() {
            warn!("User: {username} with ID: {} is inactive.", user.id);
            return Err(IggyError::UserInactive);
        }

        if let Some(password) = password
            && !crypto::verify_password(password, &user.password)
        {
            warn!(
                "Invalid password for user: {username} with ID: {}.",
                user.id
            );
            return Err(IggyError::InvalidCredentials);
        }

        if session.is_none() {
            return Ok(user);
        }

        let session = session.unwrap();
        if session.is_authenticated() {
            warn!(
                "User: {} with ID: {} was already authenticated, removing the previous session...",
                user.username,
                session.get_user_id()
            );
            self.logout_user(session)?;
        }
        session.set_user_id(user.id);
        self.client_manager
            .set_user_id(session.client_id, user.id)
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to set user_id to client, client ID: {}, user ID: {}",
                    session.client_id, user.id
                )
            })?;
        Ok(user)
    }

    pub fn logout_user(&self, session: &Session) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let client_id = session.client_id;
        self.logout_user_base(client_id)?;
        Ok(())
    }

    fn logout_user_base(&self, client_id: u32) -> Result<(), IggyError> {
        if client_id > 0 {
            self.client_manager.clear_user_id(client_id)?;
        }
        Ok(())
    }
}
