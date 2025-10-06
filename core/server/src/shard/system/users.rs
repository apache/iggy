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
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::Permissions;
use iggy_common::UserStatus;
use iggy_common::{IdKind, Identifier};
use std::cell::RefMut;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{error, warn};

static USER_ID: AtomicU32 = AtomicU32::new(1);
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
            self.permissioner.borrow().get_user(session_user_id).with_error_context(|error| {
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
        match user_id.kind {
            IdKind::Numeric => {
                let user = self
                    .users
                    .borrow()
                    .get(&user_id.get_u32_value()?)
                    .map(|user| user.clone());
                Ok(user)
            }
            IdKind::String => {
                let username = user_id.get_cow_str_value()?;
                let user = self
                    .users
                    .borrow()
                    .iter()
                    .find(|(_, user)| user.username == username)
                    .map(|(_, user)| user.clone());
                Ok(user)
            }
        }
    }

    pub fn get_user_mut(&self, user_id: &Identifier) -> Result<RefMut<'_, User>, IggyError> {
        match user_id.kind {
            IdKind::Numeric => {
                let user_id = user_id.get_u32_value()?;
                let users = self.users.borrow_mut();
                let exists = users.contains_key(&user_id);
                if !exists {
                    return Err(IggyError::ResourceNotFound(user_id.to_string()));
                }
                Ok(RefMut::map(users, |u| {
                    let user = u.get_mut(&user_id);
                    user.unwrap()
                }))
            }
            IdKind::String => {
                let username = user_id.get_cow_str_value()?;
                let users = self.users.borrow_mut();
                let exists = users.iter().any(|(_, user)| user.username == username);
                if !exists {
                    return Err(IggyError::ResourceNotFound(user_id.to_string()));
                }
                Ok(RefMut::map(users, |u| {
                    let user = u
                        .iter_mut()
                        .find(|(_, user)| user.username == username)
                        .map(|(_, user)| user);
                    user.unwrap()
                }))
            }
        }
    }

    pub async fn get_users(&self, session: &Session) -> Result<Vec<User>, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
        .borrow()
            .get_users(session.get_user_id())
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get users for user with id: {}",
                    session.get_user_id()
                )
            })?;
        Ok(self.users.borrow().values().cloned().collect())
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
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to create user for user with id: {}",
                    session.get_user_id()
                )
            })?;

        if self
            .users
            .borrow()
            .iter()
            .any(|(_, user)| user.username == username)
        {
            error!("User: {username} already exists.");
            return Err(IggyError::UserAlreadyExists);
        }

        if self.users.borrow().len() >= MAX_USERS {
            error!("Available users limit reached.");
            return Err(IggyError::UsersLimitReached);
        }

        // TODO: Tech debt, replace with Slab.
        USER_ID.fetch_add(1, Ordering::SeqCst);
        let current_user_id = USER_ID.load(Ordering::SeqCst);
        self.create_user_base(current_user_id, username, password, status, permissions)?;
        self.get_user(&current_user_id.try_into()?)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get user with id: {current_user_id}"
                )
            })
    }

    pub fn create_user_bypass_auth(
        &self,
        user_id: u32,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.create_user_base(user_id, username, password, status, permissions)?;
        Ok(())
    }

    fn create_user_base(
        &self,
        user_id: u32,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        let user = User::new(user_id, username, password, status, permissions.clone());
        self.permissioner
            .borrow_mut()
            .init_permissions_for_user(user_id, permissions);
        self.users.borrow_mut().insert(user.id, user);
        self.metrics.increment_users(1);
        Ok(())
    }

    pub fn delete_user(&self, session: &Session, user_id: &Identifier) -> Result<User, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .borrow()
            .delete_user(session.get_user_id())
            .with_error_context(|error| {
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
        let existing_user_id;
        let existing_username;
        {
            let user = self.get_user(user_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get user with id: {user_id}")
            })?;
            if user.is_root() {
                error!("Cannot delete the root user.");
                return Err(IggyError::CannotDeleteUser(user.id));
            }

            existing_user_id = user.id;
            existing_username = user.username.clone();
        }

        let user = self
            .users
            .borrow_mut()
            .remove(&existing_user_id)
            .ok_or(IggyError::ResourceNotFound(user_id.to_string()))?;
        self.permissioner
            .borrow_mut()
            .delete_permissions_for_user(existing_user_id);
        self.client_manager.borrow_mut()
            .delete_clients_for_user(existing_user_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to delete clients for user with ID: {existing_user_id}"
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
            .with_error_context(|error| {
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
        if let Some(username) = username.to_owned() {
            let user = self.get_user(user_id)?;
            let existing_user = self.get_user(&username.to_owned().try_into()?);
            if existing_user.is_ok() && existing_user.unwrap().id != user.id {
                error!("User: {username} already exists.");
                return Err(IggyError::UserAlreadyExists);
            }
        }

        let mut user = self.get_user_mut(user_id).with_error_context(|error| {
            format!("{COMPONENT} update user (error: {error}) - failed to get mutable reference to the user with id: {user_id}")
        })?;
        if let Some(username) = username {
            user.username = username;
        }

        if let Some(status) = status {
            user.status = status;
        }
        let cloned_user = user.clone();
        drop(user);

        Ok(cloned_user)
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
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to update permissions for user with id: {}", session.get_user_id()
                    )
                })?;
            let user: User = self.get_user(user_id).with_error_context(|error| {
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
        {
            let user: User = self.get_user(user_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get user with id: {user_id}")
            })?;

            self.permissioner
                .borrow_mut()
                .update_permissions_for_user(user.id, permissions.clone());
        }

        {
            let mut user = self.get_user_mut(user_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} update user permissions (error: {error}) - failed to get mutable reference to the user with id: {user_id}"
                )
            })?;
            user.permissions = permissions;
        }

        Ok(())
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
            let user = self.get_user(user_id).with_error_context(|error| {
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
        let mut user = self.get_user_mut(user_id).with_error_context(|error| {
            format!("{COMPONENT} change password (error: {error}) - failed to get mutable reference to the user with id: {user_id}")
        })?;
        if !crypto::verify_password(current_password, &user.password) {
            error!(
                "Invalid current password for user: {} with ID: {user_id}.",
                user.username
            );
            return Err(IggyError::InvalidCredentials);
        }

        user.password = crypto::hash_password(new_password);
        Ok(())
    }

    pub fn login_user_event(
        &self,
        client_id: u32,
        username: &str,
        password: &str,
    ) -> Result<(), IggyError> {
        let active_sessions = self.active_sessions.borrow();
        let session = active_sessions
            .iter()
            .find(|s| s.client_id == client_id)
            .expect(format!("At this point session for {}, should exist.", client_id).as_str());
        self.login_user_with_credentials(username, Some(password), Some(session))?;
        Ok(())
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
        let mut client_manager = self.client_manager.borrow_mut();
        client_manager
            .set_user_id(session.client_id, user.id)
            .with_error_context(|error| {
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
        let user_id = session.get_user_id();
        self.logout_user_base(user_id, client_id)?;
        Ok(())
    }

    fn logout_user_base(&self, user_id: u32, client_id: u32) -> Result<(), IggyError> {
        let user = self
            .get_user(&Identifier::numeric(user_id)?)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get user with id: {}",
                    user_id,
                )
            })?;
        if client_id > 0 {
            let mut client_manager = self.client_manager.borrow_mut();
            client_manager.clear_user_id(client_id)?;
        }
        Ok(())
    }
}
