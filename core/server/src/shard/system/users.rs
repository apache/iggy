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
use crate::metadata::UserMeta;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use crate::streaming::users::user::User;
use crate::streaming::utils::crypto;
use dashmap::DashMap;
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::Permissions;
use iggy_common::UserStatus;
use std::sync::Arc;
use tracing::{error, warn};

const MAX_USERS: usize = u32::MAX as usize;

impl IggyShard {
    fn user_from_meta(&self, meta: &UserMeta) -> User {
        let pats = self.metadata.get_user_personal_access_tokens(meta.id);
        let pat_map = DashMap::new();
        for pat in pats {
            pat_map.insert(pat.token.clone(), pat);
        }
        User {
            id: meta.id,
            status: meta.status,
            username: meta.username.to_string(),
            password: meta.password_hash.to_string(),
            created_at: meta.created_at,
            permissions: meta.permissions.as_ref().map(|p| (**p).clone()),
            personal_access_tokens: pat_map,
        }
    }

    fn get_user_from_metadata(&self, identifier: &Identifier) -> Result<Option<User>, IggyError> {
        let user_id = match self.metadata.get_user_id(identifier) {
            Some(id) => id,
            None => return Ok(None),
        };

        Ok(self
            .metadata
            .get_user(user_id)
            .map(|meta| self.user_from_meta(&meta)))
    }

    pub fn find_user(&self, user_id: &Identifier) -> Result<Option<User>, IggyError> {
        self.try_get_user(user_id)
    }

    pub fn get_user(&self, user_id: &Identifier) -> Result<User, IggyError> {
        self.try_get_user(user_id)?
            .ok_or(IggyError::ResourceNotFound(user_id.to_string()))
    }

    pub fn try_get_user(&self, user_id: &Identifier) -> Result<Option<User>, IggyError> {
        self.get_user_from_metadata(user_id)
    }

    pub fn get_users(&self) -> Vec<User> {
        self.metadata
            .get_all_users()
            .iter()
            .map(|meta| self.user_from_meta(meta))
            .collect()
    }

    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<User, IggyError> {
        let password_hash = crypto::hash_password(password);

        let user_id = self
            .writer()
            .create_user(
                &self.metadata,
                Arc::from(username),
                Arc::from(password_hash.as_str()),
                status,
                permissions.map(Arc::new),
                MAX_USERS,
            )
            .inspect_err(|e| match e {
                IggyError::UserAlreadyExists => error!("User: {username} already exists."),
                IggyError::UsersLimitReached => error!("Available users limit reached."),
                _ => {}
            })?;

        self.metrics.increment_users(1);

        self.get_user(&user_id.try_into()?).error(|e: &IggyError| {
            format!("{COMPONENT} (error: {e}) - failed to get user with id: {user_id}")
        })
    }

    pub fn delete_user(&self, user_id: &Identifier) -> Result<User, IggyError> {
        self.delete_user_base(user_id)
    }

    fn delete_user_base(&self, user_id: &Identifier) -> Result<User, IggyError> {
        let user = self.get_user(user_id).error(|e: &IggyError| {
            format!("{COMPONENT} (error: {e}) - failed to get user with id: {user_id}")
        })?;

        if user.is_root() {
            error!("Cannot delete the root user.");
            return Err(IggyError::CannotDeleteUser(user.id));
        }

        let user_u32_id = user.id;

        self.client_manager
            .delete_clients_for_user(user_u32_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to delete clients for user with ID: {user_u32_id}"
                )
            })?;
        self.metrics.decrement_users(1);

        self.writer().delete_user(user_u32_id);

        Ok(user)
    }

    pub fn update_user(
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
        let numeric_user_id = user.id;

        let updated_meta = self.writer().update_user(
            &self.metadata,
            numeric_user_id,
            username.map(|u| Arc::from(u.as_str())),
            status,
        )?;

        Ok(self.user_from_meta(&updated_meta))
    }

    pub fn update_permissions(
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
        let user: User = self.get_user(user_id).error(|e: &IggyError| {
            format!("{COMPONENT} (error: {e}) - failed to get user with id: {user_id}")
        })?;

        let current_meta = self
            .metadata
            .get_user(user.id)
            .ok_or_else(|| IggyError::ResourceNotFound(user_id.to_string()))?;

        let updated_meta = UserMeta {
            id: current_meta.id,
            username: current_meta.username,
            password_hash: current_meta.password_hash,
            status: current_meta.status,
            permissions: permissions.map(Arc::new),
            created_at: current_meta.created_at,
        };

        self.writer().update_user_meta(user.id, updated_meta);

        Ok(())
    }

    pub fn change_password(
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
        let user = self.get_user(user_id).error(|e: &IggyError| {
            format!(
                "{COMPONENT} change password (error: {e}) - failed to get user with id: {user_id}"
            )
        })?;

        // Verify current password
        if !crypto::verify_password(current_password, &user.password) {
            error!(
                "Invalid current password for user: {} with ID: {user_id}.",
                user.username
            );
            return Err(IggyError::InvalidCredentials);
        }

        let current_meta = self
            .metadata
            .get_user(user.id)
            .ok_or_else(|| IggyError::ResourceNotFound(user_id.to_string()))?;

        let new_password_hash = crypto::hash_password(new_password);
        let updated_meta = UserMeta {
            id: current_meta.id,
            username: current_meta.username,
            password_hash: Arc::from(new_password_hash.as_str()),
            status: current_meta.status,
            permissions: current_meta.permissions,
            created_at: current_meta.created_at,
        };

        self.writer().update_user_meta(user.id, updated_meta);

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
        self.client_manager
            .set_user_id(session.client_id, user.id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to set user_id to client, client ID: {}, user ID: {}",
                    session.client_id, user.id
                )
            })?;
        Ok(user)
    }

    pub fn logout_user(&self, session: &Session) -> Result<(), IggyError> {
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
