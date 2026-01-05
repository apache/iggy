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
            self.permissioner.get_user(session_user_id).error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to get user with ID: {user_id} for current user with ID: {session_user_id}"
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
        // Read from SharedMetadata for cross-shard consistency
        let metadata = self.shared_metadata.load();
        let user_meta = metadata.get_user(user_id);
        Ok(user_meta.map(|meta| User::from_metadata(&meta)))
    }

    pub async fn get_users(&self, session: &Session) -> Result<Vec<User>, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .get_users(session.get_user_id())
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to get users for user with id: {}",
                    session.get_user_id()
                )
            })?;

        // Read from SharedMetadata for cross-shard consistency
        let metadata = self.shared_metadata.load();
        let users: Vec<User> = metadata.users.values().map(User::from_metadata).collect();
        Ok(users)
    }

    /// Create a user. Returns the user metadata.
    /// Writes only to SharedMetadata (single source of truth).
    pub fn create_user(
        &self,
        session: &Session,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<UserMeta, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .create_user(session.get_user_id())
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to create user for user with id: {}",
                    session.get_user_id()
                )
            })?;

        // Check SharedMetadata for existence (source of truth)
        if self.shared_metadata.user_exists_by_name(username) {
            error!("User: {username} already exists.");
            return Err(IggyError::UserAlreadyExists);
        }

        // Check user count against SharedMetadata
        let user_count = self.shared_metadata.load().users.len();
        if user_count >= MAX_USERS {
            error!("Available users limit reached.");
            return Err(IggyError::UsersLimitReached);
        }

        // Create in SharedMetadata (single source of truth)
        let user_meta = self.shared_metadata.create_user(
            username.to_string(),
            crypto::hash_password(password),
            status,
            permissions.clone(),
        )?;

        self.metrics.increment_users(1);
        Ok(user_meta)
    }

    /// Delete a user. Returns the deleted user metadata.
    /// Writes only to SharedMetadata (single source of truth).
    pub fn delete_user(
        &self,
        session: &Session,
        user_id: &Identifier,
    ) -> Result<UserMeta, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .delete_user(session.get_user_id())
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to delete user for user with id: {}",
                    session.get_user_id()
                )
            })?;

        // Get user from SharedMetadata (source of truth) first to check root status
        let (user_u32_id, is_root) = {
            let metadata = self.shared_metadata.load();
            let user_meta = metadata
                .get_user(user_id)
                .ok_or_else(|| IggyError::ResourceNotFound(user_id.to_string()))?;
            (
                user_meta.id,
                user_meta.status == UserStatus::Active && user_meta.id == 0,
            )
        };

        // Cannot delete root user
        if is_root {
            error!("Cannot delete the root user.");
            return Err(IggyError::CannotDeleteUser(user_u32_id));
        }

        // Delete from SharedMetadata (single source of truth)
        let deleted_meta = self.shared_metadata.delete_user(user_id)?;

        // Clean up clients
        self.client_manager
            .delete_clients_for_user(user_u32_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to delete clients for user with ID: {user_u32_id}"
                )
            })?;

        self.metrics.decrement_users(1);
        Ok(deleted_meta)
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
            .update_user(session.get_user_id())
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to update user for user with id: {}",
                    session.get_user_id()
                )
            })?;

        // Validate user exists and check for username conflicts
        let user = self.get_user(user_id)?;
        let numeric_user_id = user.id;

        if let Some(ref new_username) = username {
            let existing_user = self.get_user(&new_username.to_owned().try_into()?);
            if existing_user.is_ok() && existing_user.unwrap().id != user.id {
                error!("User: {new_username} already exists.");
                return Err(IggyError::UserAlreadyExists);
            }
        }

        // Update SharedMetadata (single source of truth)
        self.shared_metadata
            .update_user(user_id, username, status)?;

        // Return updated user from SharedMetadata using numeric ID
        // (original identifier may no longer be valid if username was changed)
        let numeric_id = Identifier::numeric(numeric_user_id)?;
        self.get_user(&numeric_id).error(|e: &IggyError| {
            format!("{COMPONENT} update user (error: {e}) - failed to get updated user with id: {numeric_user_id}")
        })
    }

    pub fn update_permissions(
        &self,
        session: &Session,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;

        self.permissioner
            .update_permissions(session.get_user_id())
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to update permissions for user with id: {}",
                    session.get_user_id()
                )
            })?;

        let user: User = self.get_user(user_id).error(|e: &IggyError| {
            format!("{COMPONENT} (error: {e}) - failed to get user with id: {user_id}")
        })?;

        if user.is_root() {
            error!("Cannot change the root user permissions.");
            return Err(IggyError::CannotChangePermissions(user.id));
        }

        // Update SharedMetadata (single source of truth)
        self.shared_metadata
            .update_permissions(user_id, permissions)?;

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

        let user = self.get_user(user_id).error(|e: &IggyError| {
            format!("{COMPONENT} (error: {e}) - failed to get user with id: {user_id}")
        })?;

        let session_user_id = session.get_user_id();
        if user.id != session_user_id {
            self.permissioner.change_password(session_user_id)?;
        }

        // Validate current password
        if !crypto::verify_password(current_password, &user.password) {
            error!(
                "Invalid current password for user: {} with ID: {user_id}.",
                user.username
            );
            return Err(IggyError::InvalidCredentials);
        }

        // Update SharedMetadata (single source of truth)
        self.shared_metadata
            .change_password(user_id, crypto::hash_password(new_password))?;

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
