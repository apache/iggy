// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{
    permissioner::Permissioner,
    stm::{ApplyState, StateCommand},
};
use ahash::AHashMap;
use bytes::Bytes;
use iggy_common::change_password::ChangePassword;
use iggy_common::create_user::CreateUser;
use iggy_common::delete_user::DeleteUser;
use iggy_common::update_permissions::UpdatePermissions;
use iggy_common::update_user::UpdateUser;
use iggy_common::{
    BytesSerializable, Identifier, IggyError, IggyTimestamp, Permissions, PersonalAccessToken,
    UserId, UserStatus,
    header::{Operation, PrepareHeader},
    message::Message,
};
use slab::Slab;
use std::cell::RefCell;

#[derive(Debug, Clone)]
pub struct User {
    pub id: UserId,
    pub username: String,
    pub password: String,
    pub status: UserStatus,
    pub created_at: IggyTimestamp,
    pub permissions: Option<Permissions>,
    pub personal_access_tokens: AHashMap<String, PersonalAccessToken>,
}

impl User {
    pub fn new(
        username: String,
        password: String,
        status: UserStatus,
        created_at: IggyTimestamp,
        permissions: Option<Permissions>,
    ) -> Self {
        Self {
            id: 0,
            username,
            password,
            status,
            created_at,
            permissions,
            personal_access_tokens: AHashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Users {
    index: RefCell<AHashMap<String, usize>>,
    items: RefCell<Slab<User>>,
    permissioner: RefCell<Permissioner>,
}

impl Users {
    pub fn new() -> Self {
        Self {
            index: RefCell::new(AHashMap::with_capacity(1024)),
            items: RefCell::new(Slab::with_capacity(1024)),
            permissioner: RefCell::new(Permissioner::new()),
        }
    }

    /// Insert a user and return the assigned ID
    pub fn insert(&self, user: User) -> usize {
        let mut items = self.items.borrow_mut();
        let mut index = self.index.borrow_mut();

        let username = user.username.clone();
        let id = items.insert(user);
        items[id].id = id as u32;
        index.insert(username, id);
        id
    }

    /// Get user by ID
    pub fn get(&self, id: usize) -> Option<User> {
        self.items.borrow().get(id).cloned()
    }

    /// Get user by username or ID (via Identifier enum)
    pub fn get_by_identifier(&self, identifier: &Identifier) -> Result<Option<User>, IggyError> {
        match identifier.kind {
            iggy_common::IdKind::Numeric => {
                let id = identifier.get_u32_value()? as usize;
                Ok(self.items.borrow().get(id).cloned())
            }
            iggy_common::IdKind::String => {
                let username = identifier.get_string_value()?;
                let index = self.index.borrow();
                if let Some(&id) = index.get(&username) {
                    Ok(self.items.borrow().get(id).cloned())
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Remove user by ID
    pub fn remove(&self, id: usize) -> Option<User> {
        let mut items = self.items.borrow_mut();
        let mut index = self.index.borrow_mut();

        if !items.contains(id) {
            return None;
        }

        let user = items.remove(id);
        index.remove(&user.username);
        Some(user)
    }

    /// Check if user exists
    pub fn contains(&self, identifier: &Identifier) -> bool {
        match identifier.kind {
            iggy_common::IdKind::Numeric => {
                if let Ok(id) = identifier.get_u32_value() {
                    self.items.borrow().contains(id as usize)
                } else {
                    false
                }
            }
            iggy_common::IdKind::String => {
                if let Ok(username) = identifier.get_string_value() {
                    self.index.borrow().contains_key(&username)
                } else {
                    false
                }
            }
        }
    }

    /// Get all users as a Vec
    pub fn values(&self) -> Vec<User> {
        self.items
            .borrow()
            .iter()
            .map(|(_, u): (usize, &User)| u.clone())
            .collect()
    }

    /// Get number of users
    pub fn len(&self) -> usize {
        self.items.borrow().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
    }

    /// Check if username already exists
    pub fn username_exists(&self, username: &str) -> bool {
        self.index.borrow().contains_key(username)
    }

    /// Get ID by username
    pub fn get_id_by_username(&self, username: &str) -> Option<usize> {
        self.index.borrow().get(username).copied()
    }

    /// Initialize permissions for a user
    pub fn init_permissions(&self, user_id: UserId, permissions: Option<Permissions>) {
        self.permissioner
            .borrow_mut()
            .init_permissions_for_user(user_id, permissions);
    }

    /// Update permissions for a user
    pub fn update_permissions(&self, user_id: UserId, permissions: Option<Permissions>) {
        self.permissioner
            .borrow_mut()
            .update_permissions_for_user(user_id, permissions);
    }

    /// Delete permissions for a user
    pub fn delete_permissions(&self, user_id: UserId) {
        self.permissioner
            .borrow_mut()
            .delete_permissions_for_user(user_id);
    }

    /// Update username
    pub fn update_username(
        &self,
        identifier: &Identifier,
        new_username: String,
    ) -> Result<(), IggyError> {
        let id = match identifier.kind {
            iggy_common::IdKind::Numeric => identifier.get_u32_value()? as usize,
            iggy_common::IdKind::String => {
                let username = identifier.get_string_value()?;
                let index = self.index.borrow();
                *index
                    .get(&username)
                    .ok_or_else(|| IggyError::ResourceNotFound(username.to_string()))?
            }
        };

        let old_username = {
            let items = self.items.borrow();
            let user = items
                .get(id)
                .ok_or_else(|| IggyError::ResourceNotFound(identifier.to_string()))?;
            user.username.clone()
        };

        if old_username == new_username {
            return Ok(());
        }

        tracing::trace!(
            "Updating username: '{}' → '{}' for user ID: {}",
            old_username,
            new_username,
            id
        );

        {
            let mut items = self.items.borrow_mut();
            let user = items
                .get_mut(id)
                .ok_or_else(|| IggyError::ResourceNotFound(identifier.to_string()))?;
            user.username = new_username.clone();
        }

        let mut index = self.index.borrow_mut();
        index.remove(&old_username);
        index.insert(new_username, id);

        Ok(())
    }
}

#[derive(Debug)]
pub enum UsersCommand {
    Create(CreateUser),
    Update(UpdateUser),
    Delete(DeleteUser),
    ChangePassword(ChangePassword),
    UpdatePermissions(UpdatePermissions),
}

impl StateCommand for Users {
    type Command = UsersCommand;
    type Input = Message<PrepareHeader>;

    fn into_command(input: &Self::Input) -> Option<Self::Command> {
        // TODO: rework this thing, so we don't copy the bytes on each request
        let body = Bytes::copy_from_slice(input.body());
        match input.header().operation {
            Operation::CreateUser => Some(UsersCommand::Create(
                CreateUser::from_bytes(body.clone()).unwrap(),
            )),
            Operation::UpdateUser => Some(UsersCommand::Update(
                UpdateUser::from_bytes(body.clone()).unwrap(),
            )),
            Operation::DeleteUser => Some(UsersCommand::Delete(
                DeleteUser::from_bytes(body.clone()).unwrap(),
            )),
            Operation::ChangePassword => Some(UsersCommand::ChangePassword(
                ChangePassword::from_bytes(body.clone()).unwrap(),
            )),
            Operation::UpdatePermissions => Some(UsersCommand::UpdatePermissions(
                UpdatePermissions::from_bytes(body.clone()).unwrap(),
            )),
            _ => None,
        }
    }
}

impl ApplyState for Users {
    type Output = ();

    fn do_apply(&self, cmd: Self::Command) -> Self::Output {
        match cmd {
            UsersCommand::Create(payload) => todo!("Handle Create user with {:?}", payload),
            UsersCommand::Update(payload) => todo!("Handle Update user with {:?}", payload),
            UsersCommand::Delete(payload) => todo!("Handle Delete user with {:?}", payload),
            UsersCommand::ChangePassword(payload) => {
                todo!("Handle Change password with {:?}", payload)
            }
            UsersCommand::UpdatePermissions(payload) => {
                todo!("Handle Update permissions with {:?}", payload)
            }
        }
    }
}
