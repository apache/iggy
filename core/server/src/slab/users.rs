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

use crate::streaming::users::user::User;
use ahash::AHashMap;
use iggy_common::{Identifier, IggyError};
use slab::Slab;
use std::cell::RefCell;

const CAPACITY: usize = 1024;

#[derive(Debug, Clone, Default)]
pub struct Users {
    index: RefCell<AHashMap<String, usize>>,
    users: RefCell<Slab<User>>,
}

impl Users {
    pub fn new() -> Self {
        Self {
            index: RefCell::new(AHashMap::with_capacity(CAPACITY)),
            users: RefCell::new(Slab::with_capacity(CAPACITY)),
        }
    }

    /// Insert a user and return the assigned ID (auto-incremented by Slab)
    pub fn insert(&self, user: User) -> usize {
        let username = user.username.clone();
        let mut users = self.users.borrow_mut();
        let mut index = self.index.borrow_mut();

        let id = users.insert(user);
        users[id].id = id as u32;
        index.insert(username, id);
        id
    }

    /// Get user by ID (numeric identifier)
    pub fn get(&self, id: usize) -> Option<User> {
        self.users.borrow().get(id).cloned()
    }

    /// Get user by username or ID (via Identifier enum)
    pub fn get_by_identifier(&self, identifier: &Identifier) -> Result<Option<User>, IggyError> {
        match identifier.kind {
            iggy_common::IdKind::Numeric => {
                let id = identifier.get_u32_value()? as usize;
                Ok(self.users.borrow().get(id).cloned())
            }
            iggy_common::IdKind::String => {
                let username = identifier.get_string_value()?;
                let index = self.index.borrow();
                if let Some(&id) = index.get(&username) {
                    Ok(self.users.borrow().get(id).cloned())
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Remove user by Slab index
    pub fn remove(&self, id: usize) -> Option<User> {
        let mut users = self.users.borrow_mut();
        let mut index = self.index.borrow_mut();

        if !users.contains(id) {
            return None;
        }

        let user = users.remove(id);
        index.remove(&user.username);
        Some(user)
    }

    /// Check if user exists
    pub fn contains(&self, identifier: &Identifier) -> bool {
        match identifier.kind {
            iggy_common::IdKind::Numeric => {
                if let Ok(id) = identifier.get_u32_value() {
                    self.users.borrow().contains(id as usize)
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
        self.users.borrow().iter().map(|(_, u)| u.clone()).collect()
    }

    /// Get number of users
    pub fn len(&self) -> usize {
        self.users.borrow().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.users.borrow().is_empty()
    }

    /// Get mutable access to a user via closure (to avoid RefMut escaping issues)
    pub fn with_user_mut<F, R>(&self, identifier: &Identifier, f: F) -> Result<R, IggyError>
    where
        F: FnOnce(&mut User) -> R,
    {
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

        let mut users = self.users.borrow_mut();
        let user = users
            .get_mut(id)
            .ok_or_else(|| IggyError::ResourceNotFound(identifier.to_string()))?;
        Ok(f(user))
    }

    /// Check if username already exists (for validation during create/update)
    pub fn username_exists(&self, username: &str) -> bool {
        self.index.borrow().contains_key(username)
    }

    /// Get ID by username (useful for cross-references)
    pub fn get_id_by_username(&self, username: &str) -> Option<usize> {
        self.index.borrow().get(username).copied()
    }

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
            let users = self.users.borrow();
            let user = users
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
            let mut users = self.users.borrow_mut();
            let user = users
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
