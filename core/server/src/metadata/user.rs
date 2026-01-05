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

use iggy_common::{IggyTimestamp, Permissions, UserStatus};
use imbl::HashMap as ImHashMap;

/// Personal access token metadata.
#[derive(Debug, Clone)]
pub struct PersonalAccessTokenMeta {
    pub name: String,
    pub token_hash: String,
    pub expiry_at: Option<IggyTimestamp>,
}

impl PersonalAccessTokenMeta {
    pub fn new(name: String, token_hash: String, expiry_at: Option<IggyTimestamp>) -> Self {
        Self {
            name,
            token_hash,
            expiry_at,
        }
    }

    pub fn is_expired(&self, now: IggyTimestamp) -> bool {
        self.expiry_at
            .is_some_and(|expiry| now.as_micros() >= expiry.as_micros())
    }
}

/// User metadata stored in the shared snapshot.
#[derive(Debug, Clone)]
pub struct UserMeta {
    pub id: u32,
    pub username: String,
    pub password_hash: String,
    pub created_at: IggyTimestamp,
    pub status: UserStatus,
    pub permissions: Option<Permissions>,
    pub personal_access_tokens: ImHashMap<String, PersonalAccessTokenMeta>,
}

impl UserMeta {
    pub fn new(
        id: u32,
        username: String,
        password_hash: String,
        created_at: IggyTimestamp,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Self {
        Self {
            id,
            username,
            password_hash,
            created_at,
            status,
            permissions,
            personal_access_tokens: ImHashMap::new(),
        }
    }
}
