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

use gloo_storage::{LocalStorage, Storage};
use serde::{Deserialize, Serialize};

use crate::models::IdentityInfo;

const SESSION_KEY: &str = "iggy_session";

/// The signed-in user's token, persisted so a reload keeps the session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub user_id: u32,
    pub access_token: String,
    /// Unix seconds, as returned by `/users/login`.
    pub expiry: u64,
}

impl Session {
    /// Restores the stored session, dropping it if the token has expired.
    pub fn restore() -> Option<Self> {
        let session: Self = LocalStorage::get(SESSION_KEY).ok()?;
        if session.is_expired() {
            Self::clear();
            return None;
        }
        Some(session)
    }

    pub fn persist(&self) {
        // A failed write only costs the user a sign-in after the next reload.
        let _ = LocalStorage::set(SESSION_KEY, self);
    }

    pub fn clear() {
        LocalStorage::delete(SESSION_KEY);
    }

    fn is_expired(&self) -> bool {
        let now_secs = (js_sys::Date::now() / 1000.0) as u64;
        self.expiry <= now_secs
    }
}

impl TryFrom<IdentityInfo> for Session {
    type Error = MissingToken;

    fn try_from(identity: IdentityInfo) -> Result<Self, Self::Error> {
        let token = identity.access_token.ok_or(MissingToken)?;
        Ok(Self {
            user_id: identity.user_id,
            access_token: token.token,
            expiry: token.expiry,
        })
    }
}

/// The server answered a login without an access token, which happens when
/// JWT auth is disabled for HTTP.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("the server did not return an access token")]
pub struct MissingToken;
