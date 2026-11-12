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

use serde::{Deserialize, Serialize};
use web_sys::Storage;

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
    /// Blocked or unreadable storage counts as no session.
    pub fn restore() -> Option<Self> {
        let storage = local_storage()?;
        let stored = storage.get_item(SESSION_KEY).ok()??;
        let session: Self = serde_json::from_str(&stored).ok()?;
        if session.is_expired() {
            let _ = storage.remove_item(SESSION_KEY);
            return None;
        }
        Some(session)
    }

    pub fn persist(&self) {
        // A failed write only costs the user a sign-in after the next reload.
        let (Some(storage), Ok(json)) = (local_storage(), serde_json::to_string(self)) else {
            return;
        };
        let _ = storage.set_item(SESSION_KEY, &json);
    }

    /// Removes this session from storage. Tabs share the stored session, so
    /// one that another tab saved after signing in again is left alone.
    pub fn forget(&self) {
        let Some(storage) = local_storage() else {
            return;
        };
        let stored = storage
            .get_item(SESSION_KEY)
            .ok()
            .flatten()
            .and_then(|stored| serde_json::from_str::<Self>(&stored).ok());
        if stored.is_none_or(|stored| stored.access_token == self.access_token) {
            let _ = storage.remove_item(SESSION_KEY);
        }
    }

    fn is_expired(&self) -> bool {
        let now_secs = (js_sys::Date::now() / 1000.0) as u64;
        self.expiry <= now_secs
    }
}

impl From<IdentityInfo> for Session {
    fn from(identity: IdentityInfo) -> Self {
        Self {
            user_id: identity.user_id,
            access_token: identity.access_token.token,
            expiry: identity.access_token.expiry,
        }
    }
}

/// `None` when the browser blocks site storage, where reading
/// `window.localStorage` throws.
fn local_storage() -> Option<Storage> {
    web_sys::window()?.local_storage().ok().flatten()
}
