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

#[cfg(keyring_supported)]
use std::sync::Mutex;

#[cfg(target_os = "macos")]
use apple_native_keyring_store::keychain::Store;
use keyring_core::{Entry, error::Result};
use tracing::warn;
#[cfg(target_os = "windows")]
use windows_native_keyring_store::store::Store;
#[cfg(secret_service_keyring)]
use zbus_secret_service_keyring_store::store::Store;

use crate::commands::cli_command::PRINT_TARGET;

const SESSION_TOKEN_NAME: &str = "iggy-cli-session";
const SESSION_KEYRING_SERVICE_NAME: &str = "iggy-cli-session";

/// `keyring-core` 1.x has no implicit default backend. Register one on the
/// first `Entry::new` so unrelated CLI subcommands never touch the OS keyring.
///
/// The mutex serializes concurrent callers so two threads cannot both observe
/// an empty default store and race to install one. `get_default_store()` acts
/// as the idempotency check, so a transient `Store::new()` failure stays
/// retryable on the next call.
#[cfg(keyring_supported)]
pub fn ensure_default_store() -> Result<()> {
    static INIT_LOCK: Mutex<()> = Mutex::new(());
    let _guard = INIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    if keyring_core::get_default_store().is_some() {
        return Ok(());
    }
    let store = Store::new()?;
    keyring_core::set_default_store(store);
    Ok(())
}

#[cfg(not(keyring_supported))]
pub fn ensure_default_store() -> Result<()> {
    Ok(())
}

pub struct ServerSession {
    server_address: String,
}

impl ServerSession {
    pub fn new(server_address: String) -> Self {
        Self { server_address }
    }

    pub fn get_server_address(&self) -> &str {
        &self.server_address
    }

    fn get_service_name(&self) -> String {
        format!("{SESSION_KEYRING_SERVICE_NAME}:{}", self.server_address)
    }

    pub fn get_token_name(&self) -> String {
        String::from(SESSION_TOKEN_NAME)
    }

    pub fn is_active(&self) -> bool {
        if let Err(e) = ensure_default_store() {
            warn!(target: PRINT_TARGET, "keyring backend unavailable, treating session as inactive: {e}");
            return false;
        }
        if let Ok(entry) = Entry::new(&self.get_service_name(), &self.get_token_name()) {
            return entry.get_password().is_ok();
        }

        false
    }

    pub fn store(&self, token: &str) -> Result<()> {
        ensure_default_store()?;
        let entry = Entry::new(&self.get_service_name(), &self.get_token_name())?;
        entry.set_password(token)?;
        Ok(())
    }

    pub fn get_token(&self) -> Option<String> {
        if let Err(e) = ensure_default_store() {
            warn!(target: PRINT_TARGET, "keyring backend unavailable, cannot read session token: {e}");
            return None;
        }
        if let Ok(entry) = Entry::new(&self.get_service_name(), &self.get_token_name())
            && let Ok(token) = entry.get_password()
        {
            return Some(token);
        }

        None
    }

    pub fn delete(&self) -> Result<()> {
        ensure_default_store()?;
        let entry = Entry::new(&self.get_service_name(), &self.get_token_name())?;
        entry.delete_credential()
    }
}
