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

#[cfg(all(feature = "login-session", secret_service_keyring))]
mod backend {
    use std::sync::OnceLock;
    use zbus_secret_service_keyring_store::store::Store;

    /// Single global init result. `OnceLock` (not `Once`) so a failed
    /// `Store::new` doesn't poison the slot and cascade `PoisonError` to every
    /// subsequent test in the same process; the original error is replayed
    /// verbatim instead.
    static KEYRING_INIT: OnceLock<Result<(), String>> = OnceLock::new();

    pub(crate) fn ensure_keyring_store() {
        let result = KEYRING_INIT.get_or_init(|| match Store::new() {
            Ok(store) => {
                keyring_core::set_default_store(store);
                Ok(())
            }
            Err(err) => Err(format!(
                "failed to create zbus secret-service keyring store: {err}. \
                 Ensure DBUS_SESSION_BUS_ADDRESS is set and a secret-service \
                 provider (gnome-keyring-daemon, KWallet, KeePassXC) is running."
            )),
        });
        if let Err(msg) = result {
            panic!("{msg}");
        }
    }
}

#[cfg(all(feature = "login-session", secret_service_keyring))]
pub(crate) use backend::ensure_keyring_store;

#[cfg(not(all(feature = "login-session", secret_service_keyring)))]
#[allow(dead_code)]
pub(crate) fn ensure_keyring_store() {}
