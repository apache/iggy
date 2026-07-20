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
    use iggy_cli::commands::binary_system::session::ServerSession;
    use std::net::SocketAddr;
    use std::sync::OnceLock;
    use zbus_secret_service_keyring_store::store::Store;

    /// Single global init result. `OnceLock` (not `Once`) so a failed
    /// `Store::new` doesn't poison the slot and cascade `PoisonError` to every
    /// subsequent test in the same process; the original error is replayed
    /// verbatim instead.
    static KEYRING_INIT: OnceLock<Result<(), String>> = OnceLock::new();

    fn init_store() -> &'static Result<(), String> {
        KEYRING_INIT.get_or_init(|| match Store::new() {
            Ok(store) => {
                keyring_core::set_default_store(store);
                Ok(())
            }
            Err(err) => Err(format!(
                "failed to create zbus secret-service keyring store: {err}. \
                 Ensure DBUS_SESSION_BUS_ADDRESS is set and a secret-service \
                 provider (gnome-keyring-daemon, KWallet, KeePassXC) is running."
            )),
        })
    }

    pub(crate) fn ensure_keyring_store() {
        if let Err(msg) = init_store() {
            panic!("{msg}");
        }
    }

    /// Drop the CLI's stored session token for `address`, if one is there.
    ///
    /// Runs on both the setup and teardown paths. Errors are swallowed rather
    /// than asserted on: an absent entry is the normal case, and on teardown a
    /// panic during unwind aborts the process and buries the failure under test.
    pub(crate) fn clear_session_entry(address: SocketAddr) {
        if init_store().is_err() {
            return;
        }
        let _ = ServerSession::new(address.to_string()).delete();
    }
}

#[cfg(not(all(feature = "login-session", secret_service_keyring)))]
mod backend {
    use std::net::SocketAddr;

    #[expect(dead_code)]
    pub(crate) fn ensure_keyring_store() {}

    pub(crate) fn clear_session_entry(_address: SocketAddr) {}
}

#[allow(unused_imports)]
pub(crate) use backend::{clear_session_entry, ensure_keyring_store};
