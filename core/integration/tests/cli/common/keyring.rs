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

#[cfg(target_os = "linux")]
use dbus_secret_service_keyring_store::store::Store;
#[cfg(target_os = "linux")]
use std::sync::Once;

/// `keyring-core` 1.x has no implicit default backend; tests must register one
/// before any `Entry::new` (directly or via `ServerSession`).
#[cfg(target_os = "linux")]
pub(crate) fn ensure_keyring_store() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let store = Store::new().expect("Failed to create dbus secret-service store");
        keyring_core::set_default_store(store);
    });
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn ensure_keyring_store() {}
