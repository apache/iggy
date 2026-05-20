/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

//! Thin wrappers around `sd_notify` so every systemd interaction on the server
//! side lives in one place (mirrors `core/ai/mcp/src/systemd.rs`).

use tracing::warn;

/// Tell systemd the service has finished start-up (`READY=1`).
pub fn notify_ready() {
    if let Err(e) = sd_notify::notify(&[sd_notify::NotifyState::Ready]) {
        warn!("Failed to send systemd READY=1 notification: {e}");
    }
}

/// Tell systemd the service has begun shutting down (`STOPPING=1`).
pub fn notify_stopping() {
    let _ = sd_notify::notify(&[sd_notify::NotifyState::Stopping]);
}

/// Surface a non-fatal shutdown problem in `systemctl status` / journald.
pub fn notify_status(status: &str) {
    let _ = sd_notify::notify(&[sd_notify::NotifyState::Status(status)]);
}

/// Send a single watchdog keep-alive ping (`WATCHDOG=1`).
pub fn ping_watchdog() {
    if let Err(e) = sd_notify::notify(&[sd_notify::NotifyState::Watchdog]) {
        warn!("Failed to send systemd watchdog ping: {e}");
    }
}
