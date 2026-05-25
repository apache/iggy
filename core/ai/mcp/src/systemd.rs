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

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub fn notify_ready() {
    if let Err(e) = sd_notify::notify(&[sd_notify::NotifyState::Ready]) {
        warn!("Failed to send systemd READY=1 notification: {e}");
    }
}

pub fn notify_stopping() {
    let _ = sd_notify::notify(&[sd_notify::NotifyState::Stopping]);
}

/// Spawn the watchdog keep-alive task. It stops cooperatively when `cancel`
/// fires (driven by the SIGINT/SIGTERM handler in `main`).
pub fn spawn_watchdog(cancel: CancellationToken) {
    let Some(timeout) = sd_notify::watchdog_enabled() else {
        return;
    };

    let interval = timeout / 2;
    info!(
        "Systemd watchdog enabled, pinging every {}s (timeout: {}s).",
        interval.as_secs(),
        timeout.as_secs()
    );

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = sd_notify::notify(&[sd_notify::NotifyState::Watchdog]) {
                        warn!("Failed to send systemd watchdog ping: {e}");
                    }
                }
            }
        }
    });
}
