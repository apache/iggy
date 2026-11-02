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

//! Thin wrappers around `sd_notify` so every systemd interaction on the
//! server side lives in one place (mirrors `core/ai/mcp/src/systemd.rs`).

use crate::server_error::{ServerError, ShardJoinFailureKind};
use message_bus::{IggyMessageBus, ShutdownToken};
use std::time::Duration;
use tracing::{info, warn};

/// Tell systemd the service has finished start-up (`READY=1`).
pub fn notify_ready() {
    if let Err(error) = sd_notify::notify(&[sd_notify::NotifyState::Ready]) {
        warn!("Failed to send systemd READY=1 notification: {error}");
    }
}

/// Tell systemd the service has begun shutting down (`STOPPING=1`), which
/// also stops the watchdog timer from counting against a long drain.
pub fn notify_stopping() {
    let _ = sd_notify::notify(&[sd_notify::NotifyState::Stopping]);
}

/// Surface a dirty shutdown in `systemctl status` / journald.
pub fn notify_shutdown_failure(error: &ServerError) {
    let wedged = matches!(
        error,
        ServerError::ShardJoinFailures { failures }
            if failures
                .iter()
                .any(|failure| matches!(failure.kind, ShardJoinFailureKind::Wedged { .. }))
    );
    let status = if wedged {
        "graceful shutdown timed out"
    } else {
        "shard threads failed during shutdown"
    };
    let _ = sd_notify::notify(&[sd_notify::NotifyState::Status(status)]);
}

/// Start the `WATCHDOG=1` keep-alive. Does nothing unless the unit set
/// `WatchdogSec=`. Tracked on the bus so `bus.shutdown()` reaps the task.
pub fn spawn_watchdog(bus: &IggyMessageBus) {
    let Some(timeout) = sd_notify::watchdog_enabled() else {
        return;
    };

    let interval = timeout / 2;
    info!(
        "Systemd watchdog enabled, pinging every {}s (timeout: {}s).",
        interval.as_secs(),
        timeout.as_secs()
    );

    let handle = compio::runtime::spawn(run_watchdog(bus.token(), interval));
    bus.track_background(handle);
}

async fn run_watchdog(token: ShutdownToken, interval: Duration) {
    while token.sleep_or_shutdown(interval).await {
        if let Err(error) = sd_notify::notify(&[sd_notify::NotifyState::Watchdog]) {
            warn!("Failed to send systemd watchdog ping: {error}");
        }
    }
}
