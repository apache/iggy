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

use crate::{IggyDuration, NonZeroIggyDuration};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// WebSocket client reconnection configuration.
/// It consists of the following fields:
/// - `enabled`: whether to enable reconnection.
/// - `max_retries`: the retries of one reconnect burst. If None, will retry infinitely.
/// - `interval`: the full-jitter exponential backoff base, capped at 10 seconds.
/// - `reestablish_after`: the stable connection duration that resets the retry count and backoff.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebSocketClientReconnectionConfig {
    /// Whether to enable reconnection.
    pub enabled: bool,
    /// Retry passes after the initial bootstrap pass, and consecutive background
    /// dial attempts per replica. `None` permits unlimited retries; `Some(0)`
    /// permits only the initial bootstrap pass. The limit covers one reconnect
    /// burst: while the client is signed in, each `IggyClient` heartbeat
    /// restarts the replica links that used it up, with a fresh budget. A
    /// transport client without `IggyClient` has no heartbeat, so only
    /// `connect()` restarts them.
    pub max_retries: Option<u32>,
    /// Full-jitter exponential backoff base, capped at 10 seconds.
    pub interval: NonZeroIggyDuration,
    /// Stable connection duration that resets the retry count and backoff.
    pub reestablish_after: IggyDuration,
}

impl Default for WebSocketClientReconnectionConfig {
    fn default() -> Self {
        WebSocketClientReconnectionConfig {
            enabled: true,
            max_retries: None,
            interval: NonZeroIggyDuration::from_str("1s").unwrap(),
            reestablish_after: IggyDuration::from_str("5s").unwrap(),
        }
    }
}

impl Display for WebSocketClientReconnectionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, max_retries: {:?}, interval: {}, reestablish_after: {} }}",
            self.enabled, self.max_retries, self.interval, self.reestablish_after
        )
    }
}
