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

use crate::IggyDuration;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// WebSocket client reconnection configuration.
/// It consists of the following fields:
/// - `enabled`: whether to enable reconnection.
/// - `max_retries`: the maximum number of retries. If None, will retry infinitely.
/// - `interval`: the interval between retries.
/// - `reestablish_after`: the time to wait before attempting to reestablish connection.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebSocketClientReconnectionConfig {
    /// Whether to enable reconnection.
    pub enabled: bool,
    /// The maximum number of retries. If None, will retry infinitely.
    pub max_retries: Option<u32>,
    /// The interval between retries.
    pub interval: IggyDuration,
    /// The time to wait before attempting to reestablish connection.
    pub reestablish_after: IggyDuration,
}

impl Default for WebSocketClientReconnectionConfig {
    fn default() -> Self {
        WebSocketClientReconnectionConfig {
            enabled: true,
            max_retries: None,
            interval: IggyDuration::from_str("1s").unwrap(),
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
