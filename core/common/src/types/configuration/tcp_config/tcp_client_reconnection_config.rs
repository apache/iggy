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
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct TcpClientReconnectionConfig {
    /// Whether to retry failed bootstrap passes and maintain replica links.
    /// The first bootstrap pass still tries every known endpoint when disabled.
    pub enabled: bool,
    /// Retry passes after the initial bootstrap pass, and consecutive background
    /// dial attempts per replica. `None` permits unlimited retries; `Some(0)`
    /// permits only the initial bootstrap pass. The limit covers one reconnect
    /// burst: while the client is signed in, each `IggyClient` heartbeat
    /// restarts the replica links that used it up, with a fresh budget. A
    /// transport client without `IggyClient` has no heartbeat, so only
    /// `connect()` restarts them.
    pub max_retries: Option<u32>,
    /// Full-jitter exponential backoff base, capped at 10 seconds. The initial
    /// bootstrap pass is immediate; every background dial waits its jitter.
    pub interval: NonZeroIggyDuration,
    /// Stable connection duration that resets the retry count and backoff.
    pub reestablish_after: IggyDuration,
}

impl Default for TcpClientReconnectionConfig {
    fn default() -> TcpClientReconnectionConfig {
        TcpClientReconnectionConfig {
            enabled: true,
            max_retries: None,
            interval: NonZeroIggyDuration::from_str("1s").unwrap(),
            reestablish_after: IggyDuration::from_str("5s").unwrap(),
        }
    }
}
