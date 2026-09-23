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
    /// Whether a lost connection is redialed at all. With this off the
    /// endpoints the client knows still get one pass, since they were
    /// configured to be tried, but nothing is retried after it.
    pub enabled: bool,
    /// How many passes over the known endpoints *after the first*, or `None`
    /// for unlimited. `Some(0)` still makes that one pass, since the endpoints
    /// were configured to be tried.
    ///
    /// Passes, not dials: one pass tries the endpoint the client is on, the
    /// addresses it was configured with, and every node the roster named, so a
    /// survivor is reached inside the first pass rather than one delay per
    /// endpoint.
    ///
    /// The number is not portable across SDKs. Each counts the same setting in
    /// its own terms, and `0` means something different in every one of them, so
    /// a deployment that runs several has to set this per SDK rather than copy
    /// one value across:
    ///
    /// | SDK  | `N`                                    | `0`                                       | unlimited        |
    /// | ---- | -------------------------------------- | ----------------------------------------- | ---------------- |
    /// | Rust | `N` passes after a first, unpaced one   | that first pass alone                     | `None`           |
    /// | C#   | as Rust                                | unlimited                                 | `0`              |
    /// | Go   | `N` passes, the first one of them       | unlimited                                 | `0`              |
    /// | Java | `N` passes, the first one of them       | one pass, and only with several endpoints  | a large `N`      |
    /// | Node | `N` passes, the first one of them       | no pass at all                            | a large `N`      |
    pub max_retries: Option<u32>,
    /// Delay between passes. The first pass runs at once when the client knows
    /// more than one endpoint.
    pub interval: NonZeroIggyDuration,
    /// Cooldown before redialing the endpoint of the last successful
    /// connection, measured from when that connection was established rather
    /// than from when it was lost: a session that outlived this interval is
    /// redialed with no wait at all, which is the point -- the pace limit is
    /// there for connections that keep dropping straight away.
    ///
    /// Owed to that endpoint alone: the others are dialed without waiting, and
    /// the paced one goes last in the pass.
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
