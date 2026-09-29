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

//! Accept-loop handling of descriptor exhaustion.

use server_common::fatal::is_descriptor_exhaustion;
use std::io;
use std::time::Duration;

/// How long an accept loop waits after `EMFILE` or `ENFILE`.
///
/// The kernel keeps the connection in the listen backlog, so an immediate
/// retry fails again and spins the shard at full CPU while the descriptor
/// table stays full.
pub const DESCRIPTOR_EXHAUSTION_BACKOFF: Duration = Duration::from_secs(1);

/// Wait [`DESCRIPTOR_EXHAUSTION_BACKOFF`] if `error` says no file descriptor
/// is free, and return at once for any other `accept()` error.
///
/// An accept loop only waits. It does not record the exhaustion for the exit
/// status as the storage write paths do, because no write failed.
/// `[message_bus] connections_max` keeps clients below the descriptor limit,
/// so an accept reaches `EMFILE` only when that cap is off or set too high, or
/// when storage holds the descriptors. A process exit here would let clients
/// stop the node in the first two cases.
#[allow(clippy::future_not_send)]
pub async fn pause_after_accept_error(error: &io::Error) {
    if is_descriptor_exhaustion(error) {
        compio::time::sleep(DESCRIPTOR_EXHAUSTION_BACKOFF).await;
    }
}
