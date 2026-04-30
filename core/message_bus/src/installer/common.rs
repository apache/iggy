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

//! Shared install-path helpers used by both the replica and client
//! install paths.

use crate::lifecycle::RejectedRegistration;
use std::time::{Duration, Instant};

/// Drive a losing-insert's transport + dispatch [`compio::runtime::JoinHandle`]s
/// to completion (or force-cancel at the deadline).
///
/// The winning entry must never be touched here; `install_aborted` has
/// already told both tasks to skip post-loop cleanup. Closing the
/// sender wakes the transport's writer; triggering the per-connection
/// shutdown wakes the transport's reader off its `io_uring` read SQE
/// without waiting for peer EOF. Awaiting both handles with
/// `close_peer_timeout` budget guarantees neither task can outlive the
/// race on a half-open socket. `compio::runtime::JoinHandle::drop`
/// detaches, so letting the handles go out of scope would leak the
/// tasks.
///
/// Both handles share a single `timeout` budget: after the transport
/// returns (or is cancelled) the dispatch only gets the remaining time.
/// Two independent full-timeout awaits would let a stuck loser occupy
/// up to `2 * timeout` of shutdown wall-clock on its own.
#[allow(clippy::future_not_send)]
pub(super) async fn drain_rejected_registration(rejected: RejectedRegistration, timeout: Duration) {
    let RejectedRegistration {
        sender,
        writer_handle,
        reader_handle,
        conn_shutdown,
    } = rejected;
    sender.close();
    conn_shutdown.trigger();
    let deadline = Instant::now() + timeout;
    let _ = compio::time::timeout(timeout, writer_handle).await;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if !remaining.is_zero() {
        let _ = compio::time::timeout(remaining, reader_handle).await;
    }
}
