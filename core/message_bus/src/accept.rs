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

//! Accept-loop handling of resource exhaustion.

use crate::lifecycle::ShutdownToken;
use server_common::fatal::is_descriptor_exhaustion;
use std::io;
use std::time::Duration;

/// How long an accept loop waits after `accept()` found no free file
/// descriptor (`EMFILE`, `ENFILE`) or no free kernel memory (`ENOMEM`,
/// `ENOBUFS`).
///
/// The kernel keeps the connection in the listen backlog, so an immediate
/// retry fails again and spins the shard at full CPU while the resource stays
/// exhausted.
pub const RESOURCE_EXHAUSTION_BACKOFF: Duration = Duration::from_secs(1);

/// Wait [`RESOURCE_EXHAUSTION_BACKOFF`] if `error` says that no descriptor or
/// no kernel memory is free, and return at once for any other `accept()`
/// error.
///
/// The wait ends early when `shutdown` fires, so a stop during a burst of
/// errors does not wait for it.
///
/// An accept loop only waits. It does not record the exhaustion for the exit
/// status as the storage write paths do, because no write failed.
/// `[message_bus] connections_max` keeps clients below the descriptor limit,
/// so an accept reaches `EMFILE` only when that cap is off or set too high, or
/// when storage holds the descriptors. A process exit here would let clients
/// stop the node in the first two cases.
#[allow(clippy::future_not_send)]
pub async fn pause_after_accept_error(error: &io::Error, shutdown: &ShutdownToken) {
    if is_resource_exhaustion(error) {
        shutdown
            .sleep_or_shutdown(RESOURCE_EXHAUSTION_BACKOFF)
            .await;
    }
}

/// The `accept(2)` errors that say a resource of the process or the host is
/// used up: descriptors, or kernel memory for the socket.
fn is_resource_exhaustion(error: &io::Error) -> bool {
    is_descriptor_exhaustion(error)
        || error
            .raw_os_error()
            .is_some_and(|code| code == libc::ENOMEM || code == libc::ENOBUFS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::Shutdown;
    use std::time::Instant;

    #[test]
    fn given_accept_error_when_classifying_should_pause_only_on_used_up_resources() {
        for code in [libc::EMFILE, libc::ENFILE, libc::ENOMEM, libc::ENOBUFS] {
            assert!(is_resource_exhaustion(&io::Error::from_raw_os_error(code)));
        }
        for code in [libc::ECONNABORTED, libc::EINTR] {
            assert!(!is_resource_exhaustion(&io::Error::from_raw_os_error(code)));
        }
    }

    #[compio::test]
    async fn given_shutdown_when_pausing_after_exhaustion_should_return_early() {
        let (shutdown, token) = Shutdown::new();
        shutdown.trigger();
        let started = Instant::now();
        pause_after_accept_error(&io::Error::from_raw_os_error(libc::EMFILE), &token).await;
        assert!(started.elapsed() < RESOURCE_EXHAUSTION_BACKOFF);
    }
}
