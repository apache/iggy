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

//! Per-connection writer task.
//!
//! Drains the per-peer mpsc and writes batched consensus frames in a single
//! `writev` syscall via `compio::io::AsyncWriteExt::write_vectored_all`.
//!
//! Single-producer (the bus `send_to_*` path) -> single-consumer (this task),
//! per peer. The mpsc bound provides backpressure: callers `try_send` and
//! receive [`SendError::Backpressure`](crate::SendError::Backpressure) when
//! the queue is full. VSR retransmits dropped messages from the WAL.
//!
//! Lifecycle: the task exits cleanly when either
//! - the bus shutdown token fires (preferred path during graceful shutdown),
//! - the `Sender` side is dropped/closed (peer unregistered), or
//! - a write to the wire fails (broken connection).

use crate::lifecycle::{BusMessage, BusReceiver, ShutdownToken};
use compio::io::AsyncWriteExt;
use compio::net::{OwnedWriteHalf, TcpStream};
use futures::FutureExt;
use tracing::{debug, error, trace};

/// Run the per-connection writer loop until the channel closes, the
/// shutdown token fires, or a write fails.
///
/// `max_batch` caps how many messages a single `writev` syscall coalesces.
/// Larger batches reduce syscalls per N messages at the cost of memory
/// per batch and worst-case latency for the head-of-batch message.
#[allow(clippy::future_not_send)] // single-threaded compio
pub async fn run(
    rx: BusReceiver,
    mut write_half: OwnedWriteHalf<TcpStream>,
    token: ShutdownToken,
    label: &'static str,
    peer: String,
    max_batch: usize,
) {
    let mut batch: Vec<BusMessage> = Vec::with_capacity(max_batch);
    // Build the shutdown wait future once and re-poll the same pinned
    // instance every loop iteration. Re-creating `token.wait().fuse()`
    // per batch re-registers the async-channel waker; a shared fused
    // future resolves in-place the moment the token fires and then
    // yields Ready on every subsequent poll.
    let mut shutdown_fut = Box::pin(token.wait().fuse());

    loop {
        let first = futures::select! {
            () = shutdown_fut.as_mut() => {
                debug!(%label, %peer, "writer task: shutdown token fired");
                return;
            }
            msg = rx.recv().fuse() => {
                if let Ok(m) = msg {
                    m
                } else {
                    debug!(%label, %peer, "writer task: channel closed");
                    return;
                }
            }
        };

        batch.push(first);

        // Drain the rest non-blocking up to max_batch so a single writev
        // syscall covers as many messages as the queue currently holds.
        while batch.len() < max_batch {
            match rx.try_recv() {
                Ok(m) => batch.push(m),
                Err(_) => break,
            }
        }

        let drained = batch.len();
        trace!(%label, %peer, batch = drained, "writev batch");

        // Single writev for the whole batch. write_vectored_all loops
        // internally on partial writes until the full batch lands or the
        // socket errors.
        let to_write = std::mem::take(&mut batch);
        let compio::BufResult(result, mut returned) = write_half.write_vectored_all(to_write).await;
        if let Err(e) = result {
            // Error (not warn) because the batch is now on the floor:
            // VSR's prepare-timeout or view-change will recover, but the
            // operator needs a loud diagnostic to correlate with the
            // underlying network fault. `batch_len` lets them estimate
            // how many messages were in flight when the write failed.
            error!(
                %label,
                %peer,
                error = ?e,
                batch_len = drained,
                "writer task: write_vectored_all failed, dropping batch"
            );
            return;
        }

        // Reuse the Vec allocation across iterations.
        returned.clear();
        batch = returned;
    }
}
