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

//! Leader-driven periodic cleanup of expired personal access tokens.
//!
//! Spawned on shard 0 of every node, but a pass acts only on the caught-up
//! metadata primary: the delete is a replicated mutation, so the leader
//! proposes it once and every replica applies the commit. Backups never
//! propose, so cleanup cannot race across the cluster.

use crate::bootstrap::ServerNgShard;
use consensus::MetadataHandle;
use iggy_binary_protocol::WireName;
use iggy_common::IggyTimestamp;
use metadata::impls::metadata::StreamsFrontend;
use shard::Receiver;
use std::rc::Rc;
use std::time::Duration;
use tracing::{info, trace, warn};

/// Upper bound on tokens deleted in a single pass. Each delete is its own
/// replicated consensus round on shard 0, so an uncapped pass over a large
/// batch (first tick after downtime, or many tokens sharing one expiry) would
/// monopolize the metadata pipeline. A pass that fills the cap re-runs
/// immediately instead of waiting a full interval, so a backlog still drains
/// promptly while yielding between batches.
const MAX_DELETIONS_PER_PASS: usize = 256;

/// Outcome of one cleanup pass.
enum Pass {
    /// Stop was observed mid-pass; the run loop should exit.
    Stopped,
    /// A full [`MAX_DELETIONS_PER_PASS`] batch was deleted and more may remain;
    /// run another pass without waiting for the next tick.
    HitCap,
    /// The due tokens were drained (or none were due, or leadership was lost);
    /// wait for the next tick.
    Drained,
}

/// Run the cleaner until `stop` fires. Wakes every `interval`; expiry is
/// wall-clock driven, so no metadata-commit wake is needed.
pub async fn run_pat_cleaner(shard: Rc<ServerNgShard>, stop: Receiver<()>, interval: Duration) {
    trace!(
        shard = shard.id,
        interval_ms = interval.as_millis(),
        "personal access token cleaner started"
    );
    'ticks: loop {
        // `Ok(_)`: stop signalled -> exit. `Err(_)`: interval elapsed -> run
        // capped passes until the backlog drains, stop fires, or leadership is
        // lost, then wait for the next tick.
        match compio::time::timeout(interval, stop.recv()).await {
            Ok(_) => break 'ticks,
            Err(_) => loop {
                match clean_expired_tokens(&shard, &stop).await {
                    // Full batch deleted; loop again immediately to keep
                    // draining without waiting for the next tick.
                    Pass::HitCap => {}
                    Pass::Drained => break,
                    Pass::Stopped => break 'ticks,
                }
            },
        }
    }
    trace!(shard = shard.id, "personal access token cleaner exited");
}

/// Run one cleanup pass, deleting at most [`MAX_DELETIONS_PER_PASS`] tokens.
async fn clean_expired_tokens(shard: &Rc<ServerNgShard>, stop: &Receiver<()>) -> Pass {
    let metadata = shard.plane.metadata();
    if !metadata.is_caught_up_primary() {
        return Pass::Drained;
    }

    let now = IggyTimestamp::now();
    // The read guard is released when the closure returns, before any
    // `submit_*` await below.
    let expired = metadata
        .mux_stm
        .users()
        .read(|users| users.expired_personal_access_tokens(now));

    if expired.is_empty() {
        return Pass::Drained;
    }

    let due = expired.len();
    let mut removed = 0usize;
    let mut stop_requested = false;
    for (user_id, name) in expired.into_iter().take(MAX_DELETIONS_PER_PASS) {
        // Observe stop between tokens: an in-flight submit is not
        // cancel-safe, so let the current delete finish rather than cut it.
        // Bounds shutdown delay to one submit, not the whole batch.
        if stop.try_recv().is_ok() {
            stop_requested = true;
            break;
        }
        // `name` came from a stored PAT, originally a validated `WireName`, so
        // reconstruction is infallible in practice. Guard anyway: a corrupted
        // snapshot carrying an invalid name should skip the entry, not panic
        // the whole cleaner.
        let Ok(wire_name) = WireName::new(name.as_ref()) else {
            warn!(user_id, %name, "skipping personal access token with invalid name");
            continue;
        };
        match metadata
            .submit_delete_personal_access_token_in_process(user_id, wire_name)
            .await
        {
            Ok(_) => removed += 1,
            // Leadership lost or the node fell behind mid-pass; the next
            // primary cleans up on its own tick. Stop the batch.
            Err(error) => {
                trace!(
                    user_id,
                    ?error,
                    "stopping personal access token cleanup pass"
                );
                break;
            }
        }
    }

    if removed > 0 {
        info!(removed, "removed expired personal access tokens");
    }

    if stop_requested {
        Pass::Stopped
    } else if due > MAX_DELETIONS_PER_PASS && removed == MAX_DELETIONS_PER_PASS {
        Pass::HitCap
    } else {
        Pass::Drained
    }
}
