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

//! End-to-end reproduction of the in-process catch-up stall, driven only
//! through the public Iggy SDK.
//!
//! Each `login_user` is a `Register` submitted through the metadata consensus
//! group. A burst of concurrent logins pipelines those registers, so the
//! primary's `commit_max` (agreed) runs microseconds ahead of `commit_min`
//! (applied). A register landing in that window is rejected with `NotCaughtUp`
//! and silently dropped (no reply), so that client's `login_user` makes no
//! progress until the SDK read-timeout (~30s) fires.
//!
//! The fix waits the window out instead of dropping, so every login returns
//! promptly. The assertion below fails on the buggy server (one or more logins
//! stall past the threshold) and passes once the wait is in place.

#![cfg(feature = "vsr")]

use iggy::prelude::*;
use integration::iggy_harness;
use std::time::{Duration, Instant};

/// Concurrent cold logins. The original report reproduced with only "a few";
/// a larger burst makes the timing window reliable without being heavy.
const CLIENTS: usize = 100;

/// A healthy login returns in single-digit milliseconds; a dropped one only
/// recovers via the ~30s SDK read-timeout. Anything above this threshold is
/// the stall, not normal latency, and the gap is wide enough to be CI-robust.
const STALL_THRESHOLD: Duration = Duration::from_secs(10);

#[iggy_harness(test_client_transport = [Tcp])]
async fn given_many_concurrent_logins_when_primary_pipelines_should_not_stall(
    harness: &TestHarness,
) {
    // Connect everyone first (unauthenticated: no metadata op yet), so the
    // logins fire as one tight burst and overlap inside the pipelining window.
    let clients = harness.new_clients(CLIENTS).await.unwrap();

    let logins = clients.iter().map(|client| async move {
        let started = Instant::now();
        let outcome = tokio::time::timeout(
            STALL_THRESHOLD,
            client.login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD),
        )
        .await;
        (started.elapsed(), outcome)
    });
    let results = futures::future::join_all(logins).await;

    let mut stalled: Vec<(usize, Duration)> = Vec::new();
    let mut slowest = Duration::ZERO;
    for (index, (elapsed, outcome)) in results.iter().enumerate() {
        slowest = slowest.max(*elapsed);
        match outcome {
            Ok(Ok(_identity)) => {}
            Ok(Err(error)) => panic!("login {index} failed (not a stall): {error}"),
            Err(_timed_out) => stalled.push((index, *elapsed)),
        }
    }

    assert!(
        stalled.is_empty(),
        "{}/{CLIENTS} logins stalled past {STALL_THRESHOLD:?} (slowest {slowest:?}): {stalled:?}. \
         A Register landed in the commit_min < commit_max window and was silently dropped.",
        stalled.len(),
    );
}
