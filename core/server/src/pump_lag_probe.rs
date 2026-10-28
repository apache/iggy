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

//! Tiered storage proof of concept: how late a 1 ms timer fires on each shard, which is how long
//! the shard's executor was busy elsewhere, the pump included. Enabled by
//! `TIERED_POC_LAG_PROBE=true` independently of tiering, so a baseline run compares with a tiered
//! one.

use shard::Receiver;
use std::time::{Duration, Instant};
use tracing::info;

const PERIOD: Duration = Duration::from_millis(1);
const REPORT_EVERY: Duration = Duration::from_secs(10);

pub fn enabled() -> bool {
    std::env::var("TIERED_POC_LAG_PROBE").is_ok_and(|value| value == "true")
}

/// Run until `stop` fires, logging lag percentiles every `REPORT_EVERY` and for the whole run on
/// exit.
pub async fn run_pump_lag_probe(shard_id: u16, stop: Receiver<()>) {
    let mut window = Vec::with_capacity(16_384);
    let mut run = Vec::with_capacity(262_144);
    let mut reported_at = Instant::now();
    loop {
        let started = Instant::now();
        if compio::time::timeout(PERIOD, stop.recv()).await.is_ok() {
            break;
        }
        let lag = started.elapsed().saturating_sub(PERIOD);
        window.push(u32::try_from(lag.as_micros()).unwrap_or(u32::MAX));
        if reported_at.elapsed() >= REPORT_EVERY {
            report(shard_id, "window", &mut window);
            run.append(&mut window);
            reported_at = Instant::now();
        }
    }
    run.append(&mut window);
    report(shard_id, "run", &mut run);
}

fn report(shard_id: u16, scope: &'static str, samples: &mut [u32]) {
    let Some(&max_us) = samples.iter().max() else {
        return;
    };
    samples.sort_unstable();
    let at = |permille: usize| samples[(samples.len() - 1) * permille / 1000];
    info!(
        shard = shard_id,
        scope,
        samples = samples.len(),
        p50_us = at(500),
        p99_us = at(990),
        p999_us = at(999),
        max_us,
        "pump lag"
    );
}
