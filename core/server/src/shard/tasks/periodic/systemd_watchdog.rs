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

use crate::shard::IggyShard;
use crate::shard::systemd;
use iggy_common::IggyError;
use std::rc::Rc;
use tracing::info;

pub fn spawn_systemd_watchdog(shard: Rc<IggyShard>) {
    let Some(timeout) = sd_notify::watchdog_enabled() else {
        return;
    };

    let interval = timeout / 2;
    info!(
        "Systemd watchdog enabled, pinging every {}s (timeout: {}s).",
        interval.as_secs(),
        timeout.as_secs()
    );

    shard
        .task_registry
        .periodic("systemd_watchdog")
        .every(interval)
        .tick(move |_shutdown| ping_watchdog())
        .spawn();
}

async fn ping_watchdog() -> Result<(), IggyError> {
    systemd::ping_watchdog();
    Ok(())
}
