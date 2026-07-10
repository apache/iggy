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

use crate::context::RuntimeContext;
use crate::metrics::ConnectorType;
use iggy_common::{IggyTimestamp, SemanticVersion};
use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStats};
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use sysinfo::{Pid, ProcessesToUpdate, System};
use system_stats::{cgroup_scoped_memory, scoped_total_cpu_usage};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const SEMANTIC_VERSION: SemanticVersion = SemanticVersion::parse_const(VERSION);

static SYSINFO: OnceLock<Mutex<System>> = OnceLock::new();

pub async fn get_runtime_stats(context: &Arc<RuntimeContext>) -> ConnectorRuntimeStats {
    let pid = std::process::id();
    let system = probe_system(pid);

    let sources = context.sources.get_all().await;
    let sinks = context.sinks.get_all().await;

    let sources_total = context.metrics.get_sources_total();
    let sinks_total = context.metrics.get_sinks_total();
    let sources_running = context.metrics.get_sources_running();
    let sinks_running = context.metrics.get_sinks_running();

    let mut connectors = Vec::with_capacity(sources.len() + sinks.len());
    for source in &sources {
        let version_semver = SemanticVersion::from_str(&source.version)
            .ok()
            .and_then(|v| v.get_numeric_version().ok());
        connectors.push(ConnectorStats {
            key: source.key.clone(),
            name: source.name.clone(),
            connector_type: "source".to_owned(),
            version: source.version.clone(),
            version_semver,
            status: source.status,
            enabled: source.enabled,
            messages_produced: Some(context.metrics.get_messages_produced(&source.key)),
            messages_sent: Some(context.metrics.get_messages_sent(&source.key)),
            messages_consumed: None,
            messages_processed: None,
            messages_filtered: Some(
                context
                    .metrics
                    .get_messages_filtered(&source.key, ConnectorType::Source),
            ),
            errors: context
                .metrics
                .get_errors(&source.key, ConnectorType::Source),
        });
    }
    for sink in &sinks {
        let version_semver = SemanticVersion::from_str(&sink.version)
            .ok()
            .and_then(|v| v.get_numeric_version().ok());
        connectors.push(ConnectorStats {
            key: sink.key.clone(),
            name: sink.name.clone(),
            connector_type: "sink".to_owned(),
            version: sink.version.clone(),
            version_semver,
            status: sink.status,
            enabled: sink.enabled,
            messages_produced: None,
            messages_sent: None,
            messages_consumed: Some(context.metrics.get_messages_consumed(&sink.key)),
            messages_processed: Some(context.metrics.get_messages_processed(&sink.key)),
            messages_filtered: Some(
                context
                    .metrics
                    .get_messages_filtered(&sink.key, ConnectorType::Sink),
            ),
            errors: context.metrics.get_errors(&sink.key, ConnectorType::Sink),
        });
    }

    let now = IggyTimestamp::now().as_micros();
    let start = context.start_time.as_micros();
    let run_time = now.saturating_sub(start);

    ConnectorRuntimeStats {
        connectors_runtime_version: VERSION.to_owned(),
        connectors_runtime_version_semver: SEMANTIC_VERSION.get_numeric_version().ok(),
        process_id: pid,
        cpu_usage: system.cpu_usage,
        total_cpu_usage: system.total_cpu_usage,
        memory_usage: system.memory_usage,
        total_memory: system.total_memory,
        available_memory: system.available_memory,
        run_time,
        start_time: start,
        sources_total,
        sources_running,
        sinks_total,
        sinks_running,
        connectors,
    }
}

struct SystemProbe {
    cpu_usage: f32,
    total_cpu_usage: f32,
    memory_usage: u64,
    total_memory: u64,
    available_memory: u64,
}

/// CPU and memory scoped to what this process may actually use. On a
/// shared host the runtime runs cpuset- or cgroup-confined; the host-wide
/// `sysinfo` numbers would report the neighbors' CPU load and a memory
/// total the runtime can never allocate, and they leak host sizing to
/// whoever can read `/stats`.
fn probe_system(pid: u32) -> SystemProbe {
    let mut system = SYSINFO
        .get_or_init(|| Mutex::new(System::new_all()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    system.refresh_cpu_all();
    system.refresh_memory();
    system.refresh_processes(ProcessesToUpdate::Some(&[Pid::from_u32(pid)]), true);

    let mut probe = SystemProbe {
        cpu_usage: 0.0,
        total_cpu_usage: scoped_total_cpu_usage(&system),
        memory_usage: 0,
        total_memory: system.total_memory(),
        available_memory: system.available_memory(),
    };

    if let Some(process) = system.process(Pid::from_u32(pid)) {
        probe.cpu_usage = process.cpu_usage();
        probe.memory_usage = process.memory();
        if let Some(memory) = cgroup_scoped_memory(&system, process) {
            probe.total_memory = memory.total;
            probe.available_memory = memory.available;
        }
    }

    probe
}
