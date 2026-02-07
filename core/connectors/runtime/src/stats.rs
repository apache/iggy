/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::context::RuntimeContext;
use crate::metrics::ConnectorType;
use iggy_common::{IggyTimestamp, SemanticVersion};
use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStats};
use std::str::FromStr;
use std::sync::Arc;
use sysinfo::System;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const SEMANTIC_VERSION: SemanticVersion = SemanticVersion::parse_const(VERSION);

pub async fn get_runtime_stats(context: &Arc<RuntimeContext>) -> ConnectorRuntimeStats {
    let pid = std::process::id();

    let mut system = System::new_all();
    system.refresh_cpu_all();
    system.refresh_memory();
    system.refresh_processes(
        sysinfo::ProcessesToUpdate::Some(&[sysinfo::Pid::from_u32(pid)]),
        true,
    );

    let total_cpu_usage = system.global_cpu_usage();
    let total_memory = system.total_memory();
    let available_memory = system.available_memory();

    let (cpu_usage, memory_usage) = system
        .process(sysinfo::Pid::from_u32(pid))
        .map(|p| (p.cpu_usage(), p.memory()))
        .unwrap_or((0.0, 0));

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
        cpu_usage,
        total_cpu_usage,
        memory_usage,
        total_memory,
        available_memory,
        run_time,
        start_time: start,
        sources_total,
        sources_running,
        sinks_total,
        sinks_running,
        connectors,
    }
}
