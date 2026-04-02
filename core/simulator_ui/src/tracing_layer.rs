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

use std::collections::VecDeque;
use std::fmt::Write;
use std::sync::{Arc, Mutex};

use crate::util::recover_lock;
use tracing::Subscriber;
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

const MAX_RAW_LINES: usize = 500;

#[derive(Debug, Clone, Default)]
pub struct CapturedSimEvent {
    pub sim_event: String,
    pub replica_id: Option<u64>,
    pub view: Option<u64>,
    pub log_view: Option<u64>,
    pub op: Option<u64>,
    pub commit: Option<u64>,
    pub status: Option<String>,
    pub role: Option<String>,
    pub client_id: Option<u128>,
    pub request_id: Option<u64>,
    pub operation: Option<String>,
    pub pipeline_depth: Option<u64>,
    pub ack_from_replica: Option<u64>,
    pub ack_count: Option<u64>,
    pub quorum: Option<u64>,
    pub quorum_reached: Option<bool>,
    pub old_view: Option<u64>,
    pub new_view: Option<u64>,
    pub reason: Option<String>,
    pub action: Option<String>,
    pub target_replica: Option<u64>,
}

pub type EventBuffer = Arc<Mutex<Vec<CapturedSimEvent>>>;
pub type RawLineBuffer = Arc<Mutex<VecDeque<String>>>;

struct SimEventVisitor(CapturedSimEvent);

impl SimEventVisitor {
    fn record_unsigned(&mut self, field: &Field, value: u64) {
        match field.name() {
            "replica_id" => self.0.replica_id = Some(value),
            "view" => self.0.view = Some(value),
            "log_view" => self.0.log_view = Some(value),
            "op" => self.0.op = Some(value),
            "commit" => self.0.commit = Some(value),
            "request_id" => self.0.request_id = Some(value),
            "pipeline_depth" => self.0.pipeline_depth = Some(value),
            "ack_from_replica" => self.0.ack_from_replica = Some(value),
            "ack_count" => self.0.ack_count = Some(value),
            "quorum" => self.0.quorum = Some(value),
            "old_view" => self.0.old_view = Some(value),
            "new_view" => self.0.new_view = Some(value),
            "target_replica" => self.0.target_replica = Some(value),
            _ => {}
        }
    }
}

impl Visit for SimEventVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "cluster_id" | "namespace_raw" | "stream_id" | "topic_id" | "partition_id"
            | "action_commit" | "parent_checksum" | "prepare_checksum" => {}
            _ => self.record_unsigned(field, value),
        }
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        if field.name() == "client_id" {
            self.0.client_id = Some(value);
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        // Tracing routes u8/u16/u32 through record_i64. All fields are unsigned,
        // so negative values are unexpected — clamp to 0 defensively.
        let clamped = u64::try_from(value).unwrap_or(0);
        self.record_unsigned(field, clamped);
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "quorum_reached" {
            self.0.quorum_reached = Some(value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "sim_event" => self.0.sim_event = value.to_string(),
            "status" => self.0.status = Some(value.to_string()),
            "role" => self.0.role = Some(value.to_string()),
            "operation" => self.0.operation = Some(value.to_string()),
            "reason" => self.0.reason = Some(value.to_string()),
            "action" => self.0.action = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

pub struct SimEventLayer {
    buffer: EventBuffer,
    raw_lines: RawLineBuffer,
}

impl SimEventLayer {
    pub fn new(buffer: EventBuffer, raw_lines: RawLineBuffer) -> Self {
        Self { buffer, raw_lines }
    }
}

impl<S: Subscriber> Layer<S> for SimEventLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "iggy.sim" {
            return;
        }

        let mut visitor = SimEventVisitor(CapturedSimEvent::default());
        event.record(&mut visitor);

        if visitor.0.sim_event.is_empty() {
            return;
        }

        let line = format_raw_line(&visitor.0);

        let mut raw = recover_lock(&self.raw_lines);
        if raw.len() >= MAX_RAW_LINES {
            raw.pop_front();
        }
        raw.push_back(line);
        drop(raw);

        recover_lock(&self.buffer).push(visitor.0);
    }
}

fn format_raw_line(event: &CapturedSimEvent) -> String {
    let mut line = format!("DEBUG iggy.sim: {}", event.sim_event);
    if let Some(rid) = event.replica_id {
        let _ = write!(line, " R{rid}");
    }
    if let Some(ref action) = event.action {
        let _ = write!(line, " action={action}");
    }
    if let Some(op) = event.op {
        let _ = write!(line, " op={op}");
    }
    if let Some(ref status) = event.status {
        let _ = write!(line, " status={status}");
    }
    if let Some(ref role) = event.role {
        let _ = write!(line, " role={role}");
    }
    if let Some(view) = event.view {
        let _ = write!(line, " view={view}");
    }
    if let Some(commit) = event.commit {
        let _ = write!(line, " commit={commit}");
    }
    if let Some(ack_from) = event.ack_from_replica {
        let _ = write!(line, " ack_from=R{ack_from}");
    }
    if let Some(ack_count) = event.ack_count {
        let _ = write!(line, " acks={ack_count}");
    }
    if let Some(quorum) = event.quorum {
        let _ = write!(line, " quorum={quorum}");
    }
    if let Some(quorum_reached) = event.quorum_reached {
        let _ = write!(line, " quorum_reached={quorum_reached}");
    }
    if let Some(target) = event.target_replica {
        let _ = write!(line, " target=R{target}");
    }
    if let Some(ref reason) = event.reason {
        let _ = write!(line, " reason={reason}");
    }
    if let Some(pipeline_depth) = event.pipeline_depth {
        let _ = write!(line, " pipeline={pipeline_depth}");
    }
    line
}
