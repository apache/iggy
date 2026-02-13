/* Licensed to the Apache Software Foundation (ASF) under one
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

use crate::ops::{Op, OpOutcome};
use serde::Serialize;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};

const FLUSH_INTERVAL_ENTRIES: usize = 64;
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

pub struct WorkerTraceWriter {
    writer: BufWriter<File>,
    worker_id: u32,
    start: Instant,
    pending: usize,
    last_flush: Instant,
}

#[derive(Serialize)]
struct TraceEntry<'a> {
    seq: u64,
    t_us: u64,
    worker: u32,
    phase: &'static str,
    #[serde(flatten)]
    data: TraceData<'a>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum TraceData<'a> {
    Intent {
        op: &'a Op,
    },
    Outcome {
        result: &'static str,
        latency_us: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<&'a str>,
    },
}

impl WorkerTraceWriter {
    pub fn new(output_dir: &Path, worker_id: u32, start: Instant) -> io::Result<Self> {
        let path = output_dir.join(format!("trace-worker-{worker_id}.jsonl"));
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            worker_id,
            start,
            pending: 0,
            last_flush: Instant::now(),
        })
    }

    pub fn write_intent(&mut self, seq: u64, op: &Op) {
        let entry = TraceEntry {
            seq,
            t_us: self.start.elapsed().as_micros() as u64,
            worker: self.worker_id,
            phase: "intent",
            data: TraceData::Intent { op },
        };
        self.write_entry(&entry);
    }

    pub fn write_outcome(&mut self, seq: u64, outcome: &OpOutcome, latency: Duration) {
        let (detail, error) = match outcome {
            OpOutcome::Success { detail } => (detail.as_deref(), None),
            OpOutcome::Error { context, error, .. } => {
                (Some(context.as_str()), Some(error.as_str()))
            }
        };
        let entry = TraceEntry {
            seq,
            t_us: self.start.elapsed().as_micros() as u64,
            worker: self.worker_id,
            phase: "outcome",
            data: TraceData::Outcome {
                result: outcome.result_tag(),
                latency_us: latency.as_micros() as u64,
                detail,
                error,
            },
        };
        self.write_entry(&entry);
    }

    fn write_entry<T: Serialize>(&mut self, entry: &T) {
        // Serialization to a BufWriter; I/O errors here are non-fatal for chaos testing
        if serde_json::to_writer(&mut self.writer, entry).is_ok() {
            let _ = self.writer.write_all(b"\n");
            self.pending += 1;
            self.maybe_flush();
        }
    }

    fn maybe_flush(&mut self) {
        if self.pending >= FLUSH_INTERVAL_ENTRIES || self.last_flush.elapsed() >= FLUSH_INTERVAL {
            self.flush();
        }
    }

    pub fn flush(&mut self) {
        let _ = self.writer.flush();
        self.pending = 0;
        self.last_flush = Instant::now();
    }
}
