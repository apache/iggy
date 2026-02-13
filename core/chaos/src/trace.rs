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

//! Channel-based trace writer.
//!
//! Workers serialize trace entries and send them through a bounded mpsc channel
//! to a single background writer task. This avoids per-worker file handles and
//! blocking tokio threads on I/O flushes.

use crate::ops::{Op, OpDetail, OpOutcome};
use serde::Serialize;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const CHANNEL_CAPACITY: usize = 4096;
const FLUSH_INTERVAL_ENTRIES: usize = 64;
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// Serialized JSON line ready for writing. Pre-serialized on the sender side
/// to keep the writer's work minimal.
struct TraceMessage {
    json_line: Vec<u8>,
}

/// Per-worker sender. Cheap to clone (shares the channel).
#[derive(Clone)]
pub struct TraceSender {
    tx: mpsc::Sender<TraceMessage>,
    worker_id: u32,
    lane: &'static str,
    start: Instant,
    dropped: Arc<AtomicU64>,
}

/// Creates per-worker `TraceSender` instances that share the same channel.
pub struct TraceSenderFactory {
    tx: mpsc::Sender<TraceMessage>,
    start: Instant,
    dropped: Arc<AtomicU64>,
}

/// Background writer that drains the channel and writes to a single trace file.
pub struct TraceWriter {
    rx: mpsc::Receiver<TraceMessage>,
}

#[derive(Serialize)]
struct TraceEntry<'a> {
    seq: u64,
    t_us: u64,
    worker: u32,
    lane: &'static str,
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
        detail: Option<&'a OpDetail>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_context: Option<&'a str>,
    },
}

/// Create a trace channel pair. The factory stays with the runner to create
/// per-worker senders. The writer runs in a background task.
pub fn create_trace_channel(start: Instant) -> (TraceWriter, TraceSenderFactory) {
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let dropped = Arc::new(AtomicU64::new(0));
    let writer = TraceWriter { rx };
    let factory = TraceSenderFactory { tx, start, dropped };
    (writer, factory)
}

impl TraceSenderFactory {
    pub fn sender(&self, worker_id: u32, lane: &'static str) -> TraceSender {
        TraceSender {
            tx: self.tx.clone(),
            worker_id,
            lane,
            start: self.start,
            dropped: Arc::clone(&self.dropped),
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl TraceSender {
    pub fn write_intent(&self, seq: u64, op: &Op) {
        let entry = TraceEntry {
            seq,
            t_us: self.start.elapsed().as_micros() as u64,
            worker: self.worker_id,
            lane: self.lane,
            phase: "intent",
            data: TraceData::Intent { op },
        };
        self.send(entry);
    }

    pub fn write_outcome(&self, seq: u64, outcome: &OpOutcome, latency: Duration) {
        let (detail, error, error_context) = match outcome {
            OpOutcome::Success { detail } => (detail.as_ref(), None, None),
            OpOutcome::Error { error, context, .. } => {
                (None, Some(error.as_str()), Some(context.as_str()))
            }
        };
        let entry = TraceEntry {
            seq,
            t_us: self.start.elapsed().as_micros() as u64,
            worker: self.worker_id,
            lane: self.lane,
            phase: "outcome",
            data: TraceData::Outcome {
                result: outcome.result_tag(),
                latency_us: latency.as_micros() as u64,
                detail,
                error,
                error_context,
            },
        };
        self.send(entry);
    }

    fn send(&self, entry: TraceEntry<'_>) {
        let mut buf = match serde_json::to_vec(&entry) {
            Ok(b) => b,
            Err(_) => return,
        };
        buf.push(b'\n');

        if self.tx.try_send(TraceMessage { json_line: buf }).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl TraceWriter {
    /// Run the writer loop. Blocks until all senders are dropped (channel closed).
    /// Call this in `tokio::task::spawn_blocking` or a dedicated thread.
    pub fn run(mut self, path: &Path) -> std::io::Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        let mut pending = 0usize;
        let mut last_flush = Instant::now();

        while let Some(msg) = self.rx.blocking_recv() {
            let _ = writer.write_all(&msg.json_line);
            pending += 1;

            // Drain any immediately available entries before flushing
            while pending < FLUSH_INTERVAL_ENTRIES {
                match self.rx.try_recv() {
                    Ok(msg) => {
                        let _ = writer.write_all(&msg.json_line);
                        pending += 1;
                    }
                    Err(_) => break,
                }
            }

            if pending >= FLUSH_INTERVAL_ENTRIES || last_flush.elapsed() >= FLUSH_INTERVAL {
                let _ = writer.flush();
                pending = 0;
                last_flush = Instant::now();
            }
        }

        // Final flush + sync
        let _ = writer.flush();
        if let Ok(file) = writer.into_inner() {
            let _ = file.sync_all();
        }

        Ok(())
    }
}
