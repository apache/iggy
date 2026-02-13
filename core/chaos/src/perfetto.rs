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

//! Converts `trace.jsonl` to Chrome Trace Event format for Perfetto UI visualization.
//!
//! Each intent/outcome pair becomes a complete duration event ("X") with
//! `lane` mapped to process (pid) and `worker` to thread (tid).

use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter};
use std::path::Path;

pub struct ConvertStats {
    pub events: usize,
    pub unmatched_outcomes: usize,
}

struct PendingIntent {
    op_name: String,
    context: Option<String>,
    worker: u32,
    lane: String,
}

#[derive(Serialize)]
struct ChromeTrace {
    #[serde(rename = "traceEvents")]
    trace_events: Vec<ChromeEvent>,
    #[serde(rename = "displayTimeUnit")]
    display_time_unit: &'static str,
}

#[derive(Serialize)]
struct ChromeEvent {
    name: String,
    cat: String,
    ph: &'static str,
    ts: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    dur: Option<u64>,
    pid: u32,
    tid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<serde_json::Value>,
}

/// Convert `trace.jsonl` in `bundle_dir` to `trace.perfetto.json`.
pub fn convert(bundle_dir: &Path) -> io::Result<ConvertStats> {
    let input_path = bundle_dir.join("trace.jsonl");
    let output_path = bundle_dir.join("trace.perfetto.json");

    let file = File::open(&input_path)?;
    let reader = BufReader::new(file);

    let mut pending: HashMap<u64, PendingIntent> = HashMap::new();
    let mut events: Vec<ChromeEvent> = Vec::new();
    // lane_name → assigned pid
    let mut lane_pids: HashMap<String, u32> = HashMap::new();
    let mut unmatched_outcomes: u64 = 0;

    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }

        let entry: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let Some(seq) = entry["seq"].as_u64() else {
            continue;
        };
        let Some(phase) = entry["phase"].as_str() else {
            continue;
        };

        let lane = entry["lane"].as_str().unwrap_or("default");
        let worker = entry["worker"].as_u64().unwrap_or(0) as u32;

        let next_pid = lane_pids.len() as u32 + 1;
        lane_pids.entry(lane.to_owned()).or_insert(next_pid);

        match phase {
            "intent" => {
                let op_name = entry["op"]["type"]
                    .as_str()
                    .map(camel_to_snake)
                    .unwrap_or_default();
                let context = op_context(&entry["op"]);

                pending.insert(
                    seq,
                    PendingIntent {
                        op_name,
                        context,
                        worker,
                        lane: lane.to_owned(),
                    },
                );
            }
            "outcome" => {
                let latency_us = entry["latency_us"].as_u64().unwrap_or(0);
                let t_us = entry["t_us"].as_u64().unwrap_or(0);
                let start_us = t_us.saturating_sub(latency_us);
                let result = entry["result"].as_str().unwrap_or("unknown");

                if let Some(intent) = pending.remove(&seq) {
                    let pid = lane_pids[&intent.lane];

                    let mut args = serde_json::Map::new();
                    args.insert("result".into(), serde_json::Value::String(result.into()));
                    args.insert("seq".into(), serde_json::Value::Number(seq.into()));
                    if let Some(ctx) = &intent.context {
                        args.insert("context".into(), serde_json::Value::String(ctx.clone()));
                    }
                    if let Some(err) = entry["error"].as_str() {
                        args.insert("error".into(), serde_json::Value::String(err.into()));
                    }

                    events.push(ChromeEvent {
                        name: intent.op_name,
                        cat: "op".into(),
                        ph: "X",
                        ts: start_us,
                        dur: Some(latency_us),
                        pid,
                        tid: intent.worker,
                        args: Some(serde_json::Value::Object(args)),
                    });
                } else {
                    unmatched_outcomes += 1;
                }
            }
            _ => {}
        }
    }

    // Sort by start timestamp for consistent output
    events.sort_by_key(|e| e.ts);

    // Emit metadata events for lane and worker names
    let mut meta_events: Vec<ChromeEvent> = Vec::new();

    for (lane_name, pid) in &lane_pids {
        meta_events.push(ChromeEvent {
            name: "process_name".into(),
            cat: String::new(),
            ph: "M",
            ts: 0,
            dur: None,
            pid: *pid,
            tid: 0,
            args: Some(serde_json::json!({ "name": lane_name })),
        });
    }

    // Collect unique (pid, tid) pairs for thread name metadata
    let mut worker_threads: HashMap<(u32, u32), ()> = HashMap::new();
    for event in &events {
        worker_threads.entry((event.pid, event.tid)).or_default();
    }
    for (pid, tid) in worker_threads.keys() {
        meta_events.push(ChromeEvent {
            name: "thread_name".into(),
            cat: String::new(),
            ph: "M",
            ts: 0,
            dur: None,
            pid: *pid,
            tid: *tid,
            args: Some(serde_json::json!({ "name": format!("worker-{tid}") })),
        });
    }

    meta_events.append(&mut events);

    let trace = ChromeTrace {
        trace_events: meta_events,
        display_time_unit: "us",
    };

    let out_file = File::create(&output_path)?;
    let writer = BufWriter::new(out_file);
    serde_json::to_writer(writer, &trace).map_err(io::Error::other)?;

    Ok(ConvertStats {
        events: trace.trace_events.len(),
        unmatched_outcomes: unmatched_outcomes as usize,
    })
}

/// Extract a compact context label from the op's JSON fields (excluding "type").
fn op_context(op: &serde_json::Value) -> Option<String> {
    let obj = op.as_object()?;
    let mut parts = Vec::new();
    for (key, val) in obj {
        if key == "type" {
            continue;
        }
        match val {
            serde_json::Value::String(s) => parts.push(format!("{key}={s}")),
            serde_json::Value::Number(n) => parts.push(format!("{key}={n}")),
            _ => {}
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}

/// Convert CamelCase to snake_case.
fn camel_to_snake(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push(ch);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_camel_to_snake() {
        assert_eq!(camel_to_snake("CreateStream"), "create_stream");
        assert_eq!(camel_to_snake("SendMessages"), "send_messages");
        assert_eq!(camel_to_snake("GetStats"), "get_stats");
        assert_eq!(camel_to_snake("PollGroupMessages"), "poll_group_messages");
    }

    #[test]
    fn test_convert_roundtrip() {
        let dir = std::env::temp_dir().join("perfetto_test");
        let _ = std::fs::create_dir_all(&dir);

        let trace = r#"{"seq":1,"t_us":1000,"worker":0,"lane":"crud","phase":"intent","op":{"type":"CreateStream","name":"s1"}}
{"seq":1,"t_us":1200,"worker":0,"lane":"crud","phase":"outcome","result":"ok","latency_us":200}
{"seq":2,"t_us":1500,"worker":1,"lane":"data","phase":"intent","op":{"type":"SendMessages","stream":"s1","topic":"t1","partition":1,"count":10}}
{"seq":2,"t_us":1800,"worker":1,"lane":"data","phase":"outcome","result":"ok","latency_us":300}
"#;
        std::fs::write(dir.join("trace.jsonl"), trace).unwrap();

        let stats = convert(&dir).unwrap();
        assert_eq!(stats.unmatched_outcomes, 0);
        // 2 duration events + metadata events (2 lanes + 2 workers)
        assert!(stats.events >= 2);

        let output: serde_json::Value =
            serde_json::from_reader(File::open(dir.join("trace.perfetto.json")).unwrap()).unwrap();
        assert_eq!(output["displayTimeUnit"], "us");

        let events = output["traceEvents"].as_array().unwrap();
        let duration_events: Vec<_> = events.iter().filter(|e| e["ph"] == "X").collect();
        assert_eq!(duration_events.len(), 2);

        // First duration event: create_stream starting at 1000, dur 200
        assert_eq!(duration_events[0]["name"], "create_stream");
        assert_eq!(duration_events[0]["ts"], 1000);
        assert_eq!(duration_events[0]["dur"], 200);

        // Second: send_messages starting at 1500, dur 300
        assert_eq!(duration_events[1]["name"], "send_messages");
        assert_eq!(duration_events[1]["ts"], 1500);
        assert_eq!(duration_events[1]["dur"], 300);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_unmatched_outcome() {
        let dir = std::env::temp_dir().join("perfetto_test_unmatched");
        let _ = std::fs::create_dir_all(&dir);

        let trace = r#"{"seq":99,"t_us":5000,"worker":0,"lane":"crud","phase":"outcome","result":"ok","latency_us":100}
"#;
        std::fs::write(dir.join("trace.jsonl"), trace).unwrap();

        let stats = convert(&dir).unwrap();
        assert_eq!(stats.unmatched_outcomes, 1);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
