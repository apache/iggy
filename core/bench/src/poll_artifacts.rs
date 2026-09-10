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

use std::{
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::Path,
    sync::{Arc, Mutex, PoisonError},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use iggy::prelude::IggyError;
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    actors::BatchMetrics,
    args::{common::IggyBenchArgs, polling::PollingMode},
};

const CSV_HEADER: &str = "actor_id,sequence,elapsed_us,poll_latency_us,origin_latency_us,partition_id,messages,payload_bytes,outcome,error_code";

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
    Interrupted,
}

#[derive(Clone, Copy, Debug)]
pub enum PollOutcome {
    Messages,
    Empty,
    Error,
    Timeout,
    Cancelled,
}

#[derive(Debug)]
pub struct PollRecord {
    elapsed_us: u64,
    poll_latency_us: u64,
    origin_latency_us: Option<u64>,
    partition_id: Option<u32>,
    messages: u32,
    payload_bytes: u64,
    outcome: PollOutcome,
    error_code: Option<u32>,
}

#[derive(Debug)]
pub struct PollArtifacts {
    manifest: Value,
    actors: Mutex<Vec<ActorPollRecords>>,
    producers: Mutex<Vec<ProducerSamples>>,
}

#[derive(Debug)]
struct ActorPollRecords {
    actor_id: u32,
    measurement_started_at_unix_us: u64,
    records: Vec<PollRecord>,
}

#[derive(Debug)]
pub struct ActorPollRecorder {
    actor_id: u32,
    artifacts: Arc<PollArtifacts>,
    start: Instant,
    measurement_started_at_unix_us: u64,
    pending: Option<Instant>,
    records: Vec<PollRecord>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ProducerSample {
    elapsed_us: u64,
    completed_batches: u64,
    completed_messages: u64,
    payload_bytes: u64,
}

#[derive(Debug)]
struct ProducerSamples {
    actor_id: u32,
    measurement_started_at_unix_us: u64,
    last_completion_us: u64,
    samples: Vec<ProducerSample>,
}

pub struct ProducerRecorder {
    artifacts: Arc<PollArtifacts>,
    start: Instant,
    actor: ProducerSamples,
    latest: ProducerSample,
}

impl PollArtifacts {
    pub fn new(args: &IggyBenchArgs) -> Self {
        Self {
            manifest: json!({
                "schema_version": 1,
                "benchmark_version": env!("CARGO_PKG_VERSION"),
                "started_at": chrono::Utc::now().to_rfc3339(),
                "benchmark_kind": args.kind(),
                "transport": args.transport_command(),
                "server_address": args.server_address(),
                "high_level_api": args.high_level_api,
                "requested_polling_kind": args.polling_kind,
                "requested_latency_kind": args.latency_kind,
                "resolved_polling_kind": args.resolved_polling_kind(),
                "resolved_latency_kind": if args.high_level_api {
                    json!("sdk-batch")
                } else {
                    json!(args.resolved_latency_kind())
                },
                "auto_commit": args.resolved_polling_kind() == PollingMode::Next,
                "raw_poll_capture": !args.high_level_api,
                "raw_poll_file": "polls.csv",
                "latency_unit": "microseconds",
                "elapsed_time_origin": "actor measurement start, after warmup",
                "measurement_boundary": "SDK poll call, including SDK retries and transport",
                "message_size": args.message_size().to_string(),
                "messages_per_batch": args.messages_per_batch().to_string(),
                "message_batches": args.message_batches.map(std::num::NonZeroU32::get),
                "total_data_bytes": args.total_data.map(|size| size.as_bytes_u64()),
                "rate_limit_bytes_per_second": args.rate_limit.map(|size| size.as_bytes_u64()),
                "read_amplification": args.read_amplification(),
                "warmup_time": args.warmup_time.to_string(),
                "producers": args.producers(),
                "consumers": args.consumers(),
                "streams": args.streams(),
                "partitions": args.number_of_partitions(),
                "consumer_groups": args.number_of_consumer_groups(),
                "reuse_streams": args.reuse_streams,
                "enforce_fsync": args.enforce_fsync,
                "messages_required_to_save": args.messages_required_to_save.map(std::num::NonZeroU32::get),
                "gitref_label": args.gitref(),
                "identifier": args.identifier(),
                "remark": args.remark(),
            }),
            actors: Mutex::new(Vec::new()),
            producers: Mutex::new(Vec::new()),
        }
    }

    pub fn write(&self, directory: &Path, status: RunStatus) -> io::Result<()> {
        fs::create_dir_all(directory)?;
        let actors = self.actors.lock().unwrap_or_else(PoisonError::into_inner);
        let mut output = BufWriter::new(File::create(directory.join("polls.csv"))?);
        writeln!(output, "{CSV_HEADER}")?;
        let mut counts = [0_u64; 5];
        let mut actor_counts = Vec::with_capacity(actors.len());
        for actor in actors.iter() {
            let actor_id = actor.actor_id;
            let records = &actor.records;
            let mut actor_outcomes = [0_u64; 5];
            for (sequence, record) in records.iter().enumerate() {
                write_record(&mut output, actor_id, sequence + 1, record)?;
                let index = match record.outcome {
                    PollOutcome::Messages => 0,
                    PollOutcome::Empty => 1,
                    PollOutcome::Error => 2,
                    PollOutcome::Timeout => 3,
                    PollOutcome::Cancelled => 4,
                };
                counts[index] += 1;
                actor_outcomes[index] += 1;
            }
            actor_counts.push(json!({
                "actor_id": actor_id,
                "measurement_started_at_unix_us": actor.measurement_started_at_unix_us,
                "measurement_duration_us": records.iter().map(|record| record.elapsed_us).max().unwrap_or(0),
                "outcomes": outcome_counts(actor_outcomes),
            }));
        }
        drop(actors);
        output.flush()?;
        let mut manifest = self.manifest.clone();
        manifest["status"] = json!(status);
        manifest["updated_at"] = json!(chrono::Utc::now().to_rfc3339());
        manifest["poll_outcomes"] = outcome_counts(counts);
        manifest["actors"] = json!(actor_counts);
        manifest["producers"] = self.write_producer_samples(directory)?;
        manifest["producer_samples_file"] = json!("producer-samples.csv");
        let mut output = BufWriter::new(File::create(directory.join("run-manifest.json"))?);
        serde_json::to_writer_pretty(&mut output, &manifest)?;
        writeln!(output)?;
        output.flush()
    }

    fn write_producer_samples(&self, directory: &Path) -> io::Result<Value> {
        let producers = self
            .producers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut output = BufWriter::new(File::create(directory.join("producer-samples.csv"))?);
        writeln!(
            output,
            "actor_id,elapsed_us,completed_batches,completed_messages,payload_bytes"
        )?;
        let mut summaries = Vec::with_capacity(producers.len());
        for producer in producers.iter() {
            for sample in &producer.samples {
                writeln!(
                    output,
                    "{},{},{},{},{}",
                    producer.actor_id,
                    sample.elapsed_us,
                    sample.completed_batches,
                    sample.completed_messages,
                    sample.payload_bytes
                )?;
            }
            let last = producer.samples.last().copied().unwrap_or_default();
            summaries.push(json!({
                "actor_id": producer.actor_id,
                "measurement_started_at_unix_us": producer.measurement_started_at_unix_us,
                "measurement_duration_us": last.elapsed_us,
                "last_completion_us": producer.last_completion_us,
                "completed_batches": last.completed_batches,
                "completed_messages": last.completed_messages,
                "payload_bytes": last.payload_bytes,
            }));
        }
        drop(producers);
        output.flush()?;
        Ok(json!(summaries))
    }
}

impl ProducerRecorder {
    pub fn new(actor_id: u32, artifacts: Arc<PollArtifacts>) -> Self {
        Self {
            artifacts,
            start: Instant::now(),
            actor: ProducerSamples {
                actor_id,
                measurement_started_at_unix_us: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, micros),
                last_completion_us: 0,
                samples: vec![ProducerSample::default()],
            },
            latest: ProducerSample::default(),
        }
    }

    pub fn record_batch(&mut self, batch: &BatchMetrics) {
        self.latest.elapsed_us = micros(self.start.elapsed());
        self.latest.completed_batches += 1;
        self.latest.completed_messages += u64::from(batch.messages);
        self.latest.payload_bytes += batch.user_data_bytes;
        self.actor.last_completion_us = self.latest.elapsed_us;
        if self.latest.elapsed_us.saturating_sub(
            self.actor
                .samples
                .last()
                .map_or(0, |sample| sample.elapsed_us),
        ) >= 100_000
        {
            self.actor.samples.push(self.latest);
        }
    }
}

impl Drop for ProducerRecorder {
    fn drop(&mut self) {
        self.latest.elapsed_us = micros(self.start.elapsed());
        self.actor.samples.push(self.latest);
        self.artifacts
            .producers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(ProducerSamples {
                actor_id: self.actor.actor_id,
                measurement_started_at_unix_us: self.actor.measurement_started_at_unix_us,
                last_completion_us: self.actor.last_completion_us,
                samples: std::mem::take(&mut self.actor.samples),
            });
    }
}

impl ActorPollRecorder {
    pub fn new(actor_id: u32, artifacts: Arc<PollArtifacts>, capacity: usize) -> Self {
        Self {
            actor_id,
            artifacts,
            start: Instant::now(),
            measurement_started_at_unix_us: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, micros),
            pending: None,
            records: Vec::with_capacity(capacity.min(1_000_000)),
        }
    }

    pub fn begin_poll(&mut self) {
        self.pending = Some(Instant::now());
    }

    pub fn complete_poll(
        &mut self,
        poll_latency_us: u64,
        origin_latency_us: Option<u64>,
        partition_id: u32,
        messages: u32,
        payload_bytes: u64,
    ) {
        self.pending = None;
        self.records.push(PollRecord {
            elapsed_us: micros(self.start.elapsed()),
            poll_latency_us,
            origin_latency_us,
            partition_id: Some(partition_id),
            messages,
            payload_bytes,
            outcome: if messages == 0 {
                PollOutcome::Empty
            } else {
                PollOutcome::Messages
            },
            error_code: None,
        });
    }

    pub fn fail_poll(&mut self, error: &IggyError, poll_latency_us: u64) {
        self.pending = None;
        self.records.push(PollRecord {
            elapsed_us: micros(self.start.elapsed()),
            poll_latency_us,
            origin_latency_us: None,
            partition_id: None,
            messages: 0,
            payload_bytes: 0,
            outcome: if matches!(
                error,
                IggyError::TaskTimeout | IggyError::BackgroundSendTimeout
            ) {
                PollOutcome::Timeout
            } else {
                PollOutcome::Error
            },
            error_code: Some(error.as_code()),
        });
    }
}

impl Drop for ActorPollRecorder {
    fn drop(&mut self) {
        if let Some(start) = self.pending.take() {
            self.records.push(PollRecord {
                elapsed_us: micros(self.start.elapsed()),
                poll_latency_us: micros(start.elapsed()),
                origin_latency_us: None,
                partition_id: None,
                messages: 0,
                payload_bytes: 0,
                outcome: PollOutcome::Cancelled,
                error_code: None,
            });
        }
        self.artifacts
            .actors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(ActorPollRecords {
                actor_id: self.actor_id,
                measurement_started_at_unix_us: self.measurement_started_at_unix_us,
                records: std::mem::take(&mut self.records),
            });
    }
}

fn write_record(
    output: &mut impl Write,
    actor_id: u32,
    sequence: usize,
    record: &PollRecord,
) -> io::Result<()> {
    let outcome = match record.outcome {
        PollOutcome::Messages => "messages",
        PollOutcome::Empty => "empty",
        PollOutcome::Error => "error",
        PollOutcome::Timeout => "timeout",
        PollOutcome::Cancelled => "cancelled",
    };
    writeln!(
        output,
        "{actor_id},{sequence},{},{},{},{},{},{},{outcome},{}",
        record.elapsed_us,
        record.poll_latency_us,
        record
            .origin_latency_us
            .map_or_else(String::new, |value| value.to_string()),
        record
            .partition_id
            .map_or_else(String::new, |value| value.to_string()),
        record.messages,
        record.payload_bytes,
        record
            .error_code
            .map_or_else(String::new, |value| value.to_string())
    )
}

fn outcome_counts(counts: [u64; 5]) -> Value {
    json!({
        "messages": counts[0], "empty": counts[1], "errors": counts[2],
        "timeouts": counts[3], "cancelled": counts[4],
    })
}

fn micros(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{fs, future::pending, sync::Arc, time::Duration};

    use tokio::{sync::oneshot, task::JoinSet};

    use clap::Parser;
    use iggy::prelude::IggyError;
    use serde_json::Value;

    use super::{ActorPollRecorder, PollArtifacts, ProducerRecorder, RunStatus};
    use crate::actors::BatchMetrics;
    use crate::args::common::IggyBenchArgs;

    #[test]
    fn raw_output_retains_outcomes_and_cancelled_poll() {
        let args = IggyBenchArgs::try_parse_from([
            "iggy-bench",
            "--polling-kind",
            "next",
            "--latency-kind",
            "poll",
            "pinned-consumer",
            "tcp",
        ])
        .unwrap();
        let artifacts = Arc::new(PollArtifacts::new(&args));
        let directory =
            std::env::temp_dir().join(format!("iggy-poll-artifacts-{}", uuid::Uuid::new_v4()));
        artifacts.write(&directory, RunStatus::Running).unwrap();
        {
            let mut recorder = ActorPollRecorder::new(7, artifacts.clone(), 5);
            recorder.begin_poll();
            recorder.complete_poll(12, Some(34), 0, 2, 128);
            recorder.begin_poll();
            recorder.complete_poll(5, None, 0, 0, 0);
            recorder.begin_poll();
            recorder.fail_poll(&IggyError::CannotReadFile, 8);
            recorder.begin_poll();
            recorder.fail_poll(&IggyError::TaskTimeout, 20);
            recorder.begin_poll();
        }
        artifacts.write(&directory, RunStatus::Interrupted).unwrap();
        let csv = fs::read_to_string(directory.join("polls.csv")).unwrap();
        let rows: Vec<_> = csv.lines().collect();
        assert_eq!(rows.len(), 6);
        assert!(rows[1].contains(",12,34,0,2,128,messages,"));
        assert!(rows[2].contains(",5,,0,0,0,empty,"));
        assert!(rows[3].contains(",error,"));
        assert!(rows[4].contains(",timeout,"));
        assert!(rows[5].contains(",cancelled,"));
        let manifest: Value =
            serde_json::from_str(&fs::read_to_string(directory.join("run-manifest.json")).unwrap())
                .unwrap();
        assert_eq!(manifest["status"], "interrupted");
        assert_eq!(manifest["resolved_polling_kind"], "next");
        assert_eq!(manifest["resolved_latency_kind"], "poll");
        assert_eq!(manifest["auto_commit"], true);
        assert!(
            manifest["actors"][0]["measurement_started_at_unix_us"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(manifest["actors"][0]["measurement_duration_us"].is_u64());
        for key in ["messages", "empty", "errors", "timeouts", "cancelled"] {
            assert_eq!(manifest["poll_outcomes"][key], 1);
        }
        assert!(manifest.get("password").is_none());
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn actor_shutdown_preserves_samples_and_inflight_poll() {
        let args = IggyBenchArgs::try_parse_from(["iggy-bench", "pinned-consumer", "tcp"]).unwrap();
        let artifacts = Arc::new(PollArtifacts::new(&args));
        let actor_artifacts = artifacts.clone();
        let (ready_sender, ready_receiver) = oneshot::channel();
        let mut actors = JoinSet::new();
        actors.spawn(async move {
            let mut recorder = ActorPollRecorder::new(3, actor_artifacts, 2);
            recorder.begin_poll();
            recorder.complete_poll(11, None, 0, 1, 64);
            recorder.begin_poll();
            ready_sender.send(()).unwrap();
            pending::<()>().await;
            drop(recorder);
        });
        ready_receiver.await.unwrap();
        actors.shutdown().await;
        let recorded = artifacts.actors.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].actor_id, 3);
        assert_eq!(recorded[0].records.len(), 2);
        assert!(matches!(
            recorded[0].records[0].outcome,
            super::PollOutcome::Messages
        ));
        assert!(matches!(
            recorded[0].records[1].outcome,
            super::PollOutcome::Cancelled
        ));
        drop(recorded);
    }

    #[test]
    fn producer_samples_survive_interruption_with_completed_counters() {
        let args = IggyBenchArgs::try_parse_from(["iggy-bench", "pinned-producer", "tcp"]).unwrap();
        let artifacts = Arc::new(PollArtifacts::new(&args));
        {
            let mut recorder = ProducerRecorder::new(2, artifacts.clone());
            let batch = BatchMetrics {
                messages: 3,
                user_data_bytes: 192,
                total_bytes: 288,
                latency: Duration::from_micros(50),
            };
            recorder.record_batch(&batch);
            recorder.record_batch(&batch);
        }
        let directory =
            std::env::temp_dir().join(format!("iggy-producer-artifacts-{}", uuid::Uuid::new_v4()));
        artifacts.write(&directory, RunStatus::Interrupted).unwrap();
        let manifest: Value =
            serde_json::from_str(&fs::read_to_string(directory.join("run-manifest.json")).unwrap())
                .unwrap();
        assert_eq!(manifest["status"], "interrupted");
        let producer = &manifest["producers"][0];
        assert_eq!(producer["actor_id"], 2);
        assert_eq!(producer["completed_batches"], 2);
        assert_eq!(producer["completed_messages"], 6);
        assert_eq!(producer["payload_bytes"], 384);
        assert!(producer["measurement_started_at_unix_us"].as_u64().unwrap() > 0);
        assert!(
            producer["measurement_duration_us"].as_u64().unwrap()
                >= producer["last_completion_us"].as_u64().unwrap()
        );
        let samples = fs::read_to_string(directory.join("producer-samples.csv")).unwrap();
        assert!(samples.lines().nth(1).unwrap().ends_with(",0,0,0,0"));
        assert!(samples.lines().last().unwrap().ends_with(",2,6,384"));
        fs::remove_dir_all(directory).unwrap();
    }
}
