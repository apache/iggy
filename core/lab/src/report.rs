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

use crate::invariants::InvariantViolation;
use serde::Serialize;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::Duration;

pub struct ArtifactBundle<'a> {
    pub run_id: &'a str,
    pub seed: u64,
    pub scenario_name: &'a str,
    pub server_address: &'a str,
    pub transport: String,
    pub workers: u32,
    pub duration: Duration,
    pub total_ops: u64,
    pub passed: bool,
    pub violations: &'a [InvariantViolation],
    pub ops_target: Option<u64>,
    pub start_time_utc: &'a str,
    pub message_size: u32,
    pub messages_per_batch: u32,
    pub prefix: &'a str,
    pub no_fail_fast: bool,
    pub force_cleanup: bool,
    pub no_cleanup: bool,
    pub skip_post_run_verify: bool,
}

#[derive(Serialize)]
struct ConfigJson<'a> {
    run_id: &'a str,
    seed: u64,
    scenario: &'a str,
    server_address: &'a str,
    transport: &'a str,
    workers: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    ops_target: Option<u64>,
    start_time_utc: &'a str,
    message_size: u32,
    messages_per_batch: u32,
    prefix: &'a str,
    no_fail_fast: bool,
    force_cleanup: bool,
    no_cleanup: bool,
    skip_post_run_verify: bool,
}

#[derive(Serialize)]
struct SummaryJson<'a> {
    run_id: &'a str,
    passed: bool,
    total_ops: u64,
    duration_secs: f64,
    duration_human: &'a str,
    violation_count: usize,
    scenario: &'a str,
}

impl<'a> ArtifactBundle<'a> {
    pub fn write(&self, output_dir: &Path) -> io::Result<()> {
        fs::create_dir_all(output_dir)?;

        let config = ConfigJson {
            run_id: self.run_id,
            seed: self.seed,
            scenario: self.scenario_name,
            server_address: self.server_address,
            transport: &self.transport,
            workers: self.workers,
            ops_target: self.ops_target,
            start_time_utc: self.start_time_utc,
            message_size: self.message_size,
            messages_per_batch: self.messages_per_batch,
            prefix: self.prefix,
            no_fail_fast: self.no_fail_fast,
            force_cleanup: self.force_cleanup,
            no_cleanup: self.no_cleanup,
            skip_post_run_verify: self.skip_post_run_verify,
        };
        let config_path = output_dir.join("config.json");
        let file = fs::File::create(config_path)?;
        let mut config_writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut config_writer, &config).map_err(io::Error::other)?;
        config_writer.flush()?;

        let duration_human = format!("{:.1?}", self.duration);
        let summary = SummaryJson {
            run_id: self.run_id,
            passed: self.passed,
            total_ops: self.total_ops,
            duration_secs: self.duration.as_secs_f64(),
            duration_human: &duration_human,
            violation_count: self.violations.len(),
            scenario: self.scenario_name,
        };
        let summary_path = output_dir.join("summary.json");
        let file = fs::File::create(summary_path)?;
        let mut summary_writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut summary_writer, &summary).map_err(io::Error::other)?;
        summary_writer.flush()?;

        // violations.jsonl
        if !self.violations.is_empty() {
            let violations_path = output_dir.join("violations.jsonl");
            let file = fs::File::create(violations_path)?;
            let mut writer = BufWriter::new(file);
            for v in self.violations {
                serde_json::to_writer(&mut writer, v).map_err(io::Error::other)?;
                writer.write_all(b"\n")?;
            }
            writer.flush()?;
        }

        Ok(())
    }
}
