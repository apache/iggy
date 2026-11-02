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

use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tokio::time::Instant;

const REPORT_FILE: &str = "chaos.jsonl";

pub(super) struct Report {
    file: File,
    path: PathBuf,
    started: Instant,
}

impl Report {
    pub(super) fn create(directory: &Path, started: Instant) -> io::Result<Self> {
        let path = directory.join(REPORT_FILE);
        let file = File::create(&path)?;
        Ok(Self {
            file,
            path,
            started,
        })
    }

    pub(super) fn record(&mut self, event: &str, details: Value) -> io::Result<()> {
        let mut record = serde_json::to_vec(&json!({
            "event": event,
            "elapsed_ms": self.started.elapsed().as_millis(),
            "details": details,
        }))
        .map_err(io::Error::other)?;
        record.push(b'\n');
        self.file.write_all(&record)
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io;

    use serde_json::{Value, json};
    use tempfile::tempdir;
    use tokio::time::Instant;

    use super::Report;

    #[test]
    fn given_incomplete_run_when_report_drops_should_preserve_recorded_events() {
        let directory = tempdir().expect("create report directory");
        let mut report = Report::create(directory.path(), Instant::now()).expect("create report");
        report
            .record("metadata", json!({ "seed": 7 }))
            .expect("record metadata");
        report
            .record("kill", json!({ "node_id": 1 }))
            .expect("record fault");

        let path = report.path().to_owned();
        let before_drop = fs::read_to_string(&path).expect("read report before drop");
        drop(report);
        let after_drop = fs::read_to_string(path).expect("read report after drop");
        assert_eq!(
            before_drop, after_drop,
            "records must be written immediately"
        );

        let records: Vec<Value> = after_drop
            .lines()
            .map(|line| serde_json::from_str(line).expect("parse report event"))
            .collect();
        assert_eq!(records.len(), 2, "unfinished runs must retain both events");
        assert_eq!(records[0]["event"], "metadata");
        assert_eq!(records[0]["details"]["seed"], 7);
        assert_eq!(records[1]["event"], "kill");
        assert_eq!(records[1]["details"]["node_id"], 1);
        assert!(
            records[0]["elapsed_ms"].as_u64().expect("metadata time")
                <= records[1]["elapsed_ms"].as_u64().expect("fault time"),
            "event times must preserve execution order"
        );
    }

    #[test]
    fn given_multiline_details_when_recorded_should_preserve_jsonl_framing() {
        let directory = tempdir().expect("create report directory");
        let mut report = Report::create(directory.path(), Instant::now()).expect("create report");
        let details = json!({ "message": "node \"1\" failed\nsecond line\r\nthird line" });
        report
            .record("failure", details.clone())
            .expect("record multiline failure");

        let contents = fs::read_to_string(report.path()).expect("read report");
        assert_eq!(
            contents.lines().count(),
            1,
            "one event must occupy one line"
        );
        assert!(
            contents.ends_with('\n'),
            "each event must end with a newline"
        );
        let record: Value = serde_json::from_str(&contents).expect("parse multiline failure");
        assert_eq!(record["details"], details);
    }

    #[test]
    fn given_missing_directory_when_report_created_should_return_io_error() {
        let directory = tempdir().expect("create report parent directory");
        let result = Report::create(&directory.path().join("missing"), Instant::now());
        assert_eq!(
            result.err().expect("missing directory must fail").kind(),
            io::ErrorKind::NotFound
        );
    }

    #[test]
    fn given_unwritable_file_when_recorded_should_return_io_error() {
        let directory = tempdir().expect("create report directory");
        let mut report = Report::create(directory.path(), Instant::now()).expect("create report");
        report.file = File::open(report.path()).expect("open report read-only");

        assert!(
            report.record("metadata", json!({ "seed": 7 })).is_err(),
            "write failures must reach the caller"
        );
    }
}
