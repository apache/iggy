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

//! The compare page's configuration diff: one row per workload field and per environment fact, one
//! column per run.

use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::identity::WorkloadField;
use bench_dashboard_shared::summary::BenchmarkSummary;

use super::report_of;
use crate::components::run_labels::{MachineName, nodes_text, version_tag};
use crate::pages::run::facts::{
    BENCH_HOST_LABELS, MACHINE_LABEL, TOPOLOGY_LABEL, bench_host_values, field_value,
};
use crate::pages::run::history::NO_VALUE;

const VERSION_LABEL: &str = "Version";
const COMMIT_LABEL: &str = "Commit";

#[derive(Debug, Clone, PartialEq)]
pub struct SettingRow {
    pub label: String,
    /// One per run, the baseline first.
    pub values: Vec<String>,
}

impl SettingRow {
    /// A run shows a different value from the baseline.
    pub fn differs(&self) -> bool {
        self.values
            .split_first()
            .is_some_and(|(baseline, others)| others.iter().any(|value| value != baseline))
    }
}

/// The workload fields, the environment, and the bench host once the reports load. The rows that
/// differ from the baseline come first. The equal rows follow, so the reader also sees what the
/// runs share. Both parts keep the run page's order. Official runs don't record a commit, so the
/// commit row needs a run that records one.
pub fn setting_rows(
    runs: &[&BenchmarkSummary],
    reports: Option<&[BenchmarkReportLight]>,
) -> Vec<SettingRow> {
    let row = |label: &str, value_of: &dyn Fn(&BenchmarkSummary) -> String| SettingRow {
        label: label.to_owned(),
        values: runs.iter().map(|run| value_of(run)).collect(),
    };
    let workload = WorkloadField::ALL.into_iter().map(|field| {
        row(&field.to_string(), &|run| {
            field_value(field, &run.workload_key).text
        })
    });
    let environment = [
        row(MACHINE_LABEL, &|run| MachineName(&run.machine).to_string()),
        row(TOPOLOGY_LABEL, &|run| nodes_text(run.nodes)),
        row(VERSION_LABEL, &version_tag),
    ];
    let commit = runs.iter().any(|run| run.commit.is_some()).then(|| {
        row(COMMIT_LABEL, &|run| {
            run.commit.clone().unwrap_or_else(|| NO_VALUE.to_owned())
        })
    });
    let host = reports.into_iter().flat_map(|reports| {
        let hosts: Vec<_> = runs
            .iter()
            .map(|run| report_of(run, reports).map(|report| bench_host_values(&report.hardware)))
            .collect();
        BENCH_HOST_LABELS
            .into_iter()
            .enumerate()
            .map(move |(position, label)| SettingRow {
                label: label.to_owned(),
                values: hosts
                    .iter()
                    .map(|host| {
                        host.as_ref()
                            .and_then(|host| host[position].clone())
                            .unwrap_or_else(|| NO_VALUE.to_owned())
                    })
                    .collect(),
            })
    });
    let (differing, equal): (Vec<_>, Vec<_>) = workload
        .chain(environment)
        .chain(commit)
        .chain(host)
        .partition(SettingRow::differs);
    differing.into_iter().chain(equal).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        CANDIDATE, COMMIT, MACHINE, RELEASE, build, limited, producer, rate_limited, run,
    };
    use bench_report::numeric_parameter::BenchmarkNumericParameter;
    use uuid::Uuid;

    const OTHER_MACHINE: &str = "aws_i4i_4xlarge";
    const COMMIT_DATE: &str = "2026-11-02T10:00:00Z";

    fn labels(rows: &[SettingRow]) -> Vec<&str> {
        rows.iter().map(|row| row.label.as_str()).collect()
    }

    fn values<'a>(rows: &'a [SettingRow], label: &str) -> Option<&'a [String]> {
        rows.iter()
            .find(|row| row.label == label)
            .map(|row| row.values.as_slice())
    }

    fn report(id: u128, cpu: &str) -> BenchmarkReportLight {
        let mut report = BenchmarkReportLight {
            uuid: Uuid::from_u128(id),
            ..BenchmarkReportLight::default()
        };
        report.hardware.cpu_name = cpu.to_owned();
        report
    }

    #[test]
    fn given_runs_with_other_size_and_machine_when_diffed_should_put_differing_rows_first() {
        let base = limited(1, RELEASE);
        let mut params = rate_limited(producer());
        params.message_size = BenchmarkNumericParameter::Value(100);
        let other = run(2, params, OTHER_MACHINE, RELEASE);
        let message_size = WorkloadField::MessageSize.to_string();

        let rows = setting_rows(&[&base, &other], None);

        assert_eq!(labels(&rows)[..2], [message_size.as_str(), MACHINE_LABEL]);
        assert!(rows[..2].iter().all(SettingRow::differs));
        assert!(rows[2..].iter().all(|row| !row.differs()));
        // Every field and the environment stay, so the reader sees what the runs share.
        assert_eq!(rows.len(), WorkloadField::ALL.len() + 3);
        assert_eq!(labels(&rows)[2], WorkloadField::Kind.to_string());
    }

    #[test]
    fn given_official_runs_when_diffed_should_leave_out_commit_row() {
        let base = limited(1, RELEASE);
        let other = limited(2, CANDIDATE);

        let rows = setting_rows(&[&base, &other], None);

        assert_eq!(values(&rows, COMMIT_LABEL), None);
        assert!(values(&rows, VERSION_LABEL).is_some_and(|versions| versions[0] != versions[1]));
    }

    #[test]
    fn given_build_with_commit_when_diffed_should_show_commit_row_with_dash_for_release() {
        let base = limited(1, RELEASE);
        let other = build(2, COMMIT, COMMIT_DATE);

        let rows = setting_rows(&[&base, &other], None);

        let commits = values(&rows, COMMIT_LABEL).unwrap_or_default();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0], NO_VALUE);
        assert_ne!(commits[1], NO_VALUE);
    }

    #[test]
    fn given_loaded_reports_when_diffed_should_add_bench_host_rows_by_run() {
        let base = limited(1, RELEASE);
        let other = run(2, rate_limited(producer()), MACHINE, CANDIDATE);
        // In a different order from the runs, the way the page fetches them.
        let reports = [report(2, "AMD EPYC 9R14"), report(1, "Intel Xeon 8375C")];

        let rows = setting_rows(&[&base, &other], Some(&reports));

        assert_eq!(
            values(&rows, BENCH_HOST_LABELS[0]),
            Some(["Intel Xeon 8375C".to_owned(), "AMD EPYC 9R14".to_owned()].as_slice())
        );
        let memory = values(&rows, BENCH_HOST_LABELS[2]).unwrap_or_default();
        assert_eq!(memory, [NO_VALUE, NO_VALUE]);
        assert!(
            labels(&rows)
                .iter()
                .take(2)
                .any(|label| *label == BENCH_HOST_LABELS[0])
        );
    }
}
