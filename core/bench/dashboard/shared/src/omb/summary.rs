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

//! The summary of one OMB run that the OMB run list sends.

use std::fmt;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::identity::{RunVersion, VersionOrder};
use crate::omb::id::OmbRunId;
use crate::omb::result::{OmbPercentiles, OmbResult};
use crate::omb::setup::{OmbEnvironment, OmbSetup};
use crate::omb::workload::OmbWorkloadKey;
use crate::omb::{OMB_WINDOW_SECONDS, mebibytes_per_second};

/// The time stamp that ends a default OMB file name, as the `SimpleDateFormat` of OMB writes it.
const FILE_NAME_TIME_FORMAT: &str = "%Y-%m-%d-%H-%M-%S";
/// The length of a time stamp in [`FILE_NAME_TIME_FORMAT`], for example `2026-11-06-13-26-21`.
const FILE_NAME_TIME_LENGTH: usize = 19;
const RESULT_EXTENSION: &str = ".json";
const DISPLAY_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// Identity, workload, rates, latencies and health of one OMB run, with its setup sidecar if it
/// has one. The quantile maps and the per-window series stay out, so the list stays small.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OmbRunSummary {
    pub id: OmbRunId,
    /// The name of the result file, without its directory.
    pub file_name: String,
    /// The workload file base name, from a file name of the OMB default form
    /// `<workload file>-<driver>-<yyyy-MM-dd-HH-mm-ss>.json`. `None` for another name, which OMB
    /// writes with `--output`.
    pub workload_file: Option<String>,
    pub time: OmbRunTime,
    /// The `name` from the driver file. It is the only label of the driver configuration, for
    /// example `Iggy` or `Iggy-persisted`.
    pub driver: String,
    pub workload: OmbWorkloadKey,
    /// Windows of [`OMB_WINDOW_SECONDS`] in the test phase.
    pub windows: usize,
    /// The window count times [`OMB_WINDOW_SECONDS`]. OMB doesn't record the warm-up.
    pub length_seconds: u64,
    /// The mean over the windows.
    pub publish_messages_per_second: f64,
    /// The mean over the windows, over all subscriptions.
    pub consume_messages_per_second: f64,
    /// [`Self::publish_messages_per_second`] times the message size, in MiB/s.
    pub publish_mebibytes_per_second: f64,
    /// [`Self::consume_messages_per_second`] times the message size, in MiB/s.
    pub consume_mebibytes_per_second: f64,
    pub publish_latency_ms: OmbPercentiles,
    /// `None` if the run didn't measure end-to-end latency, for example without consumers.
    pub end_to_end_latency_ms: Option<OmbPercentiles>,
    /// `None` for a file that doesn't record the publish delay.
    pub publish_delay_p99_us: Option<f64>,
    /// Failed sends, from the error rate of each window times its length. `None` for a file that
    /// doesn't record errors.
    pub publish_errors: Option<u64>,
    /// Messages sent but not yet received at the end of the last window. 0 without windows.
    pub backlog_last: i64,
    /// The largest backlog at the end of a window. 0 without windows.
    pub backlog_max: i64,
    pub badges: Vec<OmbBadge>,
    /// The Iggy build from `[server]` of the sidecar. `None` without `[server] version`.
    pub version: Option<OmbVersion>,
    /// Version order of runs, as for iggy-bench runs. Without a version, the runs sort by time.
    pub version_order: VersionOrder,
    pub setup: Option<OmbSetup>,
}

/// When the run ran.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmbRunTime {
    /// For ordering. A file name time has no zone, so it counts as UTC here.
    pub instant: DateTime<Utc>,
    /// For display, for example `2026-11-06 13:22:01 UTC`. A file name time shows as written,
    /// without a zone.
    pub text: String,
    pub source: OmbTimeSource,
}

/// Where the run time comes from, in the order of preference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OmbTimeSource {
    /// `timestamp` of the sidecar: the start of the run, with a zone.
    Sidecar,
    /// The time stamp of a default file name: the end of the run, when OMB wrote the file, in the
    /// local time of the OMB process with no zone.
    FileName,
    /// The modification time of a file with another name: the end of the run, unless the file was
    /// copied without its time.
    FileModified,
}

/// The Iggy build that a run measured, read from `[server]` of the sidecar as iggy-bench reads
/// its `--gitref` and server version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmbVersion {
    /// For display, for example `0.10.0-rc.1` or `0.10.0 at 91e020636`.
    pub text: String,
    pub commit: Option<String>,
    /// The `gitref` is a release tag `server-X.Y.Z`.
    pub release: bool,
}

/// A warning that a run's numbers describe a different load from the one it asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OmbBadge {
    /// A window of the run has publish errors.
    PublishErrors,
}

/// The answer of `GET /api/omb/runs`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OmbRunList {
    /// `false` if the server has no OMB results directory, so the list page can tell "no
    /// directory" from "no results yet".
    pub enabled: bool,
    pub runs: Vec<OmbRunSummary>,
}

/// The runs of one workload and driver on one environment. They line up across versions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OmbHistoryKey {
    pub workload: OmbWorkloadKey,
    pub driver: String,
    /// Without the label. `None` for a run whose sidecar doesn't tell its machines.
    pub environment: Option<OmbEnvironment>,
}

/// The parts of a default OMB file name.
struct DefaultFileName<'a> {
    workload_file: Option<&'a str>,
    time: NaiveDateTime,
}

impl OmbRunSummary {
    /// Derives every value of the summary in one place. `file_name` is the name of the result
    /// file without its directory, and `modified` is its modification time. The run time comes
    /// from the sidecar, else from a default file name, else from `modified`. The driver comes
    /// from the content, because a `-` can be in both the workload file name and the driver name.
    pub fn new(
        id: OmbRunId,
        file_name: &str,
        modified: DateTime<Utc>,
        result: &OmbResult,
        setup: Option<OmbSetup>,
    ) -> Self {
        let default_name = DefaultFileName::parse(file_name, &result.driver);
        let time = OmbRunTime::new(
            setup.as_ref(),
            default_name.as_ref().map(|name| name.time),
            modified,
        );
        let server = setup.as_ref().and_then(|setup| setup.server.as_ref());
        let run_version = server.and_then(|server| {
            Some(RunVersion::new(
                server.gitref.as_deref(),
                server.version.as_deref()?,
            ))
        });
        let version_order = VersionOrder::new(
            run_version
                .as_ref()
                .and_then(|version| version.semver.clone()),
            server.and_then(|server| server.gitref_date.as_deref()),
            &time.instant.to_rfc3339(),
        );
        let windows = result.publish_rate.len();
        let publish_messages_per_second = mean(&result.publish_rate);
        let consume_messages_per_second = mean(&result.consume_rate);
        Self {
            id,
            file_name: file_name.to_owned(),
            workload_file: default_name
                .and_then(|name| name.workload_file)
                .map(str::to_owned),
            time,
            driver: result.driver.clone(),
            workload: OmbWorkloadKey::from(result),
            windows,
            length_seconds: windows as u64 * OMB_WINDOW_SECONDS,
            publish_messages_per_second,
            consume_messages_per_second,
            publish_mebibytes_per_second: mebibytes_per_second(
                publish_messages_per_second,
                result.message_size,
            ),
            consume_mebibytes_per_second: mebibytes_per_second(
                consume_messages_per_second,
                result.message_size,
            ),
            publish_latency_ms: result.publish_latency(),
            end_to_end_latency_ms: result.end_to_end_latency(),
            publish_delay_p99_us: result.publish_delay_latency().map(|delay| delay.p99),
            publish_errors: publish_errors(result),
            backlog_last: result.backlog.last().copied().unwrap_or_default(),
            backlog_max: result.backlog.iter().copied().max().unwrap_or_default(),
            badges: OmbBadge::of_run(result),
            version: run_version.map(|version| OmbVersion {
                text: version.text,
                commit: version.commit,
                release: version.release,
            }),
            version_order,
            setup,
        }
    }

    /// The environment from the sidecar, with its label.
    pub fn environment(&self) -> Option<&OmbEnvironment> {
        self.setup
            .as_ref()
            .and_then(|setup| setup.environment.as_ref())
    }

    pub fn history_key(&self) -> OmbHistoryKey {
        OmbHistoryKey {
            workload: self.workload.clone(),
            driver: self.driver.clone(),
            environment: self
                .environment()
                .filter(|environment| environment.is_known())
                .map(OmbEnvironment::without_label),
        }
    }
}

impl OmbRunTime {
    fn new(
        setup: Option<&OmbSetup>,
        file_name_time: Option<NaiveDateTime>,
        modified: DateTime<Utc>,
    ) -> Self {
        let start = setup
            .and_then(|setup| setup.timestamp.as_deref())
            .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok());
        if let Some(start) = start {
            let zone = if start.offset().local_minus_utc() == 0 {
                "UTC".to_owned()
            } else {
                start.format("%:z").to_string()
            };
            return Self {
                instant: start.with_timezone(&Utc),
                text: format!("{} {zone}", start.format(DISPLAY_TIME_FORMAT)),
                source: OmbTimeSource::Sidecar,
            };
        }
        if let Some(end) = file_name_time {
            return Self {
                instant: end.and_utc(),
                text: end.format(DISPLAY_TIME_FORMAT).to_string(),
                source: OmbTimeSource::FileName,
            };
        }
        Self {
            instant: modified,
            text: format!("{} UTC", modified.format(DISPLAY_TIME_FORMAT)),
            source: OmbTimeSource::FileModified,
        }
    }
}

impl OmbTimeSource {
    /// Whether the time is the start of the run. The other sources give its end.
    pub const fn is_start(self) -> bool {
        matches!(self, Self::Sidecar)
    }
}

impl OmbBadge {
    /// One plain sentence for a reader who doesn't know OMB.
    pub const fn explanation(self) -> &'static str {
        match self {
            Self::PublishErrors => {
                "Some sends of this run failed, so the server handled less load than the run \
                 asked for."
            }
        }
    }

    fn of_run(result: &OmbResult) -> Vec<Self> {
        let errors = result.publish_error_rate.iter().any(|rate| *rate > 0.0);
        [(errors, Self::PublishErrors)]
            .into_iter()
            .filter_map(|(applies, badge)| applies.then_some(badge))
            .collect()
    }
}

impl fmt::Display for OmbTimeSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Sidecar => "start of the run",
            Self::FileName => "end of the run, from the file name",
            Self::FileModified => "end of the run, from the file modification time",
        })
    }
}

impl fmt::Display for OmbBadge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PublishErrors => "publish errors",
        })
    }
}

impl<'a> DefaultFileName<'a> {
    /// `None` unless the name ends with `-<driver>-<yyyy-MM-dd-HH-mm-ss>.json`.
    fn parse(file_name: &'a str, driver: &str) -> Option<Self> {
        let stem = file_name.strip_suffix(RESULT_EXTENSION)?;
        let split = stem.len().checked_sub(FILE_NAME_TIME_LENGTH)?;
        let time = NaiveDateTime::parse_from_str(stem.get(split..)?, FILE_NAME_TIME_FORMAT).ok()?;
        let workload_file = stem
            .get(..split)?
            .strip_suffix('-')?
            .strip_suffix(driver)?
            .strip_suffix('-')?;
        Some(Self {
            workload_file: (!workload_file.is_empty()).then_some(workload_file),
            time,
        })
    }
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn publish_errors(result: &OmbResult) -> Option<u64> {
    let rates = &result.publish_error_rate;
    (!rates.is_empty())
        .then(|| (rates.iter().sum::<f64>() * OMB_WINDOW_SECONDS as f64).round() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::omb::setup::OmbSidecar;
    use crate::omb::test_fixtures::{
        ALL, EDGE8_100_PARTITIONS, EDGE8_DEFAULT, NO_CONSUMERS, PUBLISH_ERRORS, RC1_DEFAULT,
        RC1_PERSISTED, assert_close, assert_round_trip, modified, without_fields,
    };
    use semver::Version;
    use std::collections::HashMap;

    fn summary_named(file_name: &str, setup: Option<OmbSetup>) -> OmbRunSummary {
        OmbRunSummary::new(
            OmbRunId::of_file(NO_CONSUMERS.json),
            file_name,
            modified(),
            &NO_CONSUMERS.result(),
            setup,
        )
    }

    fn setup(text: &str) -> OmbSetup {
        text.parse::<OmbSidecar>()
            .expect("the sidecar parses")
            .setup
    }

    fn utc(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .expect("the test time parses")
            .with_timezone(&Utc)
    }

    #[test]
    fn given_every_fixture_when_summarized_should_match_numbers_of_its_file() {
        // Windows, mean publish rate, publish MiB/s, aggregated publish P99, from the files.
        let expected = [
            (
                &RC1_DEFAULT,
                18,
                150_208.107_768_000_77,
                143.249_614_494_324_48,
                1.523,
            ),
            (
                &RC1_PERSISTED,
                18,
                150_159.745_209_258_66,
                143.203_492_364_176_44,
                3.465,
            ),
            (
                &EDGE8_DEFAULT,
                18,
                150_161.547_870_740_64,
                143.205_211_516_132_98,
                1.519,
            ),
            (
                &EDGE8_100_PARTITIONS,
                30,
                122_094.416_256_637_57,
                119.232_828_375_622_63,
                1.89,
            ),
            (
                &NO_CONSUMERS,
                18,
                75_085.426_886_982_81,
                71.607_043_158_514_8,
                1.493,
            ),
            (
                &PUBLISH_ERRORS,
                18,
                73_741.851_882_661_05,
                70.325_710_184_727_73,
                7.357,
            ),
        ];

        for (fixture, windows, publish_rate, mebibytes, p99) in expected {
            let summary = fixture.summary();

            assert_eq!(summary.windows, windows, "{}", fixture.file_name);
            assert_eq!(summary.length_seconds, windows as u64 * 10);
            assert_close(summary.publish_messages_per_second, publish_rate);
            assert_close(summary.publish_mebibytes_per_second, mebibytes);
            assert_eq!(summary.publish_latency_ms.p99, p99, "{}", fixture.file_name);
            assert_eq!(summary.file_name, fixture.file_name);
            assert_eq!(summary.id, OmbRunId::of_file(fixture.json));
        }
    }

    #[test]
    fn given_message_size_when_converting_rate_should_divide_by_mebibyte() {
        let summary = EDGE8_100_PARTITIONS.summary();

        assert_eq!(mebibytes_per_second(1024.0, 1024), 1.0);
        assert_eq!(mebibytes_per_second(1_048_576.0, 1000), 1000.0);
        assert_close(
            summary.publish_mebibytes_per_second,
            summary.publish_messages_per_second * 1024.0 / 1_048_576.0,
        );
        assert_close(summary.consume_mebibytes_per_second, 119.232_751_272_623_4);
        // The same bytes in iggy-bench megabytes would read about 4.9 % higher.
        assert!(
            summary.publish_messages_per_second * 1024.0 / 1_000_000.0
                > summary.publish_mebibytes_per_second * 1.04
        );
    }

    #[test]
    fn given_run_with_publish_errors_when_summarized_should_count_them_and_badge_run() {
        let summary = PUBLISH_ERRORS.summary();

        assert_eq!(summary.publish_errors, Some(254_517));
        assert_eq!(summary.badges, [OmbBadge::PublishErrors]);
        assert_eq!(summary.backlog_last, 71);
        assert_eq!(summary.backlog_max, 829);
        assert_eq!(summary.publish_delay_p99_us, Some(105.0));
    }

    #[test]
    fn given_runs_without_errors_when_summarized_should_count_zero_and_have_no_badge() {
        for fixture in ALL
            .iter()
            .filter(|fixture| fixture.file_name != PUBLISH_ERRORS.file_name)
        {
            let summary = fixture.summary();

            assert_eq!(summary.publish_errors, Some(0), "{}", fixture.file_name);
            assert_eq!(summary.badges, [], "{}", fixture.file_name);
        }
    }

    #[test]
    fn given_old_file_without_errors_and_delay_when_summarized_should_leave_them_unknown() {
        let old = without_fields(
            RC1_DEFAULT.json,
            &[
                "publishErrorRate",
                "publishDelayLatencyAvg",
                "aggregatedPublishDelayLatencyQuantiles",
            ],
        );
        let result = OmbResult::try_from(old.as_slice()).expect("old files parse");

        let summary = OmbRunSummary::new(
            OmbRunId::of_file(&old),
            RC1_DEFAULT.file_name,
            modified(),
            &result,
            None,
        );

        assert_eq!(summary.publish_errors, None);
        assert_eq!(summary.publish_delay_p99_us, None);
        assert_eq!(summary.badges, []);
        assert_eq!(summary.publish_latency_ms.p99, 1.523);
    }

    #[test]
    fn given_run_without_consumers_when_summarized_should_not_measure_end_to_end() {
        let summary = NO_CONSUMERS.summary();

        assert_eq!(summary.workload.consumers_per_topic, 0);
        assert_eq!(summary.end_to_end_latency_ms, None);
        assert_eq!(summary.consume_messages_per_second, 0.0);
        assert_eq!(summary.consume_mebibytes_per_second, 0.0);
        assert_eq!(summary.publish_delay_p99_us, Some(68.0));
    }

    #[test]
    fn given_runs_with_consumers_when_summarized_should_carry_end_to_end_percentiles() {
        let summary = RC1_PERSISTED.summary();

        assert_eq!(
            summary
                .end_to_end_latency_ms
                .map(|latency| (latency.p50, latency.p99, latency.max)),
            Some((2.0, 4.0, 51.0))
        );
        assert_close(summary.consume_messages_per_second, 150_159.662_909_078_06);
    }

    #[test]
    fn given_default_file_names_when_summarized_should_read_workload_file_and_time() {
        let summary = NO_CONSUMERS.summary();
        let errors = PUBLISH_ERRORS.summary();

        assert_eq!(
            summary.workload_file.as_deref(),
            Some("12-topics-1-partition-1kb-75mb-no-consumers")
        );
        assert_eq!(
            summary.time,
            OmbRunTime {
                instant: utc("2026-10-22T10:21:15Z"),
                text: "2026-10-22 10:21:15".to_owned(),
                source: OmbTimeSource::FileName,
            }
        );
        // The driver `Iggy-persisted` holds a `-`, and the content tells where it starts.
        assert_eq!(errors.driver, "Iggy-persisted");
        assert_eq!(
            errors.workload_file.as_deref(),
            Some("12-topics-1-partition-1kb-75mb")
        );
        assert_eq!(errors.time.instant, utc("2026-10-15T13:33:17Z"));
        assert!(!errors.time.source.is_start());
    }

    #[test]
    fn given_custom_or_other_names_when_summarized_should_fall_back_to_modification_time() {
        for file_name in [
            "result.json",
            "12-topics-Iggy-2026-10-22.json",
            "12-topics-Kafka-2026-10-22-10-21-15.json",
            "12-topics-Iggy-2026-13-22-10-21-15.json",
            "12-topics-Iggy-2026-10-22-10-21-15.json.bak",
            "Iggy-2026-10-22-10-21-15.json",
        ] {
            let summary = summary_named(file_name, None);

            assert_eq!(summary.workload_file, None, "{file_name}");
            assert_eq!(
                summary.time,
                OmbRunTime {
                    instant: modified(),
                    text: "2026-11-13 09:30:00 UTC".to_owned(),
                    source: OmbTimeSource::FileModified,
                },
                "{file_name}"
            );
        }
    }

    #[test]
    fn given_name_with_dashes_in_workload_and_driver_when_summarized_should_split_by_driver() {
        let mut result = NO_CONSUMERS.result();
        result.driver = "Iggy-tls-fsync".to_owned();

        let summary = OmbRunSummary::new(
            OmbRunId::of_file(NO_CONSUMERS.json),
            "1-topic-16-partitions-1kb-Iggy-tls-fsync-2026-01-02-03-04-05.json",
            modified(),
            &result,
            None,
        );

        assert_eq!(
            summary.workload_file.as_deref(),
            Some("1-topic-16-partitions-1kb")
        );
        assert_eq!(summary.time.instant, utc("2026-01-02T03:04:05Z"));
    }

    #[test]
    fn given_sidecar_time_when_summarized_should_win_over_file_name_and_show_zone() {
        let utc_start = summary_named(
            NO_CONSUMERS.file_name,
            Some(setup("timestamp = \"2026-10-22T10:18:10Z\"\n")),
        );
        let offset_start = summary_named(
            "result.json",
            Some(setup("timestamp = \"2026-10-22T12:18:10+02:00\"\n")),
        );

        assert_eq!(
            utc_start.time,
            OmbRunTime {
                instant: utc("2026-10-22T10:18:10Z"),
                text: "2026-10-22 10:18:10 UTC".to_owned(),
                source: OmbTimeSource::Sidecar,
            }
        );
        assert!(utc_start.time.source.is_start());
        assert_eq!(
            utc_start.workload_file.as_deref(),
            Some("12-topics-1-partition-1kb-75mb-no-consumers")
        );
        assert_eq!(offset_start.time.instant, utc("2026-10-22T10:18:10Z"));
        assert_eq!(offset_start.time.text, "2026-10-22 12:18:10 +02:00");
    }

    #[test]
    fn given_sidecar_without_time_when_summarized_should_use_file_name_time() {
        let summary = summary_named(NO_CONSUMERS.file_name, Some(setup("suite = \"nightly\"\n")));

        assert_eq!(summary.time.source, OmbTimeSource::FileName);
    }

    #[test]
    fn given_real_sidecars_when_summarized_should_carry_setup_version_and_start() {
        let rc1 = RC1_DEFAULT.summary();
        let edge = EDGE8_DEFAULT.summary();

        assert_eq!(
            rc1.version,
            Some(OmbVersion {
                text: "0.10.0-rc.1".to_owned(),
                commit: None,
                release: false,
            })
        );
        assert_eq!(rc1.time.instant, utc("2026-11-06T13:22:01Z"));
        assert_eq!(rc1.time.source, OmbTimeSource::Sidecar);
        assert_eq!(
            rc1.workload_file.as_deref(),
            Some("12-topics-1-partition-1kb-150mb")
        );
        assert_eq!(
            rc1.environment()
                .and_then(|environment| environment.label.as_deref()),
            Some("AWS i4i.4xlarge, 3 nodes")
        );
        assert_eq!(
            rc1.setup.as_ref().and_then(|setup| setup.suite.as_deref()),
            Some("cluster-3-nodes")
        );
        assert_eq!(
            edge.version.map(|version| version.text),
            Some("0.9.1-edge.8".to_owned())
        );
        assert_eq!(
            edge.version_order.version,
            Version::parse("0.9.1-edge.8").ok()
        );
        assert_eq!(RC1_PERSISTED.summary().driver, "Iggy-persisted");
        assert_eq!(
            EDGE8_100_PARTITIONS
                .summary()
                .environment()
                .and_then(|environment| environment.server_instance.as_deref()),
            Some("i7i.xlarge")
        );
    }

    #[test]
    fn given_runs_of_two_versions_when_sorted_by_version_should_put_edge_before_candidate() {
        let mut runs = [RC1_DEFAULT.summary(), EDGE8_DEFAULT.summary()];

        runs.sort_by(|left, right| left.version_order.cmp(&right.version_order));

        assert_eq!(
            runs.map(|run| run.version.map(|version| version.text)),
            [
                Some("0.9.1-edge.8".to_owned()),
                Some("0.10.0-rc.1".to_owned())
            ]
        );
    }

    #[test]
    fn given_server_table_with_release_tag_or_commit_when_summarized_should_read_like_iggy_bench() {
        let release = summary_named(
            "result.json",
            Some(setup(
                "[server]\nversion = \"0.9.9\"\ngitref = \"server-0.10.0\"\ngitref_date = \
                 \"2026-11-20T10:00:00Z\"\n",
            )),
        );
        let commit = summary_named(
            "result.json",
            Some(setup(
                "[server]\nversion = \"0.10.0\"\ngitref = \"91e020636a1b2c3d4e5f60718293a4b5c6d7e8f9\"\n",
            )),
        );
        let gitref_only = summary_named(
            "result.json",
            Some(setup("[server]\ngitref = \"server-0.10.0\"\n")),
        );

        assert_eq!(
            release.version,
            Some(OmbVersion {
                text: "0.10.0".to_owned(),
                commit: None,
                release: true,
            })
        );
        assert_eq!(
            release.version_order,
            VersionOrder {
                version: Version::parse("0.10.0").ok(),
                gitref_date_us: Some(utc("2026-11-20T10:00:00Z").timestamp_micros()),
                timestamp_us: Some(modified().timestamp_micros()),
            }
        );
        assert_eq!(
            commit.version.map(|version| (version.text, version.commit)),
            Some((
                "0.10.0 at 91e020636".to_owned(),
                Some("91e020636".to_owned())
            ))
        );
        assert_eq!(gitref_only.version, None);
    }

    #[test]
    fn given_run_without_sidecar_when_summarized_should_order_by_time_only() {
        let summary = NO_CONSUMERS.summary();

        assert_eq!(summary.version, None);
        assert_eq!(summary.setup, None);
        assert_eq!(summary.environment(), None);
        assert_eq!(
            summary.version_order,
            VersionOrder {
                version: None,
                gitref_date_us: None,
                timestamp_us: Some(utc("2026-10-22T10:21:15Z").timestamp_micros()),
            }
        );
    }

    #[test]
    fn given_runs_of_one_workload_when_keyed_by_history_should_split_by_driver_and_machines() {
        let rc1 = RC1_DEFAULT.summary();
        let edge = EDGE8_DEFAULT.summary();
        let persisted = RC1_PERSISTED.summary();
        let mut other_machines = RC1_DEFAULT.summary();
        other_machines.setup = EDGE8_100_PARTITIONS.setup();
        let mut relabelled = RC1_DEFAULT.summary();
        if let Some(environment) = relabelled
            .setup
            .as_mut()
            .and_then(|setup| setup.environment.as_mut())
        {
            environment.label = Some("renamed".to_owned());
        }
        let mut lines: HashMap<OmbHistoryKey, Vec<OmbRunId>> = HashMap::new();

        for run in [&rc1, &edge, &persisted, &other_machines, &relabelled] {
            lines.entry(run.history_key()).or_default().push(run.id);
        }

        assert_eq!(lines.len(), 3);
        assert_eq!(lines[&rc1.history_key()], [rc1.id, edge.id, relabelled.id]);
        assert_eq!(
            rc1.history_key()
                .environment
                .and_then(|environment| environment.label),
            None
        );
        assert_eq!(NO_CONSUMERS.summary().history_key().environment, None);
    }

    #[test]
    fn given_summary_when_serialized_should_round_trip() {
        for fixture in ALL {
            assert_round_trip(&fixture.summary());
        }
        assert_round_trip(&OmbRunList {
            enabled: true,
            runs: ALL.iter().map(|fixture| fixture.summary()).collect(),
        });
        assert!(!OmbRunList::default().enabled);
    }
}
