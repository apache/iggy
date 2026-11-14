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

//! The facts that an OMB run's page lists: its latency ladders, its workload, its setup and what OMB
//! leaves out, as text built from the typed fields of the summary.

use bench_dashboard_shared::omb::OMB_WINDOW_SECONDS;
use bench_dashboard_shared::omb::result::OmbPercentiles;
use bench_dashboard_shared::omb::setup::OmbSetup;
use bench_dashboard_shared::omb::summary::{OmbRunSummary, OmbTimeSource};
use bench_dashboard_shared::omb::workload::OmbWorkloadField;
use chrono::TimeDelta;

use super::change::Kpi;
use crate::components::ladder::log_bar_pcts;
use crate::components::run_labels::{messages_text, milliseconds, nodes_text};
use crate::format::group_digits;
use crate::pages::omb::common::{
    NOT_RECORDED, P99_INDEX, PERCENTILE_LABELS, length_text, percentile_values,
};
use crate::pages::run::facts::AVERAGE_LABEL;

/// What a run without a sidecar says in place of its Environment section.
pub const NO_SIDECAR_NOTE: &str = "OMB results don't record the hardware, the Iggy version or the \
                                   OMB build, and this run has no setup.toml next to its result \
                                   that adds them.";
/// What OMB leaves out of every result, setup.toml or not.
pub const UNRECORDED: [&str; 4] = [
    "The target publish rate. The workload name often states it.",
    "The warm-up length. The numbers cover the measured phase only.",
    "The subscriptions per topic, so the consume rate can be a multiple of the publish rate.",
    "The driver settings. The driver name is the only label of the driver configuration.",
];
pub const END_TO_END_TITLE: &str = "End-to-end latency";
/// The rung hints, in the order of [`PERCENTILE_LABELS`].
const RUNG_HINTS: [&str; 7] = [
    "Half of the messages took at most this long.",
    "75 percent of the messages took at most this long.",
    "95 percent of the messages took at most this long.",
    "99 percent of the messages took at most this long.",
    "99.9 percent of the messages took at most this long.",
    "99.99 percent of the messages took at most this long.",
    "The slowest message of the measured phase.",
];
/// As the summary writes a run time, without the zone.
const TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// A label and its value, for a run's fact lists. The label of a sidecar property is its key, so
/// labels are owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fact {
    pub label: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactGroup {
    pub title: String,
    pub facts: Vec<Fact>,
}

/// One rung of a latency ladder.
#[derive(Debug, Clone, PartialEq)]
pub struct LadderRow {
    pub label: &'static str,
    pub hint: &'static str,
    pub value_ms: f64,
    /// The length of the bar on a log scale, in percent of the longest bar.
    pub bar_pct: f64,
    /// The P99 rung, which the headline numbers use.
    pub key: bool,
}

/// The rungs in the order of [`PERCENTILE_LABELS`], with the bars on a log scale. As on the
/// iggy-bench run page, the average isn't a rung, and the side panel shows it.
pub fn ladder(latency: &OmbPercentiles) -> Vec<LadderRow> {
    let values = percentile_values(latency);
    let bars = log_bar_pcts(&values);
    PERCENTILE_LABELS
        .into_iter()
        .zip(RUNG_HINTS)
        .zip(values.into_iter().zip(bars))
        .enumerate()
        .map(|(index, ((label, hint), (value_ms, bar_pct)))| LadderRow {
            label,
            hint,
            value_ms,
            bar_pct,
            key: index == P99_INDEX,
        })
        .collect()
}

/// The workload fields that the result records, then its driver and file.
pub fn workload_facts(run: &OmbRunSummary) -> Vec<Fact> {
    let workload = &run.workload;
    let field = |field: OmbWorkloadField, value: String| fact(field.to_string(), value);
    let count = |count: u32| group_digits(count.into());
    let mut facts = vec![field(OmbWorkloadField::Name, workload.name.clone())];
    facts.extend(
        run.workload_file
            .clone()
            .map(|file| fact("Workload file", file)),
    );
    facts.extend([
        fact("Driver", run.driver.clone()),
        field(OmbWorkloadField::Topics, count(workload.topics)),
        field(OmbWorkloadField::Partitions, count(workload.partitions)),
        field(
            OmbWorkloadField::ProducersPerTopic,
            count(workload.producers_per_topic),
        ),
        field(
            OmbWorkloadField::ConsumersPerTopic,
            count(workload.consumers_per_topic),
        ),
        field(
            OmbWorkloadField::MessageSize,
            format!("{} B", group_digits(workload.message_size)),
        ),
        fact(
            "Measured length",
            format!(
                "{} ({} windows of {OMB_WINDOW_SECONDS} s)",
                length_text(run.length_seconds),
                group_digits(run.windows as u64)
            ),
        ),
        fact("Result file", run.file_name.clone()),
    ]);
    facts
}

/// The average, the rates and the publish delay next to the publish ladder.
pub fn publish_side(run: &OmbRunSummary) -> Vec<Fact> {
    let delay = Kpi::PublishDelayP99;
    vec![
        average_fact(&run.publish_latency_ms),
        fact(
            "Publish rate",
            messages_text(run.publish_messages_per_second),
        ),
        fact(
            "Producers",
            group_digits(
                u64::from(run.workload.topics) * u64::from(run.workload.producers_per_topic),
            ),
        ),
        fact(delay.label(), delay.text(delay.value(run))),
    ]
}

/// The average of `latency`, the run's end-to-end latency, then the consume rate and the backlog
/// next to the end-to-end ladder.
pub fn end_to_end_side(run: &OmbRunSummary, latency: &OmbPercentiles) -> Vec<Fact> {
    vec![
        average_fact(latency),
        fact(
            "Consume rate",
            messages_text(run.consume_messages_per_second),
        ),
        fact("Backlog at the end", messages_count_text(run.backlog_last)),
        fact("Largest backlog", messages_count_text(run.backlog_max)),
    ]
}

/// One group per property table of the sidecar, with its keys in order.
pub fn property_groups(setup: &OmbSetup) -> Vec<FactGroup> {
    setup
        .properties
        .iter()
        .map(|group| FactGroup {
            title: format!("Properties: {}", group.title),
            facts: group
                .properties
                .iter()
                .map(|property| fact(property.key.clone(), property.value.text.clone()))
                .collect(),
        })
        .collect()
}

/// The run, its machines, the Iggy build and the OMB build, as the sidecar names them. A value that
/// the sidecar leaves out reads [`NOT_RECORDED`], so a reader sees what's unknown.
pub fn environment_groups(run: &OmbRunSummary, setup: &OmbSetup) -> Vec<FactGroup> {
    let or_unknown = |value: Option<String>| value.unwrap_or_else(|| NOT_RECORDED.to_owned());
    let start = run.time.source.is_start().then(|| run.time.text.clone());
    let environment = setup.environment.clone().unwrap_or_default();
    let mut machines: Vec<Fact> = environment
        .label
        .map(|label| fact("Label", label))
        .into_iter()
        .collect();
    machines.extend([
        fact("Server instance", or_unknown(environment.server_instance)),
        fact(
            "Server nodes",
            or_unknown(
                environment
                    .server_nodes
                    .map(|nodes| nodes_text(nodes as usize)),
            ),
        ),
        fact("Client instance", or_unknown(environment.client_instance)),
        fact(
            "Client hosts",
            or_unknown(
                environment
                    .client_hosts
                    .map(|hosts| group_digits(hosts.into())),
            ),
        ),
    ]);
    let server = setup.server.clone().unwrap_or_default();
    let omb = setup.omb.clone().unwrap_or_default();
    vec![
        FactGroup {
            title: "Run".to_owned(),
            facts: vec![
                fact("Suite", or_unknown(setup.suite.clone())),
                fact("Start", or_unknown(start)),
            ],
        },
        FactGroup {
            title: "Machines".to_owned(),
            facts: machines,
        },
        FactGroup {
            title: "Iggy".to_owned(),
            facts: vec![
                fact("Version", or_unknown(server.version)),
                fact("Git ref", or_unknown(server.gitref)),
                fact("Commit date", or_unknown(server.gitref_date)),
            ],
        },
        FactGroup {
            title: "OMB build".to_owned(),
            facts: vec![
                fact("Git ref", or_unknown(omb.gitref)),
                fact("Commit date", or_unknown(omb.gitref_date)),
            ],
        },
    ]
}

/// The start of the measured phase as the end of the run minus its length, for a run whose only
/// time is the one in its file name. The warm-up ran before it. `None` for a run with a known start,
/// and for a modification time, which a copy can change.
pub fn estimated_start(run: &OmbRunSummary) -> Option<String> {
    if run.time.source != OmbTimeSource::FileName {
        return None;
    }
    let length = TimeDelta::try_seconds(i64::try_from(run.length_seconds).ok()?)?;
    let start = run.time.instant.checked_sub_signed(length)?;
    Some(start.format(TIME_FORMAT).to_string())
}

fn average_fact(latency: &OmbPercentiles) -> Fact {
    fact(AVERAGE_LABEL, milliseconds(latency.avg))
}

/// For example `829 messages`. OMB counts a backlog as a signed number.
fn messages_count_text(count: i64) -> String {
    let digits = u64::try_from(count).map_or_else(|_| count.to_string(), group_digits);
    format!("{digits} messages")
}

fn fact(label: impl Into<String>, value: impl Into<String>) -> Fact {
    Fact {
        label: label.into(),
        value: value.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::ladder::{FULL_BAR_PCT, MIN_BAR_PCT};
    use crate::test_support::{
        OMB_EDGE8_100_PARTITIONS, OMB_NO_CONSUMERS, OMB_PUBLISH_ERRORS, OMB_RC1_DEFAULT,
    };

    fn texts(facts: &[Fact]) -> Vec<(&str, &str)> {
        facts
            .iter()
            .map(|fact| (fact.label.as_str(), fact.value.as_str()))
            .collect()
    }

    #[test]
    fn given_fixture_latency_when_laddered_should_list_p50_to_max_with_p99_as_key() {
        let run = OMB_RC1_DEFAULT.summary();

        let rows = ladder(&run.publish_latency_ms);

        let labels: Vec<_> = rows.iter().map(|row| row.label).collect();
        assert_eq!(
            labels,
            ["P50", "P75", "P95", "P99", "P99.9", "P99.99", "Max"]
        );
        let keys: Vec<_> = rows.iter().filter(|row| row.key).collect();
        assert_eq!(keys.len(), 1);
        assert_eq!((keys[0].label, keys[0].value_ms), ("P99", 1.523));
        assert_eq!(rows[0].value_ms, run.publish_latency_ms.p50);
        assert_eq!(rows[0].bar_pct, MIN_BAR_PCT);
        assert_eq!(rows[6].value_ms, run.publish_latency_ms.max);
        assert_eq!(rows[6].bar_pct, FULL_BAR_PCT);
        assert!(
            rows.windows(2)
                .all(|pair| pair[0].bar_pct <= pair[1].bar_pct),
            "the bars grow from P50 to Max"
        );
    }

    #[test]
    fn given_fixture_without_consumers_when_laddered_should_have_one_hint_per_rung() {
        let rows = ladder(&OMB_NO_CONSUMERS.summary().publish_latency_ms);

        assert_eq!(rows.len(), PERCENTILE_LABELS.len());
        assert_eq!(rows[0].hint, RUNG_HINTS[0]);
        assert!(rows.iter().all(|row| row.bar_pct >= MIN_BAR_PCT));
    }

    #[test]
    fn given_fixture_with_default_file_name_when_listed_should_show_workload_file_and_length() {
        let facts = workload_facts(&OMB_EDGE8_100_PARTITIONS.summary());

        assert_eq!(
            texts(&facts),
            [
                (
                    "Workload",
                    "1 topic x 100 partitions / 4p 4c / 1 KiB / 124.9 MB/s"
                ),
                ("Workload file", "1-topic-100-partitions-1kb-125mb"),
                ("Driver", "Iggy"),
                ("Topics", "1"),
                ("Partitions per topic", "100"),
                ("Producers per topic", "4"),
                ("Consumers per topic", "4"),
                ("Message size", "1,024 B"),
                ("Measured length", "5 min (30 windows of 10 s)"),
                (
                    "Result file",
                    "1-topic-100-partitions-1kb-125mb-Iggy-2026-11-02-14-55-18.json"
                ),
            ]
        );
    }

    #[test]
    fn given_fixture_sidecar_when_grouped_should_keep_group_and_key_order() {
        let setup = OMB_RC1_DEFAULT.setup().expect("the fixture has a sidecar");

        let groups = property_groups(&setup);

        let titles: Vec<_> = groups.iter().map(|group| group.title.as_str()).collect();
        assert_eq!(titles, ["Properties: kernel", "Properties: server"]);
        assert_eq!(
            texts(&groups[1].facts),
            [
                ("data_file_system", "ext4"),
                ("huge_pages", "2048"),
                ("placement_strategy", "cluster"),
                ("server_environment", "IGGY_SHARDING_CPU_ALLOCATION=all"),
            ]
        );
    }

    #[test]
    fn given_fixture_sidecar_when_environment_listed_should_mark_missing_values_not_recorded() {
        let run = OMB_RC1_DEFAULT.summary();
        let setup = run.setup.clone().expect("the fixture has a sidecar");

        let groups = environment_groups(&run, &setup);

        let shown: Vec<(&str, Vec<(&str, &str)>)> = groups
            .iter()
            .map(|group| (group.title.as_str(), texts(&group.facts)))
            .collect();
        assert_eq!(
            shown,
            [
                (
                    "Run",
                    vec![
                        ("Suite", "cluster-3-nodes"),
                        ("Start", "2026-11-06 13:22:01 UTC")
                    ]
                ),
                (
                    "Machines",
                    vec![
                        ("Label", "AWS i4i.4xlarge, 3 nodes"),
                        ("Server instance", "i4i.4xlarge"),
                        ("Server nodes", "3 nodes"),
                        ("Client instance", "i4i.4xlarge"),
                        ("Client hosts", "2"),
                    ]
                ),
                (
                    "Iggy",
                    vec![
                        ("Version", "0.10.0-rc.1"),
                        ("Git ref", NOT_RECORDED),
                        ("Commit date", NOT_RECORDED),
                    ]
                ),
                (
                    "OMB build",
                    vec![("Git ref", NOT_RECORDED), ("Commit date", NOT_RECORDED)]
                ),
            ]
        );
    }

    #[test]
    fn given_fixture_with_errors_when_side_facts_listed_should_lead_with_average() {
        let run = OMB_PUBLISH_ERRORS.summary();
        let Some(end_to_end_latency) = run.end_to_end_latency_ms else {
            panic!("the run with errors has consumers and measured end-to-end latency");
        };

        let publish = publish_side(&run);
        let end_to_end = end_to_end_side(&run, &end_to_end_latency);

        assert_eq!(
            texts(&publish[1..]),
            [
                ("Publish rate", "73.7k msg/s"),
                ("Producers", "12"),
                ("Publish delay P99", "105 µs"),
            ]
        );
        assert_eq!(
            texts(&end_to_end[2..]),
            [
                ("Backlog at the end", "71 messages"),
                ("Largest backlog", "829 messages")
            ]
        );
        let average = |facts: &[Fact]| (facts[0].label.clone(), facts[0].value.clone());
        assert_eq!(
            average(&publish),
            (
                "Average".to_owned(),
                milliseconds(run.publish_latency_ms.avg)
            )
        );
        assert_eq!(
            average(&end_to_end),
            ("Average".to_owned(), milliseconds(end_to_end_latency.avg))
        );
        assert_eq!(end_to_end[1].label, "Consume rate");
        assert_eq!(messages_count_text(-3), "-3 messages");
    }

    #[test]
    fn given_time_from_file_name_when_start_estimated_should_subtract_measured_length() {
        let no_sidecar = OMB_NO_CONSUMERS.summary();
        let mut modified = no_sidecar.clone();
        modified.time.source = OmbTimeSource::FileModified;

        assert_eq!(no_sidecar.time.text, "2026-10-22 10:21:15");
        assert_eq!(
            estimated_start(&no_sidecar).as_deref(),
            Some("2026-10-22 10:18:15")
        );
        assert_eq!(estimated_start(&OMB_RC1_DEFAULT.summary()), None);
        assert_eq!(estimated_start(&modified), None);
    }
}
