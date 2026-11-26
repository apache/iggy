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

//! The texts of an OMB run that the OMB pages and the head tags share: the workload line, the
//! environment, the version and the OMB units.

use crate::format::group_digits;
use crate::labels::{CONDITION_SEPARATOR, count_text, nodes_text};
use crate::omb::id::OmbRunId;
use crate::omb::setup::OmbEnvironment;
use crate::omb::summary::OmbRunSummary;
use crate::omb::workload::OmbWorkloadKey;

/// What the pages show for a number that has no value, such as a rate that isn't finite.
pub const NO_VALUE: &str = "-";
/// The first half of an OMB run id, which names a run in a breadcrumb, a legend or a column.
const SHORT_ID_LENGTH: usize = 8;
const MEBIBYTES_PER_GIBIBYTE: f64 = 1024.0;
const MEBIBYTES_PER_TEBIBYTE: f64 = 1_048_576.0;
const SECONDS_PER_MINUTE: u64 = 60;

/// For example `3f2a9c1b`.
pub fn short_id(id: OmbRunId) -> String {
    id.to_string().chars().take(SHORT_ID_LENGTH).collect()
}

/// A run among other runs: its short id, driver and Iggy version, for example
/// `3f2a9c1b · Iggy-persisted · 0.10.0-rc.1`.
pub fn run_name(summary: &OmbRunSummary) -> String {
    let mut parts = vec![short_id(summary.id), summary.driver.clone()];
    parts.extend(version_text(summary));
    parts.join(CONDITION_SEPARATOR)
}

/// The Iggy version from the setup sidecar, as the iggy-bench pages write it: the version of a
/// build at a commit, which the pages show apart, or the version text. `None` without a version.
pub fn version_text(summary: &OmbRunSummary) -> Option<String> {
    let version = summary.version.as_ref()?;
    Some(match (&version.commit, &summary.version_order.version) {
        (Some(_), Some(semver)) => semver.to_string(),
        _ => version.text.clone(),
    })
}

/// The sidecar's label, else the machines it names, for example
/// `i4i.4xlarge, 3 nodes · 2 client hosts of i4i.4xlarge`. `None` for an environment that names
/// nothing.
pub fn environment_text(environment: &OmbEnvironment) -> Option<String> {
    if let Some(label) = &environment.label {
        return Some(label.clone());
    }
    let server = match (&environment.server_instance, environment.server_nodes) {
        (Some(instance), Some(nodes)) => {
            Some(format!("{instance}, {}", nodes_text(nodes as usize)))
        }
        (Some(instance), None) => Some(instance.clone()),
        (None, Some(nodes)) => Some(nodes_text(nodes as usize)),
        (None, None) => None,
    };
    let clients = match (&environment.client_instance, environment.client_hosts) {
        (Some(instance), Some(hosts)) => {
            Some(format!("{} of {instance}", client_hosts_text(hosts)))
        }
        (Some(instance), None) => Some(format!("clients on {instance}")),
        (None, Some(hosts)) => Some(client_hosts_text(hosts)),
        (None, None) => None,
    };
    let parts: Vec<String> = server.into_iter().chain(clients).collect();
    (!parts.is_empty()).then(|| parts.join(CONDITION_SEPARATOR))
}

/// The run's workload in one line: topology, actors, message size and measured length.
pub fn conditions(summary: &OmbRunSummary) -> String {
    [
        topology_text(&summary.workload),
        actors_text(&summary.workload),
        message_size_text(summary.workload.message_size),
        length_text(summary.length_seconds),
    ]
    .join(CONDITION_SEPARATOR)
}

/// The run's workload as a heading: topology and actors, for example
/// `12 topics × 1 partition, 1 producer and 1 consumer per topic`. The pages name a run by it and
/// not by the workload `name`, which is free text from the workload file.
pub fn workload_heading(workload: &OmbWorkloadKey) -> String {
    format!("{}, {}", topology_text(workload), actors_text(workload))
}

/// The part of [`conditions`] that [`workload_heading`] leaves out, for example
/// `1,000 B messages · 3 min`.
pub fn size_and_length(summary: &OmbRunSummary) -> String {
    [
        message_size_text(summary.workload.message_size),
        length_text(summary.length_seconds),
    ]
    .join(CONDITION_SEPARATOR)
}

/// For example `12 topics × 1 partition`, with the partitions per topic.
pub fn topology_text(workload: &OmbWorkloadKey) -> String {
    format!(
        "{} × {}",
        count_text(workload.topics.into(), "topic", "topics"),
        count_text(workload.partitions.into(), "partition", "partitions")
    )
}

/// For example `1 producer and 1 consumer per topic`.
pub fn actors_text(workload: &OmbWorkloadKey) -> String {
    format!(
        "{} and {} per topic",
        count_text(workload.producers_per_topic.into(), "producer", "producers"),
        count_text(workload.consumers_per_topic.into(), "consumer", "consumers")
    )
}

/// For example `1,000 B messages`. For a workload with a size distribution, OMB records the
/// weighted average size.
pub fn message_size_text(bytes: u64) -> String {
    format!("{} B messages", group_digits(bytes))
}

/// The measured length, for example `3 min` or `1 min 30 s`. The warm-up isn't in it.
pub fn length_text(seconds: u64) -> String {
    let (minutes, rest) = (seconds / SECONDS_PER_MINUTE, seconds % SECONDS_PER_MINUTE);
    match (minutes, rest) {
        (0, _) => format!("{rest} s"),
        (_, 0) => format!("{minutes} min"),
        _ => format!("{minutes} min {rest} s"),
    }
}

/// A throughput in OMB's unit, for example `143 MiB/s` or `1.50 GiB/s`. OMB itself calls the
/// unit MB/s, but it divides by 1024 twice.
pub fn mebibytes_text(mebibytes_per_second: f64) -> String {
    if !mebibytes_per_second.is_finite() || mebibytes_per_second < 0.0 {
        return NO_VALUE.to_owned();
    }
    if mebibytes_per_second >= MEBIBYTES_PER_TEBIBYTE {
        format!("{:.2} TiB/s", mebibytes_per_second / MEBIBYTES_PER_TEBIBYTE)
    } else if mebibytes_per_second >= MEBIBYTES_PER_GIBIBYTE {
        format!("{:.2} GiB/s", mebibytes_per_second / MEBIBYTES_PER_GIBIBYTE)
    } else if mebibytes_per_second >= 100.0 {
        format!("{mebibytes_per_second:.0} MiB/s")
    } else {
        format!("{mebibytes_per_second:.1} MiB/s")
    }
}

/// A latency in microseconds, as OMB records the publish delay, for example `70 µs` or
/// `1,234 µs`.
pub fn microseconds(value: f64) -> String {
    if !value.is_finite() || value < 0.0 {
        NO_VALUE.to_owned()
    } else if value >= 100.0 {
        format!("{} µs", group_digits(value.round() as u64))
    } else if value >= 10.0 {
        format!("{value:.1} µs")
    } else {
        format!("{value:.2} µs")
    }
}

fn client_hosts_text(hosts: u32) -> String {
    count_text(hosts.into(), "client host", "client hosts")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::omb::test_fixtures::{
        EDGE8_100_PARTITIONS, NO_CONSUMERS, PUBLISH_ERRORS, RC1_DEFAULT, RC1_PERSISTED,
    };

    #[test]
    fn given_fixtures_when_named_should_show_short_id_driver_and_known_version() {
        let rc1 = RC1_PERSISTED.summary();
        let no_sidecar = NO_CONSUMERS.summary();

        assert_eq!(
            run_name(&rc1),
            format!("{} · Iggy-persisted · 0.10.0-rc.1", short_id(rc1.id))
        );
        assert_eq!(
            run_name(&no_sidecar),
            format!("{} · Iggy", short_id(no_sidecar.id))
        );
        assert_eq!(short_id(rc1.id).len(), SHORT_ID_LENGTH);
        assert!(rc1.id.to_string().starts_with(&short_id(rc1.id)));
        assert_eq!(version_text(&no_sidecar), None);
    }

    #[test]
    fn given_fixtures_when_conditions_written_should_name_topology_actors_size_and_length() {
        assert_eq!(
            conditions(&RC1_DEFAULT.summary()),
            "12 topics × 1 partition · 1 producer and 1 consumer per topic · 1,000 B messages · \
             3 min"
        );
        assert_eq!(
            conditions(&EDGE8_100_PARTITIONS.summary()),
            "1 topic × 100 partitions · 4 producers and 4 consumers per topic · 1,024 B messages \
             · 5 min"
        );
        assert_eq!(
            actors_text(&NO_CONSUMERS.summary().workload),
            "1 producer and 0 consumers per topic"
        );
        assert_eq!(
            workload_heading(&RC1_DEFAULT.summary().workload),
            "12 topics × 1 partition, 1 producer and 1 consumer per topic"
        );
        assert_eq!(
            size_and_length(&EDGE8_100_PARTITIONS.summary()),
            "1,024 B messages · 5 min"
        );
        assert_eq!(length_text(90), "1 min 30 s");
        assert_eq!(length_text(0), "0 s");
    }

    #[test]
    fn given_sidecar_environment_when_labeled_should_prefer_label_and_else_name_machines() {
        let rc1 = RC1_DEFAULT.summary();
        let environment = rc1
            .environment()
            .expect("the fixture has a sidecar environment");

        assert_eq!(
            environment_text(environment).as_deref(),
            Some("AWS i4i.4xlarge, 3 nodes")
        );
        assert_eq!(
            environment_text(&environment.without_label()).as_deref(),
            Some("i4i.4xlarge, 3 nodes · 2 client hosts of i4i.4xlarge")
        );
        assert_eq!(environment_text(&OmbEnvironment::default()), None);
        assert_eq!(PUBLISH_ERRORS.summary().environment(), None);
    }

    #[test]
    fn given_rates_and_latencies_when_written_should_use_omb_units() {
        let rc1 = RC1_DEFAULT.summary();

        assert_eq!(
            mebibytes_text(rc1.publish_mebibytes_per_second),
            "143 MiB/s"
        );
        assert_eq!(mebibytes_text(1536.0), "1.50 GiB/s");
        assert_eq!(mebibytes_text(42.25), "42.2 MiB/s");
        assert_eq!(mebibytes_text(f64::NAN), NO_VALUE);
        assert_eq!(microseconds(70.0), "70.0 µs");
        assert_eq!(microseconds(1234.4), "1,234 µs");
        assert_eq!(microseconds(-1.0), NO_VALUE);
    }
}
