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

//! The configuration table of the OMB compare page: one row per workload field, per setup value
//! and per sidecar property, one column per run.

use bench_dashboard_shared::omb::compare::{OmbPropertyRow, OmbSetupField};
use bench_dashboard_shared::omb::setup::OmbSetup;
use bench_dashboard_shared::omb::summary::OmbRunSummary;
use bench_dashboard_shared::omb::workload::{OmbWorkloadField, OmbWorkloadKey};

use crate::format::group_digits;
use crate::pages::omb::common::{NOT_RECORDED, length_text, version_text};

/// What a property cell shows for a run whose sidecar doesn't set the property.
pub const NOT_SET: &str = "not set";
const DRIVER_LABEL: &str = "Driver";
const WORKLOAD_FILE_LABEL: &str = "Workload file";
const WINDOWS_LABEL: &str = "Windows";
const VERSION_LABEL: &str = "Iggy version";
/// The setup values that decide the comparability level, in the order of its rule.
const SETUP_FIELDS: [OmbSetupField; 5] = [
    OmbSetupField::ServerInstance,
    OmbSetupField::ServerNodes,
    OmbSetupField::ClientInstance,
    OmbSetupField::ClientHosts,
    OmbSetupField::OmbGitref,
];

#[derive(Debug, Clone, PartialEq)]
pub struct SettingRow {
    pub label: String,
    /// One per run, the baseline first.
    pub values: Vec<String>,
    /// A run shows another value than the baseline. A property that a run doesn't set differs.
    pub differs: bool,
}

impl SettingRow {
    fn new(label: String, values: Vec<String>) -> Self {
        let differs = values
            .split_first()
            .is_some_and(|(baseline, others)| others.iter().any(|value| value != baseline));
        Self {
            label,
            values,
            differs,
        }
    }
}

/// The workload, the setup and the properties of `runs`. The rows that differ from the baseline
/// come first, and the equal rows follow, so the reader also sees what the runs share. Both parts
/// keep the order workload, setup, properties, and the properties keep their group and key order.
/// A setup row needs a run that records its value, as no OMB result records the setup.
pub fn setting_rows(runs: &[&OmbRunSummary]) -> Vec<SettingRow> {
    let row = |label: &str, value_of: &dyn Fn(&OmbRunSummary) -> String| {
        SettingRow::new(
            label.to_owned(),
            runs.iter().map(|run| value_of(run)).collect(),
        )
    };
    let field =
        |field: OmbWorkloadField| row(&field.to_string(), &|run| field_text(field, &run.workload));
    let recorded = |label: &str, value_of: &dyn Fn(&OmbRunSummary) -> Option<String>| {
        runs.iter().any(|run| value_of(run).is_some()).then(|| {
            row(label, &|run| {
                value_of(run).unwrap_or_else(|| NOT_RECORDED.to_owned())
            })
        })
    };
    // The workload file from the result's file name, without the driver and the time, which
    // differ in every run and would always count as a difference.
    let workload = [
        Some(row(DRIVER_LABEL, &|run| run.driver.clone())),
        Some(field(OmbWorkloadField::Name)),
        recorded(WORKLOAD_FILE_LABEL, &|run| run.workload_file.clone()),
    ]
    .into_iter()
    .flatten()
    .chain(
        OmbWorkloadField::ALL
            .into_iter()
            .filter(|field| *field != OmbWorkloadField::Name)
            .map(field),
    )
    .chain([row(WINDOWS_LABEL, &windows_text)]);
    let setup = [recorded(VERSION_LABEL, &version_text)]
        .into_iter()
        .chain(SETUP_FIELDS.map(|field| {
            recorded(&field.to_string(), &|run| {
                setup_text(field, run.setup.as_ref())
            })
        }))
        .flatten();
    let properties = OmbPropertyRow::of_runs(runs)
        .into_iter()
        .map(|property| SettingRow {
            label: property.key.to_string(),
            values: property
                .values
                .into_iter()
                .map(|value| value.map_or_else(|| NOT_SET.to_owned(), |value| value.text))
                .collect(),
            differs: property.differs,
        });
    let (differing, equal): (Vec<_>, Vec<_>) = workload
        .chain(setup)
        .chain(properties)
        .partition(|row| row.differs);
    differing.into_iter().chain(equal).collect()
}

/// The match makes a new workload field a compile error here, so it can't be left out.
fn field_text(field: OmbWorkloadField, workload: &OmbWorkloadKey) -> String {
    match field {
        OmbWorkloadField::Name => workload.name.clone(),
        OmbWorkloadField::MessageSize => format!("{} B", group_digits(workload.message_size)),
        OmbWorkloadField::Topics => group_digits(workload.topics.into()),
        OmbWorkloadField::Partitions => group_digits(workload.partitions.into()),
        OmbWorkloadField::ProducersPerTopic => group_digits(workload.producers_per_topic.into()),
        OmbWorkloadField::ConsumersPerTopic => group_digits(workload.consumers_per_topic.into()),
    }
}

/// For example `18 (3 min)`: the windows of the test phase and their length.
fn windows_text(run: &OmbRunSummary) -> String {
    format!("{} ({})", run.windows, length_text(run.length_seconds))
}

fn setup_text(field: OmbSetupField, setup: Option<&OmbSetup>) -> Option<String> {
    let environment = setup.and_then(|setup| setup.environment.as_ref());
    match field {
        OmbSetupField::ServerInstance => environment?.server_instance.clone(),
        OmbSetupField::ServerNodes => environment?
            .server_nodes
            .map(|nodes| group_digits(nodes.into())),
        OmbSetupField::ClientInstance => environment?.client_instance.clone(),
        OmbSetupField::ClientHosts => environment?
            .client_hosts
            .map(|hosts| group_digits(hosts.into())),
        OmbSetupField::OmbGitref => setup?.omb.as_ref()?.gitref.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{OMB_EDGE8_DEFAULT, OMB_NO_CONSUMERS, OMB_RC1_DEFAULT};

    const DATA_FILE_SYSTEM: &str = "server / data_file_system";

    fn labels(rows: &[SettingRow]) -> Vec<&str> {
        rows.iter().map(|row| row.label.as_str()).collect()
    }

    fn values<'a>(rows: &'a [SettingRow], label: &str) -> Option<&'a [String]> {
        rows.iter()
            .find(|row| row.label == label)
            .map(|row| row.values.as_slice())
    }

    #[test]
    fn given_two_versions_with_property_missing_when_diffed_should_put_differing_rows_first() {
        let (rc1, edge8) = (OMB_RC1_DEFAULT.summary(), OMB_EDGE8_DEFAULT.summary());

        let rows = setting_rows(&[&rc1, &edge8]);

        assert_eq!(labels(&rows)[..2], [VERSION_LABEL, DATA_FILE_SYSTEM]);
        assert!(rows[..2].iter().all(|row| row.differs));
        assert!(rows[2..].iter().all(|row| !row.differs));
        assert_eq!(
            values(&rows, WORKLOAD_FILE_LABEL),
            Some(
                [
                    "12-topics-1-partition-1kb-150mb".to_owned(),
                    "12-topics-1-partition-1kb-150mb".to_owned()
                ]
                .as_slice()
            )
        );
        assert_eq!(
            values(&rows, DATA_FILE_SYSTEM),
            Some(["ext4".to_owned(), NOT_SET.to_owned()].as_slice())
        );
        assert_eq!(
            values(&rows, VERSION_LABEL),
            Some(["0.10.0-rc.1".to_owned(), "0.9.1-edge.8".to_owned()].as_slice())
        );
        // The equal rows keep the order workload, setup, properties.
        assert_eq!(labels(&rows)[2], DRIVER_LABEL);
        assert_eq!(labels(&rows).last(), Some(&"server / server_environment"));
        // Neither sidecar records an OMB build, so the row is left out.
        assert_eq!(values(&rows, &OmbSetupField::OmbGitref.to_string()), None);
    }

    #[test]
    fn given_run_without_sidecar_when_diffed_should_show_not_set_and_not_recorded() {
        let (rc1, no_consumers) = (OMB_RC1_DEFAULT.summary(), OMB_NO_CONSUMERS.summary());

        let rows = setting_rows(&[&rc1, &no_consumers]);

        let property_rows: Vec<&SettingRow> = rows
            .iter()
            .filter(|row| row.label.contains(" / "))
            .collect();
        assert_eq!(property_rows.len(), 7);
        assert!(
            property_rows
                .iter()
                .all(|row| row.differs && row.values[1] == NOT_SET)
        );
        assert_eq!(
            values(&rows, &OmbSetupField::ServerInstance.to_string()),
            Some(["i4i.4xlarge".to_owned(), NOT_RECORDED.to_owned()].as_slice())
        );
        assert_eq!(
            labels(&rows)[..3],
            [
                OmbWorkloadField::Name.to_string().as_str(),
                WORKLOAD_FILE_LABEL,
                OmbWorkloadField::ConsumersPerTopic.to_string().as_str()
            ]
        );
        assert_eq!(
            values(&rows, &OmbWorkloadField::ConsumersPerTopic.to_string()),
            Some(["1".to_owned(), "0".to_owned()].as_slice())
        );
    }

    #[test]
    fn given_fixture_workload_when_diffed_should_write_sizes_counts_and_windows() {
        let rc1 = OMB_RC1_DEFAULT.summary();

        let rows = setting_rows(&[&rc1, &rc1]);

        assert!(rows.iter().all(|row| !row.differs));
        assert_eq!(
            values(&rows, &OmbWorkloadField::MessageSize.to_string()),
            Some(["1,000 B".to_owned(), "1,000 B".to_owned()].as_slice())
        );
        assert_eq!(
            values(&rows, WINDOWS_LABEL).map(|values| values[0].as_str()),
            Some("18 (3 min)")
        );
        assert_eq!(
            values(&rows, DRIVER_LABEL).map(|values| values[0].as_str()),
            Some("Iggy")
        );
    }
}
