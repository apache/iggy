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

//! How far two OMB runs compare, and which sidecar properties differ between runs.

use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::omb::setup::{OmbEnvironment, OmbPropertyValue, OmbSetup};
use crate::omb::summary::OmbRunSummary;
use crate::omb::workload::OmbWorkloadField;

/// How comparable two OMB runs are. The driver doesn't count, so two driver configurations of one
/// workload compare like for like. Properties don't count either, because the dashboard can't
/// know which of them changes a result, so the pages name the properties that differ.
///
/// The levels go from closest to furthest, so the furthest level among several pairs is their
/// maximum. A known difference is further than an unknown one, so "Setup differs" comes after
/// "Setup unknown".
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OmbComparability {
    /// The same workload key, the same environment and the same OMB build.
    LikeForLike,
    /// The same workload key, and the sidecars record no machine value in common, so nothing
    /// tells that the runs used the same machines.
    SetupUnknown,
    /// The same workload key, with other machines or another OMB build. Only the values that both
    /// runs record count.
    SetupDiffers(Vec<OmbSetupField>),
    /// Another workload key.
    WorkloadDiffers(Vec<OmbWorkloadField>),
}

/// One value of the sidecar that decides the comparability level. The environment label is for
/// display only, so it has no variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OmbSetupField {
    ServerInstance,
    ServerNodes,
    ClientInstance,
    ClientHosts,
    /// `[omb] gitref`: the OMB build holds the driver code.
    OmbGitref,
}

/// A sidecar property: a group title and a key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OmbPropertyKey {
    pub group: String,
    pub key: String,
}

/// One property across a set of runs, for the configuration table of the compare page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmbPropertyRow {
    pub key: OmbPropertyKey,
    /// One value per run, in the order of the runs. `None` if the run doesn't set the property.
    pub values: Vec<Option<OmbPropertyValue>>,
    /// A property that a run doesn't set differs.
    pub differs: bool,
}

impl OmbComparability {
    /// The level of `other` against `baseline`. The workload key decides first. Then a value that
    /// both sidecars record and that differs gives "Setup differs". Then sidecars without a
    /// machine value in common give "Setup unknown".
    pub fn between(baseline: &OmbRunSummary, other: &OmbRunSummary) -> Self {
        let workload = baseline.workload.differences(&other.workload);
        if !workload.is_empty() {
            return Self::WorkloadDiffers(workload);
        }
        let shared = shared_setup_values(baseline.setup.as_ref(), other.setup.as_ref());
        let differing: Vec<_> = shared
            .iter()
            .filter_map(|&(field, differs)| differs.then_some(field))
            .collect();
        if !differing.is_empty() {
            return Self::SetupDiffers(differing);
        }
        // The OMB build tells nothing about the machines.
        if shared
            .iter()
            .all(|(field, _)| *field == OmbSetupField::OmbGitref)
        {
            return Self::SetupUnknown;
        }
        Self::LikeForLike
    }

    /// One plain sentence for a reader who doesn't know OMB.
    pub const fn explanation(&self) -> &'static str {
        match self {
            Self::LikeForLike => {
                "The runs ran the same workload on the same setup, so a change comes from the \
                 Iggy version, the driver configuration or the properties."
            }
            Self::SetupUnknown => {
                "OMB results do not record hardware. Make sure that the runs used the same \
                 machines before you read a change as an effect of the version or the \
                 configuration."
            }
            Self::SetupDiffers(_) => {
                "The runs ran the same workload on another setup, so a change mixes the Iggy \
                 version and configuration with the hardware or the OMB build."
            }
            Self::WorkloadDiffers(_) => {
                "The runs ran different workloads, so the page shows their values side by side \
                 with no change."
            }
        }
    }
}

impl OmbPropertyKey {
    /// The properties that differ across `runs`, in group and key order. A property that a run
    /// doesn't set differs, and a run without a sidecar sets none.
    pub fn differing(runs: &[&OmbRunSummary]) -> Vec<Self> {
        OmbPropertyRow::of_runs(runs)
            .into_iter()
            .filter(|row| row.differs)
            .map(|row| row.key)
            .collect()
    }
}

impl OmbPropertyRow {
    /// Every property of `runs`, the rows that differ first, each part in group and key order.
    pub fn of_runs(runs: &[&OmbRunSummary]) -> Vec<Self> {
        let keys: BTreeSet<OmbPropertyKey> = runs
            .iter()
            .filter_map(|run| run.setup.as_ref())
            .flat_map(OmbSetup::properties)
            .map(|(group, property)| OmbPropertyKey {
                group: group.to_owned(),
                key: property.key.clone(),
            })
            .collect();
        let mut rows: Vec<_> = keys
            .into_iter()
            .map(|key| {
                let values: Vec<_> = runs
                    .iter()
                    .map(|run| property_value(run.setup.as_ref(), &key))
                    .collect();
                let differs = values.windows(2).any(|pair| pair[0] != pair[1])
                    || values.iter().any(Option::is_none);
                Self {
                    key,
                    values,
                    differs,
                }
            })
            .collect();
        // A stable sort keeps the key order inside each part.
        rows.sort_by_key(|row| !row.differs);
        rows
    }
}

impl fmt::Display for OmbComparability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::LikeForLike => "Like for like",
            Self::SetupUnknown => "Setup unknown",
            Self::SetupDiffers(_) => "Setup differs",
            Self::WorkloadDiffers(_) => "Workload differs",
        })
    }
}

impl fmt::Display for OmbSetupField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ServerInstance => "Server instance",
            Self::ServerNodes => "Server nodes",
            Self::ClientInstance => "Client instance",
            Self::ClientHosts => "Client hosts",
            Self::OmbGitref => "OMB build",
        })
    }
}

impl fmt::Display for OmbPropertyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} / {}", self.group, self.key)
    }
}

/// The setup fields that both runs record, each with true if the values differ, in field order.
/// The destructuring makes a new environment value a compile error here, so it can't be left out
/// of the comparison.
fn shared_setup_values(
    left: Option<&OmbSetup>,
    right: Option<&OmbSetup>,
) -> Vec<(OmbSetupField, bool)> {
    let unknown = OmbEnvironment::default();
    let OmbEnvironment {
        label: _,
        server_instance,
        server_nodes,
        client_instance,
        client_hosts,
    } = environment(left).unwrap_or(&unknown);
    let other = environment(right).unwrap_or(&unknown);
    [
        (
            OmbSetupField::ServerInstance,
            differ(server_instance.as_ref(), other.server_instance.as_ref()),
        ),
        (
            OmbSetupField::ServerNodes,
            differ(server_nodes.as_ref(), other.server_nodes.as_ref()),
        ),
        (
            OmbSetupField::ClientInstance,
            differ(client_instance.as_ref(), other.client_instance.as_ref()),
        ),
        (
            OmbSetupField::ClientHosts,
            differ(client_hosts.as_ref(), other.client_hosts.as_ref()),
        ),
        (
            OmbSetupField::OmbGitref,
            differ(omb_gitref(left), omb_gitref(right)),
        ),
    ]
    .into_iter()
    .filter_map(|(field, differs)| differs.map(|differs| (field, differs)))
    .collect()
}

/// Whether two values differ, or `None` unless both sides record one.
fn differ<T: PartialEq + ?Sized>(left: Option<&T>, right: Option<&T>) -> Option<bool> {
    Some(left? != right?)
}

fn environment(setup: Option<&OmbSetup>) -> Option<&OmbEnvironment> {
    setup.and_then(|setup| setup.environment.as_ref())
}

fn omb_gitref(setup: Option<&OmbSetup>) -> Option<&str> {
    setup
        .and_then(|setup| setup.omb.as_ref())
        .and_then(|omb| omb.gitref.as_deref())
}

fn property_value(setup: Option<&OmbSetup>, key: &OmbPropertyKey) -> Option<OmbPropertyValue> {
    setup?
        .properties()
        .find(|(group, property)| *group == key.group && property.key == key.key)
        .map(|(_, property)| property.value.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::omb::setup::{OmbBuild, OmbProperty, OmbPropertyGroup, OmbScalarKind, OmbSidecar};
    use crate::omb::test_fixtures::{
        EDGE8_100_PARTITIONS, EDGE8_DEFAULT, NO_CONSUMERS, PUBLISH_ERRORS, RC1_DEFAULT,
        RC1_PERSISTED,
    };

    fn with_setup(mut run: OmbRunSummary, text: Option<&str>) -> OmbRunSummary {
        run.setup = text.map(|text| {
            text.parse::<OmbSidecar>()
                .expect("the test sidecar parses")
                .setup
        });
        run
    }

    fn integer(text: &str) -> Option<OmbPropertyValue> {
        Some(OmbPropertyValue {
            text: text.to_owned(),
            kind: OmbScalarKind::Integer,
        })
    }

    fn key(group: &str, key: &str) -> OmbPropertyKey {
        OmbPropertyKey {
            group: group.to_owned(),
            key: key.to_owned(),
        }
    }

    #[test]
    fn given_real_runs_of_one_workload_and_setup_when_compared_should_be_like_for_like() {
        let rc1 = RC1_DEFAULT.summary();

        assert_eq!(
            OmbComparability::between(&rc1, &RC1_PERSISTED.summary()),
            OmbComparability::LikeForLike
        );
        assert_eq!(
            OmbComparability::between(&rc1, &EDGE8_DEFAULT.summary()),
            OmbComparability::LikeForLike
        );
        assert_eq!(
            OmbComparability::between(&rc1, &rc1),
            OmbComparability::LikeForLike
        );
    }

    #[test]
    fn given_real_runs_of_other_workloads_when_compared_should_name_workload_fields() {
        let rc1 = RC1_DEFAULT.summary();

        assert_eq!(
            OmbComparability::between(&rc1, &EDGE8_100_PARTITIONS.summary()),
            OmbComparability::WorkloadDiffers(OmbWorkloadField::ALL.to_vec())
        );
        assert_eq!(
            OmbComparability::between(&rc1, &NO_CONSUMERS.summary()),
            OmbComparability::WorkloadDiffers(vec![
                OmbWorkloadField::Name,
                OmbWorkloadField::ConsumersPerTopic
            ])
        );
        assert_eq!(
            OmbComparability::between(&rc1, &PUBLISH_ERRORS.summary()),
            OmbComparability::WorkloadDiffers(vec![OmbWorkloadField::Name])
        );
    }

    #[test]
    fn given_same_workload_on_other_machines_when_compared_should_name_setup_fields() {
        let rc1 = RC1_DEFAULT.summary();
        let mut other_machines = RC1_PERSISTED.summary();
        other_machines.setup = EDGE8_100_PARTITIONS.setup();

        assert_eq!(
            OmbComparability::between(&rc1, &other_machines),
            OmbComparability::SetupDiffers(vec![
                OmbSetupField::ServerInstance,
                OmbSetupField::ClientInstance
            ])
        );
    }

    #[test]
    fn given_same_workload_with_other_omb_build_when_compared_should_name_build() {
        let rc1 = RC1_DEFAULT.summary();
        let mut first_build = RC1_PERSISTED.summary();
        let mut second_build = RC1_PERSISTED.summary();
        for (run, gitref) in [
            (&mut first_build, "a1b2c3d"),
            (&mut second_build, "e4f5a6b"),
        ] {
            if let Some(setup) = run.setup.as_mut() {
                setup.omb = Some(OmbBuild {
                    gitref: Some(gitref.to_owned()),
                    gitref_date: None,
                });
            }
        }

        assert_eq!(
            OmbComparability::between(&first_build, &second_build),
            OmbComparability::SetupDiffers(vec![OmbSetupField::OmbGitref])
        );
        // The baseline records no build, so the build doesn't count.
        assert_eq!(
            OmbComparability::between(&rc1, &first_build),
            OmbComparability::LikeForLike
        );
    }

    #[test]
    fn given_run_without_known_environment_when_compared_should_be_setup_unknown() {
        let rc1 = RC1_DEFAULT.summary();
        let without_sidecar = with_setup(RC1_PERSISTED.summary(), None);
        let label_only = with_setup(
            RC1_PERSISTED.summary(),
            Some("[environment]\nlabel = \"AWS i4i.4xlarge, 3 nodes\"\n"),
        );
        let no_environment = with_setup(RC1_PERSISTED.summary(), Some("suite = \"nightly\"\n"));

        for other in [&without_sidecar, &label_only, &no_environment] {
            assert_eq!(
                OmbComparability::between(&rc1, other),
                OmbComparability::SetupUnknown
            );
            assert_eq!(
                OmbComparability::between(other, &rc1),
                OmbComparability::SetupUnknown
            );
        }
    }

    #[test]
    fn given_partial_environments_when_compared_should_compare_only_shared_values() {
        let nodes_only = with_setup(
            RC1_PERSISTED.summary(),
            Some("[environment]\nserver_nodes = 3\n"),
        );
        let other_nodes = with_setup(
            RC1_PERSISTED.summary(),
            Some("[environment]\nserver_nodes = 1\n"),
        );
        let rc1 = RC1_DEFAULT.summary();

        assert_eq!(
            OmbComparability::between(&rc1, &nodes_only),
            OmbComparability::LikeForLike
        );
        assert_eq!(
            OmbComparability::between(&rc1, &other_nodes),
            OmbComparability::SetupDiffers(vec![OmbSetupField::ServerNodes])
        );
    }

    #[test]
    fn given_known_difference_and_unknown_environment_when_compared_should_be_setup_differs() {
        let first = with_setup(
            RC1_DEFAULT.summary(),
            Some("[environment]\nlabel = \"Cloud\"\n[omb]\ngitref = \"a1b2c3d\"\n"),
        );
        let second = with_setup(
            RC1_PERSISTED.summary(),
            Some("[omb]\ngitref = \"e4f5a6b\"\n"),
        );

        assert_eq!(
            OmbComparability::between(&first, &second),
            OmbComparability::SetupDiffers(vec![OmbSetupField::OmbGitref])
        );
    }

    #[test]
    fn given_no_machine_value_in_common_when_compared_should_be_setup_unknown() {
        let instance_only = with_setup(
            RC1_DEFAULT.summary(),
            Some("[environment]\nserver_instance = \"i4i.4xlarge\"\n"),
        );
        let hosts_only = with_setup(
            RC1_PERSISTED.summary(),
            Some("[environment]\nclient_hosts = 2\n"),
        );
        let build_only = with_setup(
            RC1_PERSISTED.summary(),
            Some("[omb]\ngitref = \"a1b2c3d\"\n"),
        );

        for (first, second) in [
            (&instance_only, &hosts_only),
            (&hosts_only, &instance_only),
            (&build_only, &build_only),
        ] {
            assert_eq!(
                OmbComparability::between(first, second),
                OmbComparability::SetupUnknown
            );
        }
    }

    #[test]
    fn given_levels_when_ordered_should_go_from_like_for_like_to_workload_differs() {
        let levels = [
            OmbComparability::LikeForLike,
            OmbComparability::SetupUnknown,
            OmbComparability::SetupDiffers(vec![OmbSetupField::OmbGitref]),
            OmbComparability::WorkloadDiffers(vec![OmbWorkloadField::Name]),
        ];

        assert!(levels.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            levels.iter().map(ToString::to_string).collect::<Vec<_>>(),
            [
                "Like for like",
                "Setup unknown",
                "Setup differs",
                "Workload differs"
            ]
        );
        assert!(
            levels[1]
                .explanation()
                .starts_with("OMB results do not record hardware.")
        );
    }

    #[test]
    fn given_real_runs_when_listing_properties_should_name_property_that_one_run_lacks() {
        let rc1 = RC1_DEFAULT.summary();
        let persisted = RC1_PERSISTED.summary();
        let edge = EDGE8_DEFAULT.summary();

        assert_eq!(OmbPropertyKey::differing(&[&rc1, &persisted]), []);
        assert_eq!(
            OmbPropertyKey::differing(&[&rc1, &edge]),
            [key("server", "data_file_system")]
        );
        assert_eq!(
            OmbPropertyKey::differing(&[&rc1, &EDGE8_100_PARTITIONS.summary()]),
            [
                key("server", "data_file_system"),
                key("server", "huge_pages")
            ]
        );
    }

    #[test]
    fn given_runs_with_other_property_values_when_listing_rows_should_put_differing_rows_first() {
        let base = with_setup(
            RC1_DEFAULT.summary(),
            Some(
                "[properties.kernel]\n\"vm.dirty_bytes\" = 2147483648\n\"vm.swappiness\" = 10\n\
                 [properties.server]\nhuge_pages = 2048\n",
            ),
        );
        let changed = with_setup(
            RC1_PERSISTED.summary(),
            Some(
                "[properties.kernel]\n\"vm.dirty_bytes\" = 4294967296\n\"vm.swappiness\" = 10\n\
                 [properties.server]\nhuge_pages = 2048\n",
            ),
        );
        let float = with_setup(
            RC1_PERSISTED.summary(),
            Some(
                "[properties.kernel]\n\"vm.dirty_bytes\" = 2147483648\n\"vm.swappiness\" = 10.0\n\
                 [properties.server]\nhuge_pages = 2048\n",
            ),
        );

        assert_eq!(
            OmbPropertyRow::of_runs(&[&base, &changed]),
            [
                OmbPropertyRow {
                    key: key("kernel", "vm.dirty_bytes"),
                    values: vec![integer("2147483648"), integer("4294967296")],
                    differs: true,
                },
                OmbPropertyRow {
                    key: key("kernel", "vm.swappiness"),
                    values: vec![integer("10"), integer("10")],
                    differs: false,
                },
                OmbPropertyRow {
                    key: key("server", "huge_pages"),
                    values: vec![integer("2048"), integer("2048")],
                    differs: false,
                },
            ]
        );
        // The same digits as another type differ.
        assert_eq!(
            OmbPropertyKey::differing(&[&base, &float]),
            [key("kernel", "vm.swappiness")]
        );
    }

    #[test]
    fn given_run_without_sidecar_when_listing_rows_should_mark_every_property_not_set() {
        let rc1 = RC1_DEFAULT.summary();
        let without_sidecar = NO_CONSUMERS.summary();

        let rows = OmbPropertyRow::of_runs(&[&rc1, &without_sidecar]);

        assert_eq!(rows.len(), 7);
        assert!(
            rows.iter()
                .all(|row| row.differs && row.values[1].is_none())
        );
        assert_eq!(
            OmbPropertyRow::of_runs(&[&without_sidecar, &without_sidecar]),
            []
        );
        assert_eq!(OmbPropertyRow::of_runs(&[]), []);
    }

    #[test]
    fn given_one_run_when_listing_rows_should_list_its_properties_as_not_differing() {
        let rc1 = RC1_DEFAULT.summary();
        let setup = rc1.setup.as_ref().expect("the fixture has a sidecar");
        let group = |title: &str| -> Option<&OmbPropertyGroup> {
            setup.properties.iter().find(|group| group.title == title)
        };

        let rows = OmbPropertyRow::of_runs(&[&rc1]);

        assert!(rows.iter().all(|row| !row.differs));
        assert_eq!(
            rows.iter()
                .map(|row| row.key.to_string())
                .collect::<Vec<_>>(),
            [
                "kernel / vm.dirty_background_bytes",
                "kernel / vm.dirty_bytes",
                "kernel / vm.dirty_expire_centisecs",
                "server / data_file_system",
                "server / huge_pages",
                "server / placement_strategy",
                "server / server_environment",
            ]
        );
        assert_eq!(
            group("server").map(|group| &group.properties[0]),
            Some(&OmbProperty {
                key: "data_file_system".to_owned(),
                value: OmbPropertyValue {
                    text: "ext4".to_owned(),
                    kind: OmbScalarKind::String
                }
            })
        );
    }

    #[test]
    fn given_comparability_when_serialized_should_round_trip_with_fields() {
        for level in [
            OmbComparability::LikeForLike,
            OmbComparability::SetupUnknown,
            OmbComparability::SetupDiffers(vec![OmbSetupField::ClientHosts]),
            OmbComparability::WorkloadDiffers(vec![OmbWorkloadField::MessageSize]),
        ] {
            let json = serde_json::to_string(&level).expect("levels serialize");

            assert_eq!(
                serde_json::from_str::<OmbComparability>(&json).expect("levels deserialize"),
                level
            );
        }
        assert_eq!(
            serde_json::to_string(&OmbComparability::SetupDiffers(vec![
                OmbSetupField::OmbGitref
            ]))
            .expect("levels serialize"),
            r#"{"setup_differs":["omb_gitref"]}"#
        );
    }
}
