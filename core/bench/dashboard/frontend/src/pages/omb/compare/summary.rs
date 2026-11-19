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

//! What the summary card and the run cards of the OMB compare page say about how far each run
//! compares with the baseline: the levels, their texts, and the fields, drivers and properties that
//! differ.

use bench_dashboard_shared::omb::compare::{OmbComparability, OmbPropertyKey};
use bench_dashboard_shared::omb::summary::OmbRunSummary;

const FIELD_SEPARATOR: &str = ", ";
const SAME_PROPERTIES: &str = "Every run sets the same properties to the same values.";

/// The texts under the summary's level: the explanation of the furthest level, then the setup
/// warning when another run's setup is unknown, so a further level never hides the warning. Empty
/// without levels.
pub fn level_texts(levels: &[OmbComparability]) -> Vec<&'static str> {
    let Some(furthest) = levels.iter().max() else {
        return Vec::new();
    };
    let unknown = OmbComparability::SetupUnknown;
    let mut texts = vec![furthest.explanation()];
    if *furthest != unknown && levels.contains(&unknown) {
        texts.push(unknown.explanation());
    }
    texts
}

/// The workload fields or the setup values that differ from the baseline at `level`, for example
/// `Workload, Consumers per topic`. `None` at a level that names no difference.
pub fn level_fields(level: &OmbComparability) -> Option<String> {
    let names: Vec<String> = match level {
        OmbComparability::WorkloadDiffers(fields) => {
            fields.iter().map(ToString::to_string).collect()
        }
        OmbComparability::SetupDiffers(fields) => fields.iter().map(ToString::to_string).collect(),
        OmbComparability::LikeForLike | OmbComparability::SetupUnknown => return None,
    };
    (!names.is_empty()).then(|| names.join(FIELD_SEPARATOR))
}

/// The summary's line about the drivers: the names of the runs in run order, or the one name that
/// every run shares. The driver doesn't change the level, because comparing two driver
/// configurations of one workload is a fair comparison, so the line tells the reader that a change
/// can come from the driver. `None` without runs.
pub fn drivers_text(runs: &[&OmbRunSummary]) -> Option<String> {
    let mut drivers: Vec<&str> = Vec::with_capacity(runs.len());
    for run in runs {
        if !drivers.contains(&run.driver.as_str()) {
            drivers.push(&run.driver);
        }
    }
    match drivers.as_slice() {
        [] => None,
        [driver] => Some(format!("Every run uses the same driver: {driver}.")),
        _ => Some(format!(
            "Drivers that differ: {}.",
            drivers.join(FIELD_SEPARATOR)
        )),
    }
}

/// The summary's line about the sidecar properties: the ones that differ, as `group / key`, or that
/// they're all equal. Properties don't change the level, because the page can't know which of them
/// changes a result, so the reader decides. `None` when no run sets a property.
pub fn properties_text(runs: &[&OmbRunSummary]) -> Option<String> {
    let has_properties = runs
        .iter()
        .filter_map(|run| run.setup.as_ref())
        .any(|setup| setup.properties().next().is_some());
    if !has_properties {
        return None;
    }
    let differing: Vec<String> = OmbPropertyKey::differing(runs)
        .iter()
        .map(ToString::to_string)
        .collect();
    Some(if differing.is_empty() {
        SAME_PROPERTIES.to_owned()
    } else {
        format!(
            "Properties that differ: {}.",
            differing.join(FIELD_SEPARATOR)
        )
    })
}

/// The colors of a level. A level that limits what a change says gets the accent, and different
/// workloads, which have no change, get the color of a worse change, as on the iggy-bench page.
pub const fn level_look(level: &OmbComparability) -> &'static str {
    match level {
        OmbComparability::LikeForLike => "compare-level-like",
        OmbComparability::SetupUnknown => "omb-compare-level-unknown",
        OmbComparability::SetupDiffers(_) => "compare-level-environment",
        OmbComparability::WorkloadDiffers(_) => "compare-level-different",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        OMB_EDGE8_100_PARTITIONS, OMB_EDGE8_DEFAULT, OMB_NO_CONSUMERS, OMB_PUBLISH_ERRORS,
        OMB_RC1_DEFAULT, OMB_RC1_PERSISTED,
    };

    /// The warning that the page must show word for word when a run's setup is unknown.
    const SETUP_UNKNOWN_WARNING: &str = "OMB results do not record hardware. Make sure that the runs \
                                         used the same machines before you read a change as an \
                                         effect of the version or the configuration.";

    fn levels_against_rc1(others: &[&OmbRunSummary]) -> Vec<OmbComparability> {
        let rc1 = OMB_RC1_DEFAULT.summary();
        others
            .iter()
            .map(|other| OmbComparability::between(&rc1, other))
            .collect()
    }

    #[test]
    fn given_like_for_like_runs_when_texts_picked_should_say_where_change_comes_from() {
        let levels =
            levels_against_rc1(&[&OMB_RC1_PERSISTED.summary(), &OMB_EDGE8_DEFAULT.summary()]);

        let texts = level_texts(&levels);

        assert_eq!(texts.len(), 1);
        assert!(texts[0].ends_with(
            "a change comes from the Iggy version, the driver configuration or the properties."
        ));
    }

    #[test]
    fn given_run_without_sidecar_of_same_workload_when_texts_picked_should_warn_word_for_word() {
        let mut no_sidecar = OMB_RC1_PERSISTED.summary();
        no_sidecar.setup = None;

        let levels = levels_against_rc1(&[&no_sidecar]);

        assert_eq!(levels, [OmbComparability::SetupUnknown]);
        assert_eq!(level_texts(&levels), [SETUP_UNKNOWN_WARNING]);
        assert_eq!(level_fields(&levels[0]), None);
    }

    #[test]
    fn given_unknown_setup_next_to_other_workload_when_texts_picked_should_keep_warning() {
        let mut no_sidecar = OMB_RC1_PERSISTED.summary();
        no_sidecar.setup = None;

        let levels = levels_against_rc1(&[&no_sidecar, &OMB_NO_CONSUMERS.summary()]);

        let texts = level_texts(&levels);

        assert_eq!(
            texts,
            [
                OmbComparability::WorkloadDiffers(Vec::new()).explanation(),
                SETUP_UNKNOWN_WARNING
            ]
        );
        assert!(texts[0].contains("side by side with no change"));
        assert!(level_texts(&[]).is_empty());
    }

    #[test]
    fn given_other_workload_or_machines_when_fields_named_should_list_what_differs() {
        let mut other_machines = OMB_RC1_PERSISTED.summary();
        other_machines.setup = OMB_EDGE8_100_PARTITIONS.setup();

        let levels = levels_against_rc1(&[&OMB_NO_CONSUMERS.summary(), &other_machines]);

        assert_eq!(
            level_fields(&levels[0]).as_deref(),
            Some("Workload, Consumers per topic")
        );
        assert_eq!(
            level_fields(&levels[1]).as_deref(),
            Some("Server instance, Client instance")
        );
        assert_eq!(level_look(&levels[1]), "compare-level-environment");
        assert_eq!(level_texts(&levels).len(), 1);
    }

    #[test]
    fn given_fixture_drivers_when_summarized_should_name_each_driver_once_in_run_order() {
        let (rc1, persisted, edge8) = (
            OMB_RC1_DEFAULT.summary(),
            OMB_RC1_PERSISTED.summary(),
            OMB_EDGE8_DEFAULT.summary(),
        );

        assert_eq!(
            drivers_text(&[&rc1, &persisted, &edge8]).as_deref(),
            Some("Drivers that differ: Iggy, Iggy-persisted.")
        );
        assert_eq!(
            drivers_text(&[&persisted, &rc1]).as_deref(),
            Some("Drivers that differ: Iggy-persisted, Iggy.")
        );
        assert_eq!(
            drivers_text(&[&rc1, &edge8]).as_deref(),
            Some("Every run uses the same driver: Iggy.")
        );
        assert_eq!(drivers_text(&[]), None);
    }

    #[test]
    fn given_fixture_sidecars_when_properties_summarized_should_name_differing_keys() {
        let (rc1, persisted, edge8) = (
            OMB_RC1_DEFAULT.summary(),
            OMB_RC1_PERSISTED.summary(),
            OMB_EDGE8_DEFAULT.summary(),
        );

        assert_eq!(
            properties_text(&[&rc1, &edge8]).as_deref(),
            Some("Properties that differ: server / data_file_system.")
        );
        assert_eq!(
            properties_text(&[&rc1, &persisted]).as_deref(),
            Some(SAME_PROPERTIES)
        );
        assert_eq!(
            properties_text(&[&OMB_NO_CONSUMERS.summary(), &OMB_PUBLISH_ERRORS.summary()]),
            None
        );
    }
}
