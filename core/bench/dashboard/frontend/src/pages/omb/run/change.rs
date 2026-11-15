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

//! An OMB run's headline numbers and their change on its history line: from the median of the
//! previous version to the median of the run's version, as on the iggy-bench pages, or from the
//! previous run on a line without versions. OMB runs have no noise rule yet, so a change is a plain
//! percent without a verdict.

use bench_dashboard_shared::delta::ValueRange;
use bench_dashboard_shared::omb::summary::OmbRunSummary;
use yew::prelude::*;

use super::history::Baseline;

use crate::components::headline::neutral_chip;
use crate::components::run_labels::{messages_text, milliseconds};
use crate::pages::omb::common::{
    END_TO_END_LATENCY_HINT, NOT_MEASURED, NOT_RECORDED, PUBLISH_DELAY_HINT, PUBLISH_LATENCY_HINT,
    PUBLISH_THROUGHPUT_HINT, change_text, mebibytes_text, microseconds, run_name, short_id,
    version_text,
};
use crate::pages::run::change::{percent_change, range_text};

/// The headline numbers, in the order of the strip.
pub const KPIS: [Kpi; 4] = [
    Kpi::PublishThroughput,
    Kpi::PublishP99,
    Kpi::EndToEndP99,
    Kpi::PublishDelayP99,
];

/// One headline number of an OMB run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kpi {
    /// In MiB/s.
    PublishThroughput,
    /// In ms.
    PublishP99,
    /// In ms.
    EndToEndP99,
    /// In µs.
    PublishDelayP99,
}

/// One number's change from the baseline of the run.
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    pub percent: f64,
    /// The previous version, or the previous run as short as it can be told from the page's run.
    pub versus: String,
    /// Both sides and their values, on hover.
    pub values: String,
}

impl Kpi {
    pub const fn label(self) -> &'static str {
        match self {
            Self::PublishThroughput => "Publish throughput",
            Self::PublishP99 => "Publish P99",
            Self::EndToEndP99 => "End-to-end P99",
            Self::PublishDelayP99 => "Publish delay P99",
        }
    }

    pub const fn hint(self) -> &'static str {
        match self {
            Self::PublishThroughput => PUBLISH_THROUGHPUT_HINT,
            Self::PublishP99 => PUBLISH_LATENCY_HINT,
            Self::EndToEndP99 => END_TO_END_LATENCY_HINT,
            Self::PublishDelayP99 => PUBLISH_DELAY_HINT,
        }
    }

    /// The name of a chart axis with this number, with its unit.
    pub const fn axis_name(self) -> &'static str {
        match self {
            Self::PublishThroughput => "Throughput [MiB/s]",
            Self::PublishP99 | Self::EndToEndP99 => "P99 [ms]",
            Self::PublishDelayP99 => "Delay P99 [µs]",
        }
    }

    /// `None` when the run didn't measure the number or its file doesn't hold it.
    pub fn value(self, run: &OmbRunSummary) -> Option<f64> {
        match self {
            Self::PublishThroughput => Some(run.publish_mebibytes_per_second),
            Self::PublishP99 => Some(run.publish_latency_ms.p99),
            Self::EndToEndP99 => run.end_to_end_latency_ms.map(|latency| latency.p99),
            Self::PublishDelayP99 => run.publish_delay_p99_us,
        }
    }

    /// The value in its unit. A missing end-to-end latency was [`NOT_MEASURED`], and a missing
    /// publish delay is [`NOT_RECORDED`] by an older OMB.
    pub fn text(self, value: Option<f64>) -> String {
        match (self, value) {
            (Self::PublishThroughput, Some(value)) => mebibytes_text(value),
            (Self::PublishP99 | Self::EndToEndP99, Some(value)) => milliseconds(value),
            (Self::PublishDelayP99, Some(value)) => microseconds(value),
            (Self::PublishThroughput | Self::PublishP99 | Self::EndToEndP99, None) => {
                NOT_MEASURED.to_owned()
            }
            (Self::PublishDelayP99, None) => NOT_RECORDED.to_owned(),
        }
    }

    /// The line below the value: the message rate for the throughput.
    pub fn detail(self, run: &OmbRunSummary) -> Option<String> {
        (self == Self::PublishThroughput).then(|| messages_text(run.publish_messages_per_second))
    }

    /// `None` unless every run of both sides holds the number, as for a median on the History
    /// charts.
    pub fn change(self, run: &OmbRunSummary, baseline: &Baseline<'_>) -> Option<Change> {
        let text = |value| self.text(Some(value));
        match baseline {
            Baseline::Version(step) => {
                let (base, head) = (self.range(&step.base)?, self.range(&step.head)?);
                Some(Change {
                    percent: percent_change(base.median, head.median)?,
                    versus: step.previous.clone(),
                    values: format!(
                        "{}. {}.",
                        range_text(&step.previous, step.base.len(), &base, text),
                        range_text(&step.version, step.head.len(), &head, text)
                    ),
                })
            }
            Baseline::Run(previous) => {
                let (base, value) = (self.value(previous)?, self.value(run)?);
                Change::between(run, previous, base, value, text)
            }
        }
    }

    fn range(self, runs: &[&OmbRunSummary]) -> Option<ValueRange> {
        let values: Option<Vec<f64>> = runs.iter().map(|run| self.value(run)).collect();
        ValueRange::of_values(values?)
    }
}

impl Change {
    /// The change from `base`, the value of `previous`, to `value`, the value of `run`. `text`
    /// writes a value in its unit. `None` when `base` is 0.
    pub fn between(
        run: &OmbRunSummary,
        previous: &OmbRunSummary,
        base: f64,
        value: f64,
        text: impl Fn(f64) -> String,
    ) -> Option<Self> {
        let percent = percent_change(base, value)?;
        Some(Self {
            percent,
            versus: versus(run, previous),
            values: format!(
                "Previous run {} ({}): {}. This run: {}.",
                run_name(previous),
                previous.time.text,
                text(base),
                text(value)
            ),
        })
    }

    /// For example `+4.6% vs 0.9.1-edge.8`, or `0.0% vs 0.9.1-edge.8` for a change that rounds
    /// to zero.
    pub fn text(&self) -> String {
        format!("{} vs {}", change_text(self.percent), self.versus)
    }

    /// A gray chip, because no noise rule tells a real change apart.
    pub fn chip(&self) -> Html {
        neutral_chip(self.text(), self.values.clone())
    }
}

/// The previous run's version when it differs from the version of `run`, else its short id.
pub fn versus(run: &OmbRunSummary, previous: &OmbRunSummary) -> String {
    match version_text(previous) {
        Some(version) if version_text(run).as_ref() != Some(&version) => version,
        _ => short_id(previous.id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pages::omb::run::history::VersionStep;
    use crate::test_support::{OMB_EDGE8_DEFAULT, OMB_NO_CONSUMERS, OMB_RC1_DEFAULT, omb_rerun};

    #[test]
    fn given_fixture_runs_when_kpis_written_should_use_omb_units() {
        let run = OMB_RC1_DEFAULT.summary();

        let texts: Vec<_> = KPIS.iter().map(|kpi| kpi.text(kpi.value(&run))).collect();

        assert_eq!(texts, ["143 MiB/s", "1.52 ms", "2.00 ms", "70.0 µs"]);
        assert_eq!(
            Kpi::PublishThroughput.detail(&run).as_deref(),
            Some("150.2k msg/s")
        );
        assert_eq!(Kpi::PublishP99.detail(&run), None);
    }

    #[test]
    fn given_run_without_consumers_when_kpis_written_should_say_end_to_end_not_measured() {
        let run = OMB_NO_CONSUMERS.summary();
        let mut older_omb = OMB_RC1_DEFAULT.summary();
        older_omb.publish_delay_p99_us = None;

        assert_eq!(Kpi::EndToEndP99.value(&run), None);
        assert_eq!(Kpi::EndToEndP99.text(None), NOT_MEASURED);
        assert_eq!(Kpi::PublishDelayP99.text(None), NOT_RECORDED);
        assert_eq!(
            Kpi::PublishDelayP99.text(Kpi::PublishDelayP99.value(&older_omb)),
            NOT_RECORDED
        );
    }

    /// The step from the edge.8 fixture's version to the rc.1 fixture's version.
    fn version_step<'a>(base: &[&'a OmbRunSummary], head: &[&'a OmbRunSummary]) -> Baseline<'a> {
        Baseline::Version(VersionStep {
            previous: "0.9.1-edge.8".to_owned(),
            base: base.to_vec(),
            version: "0.10.0-rc.1".to_owned(),
            head: head.to_vec(),
        })
    }

    #[test]
    fn given_previous_version_when_changed_should_compare_values_and_name_version() {
        let run = OMB_RC1_DEFAULT.summary();
        let previous = OMB_EDGE8_DEFAULT.summary();

        let change = Kpi::PublishP99
            .change(&run, &version_step(&[&previous], &[&run]))
            .expect("both runs measured the publish P99");

        let expected = (1.523 - 1.519) / 1.519 * 100.0;
        assert!(
            (change.percent - expected).abs() < 1e-9,
            "{}",
            change.percent
        );
        assert_eq!(change.text(), "+0.3% vs 0.9.1-edge.8");
        assert_eq!(
            change.values,
            "0.9.1-edge.8: 1 run, 1.52 ms. 0.10.0-rc.1: 1 run, 1.52 ms."
        );
    }

    #[test]
    fn given_reruns_of_run_version_when_changed_should_compare_medians_not_page_run() {
        let previous = OMB_EDGE8_DEFAULT.summary();
        let rc1 = OMB_RC1_DEFAULT.summary();
        let mut slow = omb_rerun(&rc1, "00000000000000a1", 24);
        slow.publish_latency_ms.p99 = 2.0;
        let mut fast = omb_rerun(&rc1, "00000000000000a2", 48);
        fast.publish_latency_ms.p99 = 1.0;

        let change = Kpi::PublishP99
            .change(&slow, &version_step(&[&previous], &[&rc1, &slow, &fast]))
            .expect("every run measured the publish P99");

        assert_eq!(
            change.text(),
            "+0.3% vs 0.9.1-edge.8",
            "the median of rc.1 is 1.523 ms, so the slow rerun doesn't make the change +31.7%"
        );
        assert_eq!(
            change.values,
            "0.9.1-edge.8: 1 run, 1.52 ms. 0.10.0-rc.1: 3 runs, 1.00 ms to 2.00 ms, median 1.52 ms."
        );
    }

    #[test]
    fn given_previous_run_on_line_without_versions_when_changed_should_name_both_runs() {
        let run = OMB_RC1_DEFAULT.summary();
        let previous = OMB_EDGE8_DEFAULT.summary();

        let change = Kpi::PublishP99
            .change(&run, &Baseline::Run(&previous))
            .expect("both runs measured the publish P99");

        assert_eq!(change.text(), "+0.3% vs 0.9.1-edge.8");
        assert_eq!(
            change.values,
            format!(
                "Previous run {} (2026-11-03 06:19:24 UTC): 1.52 ms. This run: 1.52 ms.",
                run_name(&previous)
            )
        );
    }

    #[test]
    fn given_fixture_throughputs_that_round_to_same_value_when_changed_should_show_unsigned_zero() {
        let run = OMB_RC1_DEFAULT.summary();
        let previous = OMB_EDGE8_DEFAULT.summary();

        let change = Kpi::PublishThroughput
            .change(&run, &version_step(&[&previous], &[&run]))
            .expect("both runs measured the throughput");

        assert!(change.percent.abs() < 0.05, "{}", change.percent);
        assert_eq!(change.text(), "0.0% vs 0.9.1-edge.8");
    }

    #[test]
    fn given_runs_without_end_to_end_numbers_when_changed_should_have_no_change() {
        let run = OMB_RC1_DEFAULT.summary();
        let previous = OMB_EDGE8_DEFAULT.summary();
        let no_consumers = OMB_NO_CONSUMERS.summary();

        assert_eq!(
            Kpi::EndToEndP99.change(&run, &Baseline::Run(&no_consumers)),
            None
        );
        assert_eq!(
            Kpi::EndToEndP99.change(&no_consumers, &Baseline::Run(&run)),
            None
        );
        assert!(
            Kpi::PublishThroughput
                .change(&no_consumers, &Baseline::Run(&run))
                .is_some()
        );
        assert_eq!(
            Kpi::EndToEndP99.change(&run, &version_step(&[&previous], &[&run, &no_consumers])),
            None,
            "a version with a run that lacks the number has no median"
        );
    }

    #[test]
    fn given_previous_run_of_same_version_when_named_should_use_short_id() {
        let run = OMB_RC1_DEFAULT.summary();
        let no_version = OMB_NO_CONSUMERS.summary();

        assert_eq!(versus(&run, &run), short_id(run.id));
        assert_eq!(versus(&run, &no_version), short_id(no_version.id));
        assert_eq!(versus(&no_version, &run), "0.10.0-rc.1");
    }
}
