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

//! The latency ladder: one row per percentile, from the fastest message to the slowest, with a bar
//! on a log scale. The run pages of both frameworks draw their percentiles with it.

use yew::prelude::*;

/// The ladder's shortest bar, so the smallest value stays visible.
pub const MIN_BAR_PCT: f64 = 4.0;
pub const FULL_BAR_PCT: f64 = 100.0;

/// One row of the ladder.
#[derive(Clone, PartialEq)]
pub struct Rung {
    pub label: &'static str,
    /// What the row means, on hover over its label.
    pub hint: Option<&'static str>,
    /// The value in its unit, for example `1.52 ms`.
    pub value: String,
    /// The length of the bar in percent of the longest bar, from [`log_bar_pcts`].
    pub bar_pct: f64,
    /// The row that the page's headline numbers use, for example P99, in the accent.
    pub key: bool,
    /// The change of the row, for example a change chip. Empty for a row without one.
    pub change: Html,
}

/// `with_changes` adds a column for each row's change. Without it, the ladder leaves the column out
/// and gives the room to the bars.
pub fn percentile_ladder(rungs: &[Rung], with_changes: bool) -> Html {
    html! {
        <ol class={classes!("run-ladder", (!with_changes).then_some("run-ladder-plain"))}>
            { for rungs.iter().map(|rung| html! {
                <li class={classes!("run-ladder-row", rung.key.then_some("run-ladder-key"))}>
                    <span class="run-ladder-label" title={rung.hint}>{ rung.label }</span>
                    <span class="run-ladder-track" aria-hidden="true">
                        <i style={format!("width: {:.1}%", rung.bar_pct)}></i>
                    </span>
                    <strong>{ rung.value.clone() }</strong>
                    <span class="run-ladder-change">{ rung.change.clone() }</span>
                </li>
            }) }
        </ol>
    }
}

/// The bar length of each of `values` on a log axis, from [`MIN_BAR_PCT`] for the smallest positive
/// value to [`FULL_BAR_PCT`] for the largest. A value of 0 or less, which a log axis has no place
/// for, gets the shortest bar, and equal values get full bars.
pub fn log_bar_pcts(values: &[f64]) -> Vec<f64> {
    let positive = || values.iter().copied().filter(|value| *value > 0.0);
    let lowest = positive().fold(f64::INFINITY, f64::min);
    let span = (positive().fold(0.0, f64::max) / lowest).ln();
    values
        .iter()
        .map(|&value| {
            if value <= 0.0 {
                MIN_BAR_PCT
            } else if span > 0.0 {
                // Dividing first gives the largest value exactly 1, so its bar is exactly full.
                MIN_BAR_PCT + (FULL_BAR_PCT - MIN_BAR_PCT) * ((value / lowest).ln() / span)
            } else {
                FULL_BAR_PCT
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{OMB_NO_CONSUMERS, OMB_RC1_DEFAULT};

    #[test]
    fn given_values_over_two_decades_when_scaled_should_put_middle_decade_halfway() {
        let bars = log_bar_pcts(&[0.1, 1.0, 10.0]);

        assert_eq!(bars[0], MIN_BAR_PCT);
        assert_eq!(bars[2], FULL_BAR_PCT);
        let halfway = MIN_BAR_PCT + (FULL_BAR_PCT - MIN_BAR_PCT) / 2.0;
        assert!((bars[1] - halfway).abs() < 1e-9, "{}", bars[1]);
    }

    #[test]
    fn given_zero_or_equal_values_when_scaled_should_give_shortest_or_full_bars() {
        assert_eq!(
            log_bar_pcts(&[0.0, 2.0, 2.0]),
            [MIN_BAR_PCT, FULL_BAR_PCT, FULL_BAR_PCT]
        );
        assert_eq!(log_bar_pcts(&[0.0, -1.0]), [MIN_BAR_PCT, MIN_BAR_PCT]);
        assert!(log_bar_pcts(&[]).is_empty());
    }

    #[test]
    fn given_omb_fixture_publish_percentiles_when_scaled_should_grow_from_p50_to_max() {
        for fixture in [&OMB_RC1_DEFAULT, &OMB_NO_CONSUMERS] {
            let latency = fixture.summary().publish_latency_ms;
            let values = [
                latency.p50,
                latency.p75,
                latency.p95,
                latency.p99,
                latency.p999,
                latency.p9999,
                latency.max,
            ];

            let bars = log_bar_pcts(&values);

            assert_eq!(bars[0], MIN_BAR_PCT, "{}", fixture.file_name);
            assert_eq!(bars[6], FULL_BAR_PCT, "{}", fixture.file_name);
            assert!(
                bars.windows(2).all(|pair| pair[0] <= pair[1]),
                "{}: {bars:?}",
                fixture.file_name
            );
        }
    }
}
