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

//! The latency quantile curves of the OMB run and compare pages: the value at every percentile of a
//! merged histogram, on a log axis, against a percentile axis that counts nines. Both pages draw
//! them here, so a run's curve has the same axis on both.

use bench_dashboard_shared::omb::result::OmbQuantile;
use bench_report::plotting::chart::IggyChart;
use charming::Chart;
use charming::component::Axis;
use charming::element::{AxisLabel, AxisPointer, AxisType, JsFunction, Label, SplitLine};

use crate::pages::run::chart::LINE_OPACITY;

pub const LATENCY_AXIS_NAME: &str = "Latency [ms]";
/// The percentile axis counts nines: 1 is P90, 2 is P99, 3 is P99.9. Each step gets a split line
/// and a label, so the tail gets as much room as the median.
const NINES_STEP: f64 = 1.0;
const PERCENT: f64 = 100.0;
/// Writes an axis value in nines as a percentile, for example `P99.9`.
const NINES_LABEL: &str =
    "return 'P' + parseFloat((100 - 100 / Math.pow(10, value)).toPrecision(12));";
/// The longest label sits at the end of the axis, where the card has no room for it, so that tick
/// stays unlabeled. The axis ends past the last point, so no point loses its label.
const LAST_NINE_LABEL: &str = "if (value >= {max}) { return ''; }";
/// The same for the pointer, which ECharts hands its value inside `params`. Six digits tell the
/// points of a curve apart.
const NINES_POINTER_LABEL: &str =
    "return 'P' + parseFloat((100 - 100 / Math.pow(10, params.value)).toPrecision(6));";

/// One curve of a quantile chart.
#[derive(Debug, Clone, PartialEq)]
pub struct QuantileCurve {
    pub name: String,
    /// `[nines, value]` points from [`curve_points`].
    pub points: Vec<Vec<f64>>,
    /// The chart's palette colors a curve without one.
    pub color: Option<&'static str>,
}

impl QuantileCurve {
    /// `None` for a curve without a point to draw.
    pub fn new(
        name: impl Into<String>,
        curve: &[OmbQuantile],
        color: Option<&'static str>,
    ) -> Option<Self> {
        let points = curve_points(curve);
        (!points.is_empty()).then(|| Self {
            name: name.into(),
            points,
            color,
        })
    }
}

/// The curves on a log y axis named `axis_name`. The x axis ends at the first whole nine past the
/// last point of any curve.
pub fn quantile_options(axis_name: &str, curves: &[QuantileCurve]) -> Chart {
    let base = IggyChart::card().with_log_y_axis(axis_name);
    let mut chart = IggyChart {
        inner: base.inner.x_axis(nines_axis(axis_end(curves))),
    };
    for curve in curves {
        chart = chart.add_time_series(&curve.name, curve.points.clone(), curve.color, LINE_OPACITY);
    }
    chart.inner
}

/// How many nines `percentile` has: 1 for P90, 2 for P99, 3 for P99.9. `None` for P100 and above,
/// which are infinitely far out, and for a percentile that isn't a number.
pub fn nines(percentile: f64) -> Option<f64> {
    (percentile.is_finite() && percentile < PERCENT)
        .then(|| -(1.0 - percentile.max(0.0) / PERCENT).log10())
}

/// A log axis has no place for 0, which the fastest messages can round to, and P100 has no place on
/// the nines axis.
pub fn curve_points(curve: &[OmbQuantile]) -> Vec<Vec<f64>> {
    curve
        .iter()
        .filter(|quantile| quantile.value > 0.0)
        .filter_map(|quantile| Some(vec![nines(quantile.percentile)?, quantile.value]))
        .collect()
}

/// The first whole nine past the last point of `curves`.
fn axis_end(curves: &[QuantileCurve]) -> f64 {
    let last = curves
        .iter()
        .flat_map(|curve| curve.points.iter().map(|point| point[0]))
        .fold(0.0, f64::max);
    (last / NINES_STEP).floor() * NINES_STEP + NINES_STEP
}

fn nines_axis(end: f64) -> Axis {
    let label = format!(
        "{} {NINES_LABEL}",
        LAST_NINE_LABEL.replace("{max}", &end.to_string())
    );
    Axis::new()
        .type_(AxisType::Value)
        .min(0)
        .max(end)
        .interval(NINES_STEP)
        .axis_label(AxisLabel::new().formatter(JsFunction::new_with_args("value", &label)))
        .axis_pointer(AxisPointer::new().label(
            Label::new().formatter(JsFunction::new_with_args("params", NINES_POINTER_LABEL)),
        ))
        .split_line(SplitLine::new().show(true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::OMB_RC1_DEFAULT;

    fn curve(name: &str, points: Vec<Vec<f64>>) -> QuantileCurve {
        QuantileCurve {
            name: name.to_owned(),
            points,
            color: None,
        }
    }

    #[test]
    fn given_percentiles_when_counted_in_nines_should_space_tail_evenly() {
        let close = |percentile: f64, expected: f64| {
            let value = nines(percentile).expect("a percentile below 100 has nines");
            assert!((value - expected).abs() < 1e-9, "{percentile}: {value}");
        };

        close(0.0, 0.0);
        close(90.0, 1.0);
        close(99.0, 2.0);
        close(99.9, 3.0);
        assert_eq!(nines(100.0), None);
        assert_eq!(nines(f64::NAN), None);
    }

    #[test]
    fn given_fixture_quantiles_when_curved_should_drop_p100_and_zero_and_keep_order() {
        let detail = OMB_RC1_DEFAULT.detail();
        let publish = &detail.quantiles.publish_latency_ms;

        let points = curve_points(publish);

        let kept = publish
            .iter()
            .filter(|quantile| quantile.percentile < 100.0 && quantile.value > 0.0)
            .count();
        assert_eq!(points.len(), kept);
        assert!(points.len() > 1_000, "{}", points.len());
        assert!(points.windows(2).all(|pair| pair[0][0] <= pair[1][0]));
        assert!(points.iter().all(|point| point[1] > 0.0));
    }

    #[test]
    fn given_fixture_curves_when_read_at_two_nines_should_match_aggregated_p99() {
        let detail = OMB_RC1_DEFAULT.detail();
        let p99_nines = nines(99.0).expect("P99 has nines");
        let at_p99 = |points: &[Vec<f64>]| {
            points
                .iter()
                .find(|point| point[0] >= p99_nines)
                .map(|point| point[1])
        };

        let publish = curve_points(&detail.quantiles.publish_latency_ms);
        let end_to_end = curve_points(
            detail
                .quantiles
                .end_to_end_latency_ms
                .as_deref()
                .unwrap_or_default(),
        );

        assert_eq!(
            at_p99(&publish),
            Some(detail.summary.publish_latency_ms.p99)
        );
        assert_eq!(
            at_p99(&end_to_end),
            detail
                .summary
                .end_to_end_latency_ms
                .map(|latency| latency.p99)
        );
    }

    #[test]
    fn given_fixture_curves_when_axis_sized_should_end_on_next_whole_nine_past_last_point() {
        let detail = OMB_RC1_DEFAULT.detail();
        let curves: Vec<QuantileCurve> = [
            QuantileCurve::new("Publish", &detail.quantiles.publish_latency_ms, None),
            detail
                .quantiles
                .end_to_end_latency_ms
                .as_deref()
                .and_then(|curve| QuantileCurve::new("End-to-end", curve, None)),
        ]
        .into_iter()
        .flatten()
        .collect();
        let last = curves
            .iter()
            .flat_map(|curve| curve.points.iter().map(|point| point[0]))
            .fold(0.0, f64::max);

        let end = axis_end(&curves);

        assert_eq!(curves.len(), 2);
        assert_eq!(end.fract(), 0.0);
        assert!(end > last && end - last <= 1.0, "{last} to {end}");
        assert_eq!(axis_end(&[curve("Publish", vec![vec![2.0, 1.0]])]), 3.0);
        assert_eq!(axis_end(&[]), 1.0);
    }

    #[test]
    fn given_curve_without_drawable_point_when_built_should_be_none() {
        let only_p100 = [OmbQuantile {
            percentile: 100.0,
            value: 4.0,
        }];

        assert_eq!(QuantileCurve::new("Publish", &only_p100, None), None);
        assert_eq!(QuantileCurve::new("Publish", &[], None), None);
    }
}
