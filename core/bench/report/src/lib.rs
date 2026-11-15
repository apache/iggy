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

pub mod plotting;
pub mod types;
pub mod utils;

mod prints;

use crate::report::BenchmarkReport;
use actor_kind::ActorKind;
use charming::Chart;
use group_metrics_kind::GroupMetricsKind;
use latency_distribution::LatencyDistribution;
use plotting::chart::IggyChart;
use plotting::chart_kind::ChartKind;

pub use types::*;

const LATENCY_AXIS_NAME: &str = "Latency [ms]";

pub fn create_throughput_chart(
    report: &BenchmarkReport,
    dark: bool,
    strip_title_and_subtext: bool,
) -> Chart {
    let title = report.title(ChartKind::Throughput);

    let mut chart = IggyChart::new(&title, &report.subtext(), dark, strip_title_and_subtext)
        .with_time_x_axis()
        .with_dual_y_axis("Throughput [MB/s]", "Throughput [msg/s]");

    // Add individual metrics series
    for metrics in &report.individual_metrics {
        let actor_type = match metrics.summary.actor_kind {
            ActorKind::Producer => "Producer",
            ActorKind::Consumer => "Consumer",
            ActorKind::ProducingConsumer => "Producing Consumer",
        };

        chart = chart.add_dual_time_line_series(
            &format!("{} {} [MB/s]", actor_type, metrics.summary.actor_id),
            metrics.throughput_mb_ts.as_downsampled_charming_points(),
            None,
            0.4,
            0,
            1.0,
        );
        chart = chart.add_dual_time_line_series(
            &format!("{} {} [msg/s]", actor_type, metrics.summary.actor_id),
            metrics.throughput_msg_ts.as_downsampled_charming_points(),
            None,
            0.4,
            1,
            1.0,
        );
    }

    // Add group metrics series
    for metrics in &report.group_metrics {
        // Skip aggregate metrics in charts
        if metrics.summary.kind == GroupMetricsKind::ProducersAndConsumers {
            continue;
        }

        chart = chart.add_dual_time_line_series(
            &format!("All {}s [MB/s]", metrics.summary.kind.actor()),
            metrics
                .avg_throughput_mb_ts
                .as_downsampled_charming_points(),
            None,
            1.0,
            0,
            2.0,
        );
        chart = chart.add_dual_time_line_series(
            &format!("All {}s [msg/s]", metrics.summary.kind.actor()),
            metrics
                .avg_throughput_msg_ts
                .as_downsampled_charming_points(),
            None,
            1.0,
            1,
            2.0,
        );
    }

    chart.inner
}

pub fn create_latency_chart(
    report: &BenchmarkReport,
    dark: bool,
    strip_title_and_subtext: bool,
) -> Chart {
    let title = report.title(ChartKind::Latency);

    let mut chart = IggyChart::new(&title, &report.subtext(), dark, strip_title_and_subtext)
        .with_time_x_axis()
        .with_y_axis("Latency [ms]");

    // Add individual metrics series
    for metrics in &report.individual_metrics {
        let actor_type = match metrics.summary.actor_kind {
            ActorKind::Producer => "Producer",
            ActorKind::Consumer => "Consumer",
            ActorKind::ProducingConsumer => "Producing Consumer",
        };

        chart = chart.add_time_series(
            &format!("{} {} [ms]", actor_type, metrics.summary.actor_id),
            metrics.latency_ts.as_downsampled_charming_points(),
            None,
            0.3,
        );
    }

    for metrics in &report.group_metrics {
        // Skip aggregate metrics in charts
        if metrics.summary.kind == GroupMetricsKind::ProducersAndConsumers {
            continue;
        }

        chart = chart.add_dual_time_line_series(
            &format!("Avg {}s [ms]", metrics.summary.kind.actor()),
            metrics.avg_latency_ts.as_downsampled_charming_points(),
            None,
            1.0,
            0,
            3.0,
        );
    }

    chart.inner
}

pub fn create_latency_distribution_chart(
    report: &BenchmarkReport,
    dark: bool,
    strip_title_and_subtext: bool,
) -> Chart {
    let title = report.title(ChartKind::LatencyDistribution);
    let chart = IggyChart::new(&title, &report.subtext(), dark, strip_title_and_subtext);
    let groups = report
        .group_metrics
        .iter()
        .map(|metrics| (metrics.summary.kind, metrics.latency_distribution.as_ref()));
    add_latency_distributions(chart, LATENCY_AXIS_NAME, groups).inner
}

/// Draws a histogram and its fitted log-normal curve per group, from the distributions alone, so a
/// report without time series can draw them too. `groups` holds every group of the report in report
/// order, including those without a distribution, because a group's position picks its colors.
/// `x_axis_name` can be empty for a chart without room at the axis end.
pub fn add_latency_distributions<'a>(
    chart: IggyChart,
    x_axis_name: &str,
    groups: impl IntoIterator<Item = (GroupMetricsKind, Option<&'a LatencyDistribution>)>,
) -> IggyChart {
    let mut chart = chart.with_y_axis("Density");

    let group_colors = ["#5470C6", "#91CC75", "#EE6666", "#FAC858"];
    let pdf_colors = ["#FF4500", "#228B22", "#8B008B", "#FF8C00"];

    let mut has_data = false;

    for (idx, (kind, distribution)) in groups
        .into_iter()
        .filter(|(kind, _)| *kind != GroupMetricsKind::ProducersAndConsumers)
        .enumerate()
    {
        let Some(dist) = distribution else {
            continue;
        };

        has_data = true;
        let group_label = format!("{}s", kind.actor());
        let color_idx = idx % group_colors.len();

        chart = add_distribution_series(
            chart,
            x_axis_name,
            dist,
            &group_label,
            group_colors[color_idx],
            pdf_colors[color_idx],
        );
    }

    if !has_data {
        chart = chart.with_category_x_axis(x_axis_name, vec!["No data".to_owned()]);
    }

    chart
}

fn add_distribution_series(
    mut chart: IggyChart,
    x_axis_name: &str,
    dist: &LatencyDistribution,
    group_label: &str,
    hist_color: &str,
    pdf_color: &str,
) -> IggyChart {
    if dist.bins.is_empty() {
        return chart;
    }

    let bin_width = if dist.bins.len() > 1 {
        dist.bins[1].edge_ms - dist.bins[0].edge_ms
    } else {
        1.0
    };

    // Category labels at bin centers
    let categories: Vec<String> = dist
        .bins
        .iter()
        .map(|b| format_latency(b.edge_ms + bin_width / 2.0))
        .collect();

    chart = chart.with_category_x_axis(x_axis_name, categories);

    // Histogram bars
    let bar_data: Vec<f64> = dist.bins.iter().map(|b| b.density).collect();
    chart = chart.add_bar_series(
        &format!("{group_label} Histogram"),
        bar_data,
        hist_color,
        0.6,
    );

    // Log-normal PDF curve at bin centers
    let mu = dist.log_normal_params.mu;
    let sigma = dist.log_normal_params.sigma;
    let pdf_data: Vec<Vec<f64>> = dist
        .bins
        .iter()
        .enumerate()
        .map(|(i, b)| {
            let x = b.edge_ms + bin_width / 2.0;
            vec![i as f64, log_normal_pdf(x, mu, sigma)]
        })
        .collect();

    chart.add_smooth_line_series(
        &format!("{group_label} Log-Normal PDF"),
        pdf_data,
        pdf_color,
        2.5,
    )
}

fn format_latency(ms: f64) -> String {
    if ms < 1.0 {
        format!("{ms:.2}")
    } else if ms < 10.0 {
        format!("{ms:.1}")
    } else {
        format!("{ms:.0}")
    }
}

fn log_normal_pdf(x: f64, mu: f64, sigma: f64) -> f64 {
    if x <= 0.0 || sigma <= 0.0 {
        return 0.0;
    }
    let ln_x = x.ln();
    let exponent = -((ln_x - mu).powi(2)) / (2.0 * sigma * sigma);
    exponent.exp() / (x * sigma * std::f64::consts::TAU.sqrt())
}
