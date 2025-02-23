/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

pub mod plotting;
pub mod types;
pub mod utils;

mod prints;

use crate::report::BenchmarkReport;
use actor_kind::ActorKind;
use charming::Chart;
use group_metrics_kind::GroupMetricsKind;
use plotting::chart::IggyChart;
use plotting::chart_kind::ChartKind;

pub use types::*;

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
            metrics.throughput_mb_ts.as_charming_points(),
            None,
            0.4,
            0,
            1.0,
        );
        chart = chart.add_dual_time_line_series(
            &format!("{} {} [msg/s]", actor_type, metrics.summary.actor_id),
            metrics.throughput_msg_ts.as_charming_points(),
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
            metrics.avg_throughput_mb_ts.as_charming_points(),
            None,
            1.0,
            0,
            2.0,
        );
        chart = chart.add_dual_time_line_series(
            &format!("All {}s [msg/s]", metrics.summary.kind.actor()),
            metrics.avg_throughput_msg_ts.as_charming_points(),
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
            metrics.latency_ts.as_charming_points(),
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
            metrics.avg_latency_ts.as_charming_points(),
            None,
            1.0,
            0,
            3.0,
        );
    }

    chart.inner
}
