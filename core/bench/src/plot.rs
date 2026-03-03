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

use bench_report::report::BenchmarkReport;
use charming::theme::Theme;
use charming::{Chart, HtmlRenderer};
use iggy::prelude::IggyByteSize;
use std::path::Path;
use std::process::Command;
use std::time::Instant;
use tracing::info;

pub enum ChartType {
    Throughput,
    Latency,
    LatencyDistribution,
}

impl ChartType {
    const fn name(&self) -> &'static str {
        match self {
            Self::Throughput => "throughput",
            Self::Latency => "latency",
            Self::LatencyDistribution => "latency_distribution",
        }
    }

    fn create_chart(&self) -> fn(&BenchmarkReport, bool, bool) -> Chart {
        match self {
            Self::Throughput => bench_report::create_throughput_chart,
            Self::Latency => bench_report::create_latency_chart,
            Self::LatencyDistribution => bench_report::create_latency_distribution_chart,
        }
    }

    fn get_samples(&self, report: &BenchmarkReport) -> usize {
        match self {
            Self::Throughput => report
                .individual_metrics
                .iter()
                .map(|m| m.throughput_mb_ts.points.len())
                .sum(),
            Self::Latency => report
                .individual_metrics
                .iter()
                .filter(|m| !m.latency_ts.points.is_empty())
                .map(|m| m.latency_ts.points.len())
                .sum(),
            Self::LatencyDistribution => report
                .group_metrics
                .iter()
                .filter_map(|m| m.latency_distribution.as_ref())
                .map(|d| d.bins.len())
                .sum(),
        }
    }
}

fn open_in_browser(path: &str) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        Command::new("xdg-open").arg(path).spawn().map(|_| ())
    }

    #[cfg(target_os = "macos")]
    {
        Command::new("open").arg(path).spawn().map(|_| ())
    }

    #[cfg(target_os = "windows")]
    {
        Command::new("cmd")
            .args(["/C", "start", path])
            .spawn()
            .map(|_| ())
    }
}

pub fn plot_chart(
    report: &BenchmarkReport,
    output_directory: &str,
    chart_type: &ChartType,
    should_open_in_browser: bool,
) -> std::io::Result<()> {
    let data_processing_start = Instant::now();
    let chart = (chart_type.create_chart())(report, true, false); // Use dark theme by default
    let data_processing_time = data_processing_start.elapsed();

    let chart_render_start = Instant::now();
    let file_name = chart_type.name();
    save_chart(&chart, file_name, output_directory, 1600, 1200)?;

    if should_open_in_browser {
        let chart_path = format!("{output_directory}/{file_name}.html");
        open_in_browser(&chart_path)?;
    }

    let total_samples = chart_type.get_samples(report);
    let report_path = format!("{output_directory}/report.json");
    let report_size = IggyByteSize::from(std::fs::metadata(&report_path)?.len());

    let chart_render_time = chart_render_start.elapsed();

    info!(
        "Generated {} plot at: {}/{}.html ({} samples, report.json size: {}, data processing: {:.2?}, chart render: {:.2?})",
        file_name,
        output_directory,
        file_name,
        total_samples,
        report_size,
        data_processing_time,
        chart_render_time
    );
    Ok(())
}

fn save_chart(
    chart: &Chart,
    file_name: &str,
    output_directory: &str,
    width: u64,
    height: u64,
) -> std::io::Result<()> {
    let parent = Path::new(output_directory).parent().unwrap();
    std::fs::create_dir_all(parent)?;
    let full_output_path = Path::new(output_directory).join(format!("{file_name}.html"));

    let mut renderer = HtmlRenderer::new(file_name, width, height).theme(Theme::Dark);
    renderer
        .save(chart, &full_output_path)
        .map_err(|e| std::io::Error::other(format!("Failed to save HTML plot: {e}")))
}
