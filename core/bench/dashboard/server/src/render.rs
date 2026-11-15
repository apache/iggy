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

use crate::cache::read_report;
use crate::dedicated_thread::{DedicatedThread, ThreadStopped};
use crate::error::IggyBenchDashboardServerError;
use bench_report::report::BenchmarkReport;
use charming::Chart;
use charming::renderer::image_renderer::{ImageFormat, ImageRenderer};
use charming::theme::Theme;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::info;

const RENDER_THREAD: &str = "png-render";
const CHART_THREAD: &str = "embed-chart";

/// The chart of a run that an embed shows, named as in the `type` query parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChartType {
    Latency,
    Throughput,
    Distribution,
}

impl FromStr for ChartType {
    type Err = IggyBenchDashboardServerError;

    fn from_str(chart_type: &str) -> Result<Self, Self::Err> {
        match chart_type {
            "latency" => Ok(Self::Latency),
            "throughput" => Ok(Self::Throughput),
            "distribution" => Ok(Self::Distribution),
            _ => Err(IggyBenchDashboardServerError::BadRequest(format!(
                "Invalid chart type: '{chart_type}'. Must be 'latency', 'throughput', or 'distribution'"
            ))),
        }
    }
}

impl fmt::Display for ChartType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Latency => "latency",
            Self::Throughput => "throughput",
            Self::Distribution => "distribution",
        })
    }
}

/// A run's chart with the line that names its machine and server version.
pub struct EmbedChart {
    pub chart: Chart,
    pub hardware_line: String,
}

/// Dedicated threads that build and render the charts of the embed endpoints.
///
/// charming's SSR uses deno_core (V8) which has global state that is not
/// safe to use from multiple OS threads. All rendering is pinned to a single
/// thread; actix workers send jobs via channel and await results without blocking.
///
/// A chart build reads a whole report, up to 90 MB of JSON, so it runs on a second thread. That
/// keeps the read and parse off the actix workers, and one build at a time bounds their memory,
/// as in `SeriesBuilder`.
pub struct PngRenderPool {
    thread: DedicatedThread<RenderJob, Result<Vec<u8>, String>>,
    charts: DedicatedThread<ChartJob, Result<EmbedChart, IggyBenchDashboardServerError>>,
}

impl PngRenderPool {
    pub fn new() -> io::Result<Self> {
        let thread = DedicatedThread::start(RENDER_THREAD, |job: RenderJob| {
            Self::render(&job.chart, job.width, job.height, job.dark)
        })?;
        let charts = DedicatedThread::start(CHART_THREAD, EmbedChart::load)?;

        info!("PNG render and embed chart threads started");
        Ok(Self { thread, charts })
    }

    pub fn default_pool() -> io::Result<Self> {
        Self::new()
    }

    pub async fn load_chart(
        &self,
        report: PathBuf,
        chart_type: ChartType,
        dark: bool,
    ) -> Result<EmbedChart, IggyBenchDashboardServerError> {
        self.charts
            .run(ChartJob {
                report,
                chart_type,
                dark,
            })
            .await
            .map_err(|ThreadStopped| {
                IggyBenchDashboardServerError::InternalError(
                    "The embed chart thread has stopped".to_owned(),
                )
            })?
    }

    pub async fn render_png(
        &self,
        chart: Chart,
        width: u32,
        height: u32,
        dark: bool,
    ) -> Result<Vec<u8>, String> {
        self.thread
            .run(RenderJob {
                chart,
                width,
                height,
                dark,
            })
            .await
            .map_err(|ThreadStopped| "render pool shut down".to_string())?
    }

    fn render(chart: &Chart, width: u32, height: u32, dark: bool) -> Result<Vec<u8>, String> {
        let mut renderer = ImageRenderer::new(width, height);
        if dark {
            renderer = renderer.theme(Theme::Dark);
        }
        renderer
            .render_format(ImageFormat::Png, chart)
            .map_err(|e| format!("PNG render failed: {e}"))
    }
}

struct ChartJob {
    report: PathBuf,
    chart_type: ChartType,
    dark: bool,
}

struct RenderJob {
    chart: Chart,
    width: u32,
    height: u32,
    dark: bool,
}

impl EmbedChart {
    fn load(job: ChartJob) -> Result<Self, IggyBenchDashboardServerError> {
        let report = read_report(&job.report)?;
        let chart = match job.chart_type {
            ChartType::Latency => bench_report::create_latency_chart(&report, job.dark, false),
            ChartType::Throughput => {
                bench_report::create_throughput_chart(&report, job.dark, false)
            }
            ChartType::Distribution => {
                bench_report::create_latency_distribution_chart(&report, job.dark, false)
            }
        };
        Ok(Self {
            chart,
            hardware_line: Self::hardware_line(&report),
        })
    }

    /// A run without a `gitref` or a hardware `identifier` still loads, so the line falls back to
    /// the server's own labels, as the run list does.
    fn hardware_line(report: &BenchmarkReport) -> String {
        let machine = report
            .hardware
            .identifier
            .as_deref()
            .unwrap_or(&report.server_stats.hostname);
        let version = report
            .params
            .gitref
            .as_deref()
            .unwrap_or(&report.server_stats.iggy_server_version);
        format!(
            "{machine} @ {} (server {version})",
            report.hardware.cpu_name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    const FIXTURE_REPORT: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../fixtures/results/live-0.9.0-pinned-producer-20p/report.json"
    );

    #[test]
    fn given_report_without_gitref_and_identifier_when_naming_hardware_should_use_server_stats() {
        let mut report =
            read_report(Path::new(FIXTURE_REPORT)).expect("the fixture report is readable");
        report.params.gitref = None;
        report.hardware.identifier = None;

        assert_eq!(
            EmbedChart::hardware_line(&report),
            "ip-10-0-0-40 @ Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz (server 0.9.0)"
        );
    }
}
