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
use crate::card::CardRenderer;
use crate::dedicated_thread::{DedicatedThread, ThreadStopped};
use crate::error::IggyBenchDashboardServerError;
use bench_dashboard_shared::card::RunCard;
use bench_dashboard_shared::embed::ChartType;
use bench_report::report::BenchmarkReport;
use charming::Chart;
use charming::renderer::image_renderer::{ImageFormat, ImageRenderer};
use charming::theme::Theme;
use std::io;
use std::path::PathBuf;
use tracing::info;

/// charming writes the text between two of these marks as JavaScript, and keeps its own copy of
/// the mark private.
pub(crate) const RAW_MARK: &str = "#*#*#*#";

const RENDER_THREAD: &str = "png-render";
const CHART_THREAD: &str = "embed-chart";
const CARD_THREAD: &str = "card-render";

/// A run's chart with the line that names its machine and server version.
pub struct EmbedChart {
    pub chart: Chart,
    pub hardware_line: String,
}

/// `text` without charming's raw mark, so text from a result file stays text when
/// `Chart::to_string` writes the chart for a page or for the PNG renderer. One pass can join the
/// halves around a removed mark into a new mark, so the removal repeats.
pub fn without_raw_mark(text: &str) -> String {
    let mut plain = text.to_owned();
    while plain.contains(RAW_MARK) {
        plain = plain.replace(RAW_MARK, "");
    }
    plain
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
///
/// A preview card needs no V8, so it renders on a third thread. That thread loads the system fonts
/// for its first card.
pub struct PngRenderPool {
    thread: DedicatedThread<RenderJob, Result<Vec<u8>, String>>,
    charts: DedicatedThread<ChartJob, Result<EmbedChart, IggyBenchDashboardServerError>>,
    cards: DedicatedThread<RunCard, Result<Vec<u8>, String>>,
}

impl PngRenderPool {
    pub fn new() -> io::Result<Self> {
        Self::with_renderer(Self::render)
    }

    /// A pool whose render thread runs `render` in place of charming's V8 renderer.
    pub(crate) fn with_renderer(
        mut render: impl FnMut(&Chart, u32, u32, bool) -> Result<Vec<u8>, String> + Send + 'static,
    ) -> io::Result<Self> {
        let thread = DedicatedThread::start(RENDER_THREAD, move |job: RenderJob| {
            render(&job.chart, job.width, job.height, job.dark)
        })?;
        let charts = DedicatedThread::start(CHART_THREAD, EmbedChart::load)?;
        let mut card_renderer = None;
        let cards = DedicatedThread::start(CARD_THREAD, move |card: RunCard| {
            if card_renderer.is_none() {
                card_renderer = Some(CardRenderer::new()?);
            }
            card_renderer.as_ref().map_or_else(
                || Err("no card renderer".to_owned()),
                |renderer| renderer.render(&card),
            )
        })?;

        info!("PNG render, embed chart and card threads started");
        Ok(Self {
            thread,
            charts,
            cards,
        })
    }

    /// The `chart_type` chart of the report at `report`, with `subtext` in place of the report's
    /// own subtext.
    pub async fn load_chart(
        &self,
        report: PathBuf,
        chart_type: ChartType,
        subtext: String,
        dark: bool,
    ) -> Result<EmbedChart, IggyBenchDashboardServerError> {
        self.charts
            .run(ChartJob {
                report,
                chart_type,
                subtext,
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

    /// The preview card as a PNG of [`bench_dashboard_shared::card::CARD_SIZE`].
    pub async fn render_card(&self, card: RunCard) -> Result<Vec<u8>, String> {
        self.cards
            .run(card)
            .await
            .map_err(|ThreadStopped| "card render thread shut down".to_owned())?
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
    subtext: String,
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
        let subtext = &job.subtext;
        let chart = match job.chart_type {
            ChartType::Latency => {
                bench_report::create_latency_chart_with_subtext(&report, subtext, job.dark, false)
            }
            ChartType::Throughput => bench_report::create_throughput_chart_with_subtext(
                &report, subtext, job.dark, false,
            ),
            ChartType::Distribution => {
                bench_report::create_latency_distribution_chart_with_subtext(
                    &report, subtext, job.dark, false,
                )
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

    #[test]
    fn given_mark_split_around_another_mark_when_removing_should_leave_no_mark() {
        let split = format!("#{RAW_MARK}*#*#*#");

        assert_eq!(without_raw_mark(&split), "");
        assert_eq!(without_raw_mark(&format!("a{RAW_MARK}b")), "ab");
        assert_eq!(without_raw_mark("OMB #1 *fast*"), "OMB #1 *fast*");
    }
}
