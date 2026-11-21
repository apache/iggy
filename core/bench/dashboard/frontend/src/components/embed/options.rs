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

//! What an embed shows and how it looks, and the link and code that put it on another page. The
//! dialog keeps one [`EmbedOptions`] and builds everything from it. Nothing here needs a browser,
//! so the tests check every kind of link and code that the dialog writes.

use std::fmt;

use bench_dashboard_shared::embed::{ChartType, OmbChartType, PngSize};
use bench_dashboard_shared::omb::id::OmbRunId;
use uuid::Uuid;
use yew_router::Routable;

use crate::router::AppRoute;

/// The longest side of a custom frame.
pub const MAX_SIDE: u32 = 4096;
pub const FRAME_SIZES: [FrameSize; 3] = [
    FrameSize::pixels(800, 600),
    FrameSize::pixels(1200, 675),
    FrameSize {
        width: FrameWidth::Full,
        height: 600,
    },
];

/// The run whose charts the embeds show.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedRun {
    IggyBench(Uuid),
    Omb(OmbRunId),
}

impl EmbedRun {
    fn embed_path(self) -> String {
        match self {
            Self::IggyBench(uuid) => format!("/embed/{uuid}"),
            Self::Omb(id) => format!("/embed/omb/{id}"),
        }
    }

    /// The run's page on the dashboard, which an image links to.
    fn page_path(self) -> String {
        match self {
            Self::IggyBench(uuid) => AppRoute::Run {
                uuid: uuid.to_string(),
            },
            Self::Omb(id) => AppRoute::OmbRun { id: id.to_string() },
        }
        .to_path()
    }

    /// The names of the server's own downloads of the chart.
    fn file_stem(self) -> String {
        match self {
            Self::IggyBench(uuid) => uuid.to_string(),
            Self::Omb(id) => format!("omb_{id}"),
        }
    }

    const fn alt_prefix(self) -> &'static str {
        match self {
            Self::IggyBench(_) => "Iggy benchmark",
            Self::Omb(_) => "Iggy OMB benchmark",
        }
    }
}

/// A chart of either framework. Its `Display` is the `type` query parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedChart {
    IggyBench(ChartType),
    Omb(OmbChartType),
}

impl EmbedChart {
    pub const fn label(self) -> &'static str {
        match self {
            Self::IggyBench(ChartType::Latency) => "Latency",
            Self::IggyBench(ChartType::Throughput) => "Throughput",
            Self::IggyBench(ChartType::Distribution) => "Latency distribution",
            Self::Omb(chart) => chart.title(),
        }
    }

    /// What the chart draws, for the reader who picks one.
    pub const fn description(self) -> &'static str {
        match self {
            Self::IggyBench(ChartType::Latency) => {
                "The latency of every actor over the run, with the moving average of each group."
            }
            Self::IggyBench(ChartType::Throughput) => {
                "The throughput of every actor over the run in MB/s and messages per second, with \
                 the line of each group."
            }
            Self::IggyBench(ChartType::Distribution) => {
                "A latency histogram for each group, with its fitted log-normal curve."
            }
            Self::Omb(OmbChartType::Latency) => {
                "The publish latency P50, P99 and P99.9 of each 10 second window."
            }
            Self::Omb(OmbChartType::EndToEndLatency) => {
                "The end-to-end latency P50, P99 and P99.9 of each 10 second window."
            }
            Self::Omb(OmbChartType::Throughput) => {
                "The publish and consume rates of each 10 second window, in MiB/s."
            }
            Self::Omb(OmbChartType::Ladder) => {
                "The publish and end-to-end latency from P50 to the maximum, over the whole run."
            }
            Self::Omb(OmbChartType::Quantiles) => {
                "The publish and end-to-end latency at every percentile, on a log scale."
            }
        }
    }
}

impl fmt::Display for EmbedChart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IggyBench(chart) => chart.fmt(f),
            Self::Omb(chart) => chart.fmt(f),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedFormat {
    /// A PNG that the server draws.
    Image,
    /// A live chart in an iframe.
    Interactive,
}

impl EmbedFormat {
    pub const ALL: [Self; 2] = [Self::Image, Self::Interactive];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Image => "Image",
            Self::Interactive => "Interactive",
        }
    }

    pub const fn description(self) -> &'static str {
        match self {
            Self::Image => {
                "A PNG that the server draws. It works anywhere that an image works, such as a \
                 README on GitHub."
            }
            Self::Interactive => {
                "A live chart in an iframe. Readers can hover the lines and zoom. GitHub and many \
                 forums do not show iframes."
            }
        }
    }

    /// The kinds of code that put the embed on a page, the most common first.
    pub const fn code_kinds(self) -> &'static [CodeKind] {
        match self {
            Self::Image => &[CodeKind::Markdown, CodeKind::Html, CodeKind::Link],
            Self::Interactive => &[CodeKind::Html, CodeKind::Link],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedTheme {
    Dark,
    Light,
}

impl EmbedTheme {
    pub const ALL: [Self; 2] = [Self::Dark, Self::Light];

    pub const fn of_page(is_dark: bool) -> Self {
        if is_dark { Self::Dark } else { Self::Light }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Dark => "Dark",
            Self::Light => "Light",
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Dark => "dark",
            Self::Light => "light",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeKind {
    Markdown,
    Html,
    /// The bare URL, for a tool that takes a link.
    Link,
}

impl CodeKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Markdown => "Markdown",
            Self::Html => "HTML",
            Self::Link => "Link",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Size {
    pub width: u32,
    pub height: u32,
}

impl Size {
    pub const fn new(width: u32, height: u32) -> Self {
        Self { width, height }
    }

    /// A size that a custom frame can have.
    pub const fn is_in_range(self) -> bool {
        self.width >= 1 && self.width <= MAX_SIDE && self.height >= 1 && self.height <= MAX_SIDE
    }

    /// The scale that fits this size into `room` without making it larger, so the preview never
    /// shows a chart bigger than the other page will. A room that has no size yet gives 1.
    pub fn fit_scale(self, room: Self) -> f64 {
        if room.width == 0 || room.height == 0 || self.width == 0 || self.height == 0 {
            return 1.0;
        }
        let width = f64::from(room.width) / f64::from(self.width);
        let height = f64::from(room.height) / f64::from(self.height);
        width.min(height).min(1.0)
    }
}

impl fmt::Display for Size {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} × {}", self.width, self.height)
    }
}

impl From<PngSize> for Size {
    fn from(size: PngSize) -> Self {
        Self::new(size.width(), size.height())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameWidth {
    Pixels(u32),
    /// The width of the column on the other page.
    Full,
}

/// The size of the iframe on the other page. The chart in it scales its text to this size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameSize {
    pub width: FrameWidth,
    pub height: u32,
}

impl FrameSize {
    pub const fn pixels(width: u32, height: u32) -> Self {
        Self {
            width: FrameWidth::Pixels(width),
            height,
        }
    }

    /// The size at which the preview draws the frame in `room`, and the scale that fits it there.
    /// A full width frame takes the width of the room after the scale.
    pub fn preview(self, room: Size) -> (Size, f64) {
        match self.width {
            FrameWidth::Pixels(width) => {
                let size = Size::new(width, self.height);
                (size, size.fit_scale(room))
            }
            FrameWidth::Full => {
                let scale = Size::new(room.width.max(1), self.height).fit_scale(room);
                let width = (f64::from(room.width) / scale).round() as u32;
                (Size::new(width, self.height), scale)
            }
        }
    }
}

impl fmt::Display for FrameSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.width {
            FrameWidth::Pixels(width) => Size::new(width, self.height).fmt(f),
            FrameWidth::Full => write!(f, "Full width × {}", self.height),
        }
    }
}

/// Every choice of the dialog. The choices of the other format stay, so switching back restores
/// them.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbedOptions {
    pub chart: EmbedChart,
    pub format: EmbedFormat,
    pub theme: EmbedTheme,
    /// Image only: the `_nobg` themes.
    pub transparent: bool,
    /// Image only. The interactive chart always has its legend.
    pub legend: bool,
    pub image: PngSize,
    pub frame: FrameSize,
    /// The last kind that the reader picked. [`Self::code_kind`] gives the kind in use.
    pub code: CodeKind,
}

impl EmbedOptions {
    pub const fn new(chart: EmbedChart, theme: EmbedTheme) -> Self {
        Self {
            chart,
            format: EmbedFormat::Image,
            theme,
            transparent: false,
            legend: true,
            image: PngSize::DEFAULT,
            frame: FRAME_SIZES[0],
            code: CodeKind::Markdown,
        }
    }

    /// The picked kind of code, or the format's first kind when the format has no such kind, as
    /// Markdown for an iframe.
    pub fn code_kind(&self) -> CodeKind {
        let kinds = self.format.code_kinds();
        if kinds.contains(&self.code) {
            self.code
        } else {
            kinds[0]
        }
    }

    /// The link of the embed in its format, which the preview loads too.
    pub fn url(&self, run: EmbedRun, origin: &str) -> String {
        match self.format {
            EmbedFormat::Image => self.image_url(run, origin),
            EmbedFormat::Interactive => self.frame_url(run, origin),
        }
    }

    fn image_url(&self, run: EmbedRun, origin: &str) -> String {
        let (width, height) = (self.image.width(), self.image.height());
        let background = if self.transparent { "_nobg" } else { "" };
        format!(
            "{origin}{path}/chart.png?type={chart}&theme={theme}{background}&width={width}\
             &height={height}&legend={legend}",
            path = run.embed_path(),
            chart = self.chart,
            theme = self.theme.name(),
            legend = self.legend,
        )
    }

    fn frame_url(&self, run: EmbedRun, origin: &str) -> String {
        format!(
            "{origin}{path}?type={chart}&theme={theme}",
            path = run.embed_path(),
            chart = self.chart,
            theme = self.theme.name(),
        )
    }

    /// The code that the reader copies to the other page.
    pub fn code(&self, run: EmbedRun, origin: &str) -> String {
        let url = self.url(run, origin);
        let alt = format!("{}: {}", run.alt_prefix(), self.chart.label());
        let page = format!("{origin}{}", run.page_path());
        match (self.format, self.code_kind()) {
            (_, CodeKind::Link) => url,
            (EmbedFormat::Image, CodeKind::Markdown) => format!("[![{alt}]({url})]({page})"),
            // Half the width of the PNG, so the image stays sharp on a high density screen.
            (EmbedFormat::Image, CodeKind::Html) => format!(
                r#"<a href="{page}"><img src="{url}" alt="{alt}" width="{width}"></a>"#,
                width = self.image.width() / 2,
            ),
            (EmbedFormat::Interactive, _) => {
                let width = match self.frame.width {
                    FrameWidth::Pixels(width) => width.to_string(),
                    FrameWidth::Full => "100%".to_owned(),
                };
                format!(
                    r#"<iframe src="{url}" title="{alt}" width="{width}" height="{height}" style="border: 0" loading="lazy"></iframe>"#,
                    height = self.frame.height,
                )
            }
        }
    }

    pub fn download_name(&self, run: EmbedRun) -> String {
        format!("{}_{}.png", run.file_stem(), self.chart)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORIGIN: &str = "https://benchmarks.example";
    const UUID: &str = "6f0c8e0e-6a39-4b8c-8f8e-1f2d3c4b5a69";
    const OMB_ID: &str = "0123456789abcdef";

    fn iggy_bench() -> EmbedRun {
        EmbedRun::IggyBench(Uuid::parse_str(UUID).expect("a valid uuid"))
    }

    fn omb() -> EmbedRun {
        EmbedRun::Omb(OMB_ID.parse().expect("a valid OMB run id"))
    }

    fn options(chart: EmbedChart) -> EmbedOptions {
        EmbedOptions::new(chart, EmbedTheme::Dark)
    }

    /// The value of `name` in the query of `url`.
    fn query_value<'a>(url: &'a str, name: &str) -> Option<&'a str> {
        let (_, query) = url.split_once('?')?;
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
    }

    #[test]
    fn given_default_options_when_coded_should_link_cached_png_to_run_page_in_markdown() {
        let options = options(EmbedChart::IggyBench(ChartType::Latency));

        assert_eq!(
            options.code(iggy_bench(), ORIGIN),
            format!(
                "[![Iggy benchmark: Latency]({ORIGIN}/embed/{UUID}/chart.png?type=latency\
                 &theme=dark&width=1600&height=1200&legend=true)]({ORIGIN}/runs/{UUID})"
            )
        );
    }

    #[test]
    fn given_transparent_light_image_without_legend_when_linked_should_ask_for_nobg_theme() {
        let options = EmbedOptions {
            theme: EmbedTheme::Light,
            transparent: true,
            legend: false,
            image: PngSize::ALL[3],
            code: CodeKind::Html,
            ..options(EmbedChart::Omb(OmbChartType::Ladder))
        };

        assert_eq!(
            options.code(omb(), ORIGIN),
            format!(
                r#"<a href="{ORIGIN}/omb/runs/{OMB_ID}"><img src="{ORIGIN}/embed/omb/{OMB_ID}/chart.png?type=ladder&theme=light_nobg&width=1200&height=630&legend=false" alt="Iggy OMB benchmark: Latency ladder" width="600"></a>"#
            )
        );
    }

    #[test]
    fn given_interactive_format_when_coded_should_write_iframe_with_page_theme_only() {
        let fixed = EmbedOptions {
            format: EmbedFormat::Interactive,
            transparent: true,
            legend: false,
            ..options(EmbedChart::Omb(OmbChartType::EndToEndLatency))
        };
        let full = EmbedOptions {
            frame: FRAME_SIZES[2],
            ..fixed.clone()
        };
        let url = format!("{ORIGIN}/embed/omb/{OMB_ID}?type=end_to_end_latency&theme=dark");

        assert_eq!(
            fixed.code(omb(), ORIGIN),
            format!(
                r#"<iframe src="{url}" title="Iggy OMB benchmark: End-to-end latency" width="800" height="600" style="border: 0" loading="lazy"></iframe>"#
            )
        );
        assert!(
            full.code(omb(), ORIGIN)
                .contains(r#"width="100%" height="600""#)
        );
    }

    #[test]
    fn given_markdown_picked_when_switched_to_iframe_should_write_html_and_keep_the_pick() {
        let image = options(EmbedChart::IggyBench(ChartType::Throughput));
        let interactive = EmbedOptions {
            format: EmbedFormat::Interactive,
            ..image.clone()
        };

        assert_eq!(image.code_kind(), CodeKind::Markdown);
        assert_eq!(interactive.code_kind(), CodeKind::Html);
        assert_eq!(interactive.code, CodeKind::Markdown);
    }

    #[test]
    fn given_link_kind_when_coded_should_give_the_url_that_the_preview_loads() {
        for format in EmbedFormat::ALL {
            let options = EmbedOptions {
                format,
                code: CodeKind::Link,
                ..options(EmbedChart::IggyBench(ChartType::Distribution))
            };

            assert_eq!(
                options.code(iggy_bench(), ORIGIN),
                options.url(iggy_bench(), ORIGIN)
            );
        }
    }

    #[test]
    fn given_every_chart_when_linked_should_name_a_type_that_the_server_parses() {
        let charts = ChartType::ALL
            .map(EmbedChart::IggyBench)
            .into_iter()
            .map(|chart| (chart, iggy_bench()))
            .chain(
                OmbChartType::ALL
                    .map(EmbedChart::Omb)
                    .into_iter()
                    .map(|chart| (chart, omb())),
            );

        for (chart, run) in charts {
            for format in EmbedFormat::ALL {
                let url = EmbedOptions {
                    format,
                    ..options(chart)
                }
                .url(run, ORIGIN);
                let name = query_value(&url, "type").expect("the link names a chart");
                let parsed = match chart {
                    EmbedChart::IggyBench(_) => name.parse().map(EmbedChart::IggyBench).ok(),
                    EmbedChart::Omb(_) => name.parse().map(EmbedChart::Omb).ok(),
                };
                assert_eq!(parsed, Some(chart), "{url}");
            }
        }
    }

    #[test]
    fn given_run_when_linked_should_point_at_the_route_of_its_page() {
        assert_eq!(
            AppRoute::recognize(&iggy_bench().page_path()),
            Some(AppRoute::Run {
                uuid: UUID.to_owned()
            })
        );
        assert_eq!(
            AppRoute::recognize(&omb().page_path()),
            Some(AppRoute::OmbRun {
                id: OMB_ID.to_owned()
            })
        );
    }

    #[test]
    fn given_run_when_downloaded_should_name_file_as_the_server_does() {
        let chart = EmbedChart::Omb(OmbChartType::Quantiles);

        assert_eq!(
            options(chart).download_name(omb()),
            format!("omb_{OMB_ID}_quantiles.png")
        );
        assert_eq!(
            options(EmbedChart::IggyBench(ChartType::Latency)).download_name(iggy_bench()),
            format!("{UUID}_latency.png")
        );
    }

    #[test]
    fn given_frame_size_outside_bounds_when_checked_should_be_out_of_range() {
        assert!(Size::new(1, MAX_SIDE).is_in_range());
        assert!(!Size::new(0, 600).is_in_range());
        assert!(!Size::new(800, MAX_SIDE + 1).is_in_range());
    }

    #[test]
    fn given_room_when_previewed_should_fit_without_enlarging() {
        let room = Size::new(640, 400);
        let image = Size::from(PngSize::DEFAULT);

        assert_eq!(image.fit_scale(room), 1.0 / 3.0);
        assert_eq!(Size::new(320, 200).fit_scale(room), 1.0);
        assert_eq!(image.fit_scale(Size::new(0, 0)), 1.0);
    }

    #[test]
    fn given_full_width_frame_when_previewed_should_fill_the_room_after_scaling() {
        let full = FRAME_SIZES[2];

        assert_eq!(
            full.preview(Size::new(900, 800)),
            (Size::new(900, 600), 1.0)
        );
        let (size, scale) = full.preview(Size::new(900, 300));
        assert_eq!(scale, 0.5);
        assert_eq!(size, Size::new(1800, 600));
        assert_eq!(
            FrameSize::pixels(800, 600).preview(Size::new(400, 600)),
            (Size::new(800, 600), 0.5)
        );
    }
}
