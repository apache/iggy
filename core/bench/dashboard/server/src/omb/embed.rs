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

//! The charts of the OMB embeds. A chart shows one OMB run in the units of OMB: MiB/s, 10 second
//! windows and the latency of each message. No chart puts an OMB number next to an iggy-bench
//! number.

use crate::error::IggyBenchDashboardServerError;
use crate::render::{EmbedChart, without_raw_mark};
use bench_dashboard_shared::omb::OMB_WINDOW_SECONDS;
use bench_dashboard_shared::omb::chart::{
    END_TO_END_NAME, PUBLISH_NAME, QuantileGroup, WindowMetric, ladder_options,
    quantile_group_options, window_options,
};
use bench_dashboard_shared::omb::detail::OmbRunDetail;
use bench_dashboard_shared::omb::result::OmbPercentiles;
use bench_dashboard_shared::omb::setup::OmbEnvironment;
use bench_dashboard_shared::omb::summary::OmbRunSummary;
use bench_report::plotting::chart::IggyChart;
use std::fmt;
use std::str::FromStr;

/// The separator of the iggy-bench subtext, so the two kinds of embed read alike.
const SEPARATOR: &str = "  •  ";
const NO_MACHINES: &str = "Machines not recorded";
/// The hexadecimal digits of [`setup_line_hash`], as many as the run id has.
const SETUP_HASH_CHARS: usize = 16;

/// The chart of an OMB run that an embed shows, named as in the `type` query parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OmbChartType {
    /// The publish latency P50, P99 and P99.9 of each window.
    Latency,
    /// The end-to-end latency P50, P99 and P99.9 of each window.
    EndToEndLatency,
    /// The publish and consume throughput of each window.
    Throughput,
    /// The publish and end-to-end latency from P50 to the maximum, over the whole run.
    Ladder,
    /// The publish and end-to-end latency at every percentile of the merged histograms.
    Quantiles,
}

impl FromStr for OmbChartType {
    type Err = IggyBenchDashboardServerError;

    fn from_str(chart_type: &str) -> Result<Self, Self::Err> {
        match chart_type {
            "latency" => Ok(Self::Latency),
            "end_to_end_latency" => Ok(Self::EndToEndLatency),
            "throughput" => Ok(Self::Throughput),
            "ladder" => Ok(Self::Ladder),
            "quantiles" => Ok(Self::Quantiles),
            _ => Err(IggyBenchDashboardServerError::BadRequest(format!(
                "Invalid chart type: '{chart_type}'. Must be 'latency', 'end_to_end_latency', \
                 'throughput', 'ladder', or 'quantiles'"
            ))),
        }
    }
}

impl fmt::Display for OmbChartType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Latency => "latency",
            Self::EndToEndLatency => "end_to_end_latency",
            Self::Throughput => "throughput",
            Self::Ladder => "ladder",
            Self::Quantiles => "quantiles",
        })
    }
}

impl OmbChartType {
    const fn title(self) -> &'static str {
        match self {
            Self::Latency => WindowMetric::PublishLatency.title(),
            Self::EndToEndLatency => WindowMetric::EndToEndLatency.title(),
            Self::Throughput => WindowMetric::Throughput.title(),
            Self::Ladder => "Latency ladder",
            Self::Quantiles => "Latency percentiles",
        }
    }
}

impl EmbedChart {
    /// The `chart_type` chart of an OMB run, with the run's setup line in place of the hardware
    /// line. `None` when the run has no values for the chart, for example the end-to-end latency
    /// of a run without consumers.
    pub fn of_omb_run(detail: &OmbRunDetail, chart_type: OmbChartType, dark: bool) -> Option<Self> {
        let summary = &detail.summary;
        let base = IggyChart::new(
            &title(summary, chart_type),
            &subtext(summary, chart_type),
            dark,
            false,
        );
        let chart = match chart_type {
            OmbChartType::Latency => window_options(base, WindowMetric::PublishLatency, detail),
            OmbChartType::EndToEndLatency => {
                window_options(base, WindowMetric::EndToEndLatency, detail)
            }
            OmbChartType::Throughput => window_options(base, WindowMetric::Throughput, detail),
            OmbChartType::Ladder => ladder_options(base, summary),
            OmbChartType::Quantiles => quantile_group_options(base, QuantileGroup::Latency, detail),
        }?;
        Some(Self {
            chart,
            hardware_line: setup_line(summary),
        })
    }
}

/// A hash of the setup line, for the names of the cached PNG files. The run id covers the result
/// file, which gives the chart all its other text and values. The sidecar can change under the
/// same run id, and the setup line is the only chart text that comes from it.
pub fn setup_line_hash(summary: &OmbRunSummary) -> String {
    let hash = blake3::hash(setup_line(summary).as_bytes());
    hash.to_hex()[..SETUP_HASH_CHARS].to_owned()
}

/// For example `Publish latency - OMB 12 topics x 1 partition / 1p 1c per topic / 1000 B / 150 MB/s`,
/// with the workload name from the workload file. The page writes the chart with
/// `Chart::to_string`, so the name loses charming's raw mark.
fn title(summary: &OmbRunSummary, chart_type: OmbChartType) -> String {
    without_raw_mark(&format!(
        "{} - OMB {}",
        chart_type.title(),
        summary.workload.name
    ))
}

/// The workload, then the numbers of the whole run that the chart shows.
fn subtext(summary: &OmbRunSummary, chart_type: OmbChartType) -> String {
    let publish = || latency_line(PUBLISH_NAME, &summary.publish_latency_ms);
    let end_to_end = || {
        summary
            .end_to_end_latency_ms
            .as_ref()
            .map(|latency| latency_line(END_TO_END_NAME, latency))
    };
    let mut lines = vec![workload_line(summary)];
    match chart_type {
        OmbChartType::Throughput => lines.push(throughput_line(summary)),
        OmbChartType::Latency => lines.push(publish()),
        OmbChartType::EndToEndLatency => lines.extend(end_to_end()),
        OmbChartType::Ladder | OmbChartType::Quantiles => {
            lines.push(publish());
            lines.extend(end_to_end());
        }
    }
    lines.join("\n")
}

/// The machines, the driver and the Iggy version, for example
/// `AWS i4i.4xlarge, 3 nodes  •  Driver Iggy-persisted  •  Server 0.10.0-rc.1`. The name in the
/// OMB driver file is the only label of the driver configuration, which sets how durably the
/// server stores the messages.
fn setup_line(summary: &OmbRunSummary) -> String {
    let machines = summary
        .environment()
        .and_then(machines)
        .unwrap_or_else(|| NO_MACHINES.to_owned());
    let mut parts = vec![machines, format!("Driver {}", summary.driver)];
    parts.extend(
        summary
            .version
            .as_ref()
            .map(|version| format!("Server {}", version.text)),
    );
    parts.join(SEPARATOR)
}

/// For example `12 topics  •  1 partition per topic  •  1 producer and 1 consumer per topic  •
/// 1000 B messages  •  18 windows of 10 s`. OMB records the weighted average size for a workload
/// with a size distribution.
fn workload_line(summary: &OmbRunSummary) -> String {
    let workload = &summary.workload;
    [
        count(workload.topics.into(), "topic", "topics"),
        format!(
            "{} per topic",
            count(workload.partitions.into(), "partition", "partitions")
        ),
        format!(
            "{} and {} per topic",
            count(workload.producers_per_topic.into(), "producer", "producers"),
            count(workload.consumers_per_topic.into(), "consumer", "consumers")
        ),
        format!("{} B messages", workload.message_size),
        format!(
            "{} of {OMB_WINDOW_SECONDS} s",
            count(summary.windows as u64, "window", "windows")
        ),
    ]
    .join(SEPARATOR)
}

/// The mean rates over the windows. A run without consumers has no consume rate.
fn throughput_line(summary: &OmbRunSummary) -> String {
    let mut parts = vec![format!(
        "Mean publish {:.2} MiB/s, {:.0} msg/s",
        summary.publish_mebibytes_per_second, summary.publish_messages_per_second
    )];
    if summary.workload.consumers_per_topic > 0 {
        parts.push(format!(
            "Mean consume {:.2} MiB/s, {:.0} msg/s",
            summary.consume_mebibytes_per_second, summary.consume_messages_per_second
        ));
    }
    parts.join(SEPARATOR)
}

/// The percentiles of the merged histogram of the whole run.
fn latency_line(name: &str, latency: &OmbPercentiles) -> String {
    [
        format!("{name} latency per message"),
        format!("P50 {:.2} ms", latency.p50),
        format!("P99 {:.2} ms", latency.p99),
        format!("P99.9 {:.2} ms", latency.p999),
        format!("Max {:.2} ms", latency.max),
    ]
    .join(SEPARATOR)
}

/// The sidecar's label, else the server machines and the client hosts that it names. `None` for
/// an environment that names nothing.
fn machines(environment: &OmbEnvironment) -> Option<String> {
    if let Some(label) = &environment.label {
        return Some(label.clone());
    }
    let nodes = |nodes: u32| count(nodes.into(), "node", "nodes");
    let hosts = |hosts: u32| count(hosts.into(), "client host", "client hosts");
    let servers = match (&environment.server_instance, environment.server_nodes) {
        (Some(instance), Some(count)) => Some(format!("{instance}, {}", nodes(count))),
        (Some(instance), None) => Some(instance.clone()),
        (None, Some(count)) => Some(nodes(count)),
        (None, None) => None,
    };
    let clients = match (&environment.client_instance, environment.client_hosts) {
        (Some(instance), Some(count)) => Some(format!("{} of {instance}", hosts(count))),
        (Some(instance), None) => Some(format!("clients on {instance}")),
        (None, Some(count)) => Some(hosts(count)),
        (None, None) => None,
    };
    let parts: Vec<String> = servers.into_iter().chain(clients).collect();
    (!parts.is_empty()).then(|| parts.join(", "))
}

/// For example `1 topic` or `12 topics`.
fn count(count: u64, one: &str, many: &str) -> String {
    match count {
        1 => format!("1 {one}"),
        _ => format!("{count} {many}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::omb::test_fixtures::{FixtureRun, NO_CONSUMERS, RC1_PERSISTED};
    use crate::render::RAW_MARK;
    use bench_dashboard_shared::omb::result::OmbResult;
    use bench_dashboard_shared::omb::setup::OmbSidecar;
    use charming::Chart;
    use chrono::Utc;

    const EVERY_TYPE: [OmbChartType; 5] = [
        OmbChartType::Latency,
        OmbChartType::EndToEndLatency,
        OmbChartType::Throughput,
        OmbChartType::Ladder,
        OmbChartType::Quantiles,
    ];

    fn detail(fixture: &FixtureRun) -> OmbRunDetail {
        let json = fixture.json();
        let result = OmbResult::try_from(json.as_slice()).expect("the fixture result parses");
        let setup = fixture.has_sidecar.then(|| {
            std::fs::read_to_string(fixture.sidecar_path())
                .expect("the fixture sidecar is readable")
                .parse::<OmbSidecar>()
                .expect("the fixture sidecar parses")
                .setup
        });
        let summary =
            OmbRunSummary::new(fixture.id(), fixture.file_name, Utc::now(), &result, setup);
        OmbRunDetail::new(summary, &result)
    }

    fn series_names(chart: &Chart) -> Vec<String> {
        let option = serde_json::to_value(chart).expect("the chart serializes");
        option["series"]
            .as_array()
            .expect("the chart has series")
            .iter()
            .filter_map(|series| series["name"].as_str().map(str::to_owned))
            .collect()
    }

    fn title_and_subtext(chart: &Chart) -> (String, String) {
        let option = serde_json::to_value(chart).expect("the chart serializes");
        let title = &option["title"][0];
        (
            title["text"].as_str().unwrap_or_default().to_owned(),
            title["subtext"].as_str().unwrap_or_default().to_owned(),
        )
    }

    #[test]
    fn given_every_chart_type_when_parsed_from_its_name_should_round_trip() {
        for chart_type in EVERY_TYPE {
            assert_eq!(
                chart_type.to_string().parse::<OmbChartType>().ok(),
                Some(chart_type)
            );
        }
        let error = "distribution"
            .parse::<OmbChartType>()
            .expect_err("an iggy-bench chart type is not an OMB chart type");
        assert!(error.to_string().contains("Invalid chart type"), "{error}");
    }

    #[test]
    fn given_run_with_sidecar_when_charted_should_draw_each_type_with_setup_line() {
        let detail = detail(&RC1_PERSISTED);
        let expected = [
            (OmbChartType::Latency, vec!["P50", "P99", "P99.9"]),
            (OmbChartType::EndToEndLatency, vec!["P50", "P99", "P99.9"]),
            (OmbChartType::Throughput, vec!["Publish", "Consume"]),
            (OmbChartType::Ladder, vec!["Publish", "End-to-end"]),
            (OmbChartType::Quantiles, vec!["Publish", "End-to-end"]),
        ];

        for (chart_type, names) in expected {
            let embed = EmbedChart::of_omb_run(&detail, chart_type, true)
                .unwrap_or_else(|| panic!("the fixture run has a {chart_type} chart"));

            assert_eq!(series_names(&embed.chart), names, "{chart_type}");
            assert_eq!(
                embed.hardware_line,
                "AWS i4i.4xlarge, 3 nodes  •  Driver Iggy-persisted  •  Server 0.10.0-rc.1"
            );
        }
    }

    /// The cached PNG files are named by the run id and the setup line hash, so no other chart
    /// text may come from the sidecar.
    #[test]
    fn given_sidecar_edit_when_charted_should_change_only_setup_line_and_its_hash() {
        let original = detail(&RC1_PERSISTED);
        let result = OmbResult::try_from(RC1_PERSISTED.json().as_slice())
            .expect("the fixture result parses");
        let setup = "timestamp = \"2026-11-07T08:00:00Z\"\nsuite = \"other\"\n\n\
                     [environment]\nlabel = \"Another cluster\"\n\n\
                     [server]\nversion = \"0.10.1\"\n"
            .parse::<OmbSidecar>()
            .expect("the edited sidecar parses")
            .setup;
        let summary = OmbRunSummary::new(
            RC1_PERSISTED.id(),
            RC1_PERSISTED.file_name,
            Utc::now(),
            &result,
            Some(setup),
        );
        let edited = OmbRunDetail::new(summary, &result);

        for chart_type in EVERY_TYPE {
            let before = EmbedChart::of_omb_run(&original, chart_type, true)
                .unwrap_or_else(|| panic!("the fixture run has a {chart_type} chart"));
            let after = EmbedChart::of_omb_run(&edited, chart_type, true)
                .unwrap_or_else(|| panic!("the edited run has a {chart_type} chart"));

            assert_eq!(
                after.chart.to_string(),
                before.chart.to_string(),
                "{chart_type}"
            );
            assert_ne!(after.hardware_line, before.hardware_line, "{chart_type}");
        }
        let hash = setup_line_hash(&original.summary);
        assert_eq!(hash.len(), SETUP_HASH_CHARS);
        assert_ne!(setup_line_hash(&edited.summary), hash);
    }

    #[test]
    fn given_fixture_run_when_charted_should_title_workload_and_write_omb_units() {
        let detail = detail(&RC1_PERSISTED);

        let latency = EmbedChart::of_omb_run(&detail, OmbChartType::Latency, false)
            .expect("the fixture run has a latency chart");
        let throughput = EmbedChart::of_omb_run(&detail, OmbChartType::Throughput, false)
            .expect("the fixture run has a throughput chart");

        let (title, subtext) = title_and_subtext(&latency.chart);
        assert_eq!(
            title,
            "Publish latency - OMB 12 topics x 1 partition / 1p 1c per topic / 1000 B / 150 MB/s"
        );
        let lines: Vec<&str> = subtext.lines().collect();
        assert_eq!(
            lines[0],
            "12 topics  •  1 partition per topic  •  1 producer and 1 consumer per topic  •  \
             1000 B messages  •  18 windows of 10 s"
        );
        assert!(
            lines[1].starts_with("Publish latency per message  •  P50 "),
            "{subtext}"
        );
        assert!(lines[1].contains(" ms  •  Max "), "{subtext}");
        let (_, subtext) = title_and_subtext(&throughput.chart);
        assert!(subtext.contains("Mean publish "), "{subtext}");
        assert!(subtext.contains(" MiB/s, "), "{subtext}");
        assert!(subtext.contains("Mean consume "), "{subtext}");
        assert!(!subtext.contains("MB/s"), "{subtext}");
    }

    #[test]
    fn given_raw_mark_in_workload_name_when_charted_should_write_title_as_plain_text() {
        let mut result = OmbResult::try_from(RC1_PERSISTED.json().as_slice())
            .expect("the fixture result parses");
        result.workload = format!("\"{RAW_MARK}a\"+alert(1){RAW_MARK}");
        let summary = OmbRunSummary::new(
            RC1_PERSISTED.id(),
            RC1_PERSISTED.file_name,
            Utc::now(),
            &result,
            None,
        );
        let detail = OmbRunDetail::new(summary, &result);

        let embed = EmbedChart::of_omb_run(&detail, OmbChartType::Latency, true)
            .expect("the fixture run has a latency chart");

        let option: serde_json::Value = serde_json::from_str(&embed.chart.to_string())
            .expect("the page gets the chart as JSON without script");
        assert_eq!(
            option["title"][0]["text"],
            "Publish latency - OMB \"a\"+alert(1)"
        );
    }

    #[test]
    fn given_run_without_consumers_when_charted_should_have_no_end_to_end_chart_or_line() {
        let detail = detail(&NO_CONSUMERS);

        assert!(EmbedChart::of_omb_run(&detail, OmbChartType::EndToEndLatency, true).is_none());
        for chart_type in [
            OmbChartType::Throughput,
            OmbChartType::Ladder,
            OmbChartType::Quantiles,
        ] {
            let embed = EmbedChart::of_omb_run(&detail, chart_type, true)
                .unwrap_or_else(|| panic!("the fixture run has a {chart_type} chart"));
            assert_eq!(series_names(&embed.chart), ["Publish"], "{chart_type}");
            let (_, subtext) = title_and_subtext(&embed.chart);
            assert!(!subtext.contains("End-to-end"), "{chart_type}: {subtext}");
            assert!(!subtext.contains("Mean consume"), "{chart_type}: {subtext}");
            assert_eq!(embed.hardware_line, "Machines not recorded  •  Driver Iggy");
        }
    }

    #[test]
    fn given_environment_without_label_when_named_should_list_servers_and_clients() {
        let environment = OmbEnvironment {
            label: None,
            server_instance: Some("i4i.4xlarge".to_owned()),
            server_nodes: Some(3),
            client_instance: Some("c6in.8xlarge".to_owned()),
            client_hosts: Some(1),
        };

        assert_eq!(
            machines(&environment).as_deref(),
            Some("i4i.4xlarge, 3 nodes, 1 client host of c6in.8xlarge")
        );
        assert_eq!(
            machines(&OmbEnvironment {
                server_nodes: Some(1),
                ..OmbEnvironment::default()
            })
            .as_deref(),
            Some("1 node")
        );
        assert_eq!(machines(&OmbEnvironment::default()), None);
    }
}
