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

//! The head of each page: its title, description, canonical link, link preview and structured
//! data. The server writes the tags into the page that it sends, because link previews and most
//! crawlers do not run the app. The app sets the document title from the same [`PageHead`] when the
//! reader moves to another page, so both titles are always the same.

use crate::card::{CARD_FILE, RunCard};
use crate::config::DashboardConfig;
use crate::delta::Metric;
use crate::embed::PngSize;
use crate::format::{format_throughput_mb_s, group_digits, runs_text};
use crate::headline::{HeadlineLine, newest_release, official_runs};
use crate::html::{escape, json_script_safe};
use crate::labels::{
    CpuName, DAY_FORMAT, MachineName, conditions, count_text, durability_text, messages_text,
    metric_name, metric_value_text, milliseconds, nodes_text, rate_limit_text, short_id,
    topic_durability,
};
use crate::omb::labels::{
    actors_text, environment_text, length_text, mebibytes_text, message_size_text,
    short_id as omb_short_id, topology_text, version_text,
};
use crate::omb::summary::{OmbRunList, OmbRunSummary};
use crate::pages::{ExploreScope, omb_run_path, run_path};
use crate::summary::BenchmarkSummary;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::fmt;

pub const SITE_NAME: &str = "Apache Iggy Benchmarks";
/// For the pages whose query picks the runs. Their links repeat each other, but they link to the
/// run pages.
pub const NOINDEX_FOLLOW: &str = "noindex, follow";
/// The lead of the Methodology page, which is also its description.
pub const METHODOLOGY_LEAD: &str = "Most runs on this dashboard come from iggy-bench, a tool that \
                                    sends and polls messages on an Apache Iggy server and measures \
                                    the result. This page shows how to run it, what the numbers \
                                    mean, and when a difference is a real change. The last section \
                                    explains the OpenMessaging Benchmark runs, which have pages of \
                                    their own.";
/// The card of every page but a run page. It has the size of [`PngSize::OPEN_GRAPH`].
const SITE_CARD: &str = "/assets/og-image.png";
const SCHEMA_CONTEXT: &str = "https://schema.org";
const OVERVIEW_CRUMB: &str = "Benchmarks";
const EXPLORE_CRUMB: &str = "Explore";
const OMB_CRUMB: &str = "OMB";
const METHODOLOGY_CRUMB: &str = "Methodology";
const ISO_DAY_FORMAT: &str = "%Y-%m-%d";
const OVERVIEW_FALLBACK: &str = "Throughput and latency of Apache Iggy, measured with iggy-bench \
                                 and the OpenMessaging Benchmark, with the full report of every \
                                 run.";
const IGGY_BENCH_VARIABLES: [&str; 5] = [
    "Throughput",
    "P50 latency",
    "P90 latency",
    "P99 latency",
    "P99.9 latency",
];
const OMB_VARIABLES: [&str; 4] = [
    "Publish rate",
    "Consume rate",
    "Publish latency",
    "End-to-end latency",
];

/// The head of one page.
#[derive(Debug, Clone, PartialEq)]
pub struct PageHead {
    pub title: String,
    pub description: Option<String>,
    /// The path of the one link of the page. It has a query only when the query makes a page of
    /// its own, as `/explore?scope=all` does. A search or a filter never does. `None` for a page
    /// without a link of its own: the not-found page, and the compare pages, whose query picks
    /// the runs.
    pub path: Option<String>,
    pub image: Option<PageImage>,
    /// The robots meta value, for a page that crawlers must not index.
    pub robots: Option<&'static str>,
    /// The trail from the Overview to this page, this page included. Empty for a page without
    /// breadcrumbs.
    pub crumbs: Vec<Crumb>,
    pub schema: Option<Schema>,
}

/// A link preview image of [`PngSize::OPEN_GRAPH`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageImage {
    /// The path on this host, with the query.
    pub path: String,
    pub alt: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Crumb {
    pub name: String,
    pub path: String,
}

/// The structured data that describes the page as a whole.
#[derive(Debug, Clone, PartialEq)]
pub enum Schema {
    /// The site, on the Overview.
    WebSite,
    /// The results of one run.
    Dataset(Dataset),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Dataset {
    /// The day of the run, for example `2026-11-13`.
    pub created: Option<String>,
    pub variables: &'static [&'static str],
    /// The path of the result file of the run.
    pub download: String,
}

/// The tags of a [`PageHead`]. Every link in them is absolute, so the tags that need a link name no
/// URL without the public address of the dashboard.
#[derive(Debug, Clone, Copy)]
pub struct HeadTags<'a> {
    head: &'a PageHead,
    /// The scheme, the host and the port, without a trailing slash.
    origin: Option<&'a str>,
}

impl PageHead {
    /// The Overview names the latest release and the numbers of its headline cards.
    pub fn overview(summaries: &[BenchmarkSummary], config: &DashboardConfig) -> Self {
        let official = official_runs(summaries, config);
        let release = newest_release(official.iter().copied());
        let title = match release {
            Some(release) => format!(
                "Apache Iggy benchmarks: throughput and latency of {}",
                release.version
            ),
            None => "Apache Iggy benchmarks: throughput and latency".to_owned(),
        };
        let description = release
            .and_then(|release| headline_sentences(summaries, config, &release.version))
            .unwrap_or_else(|| OVERVIEW_FALLBACK.to_owned());
        Self {
            title,
            description: Some(description),
            path: Some("/".to_owned()),
            image: Some(site_card()),
            robots: None,
            crumbs: Vec::new(),
            schema: Some(Schema::WebSite),
        }
    }

    /// The list of `scope`. Each scope is a page of its own, so crawlers reach every run page
    /// through the links of the lists.
    pub fn explore(
        summaries: &[BenchmarkSummary],
        config: &DashboardConfig,
        scope: ExploreScope,
    ) -> Self {
        let (title, source, runs) = match scope {
            ExploreScope::Official => (
                "Official Apache Iggy benchmark runs",
                "of the official iggy-bench suites",
                official_runs(summaries, config),
            ),
            ExploreScope::All => (
                "All Apache Iggy benchmark runs",
                "of iggy-bench",
                summaries.iter().collect(),
            ),
        };
        let versions: BTreeSet<_> = runs.iter().map(|run| run.version.as_str()).collect();
        let machines: BTreeSet<_> = runs.iter().map(|run| run.machine.as_str()).collect();
        let description = format!(
            "{} {source} on {} of Apache Iggy and {}. Filter them, open the report of each run, \
             and pick runs to compare.",
            runs_text(runs.len()),
            count_text(versions.len() as u64, "version", "versions"),
            count_text(machines.len() as u64, "machine", "machines"),
        );
        Self::listing(title, description, scope.path(), EXPLORE_CRUMB)
    }

    pub fn methodology() -> Self {
        Self::listing(
            "How the Apache Iggy benchmarks work",
            METHODOLOGY_LEAD.to_owned(),
            "/methodology",
            METHODOLOGY_CRUMB,
        )
    }

    pub fn omb_runs(list: &OmbRunList) -> Self {
        let description = if list.runs.is_empty() {
            "OpenMessaging Benchmark runs of Apache Iggy, with the publish rate, the latency and \
             the result file of each run."
                .to_owned()
        } else {
            let versions: BTreeSet<_> = list.runs.iter().filter_map(version_text).collect();
            let environments: BTreeSet<_> = list
                .runs
                .iter()
                .filter_map(|run| run.environment().and_then(environment_text))
                .collect();
            format!(
                "{} of Apache Iggy with the OpenMessaging Benchmark, on {} and {}. Each run shows \
                 its publish rate, its latency and its result file.",
                runs_text(list.runs.len()),
                count_text(versions.len() as u64, "version", "versions"),
                count_text(environments.len() as u64, "environment", "environments"),
            )
        };
        Self::listing(
            "OpenMessaging Benchmark results for Apache Iggy",
            description,
            "/omb",
            OMB_CRUMB,
        )
    }

    /// The title names the durability and the rate limit, which tell apart the workloads of one
    /// kind on one machine. Repeated runs of one workload keep one title. The image is the preview
    /// card of the run. A run without a rate limit leads with its throughput alone, in the
    /// description as on the card.
    pub fn run(summary: &BenchmarkSummary) -> Self {
        let path = run_path(&summary.uuid);
        let mut workload = vec![summary.title.clone(), nodes_text(summary.nodes)];
        workload.extend(
            topic_durability(summary).map(|durability| durability_text(durability).to_owned()),
        );
        workload.extend(
            summary
                .workload_key
                .rate_limit
                .as_deref()
                .map(|rate_limit| rate_limit_text(Some(rate_limit))),
        );
        let title = format!(
            "{}: Apache Iggy {} on {}",
            workload.join(", "),
            summary.version,
            MachineName(&summary.machine)
        );
        let start = summary
            .version_order
            .timestamp_us
            .and_then(DateTime::from_timestamp_micros);
        let mut sentences = Vec::new();
        if let Some(group) = summary.group(summary.kind_family.primary_group()) {
            let throughput = format_throughput_mb_s(group.throughput_megabytes_per_second);
            sentences.push(if summary.headline_metrics().contains(&Metric::P99) {
                format!(
                    "{throughput} at P99 {}.",
                    milliseconds(group.p99_latency_ms)
                )
            } else {
                format!(
                    "{throughput}, {}.",
                    messages_text(group.throughput_messages_per_second)
                )
            });
        }
        sentences.push(format!("{}.", conditions(summary)));
        sentences.push(match start {
            Some(start) => format!("iggy-bench run of {}.", start.format(DAY_FORMAT)),
            None => "iggy-bench run.".to_owned(),
        });
        if let Some(cpu) = CpuName::of(&summary.hardware) {
            sentences.push(format!("Bench host: {cpu}."));
        }
        Self {
            title,
            description: Some(sentences.join(" ")),
            image: Some(card_image(
                &format!("/embed/{}", summary.uuid),
                &RunCard::run(summary),
            )),
            robots: None,
            crumbs: vec![
                overview_crumb(),
                crumb(EXPLORE_CRUMB, "/explore"),
                crumb(&short_id(&summary.uuid), &path),
            ],
            schema: Some(Schema::Dataset(Dataset {
                created: start.map(iso_day),
                variables: &IGGY_BENCH_VARIABLES,
                download: format!("/api/benchmark/full/{}", summary.uuid),
            })),
            path: Some(path),
        }
    }

    /// The title names the workload by its numbers, never by the workload name, which starts with
    /// the name of the workload file. The result doesn't record the target rate, so the title
    /// names the measured publish rate, which tells apart the runs of one workload at different
    /// rates.
    pub fn omb_run(summary: &OmbRunSummary) -> Self {
        let path = omb_run_path(summary.id);
        let version =
            version_text(summary).map_or_else(String::new, |version| format!(" {version}"));
        let environment = summary
            .environment()
            .and_then(environment_text)
            .map_or_else(String::new, |environment| format!(" on {environment}"));
        let title = format!(
            "Apache Iggy{version} OMB: {}, {} B at {}, {}{environment}",
            topology_text(&summary.workload),
            group_digits(summary.workload.message_size),
            messages_text(summary.publish_messages_per_second),
            summary.driver,
        );
        let description = format!(
            "Publish {} at P99 {} over {}. {}, {}, driver {}. OpenMessaging Benchmark run of {}.",
            mebibytes_text(summary.publish_mebibytes_per_second),
            milliseconds(summary.publish_latency_ms.p99),
            length_text(summary.length_seconds),
            actors_text(&summary.workload),
            message_size_text(summary.workload.message_size),
            summary.driver,
            summary.time.instant.format(DAY_FORMAT)
        );
        Self {
            title,
            description: Some(description),
            image: Some(card_image(
                &format!("/embed/omb/{}", summary.id),
                &RunCard::omb_run(summary),
            )),
            robots: None,
            crumbs: vec![
                overview_crumb(),
                crumb(OMB_CRUMB, "/omb"),
                crumb(&omb_short_id(summary.id), &path),
            ],
            schema: Some(Schema::Dataset(Dataset {
                created: Some(iso_day(summary.time.instant)),
                variables: &OMB_VARIABLES,
                download: format!("/api/omb/runs/{}/result.json", summary.id),
            })),
            path: Some(path),
        }
    }

    pub fn compare_runs() -> Self {
        Self::compare(
            "Compare iggy-bench runs of Apache Iggy",
            "The throughput and latency of the picked iggy-bench runs side by side, each against \
             the first run.",
        )
    }

    pub fn compare_releases() -> Self {
        Self::compare(
            "Compare Apache Iggy releases",
            "The official iggy-bench runs of two Apache Iggy versions, workload by workload.",
        )
    }

    pub fn omb_compare() -> Self {
        Self::compare(
            "Compare OpenMessaging Benchmark runs of Apache Iggy",
            "The publish rate and latency of the picked OpenMessaging Benchmark runs side by side, \
             each against the first run.",
        )
    }

    /// The server answers 404 with this head, so it needs no robots value.
    pub fn not_found() -> Self {
        Self {
            title: format!("Page not found: {SITE_NAME}"),
            description: None,
            path: None,
            image: None,
            robots: None,
            crumbs: Vec::new(),
            schema: None,
        }
    }

    /// The tags with absolute links on `origin`, the public address of the dashboard. Without it,
    /// the tags leave out the canonical link, the page URL, the image and the structured data.
    pub fn tags<'a>(&'a self, origin: Option<&'a str>) -> HeadTags<'a> {
        HeadTags { head: self, origin }
    }

    /// A page of its own below the Overview, with the site card.
    fn listing(title: &str, description: String, path: &str, crumb_name: &str) -> Self {
        Self {
            title: title.to_owned(),
            description: Some(description),
            path: Some(path.to_owned()),
            image: Some(site_card()),
            robots: None,
            crumbs: vec![overview_crumb(), crumb(crumb_name, path)],
            schema: None,
        }
    }

    fn compare(title: &str, description: &str) -> Self {
        Self {
            title: title.to_owned(),
            description: Some(description.to_owned()),
            path: None,
            image: Some(site_card()),
            robots: Some(NOINDEX_FOLLOW),
            crumbs: Vec::new(),
            schema: None,
        }
    }
}

impl HeadTags<'_> {
    fn url(&self, path: &str) -> Option<String> {
        self.origin.map(|origin| format!("{origin}{path}"))
    }

    fn structured_data(&self, origin: &str) -> Vec<Value> {
        let head = self.head;
        let url = |path: &str| format!("{origin}{path}");
        let mut data = Vec::new();
        if !head.crumbs.is_empty() {
            let items: Vec<Value> = head
                .crumbs
                .iter()
                .enumerate()
                .map(|(index, crumb)| {
                    json!({
                        "@type": "ListItem",
                        "position": index + 1,
                        "name": crumb.name,
                        "item": url(&crumb.path),
                    })
                })
                .collect();
            data.push(json!({
                "@context": SCHEMA_CONTEXT,
                "@type": "BreadcrumbList",
                "itemListElement": items,
            }));
        }
        match &head.schema {
            Some(Schema::WebSite) => data.push(json!({
                "@context": SCHEMA_CONTEXT,
                "@type": "WebSite",
                "name": SITE_NAME,
                "url": url("/"),
            })),
            Some(Schema::Dataset(dataset)) => {
                let mut value = json!({
                    "@context": SCHEMA_CONTEXT,
                    "@type": "Dataset",
                    "name": head.title,
                    "description": head.description,
                    "isAccessibleForFree": true,
                    "variableMeasured": dataset.variables,
                    "distribution": [{
                        "@type": "DataDownload",
                        "encodingFormat": "application/json",
                        "contentUrl": url(&dataset.download),
                    }],
                });
                if let Some(path) = &head.path {
                    value["url"] = Value::from(url(path));
                }
                if let Some(created) = &dataset.created {
                    value["dateCreated"] = Value::from(created.as_str());
                }
                data.push(value);
            }
            None => {}
        }
        data
    }
}

impl fmt::Display for HeadTags<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let head = self.head;
        let title = escape(&head.title);
        let description = head.description.as_deref().map(escape);
        writeln!(f, "<title>{title}</title>")?;
        if let Some(description) = &description {
            writeln!(f, r#"<meta name="description" content="{description}" />"#)?;
        }
        if let Some(robots) = head.robots {
            writeln!(f, r#"<meta name="robots" content="{robots}" />"#)?;
        }
        let page_url = head.path.as_deref().and_then(|path| self.url(path));
        if let Some(page_url) = &page_url {
            writeln!(f, r#"<link rel="canonical" href="{}" />"#, escape(page_url))?;
        }
        writeln!(
            f,
            r#"<meta property="og:site_name" content="{SITE_NAME}" />"#
        )?;
        writeln!(f, r#"<meta property="og:type" content="website" />"#)?;
        writeln!(f, r#"<meta property="og:title" content="{title}" />"#)?;
        if let Some(description) = &description {
            writeln!(
                f,
                r#"<meta property="og:description" content="{description}" />"#
            )?;
        }
        if let Some(page_url) = &page_url {
            writeln!(
                f,
                r#"<meta property="og:url" content="{}" />"#,
                escape(page_url)
            )?;
        }
        let image = head
            .image
            .as_ref()
            .and_then(|image| Some((self.url(&image.path)?, image)));
        match image {
            Some((image_url, image)) => {
                let size = PngSize::OPEN_GRAPH;
                writeln!(
                    f,
                    r#"<meta property="og:image" content="{}" />"#,
                    escape(&image_url)
                )?;
                writeln!(
                    f,
                    r#"<meta property="og:image:width" content="{}" />"#,
                    size.width()
                )?;
                writeln!(
                    f,
                    r#"<meta property="og:image:height" content="{}" />"#,
                    size.height()
                )?;
                writeln!(
                    f,
                    r#"<meta property="og:image:alt" content="{}" />"#,
                    escape(&image.alt)
                )?;
                writeln!(
                    f,
                    r#"<meta name="twitter:card" content="summary_large_image" />"#
                )?;
            }
            None => writeln!(f, r#"<meta name="twitter:card" content="summary" />"#)?,
        }
        if let Some(origin) = self.origin {
            for value in self.structured_data(origin) {
                writeln!(
                    f,
                    r#"<script type="application/ld+json">{}</script>"#,
                    json_script_safe(&value.to_string())
                )?;
            }
        }
        Ok(())
    }
}

/// One sentence per headline card that a release ran, after one that names the release and the
/// machine. `None` when no card has a number.
fn headline_sentences(
    summaries: &[BenchmarkSummary],
    config: &DashboardConfig,
    latest_release: &str,
) -> Option<String> {
    let lines = HeadlineLine::all_of(summaries, config);
    let mut machines = BTreeSet::new();
    let cards: Vec<String> = lines
        .iter()
        .filter_map(|line| {
            let (version, run) = line.newest_release()?;
            let metric = line.config.metric;
            let value = headline_value(metric, version.median(metric)?);
            machines.insert(run.machine.as_str());
            // A card shows the newest release that ran its workload, which can be older than the
            // latest release.
            Some(if version.version == latest_release {
                format!("{}: {value}.", line.config.title)
            } else {
                format!("{} in {}: {value}.", line.config.title, version.version)
            })
        })
        .collect();
    if cards.is_empty() {
        return None;
    }
    let lead = match machines.iter().collect::<Vec<_>>().as_slice() {
        [machine] => format!("Apache Iggy {latest_release} on {}.", MachineName(machine)),
        _ => format!("Apache Iggy {latest_release}."),
    };
    Some(format!("{lead} {}", cards.join(" ")))
}

/// For example `2.73 GB/s`, or `P99 1.90 ms` for a latency.
fn headline_value(metric: Metric, value: f64) -> String {
    match metric {
        Metric::Throughput => metric_value_text(metric, value),
        _ => format!(
            "{} {}",
            metric_name(metric),
            metric_value_text(metric, value)
        ),
    }
}

/// The preview card of a run, below `run_embed`, the embed path of the run.
fn card_image(run_embed: &str, card: &RunCard) -> PageImage {
    PageImage {
        path: format!("{run_embed}/{CARD_FILE}"),
        alt: card.alt(),
    }
}

fn site_card() -> PageImage {
    PageImage {
        path: SITE_CARD.to_owned(),
        alt: SITE_NAME.to_owned(),
    }
}

fn overview_crumb() -> Crumb {
    crumb(OVERVIEW_CRUMB, "/")
}

fn crumb(name: &str, path: &str) -> Crumb {
    Crumb {
        name: name.to_owned(),
        path: path.to_owned(),
    }
}

fn iso_day(instant: DateTime<Utc>) -> String {
    instant.format(ISO_DAY_FORMAT).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{HeadlineConfig, SuiteConfig};
    use crate::identity::KindFamily;
    use crate::omb::test_fixtures::{RC1_DEFAULT, RC1_PERSISTED};
    use crate::test_fixtures::{CLUSTER_RUN, LIVE_UNCAPPED_RUN, THRESHOLDS, report};
    use bench_report::options::BenchmarkDurability;

    const ORIGIN: &str = "https://benchmarks.iggy.apache.org";
    const CLUSTER_UUID: &str = "48587edc-6d6b-4ab5-8316-c64be4a65135";

    fn summary(json: &str) -> BenchmarkSummary {
        BenchmarkSummary::new(&report(json), &THRESHOLDS)
    }

    /// The JSON of each `<script type="application/ld+json">` element of `tags`.
    fn structured_data(tags: &str) -> Vec<Value> {
        const OPEN: &str = r#"<script type="application/ld+json">"#;
        tags.split(OPEN)
            .skip(1)
            .map(|rest| {
                let json = &rest[..rest.find("</script>").expect("the element ends")];
                serde_json::from_str(json).expect("the structured data is JSON")
            })
            .collect()
    }

    /// The shipped suites, with one headline on the suite of the cluster fixture and one that no
    /// fixture ran.
    fn config() -> DashboardConfig {
        DashboardConfig {
            suites: vec![SuiteConfig {
                name: "i4i-cluster3".to_owned(),
                title: "AWS i4i.4xlarge, 3 nodes".to_owned(),
            }],
            headlines: vec![
                HeadlineConfig {
                    suite: "i4i-cluster3".to_owned(),
                    workload: "producer-20p@800MB".to_owned(),
                    title: "Write latency, 3 nodes".to_owned(),
                    metric: Metric::P99,
                },
                HeadlineConfig {
                    suite: "i4i-cluster3".to_owned(),
                    workload: "end-to-end-20@1600MB".to_owned(),
                    title: "End-to-end latency, 3 nodes".to_owned(),
                    metric: Metric::P99,
                },
            ],
            thresholds: THRESHOLDS,
        }
    }

    /// The cluster fixture as an official release run of the first headline workload.
    fn official_release() -> BenchmarkSummary {
        let mut run = summary(CLUSTER_RUN);
        run.suite = Some("i4i-cluster3".to_owned());
        run.workload_key.workload = Some("producer-20p@800MB".to_owned());
        run.release = true;
        run
    }

    #[test]
    fn given_fixture_runs_when_building_heads_should_name_run_numbers_conditions_and_host() {
        let cluster = PageHead::run(&summary(CLUSTER_RUN));
        let uncapped = PageHead::run(&summary(LIVE_UNCAPPED_RUN));

        assert_eq!(
            cluster.title,
            "Pinned producer, 20 producers, 3 nodes, replicated, rate limit 800 MB/s: Apache Iggy \
             0.10.0 on ip-10-0-0-224"
        );
        assert_eq!(
            cluster.description.as_deref(),
            Some(
                "784 MB/s at P99 1.27 ms. 1,000 B messages · 250 per batch · 248,000 batches in \
                 total · rate limit 800 MB/s · TCP · replicated · ip-10-0-0-224 · 3 nodes. \
                 iggy-bench run of Nov 4, 2026. Bench host: Intel Xeon Platinum 8375C."
            )
        );
        assert_eq!(
            cluster.path.as_deref(),
            Some(format!("/runs/{CLUSTER_UUID}").as_str())
        );
        assert_eq!(
            uncapped.title,
            "Pinned producer, 20 producers, 1 node, replicated: Apache Iggy 0.9.0 at 71f29618b on \
             ip-10-0-0-236"
        );
    }

    #[test]
    fn given_workloads_of_one_kind_when_building_heads_should_tell_them_apart_by_title() {
        let replicated = summary(CLUSTER_RUN);
        let mut persisted = replicated.clone();
        persisted.workload_key.topic.durability = BenchmarkDurability::Persisted;
        let mut uncapped = replicated.clone();
        uncapped.workload_key.rate_limit = None;
        let mut consumer = replicated.clone();
        consumer.kind_family = KindFamily::Consumer;

        let titles: BTreeSet<_> = [&replicated, &persisted, &uncapped]
            .map(|run| PageHead::run(run).title)
            .into();

        assert_eq!(titles.len(), 3, "{titles:?}");
        assert!(
            PageHead::run(&persisted)
                .title
                .contains(", persisted, rate limit 800 MB/s: ")
        );
        assert!(PageHead::run(&uncapped).title.contains(", replicated: "));
        assert!(
            PageHead::run(&consumer)
                .title
                .contains(", 3 nodes, rate limit 800 MB/s: "),
            "a consumer run creates no topic, so it has no durability"
        );
    }

    #[test]
    fn given_rate_limited_and_uncapped_runs_when_building_head_should_lead_with_numbers_of_card() {
        let rate_limited = summary(CLUSTER_RUN);
        let uncapped = summary(LIVE_UNCAPPED_RUN);

        let image = PageHead::run(&rate_limited)
            .image
            .expect("a run page has an image");
        let uncapped_head = PageHead::run(&uncapped);

        assert_eq!(image.path, format!("/embed/{CLUSTER_UUID}/card.png"));
        assert_eq!(image.alt, RunCard::run(&rate_limited).alt());
        let description = uncapped_head.description.expect("a run has a description");
        assert!(
            !description.contains("P99") && description.contains(" msg/s. "),
            "{description}"
        );
        assert_eq!(
            uncapped_head.image.map(|image| image.alt),
            Some(RunCard::run(&uncapped).alt())
        );
    }

    #[test]
    fn given_omb_fixture_when_building_head_should_name_workload_by_numbers_not_by_file() {
        for fixture in [&RC1_DEFAULT, &RC1_PERSISTED] {
            let run = fixture.summary();

            let head = PageHead::omb_run(&run);
            let tags = head.tags(Some(ORIGIN)).to_string();

            let file_stem = run.file_name.trim_end_matches(".json");
            assert!(!tags.contains(file_stem), "{tags}");
            assert!(!tags.contains(run.workload.name.as_str()), "{tags}");
            if let Some(workload_file) = &run.workload_file {
                assert!(!tags.contains(workload_file.as_str()), "{tags}");
            }
        }
        let head = PageHead::omb_run(&RC1_DEFAULT.summary());
        assert_eq!(
            head.title,
            "Apache Iggy 0.10.0-rc.1 OMB: 12 topics × 1 partition, 1,000 B at 150.2k msg/s, Iggy \
             on AWS i4i.4xlarge, 3 nodes"
        );
        assert_eq!(
            head.description.as_deref(),
            Some(
                "Publish 143 MiB/s at P99 1.52 ms over 3 min. 1 producer and 1 consumer per \
                 topic, 1,000 B messages, driver Iggy. OpenMessaging Benchmark run of Nov 6, 2026."
            )
        );
    }

    #[test]
    fn given_omb_runs_of_one_workload_with_other_drivers_when_building_heads_should_differ() {
        let default = PageHead::omb_run(&RC1_DEFAULT.summary()).title;
        let persisted = PageHead::omb_run(&RC1_PERSISTED.summary()).title;

        assert_ne!(default, persisted);
        assert!(persisted.contains(", Iggy-persisted on "), "{persisted}");
    }

    #[test]
    fn given_public_url_when_writing_tags_should_write_absolute_links_and_structured_data() {
        let head = PageHead::run(&summary(CLUSTER_RUN));
        let page_url = format!("{ORIGIN}/runs/{CLUSTER_UUID}");

        let tags = head.tags(Some(ORIGIN)).to_string();

        assert!(tags.contains(&format!(r#"<link rel="canonical" href="{page_url}" />"#)));
        assert!(tags.contains(&format!(
            r#"<meta property="og:url" content="{page_url}" />"#
        )));
        assert!(tags.contains(&format!(
            r#"<meta property="og:image" content="{ORIGIN}/embed/{CLUSTER_UUID}/card.png" />"#
        )));
        assert!(tags.contains(r#"<meta name="twitter:card" content="summary_large_image" />"#));
        let data = structured_data(&tags);
        assert_eq!(data.len(), 2);
        let crumbs = &data[0]["itemListElement"];
        assert_eq!(data[0]["@type"], "BreadcrumbList");
        assert_eq!(crumbs[0]["item"], format!("{ORIGIN}/"));
        assert_eq!(crumbs[2]["position"], 3);
        assert_eq!(crumbs[2]["item"], page_url);
        assert_eq!(data[1]["@type"], "Dataset");
        assert_eq!(data[1]["name"], head.title);
        assert_eq!(data[1]["dateCreated"], "2026-11-04");
        assert_eq!(
            data[1]["distribution"][0]["contentUrl"],
            format!("{ORIGIN}/api/benchmark/full/{CLUSTER_UUID}")
        );
        assert_eq!(data[1].get("license"), None);
    }

    #[test]
    fn given_no_public_url_when_writing_tags_should_leave_out_links_image_and_structured_data() {
        let tags = PageHead::run(&summary(CLUSTER_RUN)).tags(None).to_string();

        for absent in ["canonical", "og:url", "og:image", "ld+json", "https://"] {
            assert!(!tags.contains(absent), "{absent} in {tags}");
        }
        assert!(tags.contains(r#"<meta name="twitter:card" content="summary" />"#));
        assert!(tags.starts_with("<title>Pinned producer, 20 producers, 3 nodes, replicated, "));
    }

    #[test]
    fn given_markup_in_run_labels_when_writing_tags_should_escape_it_everywhere() {
        let mut run = summary(CLUSTER_RUN);
        run.title = r#"</script><!--<script>alert("title")"#.to_owned();
        run.machine = r#""><script>alert('machine')</script>"#.to_owned();

        let tags = PageHead::run(&run).tags(Some(ORIGIN)).to_string();

        // The JSON strings keep `<script>` as text, which is harmless once nothing in them can end
        // the element or open a comment.
        let (html, scripts) = tags
            .split_once("<script")
            .expect("the tags end with structured data");
        assert!(!html.contains("<script>alert"), "{html}");
        assert!(!html.contains(r#""><"#), "{html}");
        assert!(html.contains("&lt;/script&gt;&lt;!--&lt;script&gt;alert(&quot;title&quot;)"));
        assert_eq!(tags.matches("</script>").count(), 2, "{tags}");
        assert!(!scripts.contains("<!--"), "{scripts}");
        let data = structured_data(&tags);
        assert_eq!(data[1]["name"], PageHead::run(&run).title);
    }

    #[test]
    fn given_compare_pages_when_building_heads_should_be_noindex_follow_without_own_link() {
        for head in [
            PageHead::compare_runs(),
            PageHead::compare_releases(),
            PageHead::omb_compare(),
        ] {
            let tags = head.tags(Some(ORIGIN)).to_string();

            assert_eq!(head.robots, Some(NOINDEX_FOLLOW));
            assert!(tags.contains(r#"<meta name="robots" content="noindex, follow" />"#));
            assert!(!tags.contains("canonical"), "{tags}");
            assert!(!tags.contains("og:url"), "{tags}");
        }
    }

    #[test]
    fn given_not_found_when_writing_tags_should_write_title_only() {
        let tags = PageHead::not_found().tags(Some(ORIGIN)).to_string();

        assert!(tags.starts_with("<title>Page not found: Apache Iggy Benchmarks</title>\n"));
        for absent in [
            "description",
            "robots",
            "canonical",
            "og:url",
            "og:image",
            "ld+json",
        ] {
            assert!(!tags.contains(absent), "{absent} in {tags}");
        }
    }

    #[test]
    fn given_official_release_when_building_overview_head_should_state_release_machine_and_cards() {
        let runs = [official_release(), summary(LIVE_UNCAPPED_RUN)];

        let head = PageHead::overview(&runs, &config());
        let tags = head.tags(Some(ORIGIN)).to_string();

        assert_eq!(
            head.title,
            "Apache Iggy benchmarks: throughput and latency of 0.10.0"
        );
        assert_eq!(
            head.description.as_deref(),
            Some("Apache Iggy 0.10.0 on ip-10-0-0-224. Write latency, 3 nodes: P99 1.27 ms.")
        );
        let data = structured_data(&tags);
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["@type"], "WebSite");
        assert_eq!(data[0]["url"], format!("{ORIGIN}/"));
    }

    #[test]
    fn given_card_on_older_release_when_building_overview_head_should_name_its_release() {
        let mut older = summary(LIVE_UNCAPPED_RUN);
        older.suite = Some("i4i-cluster3".to_owned());
        older.workload_key.workload = Some("end-to-end-20@1600MB".to_owned());
        older.release = true;
        older.version = "0.9.0".to_owned();
        let runs = [official_release(), older];

        let head = PageHead::overview(&runs, &config());

        assert_eq!(
            head.description.as_deref(),
            Some(
                "Apache Iggy 0.10.0. Write latency, 3 nodes: P99 1.27 ms. End-to-end latency, 3 \
                 nodes in 0.9.0: P99 26.6 ms."
            )
        );
    }

    #[test]
    fn given_no_official_release_when_building_overview_head_should_fall_back_to_general_text() {
        let runs = [summary(CLUSTER_RUN), summary(LIVE_UNCAPPED_RUN)];

        let head = PageHead::overview(&runs, &config());

        assert_eq!(head.title, "Apache Iggy benchmarks: throughput and latency");
        assert_eq!(head.description.as_deref(), Some(OVERVIEW_FALLBACK));
    }

    #[test]
    fn given_runs_when_building_list_heads_should_count_runs_versions_and_places() {
        let runs = [summary(CLUSTER_RUN), summary(LIVE_UNCAPPED_RUN)];
        let omb_runs = OmbRunList {
            enabled: true,
            runs: vec![RC1_DEFAULT.summary(), RC1_PERSISTED.summary()],
        };

        let explore = PageHead::explore(&runs, &config(), ExploreScope::All);
        let omb = PageHead::omb_runs(&omb_runs);

        assert_eq!(explore.title, "All Apache Iggy benchmark runs");
        assert_eq!(
            explore.description.as_deref(),
            Some(
                "2 runs of iggy-bench on 2 versions of Apache Iggy and 2 machines. Filter them, \
                 open the report of each run, and pick runs to compare."
            )
        );
        assert_eq!(explore.path.as_deref(), Some("/explore?scope=all"));
        assert_eq!(
            omb.description.as_deref(),
            Some(
                "2 runs of Apache Iggy with the OpenMessaging Benchmark, on 1 version and 1 \
                 environment. Each run shows its publish rate, its latency and its result file."
            )
        );
        assert_eq!(omb.path.as_deref(), Some("/omb"));
    }

    #[test]
    fn given_official_scope_when_building_explore_head_should_count_official_runs_on_bare_path() {
        let runs = [official_release(), summary(LIVE_UNCAPPED_RUN)];

        let official = PageHead::explore(&runs, &config(), ExploreScope::Official);
        let all = PageHead::explore(&runs, &config(), ExploreScope::All);
        let tags = all.tags(Some(ORIGIN)).to_string();

        assert_eq!(official.title, "Official Apache Iggy benchmark runs");
        assert_eq!(
            official.description.as_deref(),
            Some(
                "1 run of the official iggy-bench suites on 1 version of Apache Iggy and 1 \
                 machine. Filter them, open the report of each run, and pick runs to compare."
            )
        );
        assert_eq!(official.path.as_deref(), Some("/explore"));
        assert_ne!(official.title, all.title);
        assert!(
            tags.contains(&format!(
                r#"<link rel="canonical" href="{ORIGIN}/explore?scope=all" />"#
            )),
            "{tags}"
        );
    }
}
