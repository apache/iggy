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

//! The preview card of a run page: the image that a link preview shows. A preview shrinks the
//! image to about 500 px wide, so the card holds a few lines of large text and no chart. It names
//! the workload and the numbers that the run is about. The server draws it.

use crate::delta::Metric;
use crate::embed::PngSize;
use crate::labels::{
    CONDITION_SEPARATOR, DAY_FORMAT, MachineName, durability_text, message_size_text,
    messages_text, metric_name, metric_value_text, milliseconds, nodes_text, rate_limit_text,
    topic_durability,
};
use crate::omb::labels::{
    actors_text, environment_text, length_text, mebibytes_text,
    message_size_text as omb_message_size_text, topology_text, version_text,
};
use crate::omb::summary::OmbRunSummary;
use crate::summary::{BenchmarkSummary, UNCAPPED_HEADLINES, kind_name, workload_actors};
use chrono::DateTime;

/// The size of every card, as Open Graph and the large Twitter card want it.
pub const CARD_SIZE: PngSize = PngSize::OPEN_GRAPH;
/// The file of the card below the embed path of a run, for example `/embed/{uuid}/card.png`.
pub const CARD_FILE: &str = "card.png";
pub const IGGY_BENCH_SOURCE: &str = "iggy-bench";
pub const OMB_SOURCE: &str = "OpenMessaging Benchmark";
const PRODUCT: &str = "Apache Iggy";
const PUBLISH_THROUGHPUT: &str = "Publish throughput";
const PUBLISH_P99: &str = "Publish P99";
const END_TO_END_P99: &str = "End-to-end P99";
/// The rate limit, on the line above the numbers, sets the throughput of a rate-limited run.
const RATE_LIMITED_METRICS: [Metric; 3] = [Metric::P50, Metric::P99, Metric::P999];

/// The text of a card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCard {
    /// The tool that measured the run.
    pub source: &'static str,
    /// The kind of an iggy-bench run, or the topology of an OMB run.
    pub title: String,
    /// The workload, one line each, below the title.
    pub lines: Vec<String>,
    /// Empty when the run has no numbers for its main group.
    pub numbers: Vec<CardNumber>,
    /// The version, the machine and the day of the run.
    pub footer: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardNumber {
    pub label: &'static str,
    pub value: String,
    /// The line below the value: the message rate of a throughput.
    pub detail: Option<String>,
}

impl RunCard {
    /// A run with a rate limit shows its latency, and a run without one shows its throughput.
    pub fn run(summary: &BenchmarkSummary) -> Self {
        let key = &summary.workload_key;
        let workload = [workload_actors(key), nodes_text(summary.nodes)];
        let mut setup: Vec<String> = topic_durability(summary)
            .map(|durability| durability_text(durability).to_owned())
            .into_iter()
            .collect();
        setup.extend([
            rate_limit_text(key.rate_limit.as_deref()),
            message_size_text(key.message_size),
        ]);
        let metrics: &[Metric] = if key.rate_limit.is_some() {
            &RATE_LIMITED_METRICS
        } else {
            &UNCAPPED_HEADLINES
        };
        let numbers = summary
            .group(summary.kind_family.primary_group())
            .map(|group| {
                metrics
                    .iter()
                    .map(|&metric| CardNumber {
                        label: metric_name(metric),
                        value: metric_value_text(metric, group.value(metric)),
                        detail: (metric == Metric::Throughput)
                            .then(|| messages_text(group.throughput_messages_per_second)),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut footer = vec![
            format!("{PRODUCT} {}", summary.version),
            MachineName(&summary.machine).to_string(),
        ];
        footer.extend(
            summary
                .version_order
                .timestamp_us
                .and_then(DateTime::from_timestamp_micros)
                .map(|start| start.format(DAY_FORMAT).to_string()),
        );
        Self {
            source: IGGY_BENCH_SOURCE,
            title: kind_name(key.kind).to_owned(),
            lines: vec![
                workload.join(CONDITION_SEPARATOR),
                setup.join(CONDITION_SEPARATOR),
            ],
            numbers,
            footer: footer.join(CONDITION_SEPARATOR),
        }
    }

    /// Names the workload by its numbers, never by the workload name, which starts with the name
    /// of the workload file. The numbers are those of the run page's headline strip that the run
    /// measured, without the publish delay.
    pub fn omb_run(summary: &OmbRunSummary) -> Self {
        let workload = &summary.workload;
        let mut numbers = vec![
            CardNumber {
                label: PUBLISH_THROUGHPUT,
                value: mebibytes_text(summary.publish_mebibytes_per_second),
                detail: Some(messages_text(summary.publish_messages_per_second)),
            },
            CardNumber {
                label: PUBLISH_P99,
                value: milliseconds(summary.publish_latency_ms.p99),
                detail: None,
            },
        ];
        numbers.extend(summary.end_to_end_latency_ms.map(|latency| CardNumber {
            label: END_TO_END_P99,
            value: milliseconds(latency.p99),
            detail: None,
        }));
        let mut footer = vec![version_text(summary).map_or_else(
            || PRODUCT.to_owned(),
            |version| format!("{PRODUCT} {version}"),
        )];
        footer.extend(summary.environment().and_then(environment_text));
        footer.push(summary.time.instant.format(DAY_FORMAT).to_string());
        Self {
            source: OMB_SOURCE,
            title: topology_text(workload),
            lines: vec![
                actors_text(workload),
                [
                    omb_message_size_text(workload.message_size),
                    format!("driver {}", summary.driver),
                    length_text(summary.length_seconds),
                ]
                .join(CONDITION_SEPARATOR),
            ],
            numbers,
            footer: footer.join(CONDITION_SEPARATOR),
        }
    }

    /// The text of the card in one line, for the `alt` of the image.
    pub fn alt(&self) -> String {
        let workload = std::iter::once(self.title.as_str())
            .chain(self.lines.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join(", ");
        let numbers: Vec<String> = self
            .numbers
            .iter()
            .map(|number| format!("{} {}", number.label, number.value))
            .collect();
        if numbers.is_empty() {
            workload
        } else {
            format!("{workload}: {}", numbers.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::omb::test_fixtures::{RC1_DEFAULT, RC1_PERSISTED};
    use crate::summary::BenchmarkSummary;
    use crate::test_fixtures::{CLUSTER_RUN, LIVE_UNCAPPED_RUN, THRESHOLDS, report};

    fn summary(report_json: &str) -> BenchmarkSummary {
        BenchmarkSummary::new(&report(report_json), &THRESHOLDS)
    }

    fn labels(card: &RunCard) -> Vec<&'static str> {
        card.numbers.iter().map(|number| number.label).collect()
    }

    #[test]
    fn given_rate_limited_run_when_building_card_should_show_latency_only() {
        let card = RunCard::run(&summary(CLUSTER_RUN));

        assert_eq!(
            card,
            RunCard {
                source: IGGY_BENCH_SOURCE,
                title: "Pinned producer".to_owned(),
                lines: vec![
                    "20 producers · 3 nodes".to_owned(),
                    "replicated · rate limit 800 MB/s · 1,000 B messages".to_owned(),
                ],
                numbers: card.numbers.clone(),
                footer: "Apache Iggy 0.10.0 · ip-10-0-0-224 · Nov 4, 2026".to_owned(),
            }
        );
        assert_eq!(labels(&card), ["P50", "P99", "P99.9"]);
        assert_eq!(card.numbers[1].value, "1.27 ms");
        assert!(card.numbers.iter().all(|number| number.detail.is_none()));
    }

    #[test]
    fn given_run_without_rate_limit_when_building_card_should_show_throughput_only() {
        let card = RunCard::run(&summary(LIVE_UNCAPPED_RUN));

        assert_eq!(labels(&card), ["Throughput"]);
        assert_eq!(
            card.lines[1],
            "replicated · no rate limit · 1,000 B messages"
        );
        assert!(!card.alt().contains("P99"), "{}", card.alt());
    }

    #[test]
    fn given_run_without_main_group_when_building_card_should_keep_workload_without_numbers() {
        let mut run = summary(CLUSTER_RUN);
        run.groups.clear();

        let card = RunCard::run(&run);

        assert!(card.numbers.is_empty());
        assert_eq!(
            card.alt(),
            "Pinned producer, 20 producers · 3 nodes, replicated · rate limit 800 MB/s · 1,000 B \
             messages"
        );
    }

    #[test]
    fn given_omb_runs_when_building_card_should_name_workload_by_numbers_not_by_file() {
        for fixture in [&RC1_DEFAULT, &RC1_PERSISTED] {
            let run = fixture.summary();

            let card = RunCard::omb_run(&run);
            let text = format!("{card:?}");

            assert_eq!(card.source, OMB_SOURCE);
            assert!(
                !text.contains(&run.workload.name),
                "{} in {text}",
                run.workload.name
            );
            assert_eq!(
                labels(&card)[..2],
                [PUBLISH_THROUGHPUT, PUBLISH_P99],
                "{text}"
            );
        }
    }

    #[test]
    fn given_omb_run_with_and_without_end_to_end_latency_when_building_card_should_show_it_if_measured()
     {
        let mut run = RC1_DEFAULT.summary();
        let measured = run.end_to_end_latency_ms.is_some();

        let card = RunCard::omb_run(&run);
        run.end_to_end_latency_ms = None;
        let without = RunCard::omb_run(&run);

        assert_eq!(labels(&card).contains(&END_TO_END_P99), measured);
        assert_eq!(labels(&without), [PUBLISH_THROUGHPUT, PUBLISH_P99]);
    }
}
