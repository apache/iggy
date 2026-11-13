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

//! How to run `iggy-bench` and how to read the dashboard, for readers outside the field. Each fact
//! comes from the bench, server or `shared` crate code. The thresholds and suites come from
//! `/api/config`, so the page never repeats them.

use crate::components::command_block::CommandBlock;
use crate::components::page::{Crumb, breadcrumbs, loadable, page_head, page_shell, section_head};
use crate::components::section_nav::{NavSection, SectionNav};
use crate::format::runs_text;
use crate::state::data::{Loadable, use_dashboard_data};
use bench_dashboard_shared::config::{DashboardConfig, Thresholds};
use bench_dashboard_shared::identity::KindFamily;
use bench_dashboard_shared::metrics::Badge;
use bench_dashboard_shared::summary::BenchmarkSummary;
use yew::prelude::*;

static SECTIONS: [NavSection; 5] = [
    NavSection {
        id: "quick-start",
        label: "Quick start",
    },
    NavSection {
        id: "configure",
        label: "Configure",
    },
    NavSection {
        id: "metrics",
        label: "What the dashboard shows",
    },
    NavSection {
        id: "noise",
        label: "Noise and changes",
    },
    NavSection {
        id: "results",
        label: "Recorded results",
    },
];

/// A macro and not a const, because `concat!` only takes literals and the saved run repeats this
/// command.
macro_rules! first_run {
    () => {
        "target/release/iggy-bench --rate-limit 100MB --message-batches 100 pinned-producer \
         --producers 4 tcp"
    };
}

const BUILD_COMMAND: &str = "cargo build --release --bin iggy-server --bin iggy-bench";
const SERVER_COMMAND: &str = "target/release/iggy-server --with-default-root-credentials";
const BENCH_COMMAND: &str = first_run!();
const SAVE_COMMAND: &str = concat!(first_run!(), " output");
const EXAMPLES_COMMAND: &str = "target/release/iggy-bench examples";

/// A card of plain text, with an optional command below it. The page renders cards from these
/// tables, so the text stays in one place.
struct TextCard {
    title: &'static str,
    paragraphs: &'static [&'static str],
    command: Option<&'static str>,
}

const STEPS: [TextCard; 4] = [
    TextCard {
        title: "Build the server and the bench",
        paragraphs: &[
            "iggy-bench isn't on crates.io, so you build it from source. Cargo puts both programs \
             in target/release.",
        ],
        command: Some(BUILD_COMMAND),
    },
    TextCard {
        title: "Start a server",
        paragraphs: &[
            "Start the server in a second terminal and keep it running. The \
             --with-default-root-credentials flag gives a new root user the name iggy and the \
             password iggy, and the bench uses the same values by default. Only use the flag on \
             test machines.",
        ],
        command: Some(SERVER_COMMAND),
    },
    TextCard {
        title: "Run a benchmark",
        paragraphs: &[
            "This run starts 4 producers. Each one sends 100 batches of 1000 messages, 1000 bytes \
             each, to 127.0.0.1:8090 over TCP. Together they send 100 MB per second, so the run \
             takes about 4 seconds. The bench prints a summary at the end.",
        ],
        command: Some(BENCH_COMMAND),
    },
    TextCard {
        title: "See more examples",
        paragraphs: &["This command prints example commands for each kind of benchmark."],
        command: Some(EXAMPLES_COMMAND),
    },
];

/// One row of the options table. The defaults come from the bench's `args` module.
struct BenchOption {
    name: &'static str,
    flags: &'static [&'static str],
    default: OptionDefault,
    text: &'static str,
}

enum OptionDefault {
    /// The value that the bench uses when the command leaves the option out.
    Value(&'static str),
    /// Why there is no value: the option is off, or the command must name one.
    Unset(&'static str),
}

const OPTIONS: [BenchOption; 10] = [
    BenchOption {
        name: "Transport",
        flags: &["tcp", "quic", "http", "websocket"],
        default: OptionDefault::Unset("required"),
        text: "The protocol between the bench and the server. It goes after the kind's options. \
               The dashboard treats runs over different transports as different workloads.",
    },
    BenchOption {
        name: "Message durability",
        flags: &["--durability"],
        default: OptionDefault::Value("replicated"),
        text: "When the server confirms a write. With replicated, it confirms once a quorum of \
               replicas has the write. A quorum is the number of replicas that must confirm, for \
               example 2 of 3. With persisted, the server also waits until the quorum, or the \
               single server, has the write in recoverable stable storage.",
    },
    BenchOption {
        name: "Offset durability",
        flags: &["--consumer-offset-durability"],
        default: OptionDefault::Value("replicated"),
        text: "The same policy for consumer offsets, which are the consumers' saved read \
               positions. The bench applies both policies when it creates its topics.",
    },
    BenchOption {
        name: "Rate limit",
        flags: &["--rate-limit"],
        default: OptionDefault::Unset("no limit"),
        text: "The total rate for all actors together, in bytes per second, for example 800MB. \
               Each actor gets an equal share. In end-to-end-producing-consumer-group, only the \
               actors that send share the rate, and an actor that only polls has no limit. \
               Without a rate limit, each actor runs as fast as it can.",
    },
    BenchOption {
        name: "Consumer rate",
        flags: &["--read-amplification"],
        default: OptionDefault::Value("1.05"),
        text: "Only for Producer + consumer kinds. Each consumer gets this many times its share \
               of the rate, so it keeps up. It is an option of the kind, so it goes after the \
               kind.",
    },
    BenchOption {
        name: "Warm-up",
        flags: &["--warmup-time"],
        default: OptionDefault::Value("0s"),
        text: "How long the load runs before the bench starts recording. After the warm-up, \
               consumer-only runs read the topic again from offset zero.",
    },
    BenchOption {
        name: "Message size",
        flags: &["--message-size"],
        default: OptionDefault::Value("1000 bytes"),
        text: "The payload of each message. A range such as 200..500 also works.",
    },
    BenchOption {
        name: "Batch size",
        flags: &["--messages-per-batch"],
        default: OptionDefault::Value("1000"),
        text: "How many messages go in each batch.",
    },
    BenchOption {
        name: "Batches",
        flags: &["--message-batches"],
        default: OptionDefault::Value("1000"),
        text: "How many batches each actor does. In pinned-producer-and-consumer, the bench \
               scales the value: a producer does the value times the number of actors divided by \
               the number of producers, and a consumer divides by the number of consumers \
               instead. With equal counts, each actor does twice the value.",
    },
    BenchOption {
        name: "Total data",
        flags: &["--total-data"],
        default: OptionDefault::Unset("not set"),
        text: "The data for all actors together, instead of --message-batches. In Producer + \
               consumer kinds, the producers send that much data and the consumers read it back. \
               A command takes at most one of the two.",
    },
];

const OPTIONS_NOTE: &str = "Each option goes before the kind, apart from the two rows that say \
                            otherwise.";

// Macros and not consts, because `concat!` only takes literals. Each one opens a paragraph on this
// page, and the Overview repeats it, so both pages say the same thing.
macro_rules! throughput_rule {
    () => {
        "Throughput is the user data a group of actors moved, divided by the run time of the \
         group's slowest actor."
    };
}

macro_rules! percentile_rule {
    () => {
        "P50 is the latency that half of the batches didn't exceed. P99 and P99.9 are the same for \
         99 and 99.9 percent of the batches."
    };
}

macro_rules! noise_lead {
    () => {
        "Two runs of the same build never give the same numbers."
    };
}

pub const THROUGHPUT_RULE: &str = throughput_rule!();
pub const PERCENTILE_RULE: &str = percentile_rule!();
pub const NOISE_LEAD: &str = noise_lead!();

const METRICS: [TextCard; 3] = [
    TextCard {
        title: "Throughput",
        paragraphs: &[
            concat!(
                throughput_rule!(),
                " User data is the message payload, without headers. 1 MB is 1,000,000 bytes."
            ),
            "Older pages added up each actor's rate. That sum is higher when the actors finish at \
             different times, so older pages showed higher values for the same run.",
        ],
        command: None,
    },
    TextCard {
        title: "Latency percentiles",
        paragraphs: &[
            "The bench measures the latency of every batch. For each actor, it sorts these \
             latencies and reads the percentiles.",
            concat!(
                percentile_rule!(),
                " A group's value is the mean of its actors' percentiles."
            ),
        ],
        command: None,
    },
    TextCard {
        title: "Latency charts",
        paragraphs: &[
            "A latency chart shows the average latency of the batches in each sampling window, 10 \
             ms by default (--sampling-time). A group's line is also smoothed with a moving \
             average over 20 windows by default (--moving-average-window).",
            "A long run has more windows than a chart can draw. The dashboard then picks the \
             points that keep the line's shape and spikes.",
            "So a chart shows the level over time, and the percentiles show the slow batches. A \
             run without a rate limit only measures throughput, so the dashboard doesn't compare \
             its latency with any other run.",
        ],
        command: None,
    },
];

const RECORDS: [TextCard; 2] = [
    TextCard {
        title: "Save a run",
        paragraphs: &[
            "The bench writes each run to its own directory in performance_results. The directory \
             name ends with the start time in UTC. If that directory already exists, the bench \
             adds a suffix such as _2.",
            "--identifier sets the machine label. On EC2, it defaults to aws_ plus the instance \
             type with underscores instead of dots, for example aws_i4i_4xlarge. Elsewhere, it \
             defaults to the host name, or to unknown.",
        ],
        command: Some(SAVE_COMMAND),
    },
    TextCard {
        title: "Commands and options",
        paragraphs: &[
            "A new report has the command as you typed it, with the password hidden, and every \
             option that changes the result.",
            "Old reports have a command that the bench rebuilt from its settings, and no options. \
             For old runs, the dashboard uses the bench defaults from the day options first \
             appeared in reports.",
        ],
        command: None,
    },
];

/// The families in the order of the plain-language rules, each with its bench subcommands.
const FAMILIES: [(KindFamily, &str, [&str; 2]); 4] = [
    (
        KindFamily::Producer,
        "Producers send messages. Latency is the time from sending a batch until the server \
         confirms it.",
        ["pinned-producer", "balanced-producer"],
    ),
    (
        KindFamily::Consumer,
        "Consumers poll messages that an earlier producer run left on the server. Latency is \
         the time of one poll request.",
        ["pinned-consumer", "balanced-consumer-group"],
    ),
    (
        KindFamily::ProducerAndConsumer,
        "Producers and consumers run at the same time. For a consumer, latency is the age of the \
         first message in each polled batch, meaning the time since its producer sent it.",
        [
            "pinned-producer-and-consumer",
            "balanced-producer-and-consumer-group",
        ],
    ),
    (
        KindFamily::EndToEnd,
        "Each actor sends batches and polls them. Latency is the age of the first message in each \
         polled batch. In the group kind, an actor polls through a consumer group, so it can \
         receive batches that other actors sent.",
        [
            "end-to-end-producing-consumer",
            "end-to-end-producing-consumer-group",
        ],
    ),
];

/// Same-build reruns, measured on the dashboard's runs. Update it when new reruns come in.
const NOISE: &str = concat!(
    noise_lead!(),
    " Reruns of the 0.9.0 build on AWS i4i.4xlarge, from September to November 2026, moved \
     throughput by up to 1.4 percent, apart from runs with a badge. With a rate limit, P50 moved \
     by up to 1.9 times, P90, P95 and P99 by up to 1.6 times, and P99.9 by up to 5.4 times."
);

const DELTA_RULE: [&str; 2] = [
    "The larger median is above the smaller one by more than the metric's threshold. The threshold \
     applies to the ratio of the two, so it works the same way in both directions.",
    "The ranges don't overlap. A single run is a range of one value, and ranges that touch count \
     as overlapping.",
];

const PERCENT: f64 = 100.0;
/// Thresholds are typed in the configuration file, so four decimals keep every value exact.
const NUMBER_DECIMALS: usize = 4;
const FLAG_PREFIX: &str = "--";

#[function_component(Methodology)]
pub fn methodology() -> Html {
    let data = use_dashboard_data();
    let scroll_ref = use_node_ref();
    let [quick_start, configure, metrics, noise, results] = &SECTIONS;

    let content = html! {
        <>
            { page_head(
                breadcrumbs(&[Crumb::overview()], "Methodology"),
                Some("Methodology"),
                "How the benchmarks work",
                "Every run on this dashboard comes from iggy-bench, a tool that sends and polls \
                 messages on an Apache Iggy server and measures the result. This page shows how \
                 to run it, what the numbers mean, and when a difference is a real change.",
                Html::default(),
            ) }
            <SectionNav sections={&SECTIONS[..]} scroll_ref={scroll_ref.clone()} />

            <section id={quick_start.id} class="page-section page-container">
                { section_head(
                    quick_start.label,
                    "Run a first benchmark",
                    "You need a clone of the Apache Iggy repository and rustup. rustup installs \
                     the Rust version that the repository pins. Run each command from the \
                     repository root.",
                ) }
                <ol class="step-list">
                    { for STEPS.iter().map(|step| html! {
                        <li class="card step">{ card_body(step) }</li>
                    }) }
                </ol>
            </section>

            <section id={configure.id} class="page-section page-container">
                { section_head(
                    configure.label,
                    "Choose the workload",
                    "A command has a fixed order: global options, the kind, the kind's options, \
                     the transport with its options, and the optional output subcommand.",
                ) }
                <p class="section-text">
                    {"An actor is one producer or consumer that the bench runs. A batch is the \
                      group of messages that one request sends or polls. The kind sets which \
                      actors run."}
                </p>
                <div class="card-grid">
                    { for FAMILIES.iter().map(|(family, text, kinds)| html! {
                        <article class="card kind-card">
                            <h3>{ family.to_string() }</h3>
                            <p>{ *text }</p>
                            <p class="kind-list">
                                { for kinds.iter().map(|kind| html! { <code>{ *kind }</code> }) }
                            </p>
                        </article>
                    }) }
                </div>
                <p class="section-text">
                    { prose("Pinned kinds give each actor its own stream. Balanced kinds spread \
                             the actors over shared partitions, and their consumers read in \
                             consumer groups. With --high-level-api, a consumer measures the \
                             time to collect one batch instead.") }
                </p>
                <div class="card option-card">
                    <h3>{"Options and their defaults"}</h3>
                    <p>{ OPTIONS_NOTE }</p>
                    { option_table(&OPTIONS) }
                </div>
            </section>

            <section id={metrics.id} class="page-section page-container">
                { section_head(
                    metrics.label,
                    "What the numbers mean",
                    "The dashboard computes each value from a run's report. Old and new runs \
                     follow the same rules.",
                ) }
                <div class="card-grid">{ articles(&METRICS) }</div>
                <div class="card prose-card">
                    <h3>{"Badges"}</h3>
                    <p>
                        {"A badge warns that a run didn't get the load it asked for. Only runs \
                          with a rate limit get badges. The dashboard doesn't judge a difference \
                          as better or worse when one of the compared runs has a badge."}
                    </p>
                    { loadable(&data.config, "the badge rules", &data.reload, |config| {
                        badge_list(&config.thresholds)
                    }) }
                </div>
            </section>

            <section id={noise.id} class="page-section page-container">
                { section_head(
                    noise.label,
                    "When a difference is real",
                    NOISE,
                ) }
                <div class="card prose-card noise-card">
                    <div>
                        <h3>{"The delta rule"}</h3>
                        <p>
                            {"The dashboard compares two versions on one history line: one \
                              workload on one machine with one number of nodes. For each version, \
                              it takes the median of the runs and their range, from the lowest to \
                              the highest value. With an even number of runs, the median is the \
                              mean of the two middle values."}
                        </p>
                        <p>{"A difference is beyond run-to-run noise only if both conditions are \
                             true:"}</p>
                        <ol class="rule-list">
                            { for DELTA_RULE.iter().map(|condition| html! {
                                <li>{ *condition }</li>
                            }) }
                        </ol>
                        <p>
                            {"The dashboard marks every other difference as within run-to-run \
                              noise. A difference beyond the noise is better when throughput goes \
                              up or latency goes down, and worse in the other direction. If a run \
                              of either version has a badge, the dashboard shows the difference in \
                              gray and doesn't judge it. Latency isn't compared when a run has no \
                              rate limit."}
                        </p>
                    </div>
                    <div>
                        <h3>{"Thresholds"}</h3>
                        { loadable(&data.config, "the thresholds", &data.reload, |config| {
                            threshold_table(&config.thresholds)
                        }) }
                        <p class="card-note">
                            {"The dashboard server reads these thresholds from dashboard.toml."}
                        </p>
                    </div>
                </div>
            </section>

            <section id={results.id} class="page-section page-container">
                { section_head(
                    results.label,
                    "Where the runs come from",
                    "Add the output subcommand at the end of a command to save the run's report.",
                ) }
                <div class="card-grid">
                    { articles(&RECORDS) }
                    <article class="card">
                        <h3>{"Official runs"}</h3>
                        <p>
                            { prose("A run is official when its suite label names one of this \
                                     dashboard's suites. The output subcommand's --suite option \
                                     sets the label.") }
                        </p>
                        { loadable(&data.config, "the suites", &data.reload, |config| {
                            official_runs(config, &data.summaries, &data.reload)
                        }) }
                        <p>{"Explore lists every other run under All runs."}</p>
                    </article>
                </div>
            </section>
        </>
    };
    page_shell(&scroll_ref, content)
}

fn articles(cards: &[TextCard]) -> Html {
    cards
        .iter()
        .map(|card| html! { <article class="card">{ card_body(card) }</article> })
        .collect()
}

fn option_table(options: &[BenchOption]) -> Html {
    html! {
        <table class="option-table">
            <thead>
                <tr>
                    <th scope="col">{"Option"}</th>
                    <th scope="col">{"Default"}</th>
                    <th scope="col">{"What it does"}</th>
                </tr>
            </thead>
            <tbody>
                { for options.iter().map(|option| html! {
                    <tr>
                        <th scope="row">
                            <span class="option-name">{ option.name }</span>
                            <span class="option-flags">
                                { for option.flags.iter().map(|flag| html! {
                                    <code class="flag">{ *flag }</code>
                                }) }
                            </span>
                        </th>
                        <td class="option-default" data-label="Default">
                            { option.default.view() }
                        </td>
                        <td class="option-text"><p>{ prose(option.text) }</p></td>
                    </tr>
                }) }
            </tbody>
        </table>
    }
}

impl OptionDefault {
    fn view(&self) -> Html {
        match self {
            Self::Value(value) => html! { <code class="option-value">{ *value }</code> },
            Self::Unset(reason) => html! { <span class="option-unset">{ *reason }</span> },
        }
    }
}

fn card_body(card: &TextCard) -> Html {
    html! {
        <>
            <h3>{ card.title }</h3>
            { for card.paragraphs.iter().map(|paragraph| html! { <p>{ prose(paragraph) }</p> }) }
            if let Some(command) = card.command {
                <CommandBlock {command} />
            }
        </>
    }
}

/// Text with each command line flag in `code`, because in plain text a browser breaks lines at a
/// flag's hyphens.
fn prose(text: &str) -> Html {
    spans(text)
        .into_iter()
        .map(|span| match span {
            Span::Text(text) => html! { { text.to_owned() } },
            Span::Flag(flag) => html! { <code class="flag">{ flag.to_owned() }</code> },
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
enum Span<'a> {
    Text(&'a str),
    Flag(&'a str),
}

/// Splits `text` around flags such as `--rate-limit`. A flag ends at the first character that isn't
/// a letter, a digit or a hyphen.
fn spans(text: &str) -> Vec<Span<'_>> {
    let mut spans = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find(FLAG_PREFIX) {
        let end = rest[start + FLAG_PREFIX.len()..]
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '-'))
            .map_or(rest.len(), |len| start + FLAG_PREFIX.len() + len);
        if start > 0 {
            spans.push(Span::Text(&rest[..start]));
        }
        spans.push(Span::Flag(&rest[start..end]));
        rest = &rest[end..];
    }
    if !rest.is_empty() {
        spans.push(Span::Text(rest));
    }
    spans
}

fn badge_list(thresholds: &Thresholds) -> Html {
    html! {
        <dl class="badge-list">
            { for badge_rules(thresholds).into_iter().map(|(badge, rule)| html! {
                <div class="badge-rule">
                    <dt><span class="badge">{ badge.to_string() }</span></dt>
                    <dd>{ badge.explanation() }{" "}{ rule }</dd>
                </div>
            }) }
        </dl>
    }
}

fn threshold_table(thresholds: &Thresholds) -> Html {
    html! {
        <table class="threshold-table">
            <thead>
                <tr><th scope="col">{"Metric"}</th><th scope="col">{"A change needs"}</th></tr>
            </thead>
            <tbody>
                { for threshold_rows(thresholds).into_iter().map(|(metric, pct)| html! {
                    <tr><th scope="row">{ metric }</th><td>{ change_needed(pct) }</td></tr>
                }) }
            </tbody>
        </table>
    }
}

fn official_runs(
    config: &DashboardConfig,
    summaries: &Loadable<Vec<BenchmarkSummary>>,
    reload: &Callback<()>,
) -> Html {
    let counts = match summaries {
        Loadable::Ready(summaries) => {
            let labels: Vec<_> = summaries.iter().map(|run| run.suite.as_deref()).collect();
            Some(RunCounts::new(config, &labels))
        }
        Loadable::Loading | Loadable::Failed(_) => None,
    };
    let total = match &counts {
        Some(counts) => html! {
            <p>
                { format!(
                    "The dashboard has {}, and {} of them are official.",
                    runs_text(counts.total),
                    counts.official
                ) }
            </p>
        },
        None => loadable(summaries, "the run list", reload, |_| Html::default()),
    };
    html! {
        <>
            <ul class="suite-list">
                { for config.suites.iter().enumerate().map(|(index, suite)| {
                    let runs = counts
                        .as_ref()
                        .and_then(|counts| counts.per_suite.get(index).copied());
                    html! {
                        <li>
                            <span>{ suite.title.clone() }</span>
                            if let Some(runs) = runs {
                                <span class="suite-runs">{ runs_text(runs) }</span>
                            }
                        </li>
                    }
                }) }
            </ul>
            { total }
        </>
    }
}

/// The run count per suite, in configuration order, plus the official and overall totals.
#[derive(Debug, PartialEq, Eq)]
struct RunCounts {
    per_suite: Vec<usize>,
    official: usize,
    total: usize,
}

impl RunCounts {
    fn new(config: &DashboardConfig, labels: &[Option<&str>]) -> Self {
        let per_suite = config
            .suites
            .iter()
            .map(|suite| {
                labels
                    .iter()
                    .filter(|label| **label == Some(suite.name.as_str()))
                    .count()
            })
            .collect();
        let official = labels
            .iter()
            .filter(|label| config.is_official(**label))
            .count();
        Self {
            per_suite,
            official,
            total: labels.len(),
        }
    }
}

fn badge_rules(thresholds: &Thresholds) -> [(Badge, String); 2] {
    [
        (
            Badge::UnevenLoad,
            format!(
                "The dashboard adds it when a group's slowest actor ran longer than its fastest \
                 one by more than {} percent of the mean run time.",
                plain_number(thresholds.max_time_spread_pct)
            ),
        ),
        (
            Badge::BelowTargetRate,
            format!(
                "The dashboard adds it when the target rate is more than {} percent above the \
                 throughput the run reached. The target rate is the share of the rate limit that \
                 goes to the limited actors: the producers, the consumers of a consumer run, or \
                 the actors of an end-to-end run.",
                plain_number(thresholds.throughput_pct)
            ),
        ),
    ]
}

fn threshold_rows(thresholds: &Thresholds) -> [(&'static str, f64); 6] {
    [
        ("Throughput", thresholds.throughput_pct),
        ("P50", thresholds.p50_pct),
        ("P90", thresholds.p90_pct),
        ("P95", thresholds.p95_pct),
        ("P99", thresholds.p99_pct),
        ("P99.9", thresholds.p999_pct),
    ]
}

/// A threshold applies to the ratio of the larger value to the smaller one.
fn change_needed(pct: f64) -> String {
    format!(
        "more than {} percent ({} times)",
        plain_number(pct),
        plain_number(1.0 + pct / PERCENT)
    )
}

fn plain_number(value: f64) -> String {
    format!("{value:.NUMBER_DECIMALS$}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bench_dashboard_shared::config::SuiteConfig;

    #[test]
    fn given_flags_in_text_when_split_should_keep_punctuation_outside_the_flags() {
        assert_eq!(
            spans("consumers get 5 percent more (--read-amplification), so they keep up"),
            [
                Span::Text("consumers get 5 percent more ("),
                Span::Flag("--read-amplification"),
                Span::Text("), so they keep up"),
            ]
        );
    }

    #[test]
    fn given_flag_at_either_end_when_split_should_drop_empty_text() {
        assert_eq!(
            spans("--total-data sets the size. See --message-batches"),
            [
                Span::Flag("--total-data"),
                Span::Text(" sets the size. See "),
                Span::Flag("--message-batches"),
            ]
        );
    }

    #[test]
    fn given_text_without_flags_when_split_should_keep_it_whole() {
        assert_eq!(
            spans("1 MB is 1,000,000 bytes."),
            [Span::Text("1 MB is 1,000,000 bytes.")]
        );
        assert!(spans("").is_empty());
    }

    const SINGLE: &str = "i4i-single";
    const CLUSTER: &str = "i4i-cluster3";

    const THRESHOLDS: Thresholds = Thresholds {
        throughput_pct: 3.0,
        p50_pct: 60.0,
        p90_pct: 55.0,
        p95_pct: 50.0,
        p99_pct: 45.0,
        p999_pct: 100.0,
        max_time_spread_pct: 10.0,
    };

    fn config() -> DashboardConfig {
        DashboardConfig {
            suites: [SINGLE, CLUSTER]
                .into_iter()
                .map(|name| SuiteConfig {
                    name: name.to_owned(),
                    title: format!("{name} title"),
                })
                .collect(),
            headlines: Vec::new(),
            thresholds: THRESHOLDS,
        }
    }

    #[test]
    fn given_thresholds_when_listed_should_keep_metric_order_and_values() {
        assert_eq!(
            threshold_rows(&THRESHOLDS),
            [
                ("Throughput", 3.0),
                ("P50", 60.0),
                ("P90", 55.0),
                ("P95", 50.0),
                ("P99", 45.0),
                ("P99.9", 100.0),
            ]
        );
    }

    #[test]
    fn given_percent_threshold_when_described_should_show_ratio() {
        assert_eq!(change_needed(60.0), "more than 60 percent (1.6 times)");
        assert_eq!(change_needed(3.0), "more than 3 percent (1.03 times)");
        assert_eq!(change_needed(100.0), "more than 100 percent (2 times)");
        assert_eq!(change_needed(2.5), "more than 2.5 percent (1.025 times)");
    }

    #[test]
    fn given_thresholds_when_badge_rules_built_should_quote_their_own_threshold() {
        let thresholds = Thresholds {
            throughput_pct: 7.0,
            max_time_spread_pct: 12.5,
            ..THRESHOLDS
        };

        let [(uneven, uneven_rule), (below, below_rule)] = badge_rules(&thresholds);

        assert_eq!(uneven, Badge::UnevenLoad);
        assert!(uneven_rule.contains("more than 12.5 percent of the mean run time"));
        assert_eq!(below, Badge::BelowTargetRate);
        assert!(below_rule.contains("more than 7 percent above the throughput"));
    }

    #[test]
    fn given_labels_when_counted_should_split_runs_by_suite() {
        let labels = [
            Some(SINGLE),
            Some(CLUSTER),
            Some(SINGLE),
            Some("laptop"),
            None,
        ];

        let counts = RunCounts::new(&config(), &labels);

        assert_eq!(
            counts,
            RunCounts {
                per_suite: vec![2, 1],
                official: 3,
                total: 5,
            }
        );
    }

    #[test]
    fn given_families_when_listed_should_follow_plain_language_order() {
        let families: Vec<_> = FAMILIES.iter().map(|(family, _, _)| *family).collect();

        assert_eq!(
            families,
            [
                KindFamily::Producer,
                KindFamily::Consumer,
                KindFamily::ProducerAndConsumer,
                KindFamily::EndToEnd,
            ]
        );
    }
}
