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

//! Run labels that more than one page shows: the parts of a conditions line, the machine name, the
//! kind family mark, the version tag, and the number and date texts. They're defined once, so a run
//! reads the same on every page.

use bench_dashboard_shared::identity::{DataVolumeKey, KindFamily};
use bench_dashboard_shared::metrics::{BYTES_PER_MEGABYTE, rate_limit_bytes};
use bench_dashboard_shared::summary::BenchmarkSummary;
use bench_report::numeric_parameter::BenchmarkNumericParameter;
use bench_report::options::BenchmarkDurability;
use bench_report::params::BenchmarkDataVolume;
use chrono::{DateTime, SecondsFormat};
use std::fmt;
use uuid::Uuid;
use yew::prelude::*;

use crate::format::{format_bytes, format_count, format_ms, format_throughput_mb_s, group_digits};

pub const CONDITION_SEPARATOR: &str = " · ";
pub const RELEASE_LABEL: &str = "release";
pub const END_TO_END_THROUGHPUT_NOTE: &str = "An end-to-end actor sends and polls, so its \
                                              throughput counts both sent and received bytes, just \
                                              like its rate limit.";
pub const NO_RATE_LIMIT_NOTE: &str = "No rate limit, so this run only measures throughput.";
/// For example `Nov 4, 2026`.
const DAY_FORMAT: &str = "%b %-d, %Y";
/// The first group of a uuid, which names a run in a breadcrumb, a legend or a column.
const SHORT_ID_LENGTH: usize = 8;
/// On EC2, the bench labels a machine `aws_<family>_<size>`, the instance type with `_` instead of
/// the dot.
const EC2_LABEL_PREFIX: &str = "aws_";
const EC2_LABEL_SEPARATOR: char = '_';
const EC2_NAME_PREFIX: &str = "AWS";
const EC2_TYPE_SEPARATOR: char = '.';

/// A day the way the pages write it, plus the exact time for a `<time>` element.
#[derive(Debug, PartialEq, Eq)]
pub struct DayLabel {
    /// For example `Nov 4, 2026`.
    pub day: String,
    /// RFC 3339 in UTC.
    pub iso: String,
}

impl DayLabel {
    pub fn from_unix_micros(micros: i64) -> Option<Self> {
        let moment = DateTime::from_timestamp_micros(micros)?;
        Some(Self {
            day: moment.format(DAY_FORMAT).to_string(),
            iso: moment.to_rfc3339_opts(SecondsFormat::Secs, true),
        })
    }

    pub fn view(&self) -> Html {
        html! {
            <time datetime={self.iso.clone()} title={self.iso.clone()}>{ self.day.as_str() }</time>
        }
    }
}

/// Topic options only take effect in a run that creates the topics, so a run with `reuse_streams`,
/// or a consumer-only run, has no topic durability.
pub fn topic_durability(summary: &BenchmarkSummary) -> Option<BenchmarkDurability> {
    let key = &summary.workload_key;
    (summary.kind_family != KindFamily::Consumer && !key.client.reuse_streams)
        .then_some(key.topic.durability)
}

pub fn message_size_text(size: BenchmarkNumericParameter) -> String {
    format!("{} B messages", numeric_text(size))
}

pub fn batch_size_text(messages_per_batch: BenchmarkNumericParameter) -> String {
    format!("{} per batch", numeric_text(messages_per_batch))
}

pub fn rate_limit_text(rate_limit: Option<&str>) -> String {
    rate_limit.map_or_else(
        || "no rate limit".to_owned(),
        |rate_limit| format!("rate limit {}", rate_text(rate_limit)),
    )
}

/// A rate limit in throughput units, for example `800 MB/s`.
pub fn rate_text(rate_limit: &str) -> String {
    rate_limit_bytes(rate_limit).map_or_else(
        || rate_limit.to_owned(),
        |bytes| format_throughput_mb_s(bytes as f64 / BYTES_PER_MEGABYTE),
    )
}

pub fn data_volume_text(volume: DataVolumeKey) -> String {
    match volume {
        DataVolumeKey::Configured(BenchmarkDataVolume::MessageBatchesPerActor(batches)) => {
            format!("{} per actor", batches_text(batches.into()))
        }
        DataVolumeKey::Configured(BenchmarkDataVolume::TotalDataBytes(bytes)) => {
            format!("{} in total", format_bytes(bytes))
        }
        DataVolumeKey::CompletedBatches(batches) => format!("{} in total", batches_text(batches)),
    }
}

pub const fn durability_text(durability: BenchmarkDurability) -> &'static str {
    match durability {
        BenchmarkDurability::Replicated => "replicated",
        BenchmarkDurability::Persisted => "persisted",
    }
}

pub fn numeric_text(value: BenchmarkNumericParameter) -> String {
    match value {
        BenchmarkNumericParameter::Value(value) => group_digits(value.into()),
        BenchmarkNumericParameter::Range { min, max } => format!(
            "{} to {}",
            group_digits(min.into()),
            group_digits(max.into())
        ),
    }
}

/// A machine label as a reader sees it: `aws_i4i_4xlarge` reads `AWS i4i.4xlarge`. Only a label
/// with exactly one `_` after `aws_` converts, so the name maps back to one label. Every other
/// label, host names included, shows as it is. Filters, URLs and history lines keep the label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MachineName<'a>(pub &'a str);

impl fmt::Display for MachineName<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let instance_type = self
            .0
            .strip_prefix(EC2_LABEL_PREFIX)
            .and_then(|instance_type| instance_type.split_once(EC2_LABEL_SEPARATOR))
            .filter(|(family, size)| {
                !family.is_empty() && !size.is_empty() && !size.contains(EC2_LABEL_SEPARATOR)
            });
        match instance_type {
            Some((family, size)) => {
                write!(
                    formatter,
                    "{EC2_NAME_PREFIX} {family}{EC2_TYPE_SEPARATOR}{size}"
                )
            }
            None => formatter.write_str(self.0),
        }
    }
}

pub fn nodes_text(nodes: usize) -> String {
    match nodes {
        1 => "1 node".to_owned(),
        _ => format!("{nodes} nodes"),
    }
}

pub fn milliseconds(value: f64) -> String {
    format!("{} ms", format_ms(value))
}

pub fn messages_text(per_second: f64) -> String {
    format!("{} msg/s", format_count(per_second as u64))
}

/// For example `3f2a9c1b`.
pub fn short_id(uuid: &Uuid) -> String {
    uuid.to_string().chars().take(SHORT_ID_LENGTH).collect()
}

/// A run among other runs: its short id and version, for example `3f2a9c1b · 0.9.0`.
pub fn run_name(summary: &BenchmarkSummary) -> String {
    format!(
        "{}{CONDITION_SEPARATOR}{}",
        short_id(&summary.uuid),
        version_tag(summary)
    )
}

/// A build's display text names its commit, which the pages show separately from the version.
pub fn version_tag(summary: &BenchmarkSummary) -> String {
    match (&summary.commit, &summary.version_order.version) {
        (Some(_), Some(version)) => version.to_string(),
        _ => summary.version.clone(),
    }
}

/// The version tag, styled as a release or as a build.
pub fn version_tag_view(tag: &str, release: bool) -> Html {
    let look = if release {
        "version-release"
    } else {
        "version-build"
    };
    html! { <span class={classes!("version-tag", look)}>{ tag.to_owned() }</span> }
}

/// A letter and a color per kind family. The name is the label, so the mark reads the same to
/// a screen reader.
pub fn family_mark(family: KindFamily) -> Html {
    let (letters, class) = match family {
        KindFamily::Producer => ("P", "family-producer"),
        KindFamily::Consumer => ("C", "family-consumer"),
        KindFamily::ProducerAndConsumer => ("PC", "family-mixed"),
        KindFamily::EndToEnd => ("E", "family-end-to-end"),
    };
    let name = family.to_string();
    html! {
        <span class={classes!("family-mark", class)} role="img" aria-label={name.clone()} title={name}>
            { letters }
        </span>
    }
}

fn batches_text(batches: u64) -> String {
    match batches {
        1 => "1 batch".to_owned(),
        _ => format!("{} batches", group_digits(batches)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_ec2_label_when_shown_should_name_instance_type() {
        assert_eq!(
            MachineName("aws_i4i_4xlarge").to_string(),
            "AWS i4i.4xlarge"
        );
        assert_eq!(
            MachineName("aws_c7i-flex_large").to_string(),
            "AWS c7i-flex.large"
        );
    }

    #[test]
    fn given_ec2_label_with_dash_in_size_when_shown_should_keep_dash() {
        assert_eq!(
            MachineName("aws_m7i_metal-24xl").to_string(),
            "AWS m7i.metal-24xl"
        );
    }

    #[test]
    fn given_label_not_of_ec2_form_when_shown_should_stay_raw() {
        for label in [
            "atlas",
            "ip-10-0-0-5",
            "aws_i4i",
            "aws_i4i_4x_large",
            "aws__4xlarge",
            "aws_i4i_",
            "AWS_i4i_4xlarge",
            "lab_aws_i4i_4xlarge",
        ] {
            assert_eq!(MachineName(label).to_string(), label);
        }
    }
}
