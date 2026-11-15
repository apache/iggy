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

//! The result file that OMB writes at the end of a run: the serde model of its `TestResult`.

use std::collections::{BTreeMap, HashMap};

use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use serde_json::error::Category;
use thiserror::Error;

/// The fields that make a JSON file an OMB result. They have no default in [`OmbResult`].
const REQUIRED_FIELDS: [&str; 8] = [
    "workload",
    "driver",
    "messageSize",
    "topics",
    "partitions",
    "producersPerTopic",
    "consumersPerTopic",
    "publishRate",
];

/// One OMB result file, with the field names and types of `TestResult` in the OMB framework. The
/// lists hold one value per window of [`crate::omb::OMB_WINDOW_SECONDS`], for the test phase only:
/// OMB resets its statistics after the warm-up. Latencies are in milliseconds, except the publish
/// delay, which is in microseconds. End-to-end latencies are whole milliseconds.
///
/// The workload fields and `publishRate` are required. Every other field defaults to empty or zero,
/// because older OMB versions don't write `publishErrorRate` or the `publishDelay*` fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OmbResult {
    /// The `name` from the workload file, free text.
    pub workload: String,
    /// The `name` from the driver file, free text.
    pub driver: String,
    /// In bytes. For a workload with a message size distribution, the weighted average.
    pub message_size: u64,
    pub topics: u32,
    /// Partitions per topic.
    pub partitions: u32,
    pub producers_per_topic: u32,
    /// Consumers per subscription of a topic.
    pub consumers_per_topic: u32,

    /// Messages per second.
    pub publish_rate: Vec<f64>,
    /// Failed sends per second.
    #[serde(default)]
    pub publish_error_rate: Vec<f64>,
    /// Messages per second, over all subscriptions.
    #[serde(default)]
    pub consume_rate: Vec<f64>,
    /// Messages sent but not yet received, over all subscriptions.
    #[serde(default)]
    pub backlog: Vec<i64>,

    #[serde(default)]
    pub publish_latency_avg: Vec<f64>,
    #[serde(default)]
    pub publish_latency_50pct: Vec<f64>,
    #[serde(default)]
    pub publish_latency_75pct: Vec<f64>,
    #[serde(default)]
    pub publish_latency_95pct: Vec<f64>,
    #[serde(default)]
    pub publish_latency_99pct: Vec<f64>,
    #[serde(default)]
    pub publish_latency_999pct: Vec<f64>,
    #[serde(default)]
    pub publish_latency_9999pct: Vec<f64>,
    #[serde(default)]
    pub publish_latency_max: Vec<f64>,

    /// The time from the scheduled send to the real send, in microseconds.
    #[serde(default)]
    pub publish_delay_latency_avg: Vec<f64>,
    #[serde(default)]
    pub publish_delay_latency_50pct: Vec<i64>,
    #[serde(default)]
    pub publish_delay_latency_75pct: Vec<i64>,
    #[serde(default)]
    pub publish_delay_latency_95pct: Vec<i64>,
    #[serde(default)]
    pub publish_delay_latency_99pct: Vec<i64>,
    #[serde(default)]
    pub publish_delay_latency_999pct: Vec<i64>,
    #[serde(default)]
    pub publish_delay_latency_9999pct: Vec<i64>,
    #[serde(default)]
    pub publish_delay_latency_max: Vec<i64>,

    #[serde(default)]
    pub aggregated_publish_latency_avg: f64,
    #[serde(default)]
    pub aggregated_publish_latency_50pct: f64,
    #[serde(default)]
    pub aggregated_publish_latency_75pct: f64,
    #[serde(default)]
    pub aggregated_publish_latency_95pct: f64,
    #[serde(default)]
    pub aggregated_publish_latency_99pct: f64,
    #[serde(default)]
    pub aggregated_publish_latency_999pct: f64,
    #[serde(default)]
    pub aggregated_publish_latency_9999pct: f64,
    #[serde(default)]
    pub aggregated_publish_latency_max: f64,

    #[serde(default)]
    pub aggregated_publish_delay_latency_avg: f64,
    #[serde(default)]
    pub aggregated_publish_delay_latency_50pct: i64,
    #[serde(default)]
    pub aggregated_publish_delay_latency_75pct: i64,
    #[serde(default)]
    pub aggregated_publish_delay_latency_95pct: i64,
    #[serde(default)]
    pub aggregated_publish_delay_latency_99pct: i64,
    #[serde(default)]
    pub aggregated_publish_delay_latency_999pct: i64,
    #[serde(default)]
    pub aggregated_publish_delay_latency_9999pct: i64,
    #[serde(default)]
    pub aggregated_publish_delay_latency_max: i64,

    /// Percentile in percent, as JSON text such as `3.6778343616680805E-6`, to the latency.
    #[serde(default)]
    pub aggregated_publish_latency_quantiles: BTreeMap<String, f64>,
    #[serde(default)]
    pub aggregated_publish_delay_latency_quantiles: BTreeMap<String, i64>,

    #[serde(default)]
    pub end_to_end_latency_avg: Vec<f64>,
    #[serde(default)]
    pub end_to_end_latency_50pct: Vec<f64>,
    #[serde(default)]
    pub end_to_end_latency_75pct: Vec<f64>,
    #[serde(default)]
    pub end_to_end_latency_95pct: Vec<f64>,
    #[serde(default)]
    pub end_to_end_latency_99pct: Vec<f64>,
    #[serde(default)]
    pub end_to_end_latency_999pct: Vec<f64>,
    #[serde(default)]
    pub end_to_end_latency_9999pct: Vec<f64>,
    #[serde(default)]
    pub end_to_end_latency_max: Vec<f64>,

    #[serde(default)]
    pub aggregated_end_to_end_latency_quantiles: BTreeMap<String, f64>,

    #[serde(default)]
    pub aggregated_end_to_end_latency_avg: f64,
    #[serde(default)]
    pub aggregated_end_to_end_latency_50pct: f64,
    #[serde(default)]
    pub aggregated_end_to_end_latency_75pct: f64,
    #[serde(default)]
    pub aggregated_end_to_end_latency_95pct: f64,
    #[serde(default)]
    pub aggregated_end_to_end_latency_99pct: f64,
    #[serde(default)]
    pub aggregated_end_to_end_latency_999pct: f64,
    #[serde(default)]
    pub aggregated_end_to_end_latency_9999pct: f64,
    #[serde(default)]
    pub aggregated_end_to_end_latency_max: f64,
}

/// What a JSON file holds, told apart without the full parse. A loader skips other JSON quietly,
/// and warns about a result that doesn't parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OmbJsonKind {
    /// A JSON object with every required field of an OMB result. The full parse can still fail,
    /// for example on a field of the wrong type.
    Result,
    /// Valid JSON of another shape, so not an OMB result.
    Other,
    /// Not valid JSON, for example a file that was cut short.
    Broken,
}

/// The percentiles of one merged histogram over the whole test phase.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OmbPercentiles {
    pub avg: f64,
    pub p50: f64,
    pub p75: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub p9999: f64,
    pub max: f64,
}

/// One point of a quantile curve.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OmbQuantile {
    /// In percent, from 0 to 100.
    pub percentile: f64,
    pub value: f64,
}

#[derive(Debug, Error)]
#[error("the OMB result doesn't parse: {0}")]
pub struct OmbResultError(#[from] serde_json::Error);

impl OmbResult {
    /// The aggregated publish latency, in milliseconds.
    pub const fn publish_latency(&self) -> OmbPercentiles {
        OmbPercentiles {
            avg: self.aggregated_publish_latency_avg,
            p50: self.aggregated_publish_latency_50pct,
            p75: self.aggregated_publish_latency_75pct,
            p95: self.aggregated_publish_latency_95pct,
            p99: self.aggregated_publish_latency_99pct,
            p999: self.aggregated_publish_latency_999pct,
            p9999: self.aggregated_publish_latency_9999pct,
            max: self.aggregated_publish_latency_max,
        }
    }

    /// The aggregated end-to-end latency, in milliseconds. `None` if the run didn't measure it.
    pub fn end_to_end_latency(&self) -> Option<OmbPercentiles> {
        self.has_end_to_end_latency().then_some(OmbPercentiles {
            avg: self.aggregated_end_to_end_latency_avg,
            p50: self.aggregated_end_to_end_latency_50pct,
            p75: self.aggregated_end_to_end_latency_75pct,
            p95: self.aggregated_end_to_end_latency_95pct,
            p99: self.aggregated_end_to_end_latency_99pct,
            p999: self.aggregated_end_to_end_latency_999pct,
            p9999: self.aggregated_end_to_end_latency_9999pct,
            max: self.aggregated_end_to_end_latency_max,
        })
    }

    /// The aggregated publish delay, in microseconds. `None` if the file doesn't record it.
    pub fn publish_delay_latency(&self) -> Option<OmbPercentiles> {
        self.has_publish_delay_latency().then_some(OmbPercentiles {
            avg: self.aggregated_publish_delay_latency_avg,
            p50: self.aggregated_publish_delay_latency_50pct as f64,
            p75: self.aggregated_publish_delay_latency_75pct as f64,
            p95: self.aggregated_publish_delay_latency_95pct as f64,
            p99: self.aggregated_publish_delay_latency_99pct as f64,
            p999: self.aggregated_publish_delay_latency_999pct as f64,
            p9999: self.aggregated_publish_delay_latency_9999pct as f64,
            max: self.aggregated_publish_delay_latency_max as f64,
        })
    }

    /// Whether the run measured end-to-end latency: it has consumers, and the merged histogram
    /// has points and a max above 0. Without consumers, OMB writes zeros and an empty quantile
    /// map, which must read as "not measured", not as 0 ms.
    pub fn has_end_to_end_latency(&self) -> bool {
        self.consumers_per_topic > 0
            && !self.aggregated_end_to_end_latency_quantiles.is_empty()
            && self.aggregated_end_to_end_latency_max > 0.0
    }

    /// Whether the file records the publish delay. Older OMB versions don't write the
    /// `publishDelay*` fields.
    pub fn has_publish_delay_latency(&self) -> bool {
        !self.publish_delay_latency_avg.is_empty()
            || !self.aggregated_publish_delay_latency_quantiles.is_empty()
    }

    /// The publish latency curve, in milliseconds, sorted by percentile.
    pub fn publish_latency_quantiles(&self) -> Vec<OmbQuantile> {
        quantile_curve(&self.aggregated_publish_latency_quantiles, |value| value)
    }

    /// The end-to-end latency curve, in milliseconds, sorted by percentile. Empty without
    /// consumers.
    pub fn end_to_end_latency_quantiles(&self) -> Vec<OmbQuantile> {
        quantile_curve(&self.aggregated_end_to_end_latency_quantiles, |value| value)
    }

    /// The publish delay curve, in microseconds, sorted by percentile.
    pub fn publish_delay_latency_quantiles(&self) -> Vec<OmbQuantile> {
        quantile_curve(&self.aggregated_publish_delay_latency_quantiles, |value| {
            value as f64
        })
    }
}

impl TryFrom<&[u8]> for OmbResult {
    type Error = OmbResultError;

    fn try_from(json: &[u8]) -> Result<Self, Self::Error> {
        Ok(serde_json::from_slice(json)?)
    }
}

impl OmbJsonKind {
    /// Reads the JSON once and skips every value, so it doesn't build the lists and maps.
    pub fn of(json: &[u8]) -> Self {
        match serde_json::from_slice::<HashMap<String, IgnoredAny>>(json) {
            Ok(fields)
                if REQUIRED_FIELDS
                    .iter()
                    .all(|field| fields.contains_key(*field)) =>
            {
                Self::Result
            }
            Ok(_) => Self::Other,
            Err(error) => match error.classify() {
                Category::Data => Self::Other,
                Category::Syntax | Category::Eof | Category::Io => Self::Broken,
            },
        }
    }
}

/// Keys that don't parse as a finite number are left out.
fn quantile_curve<V: Copy>(map: &BTreeMap<String, V>, value: fn(V) -> f64) -> Vec<OmbQuantile> {
    let mut curve: Vec<_> = map
        .iter()
        .filter_map(|(percentile, &point)| {
            let percentile = percentile
                .parse::<f64>()
                .ok()
                .filter(|number| number.is_finite())?;
            Some(OmbQuantile {
                percentile,
                value: value(point),
            })
        })
        .collect();
    curve.sort_by(|left, right| left.percentile.total_cmp(&right.percentile));
    curve
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::omb::test_fixtures::{
        ALL, EDGE8_100_PARTITIONS, NO_CONSUMERS, PUBLISH_ERRORS, RC1_DEFAULT, assert_round_trip,
        without_fields,
    };
    use crate::test_fixtures::PHASE_1A_RUN;

    const OPTIONAL_FIELDS_OF_OLD_FILES: [&str; 18] = [
        "publishErrorRate",
        "publishDelayLatencyAvg",
        "publishDelayLatency50pct",
        "publishDelayLatency75pct",
        "publishDelayLatency95pct",
        "publishDelayLatency99pct",
        "publishDelayLatency999pct",
        "publishDelayLatency9999pct",
        "publishDelayLatencyMax",
        "aggregatedPublishDelayLatencyAvg",
        "aggregatedPublishDelayLatency50pct",
        "aggregatedPublishDelayLatency75pct",
        "aggregatedPublishDelayLatency95pct",
        "aggregatedPublishDelayLatency99pct",
        "aggregatedPublishDelayLatency999pct",
        "aggregatedPublishDelayLatency9999pct",
        "aggregatedPublishDelayLatencyMax",
        "aggregatedPublishDelayLatencyQuantiles",
    ];

    #[test]
    fn given_real_result_when_parsed_should_read_every_field() {
        let result = RC1_DEFAULT.result();

        assert_eq!(
            result.workload,
            "12 topics x 1 partition / 1p 1c per topic / 1000 B / 150 MB/s"
        );
        assert_eq!(result.driver, "Iggy");
        assert_eq!(result.message_size, 1000);
        assert_eq!(
            (
                result.topics,
                result.partitions,
                result.producers_per_topic,
                result.consumers_per_topic
            ),
            (12, 1, 1, 1)
        );
        for (name, windows) in [
            ("publishRate", result.publish_rate.len()),
            ("publishErrorRate", result.publish_error_rate.len()),
            ("consumeRate", result.consume_rate.len()),
            ("backlog", result.backlog.len()),
            ("publishLatency99pct", result.publish_latency_99pct.len()),
            (
                "publishDelayLatencyMax",
                result.publish_delay_latency_max.len(),
            ),
            (
                "endToEndLatency999pct",
                result.end_to_end_latency_999pct.len(),
            ),
        ] {
            assert_eq!(windows, 18, "{name}");
        }
        assert_eq!(result.publish_rate[0], 153_629.551_518_091_52);
        assert_eq!(result.aggregated_publish_latency_99pct, 1.523);
        assert_eq!(result.aggregated_publish_delay_latency_99pct, 70);
        assert_eq!(result.aggregated_end_to_end_latency_99pct, 2.0);
        assert_eq!(result.aggregated_publish_latency_quantiles.len(), 1661);
        assert_eq!(result.aggregated_end_to_end_latency_quantiles.len(), 23);
        assert_eq!(result.aggregated_publish_delay_latency_quantiles.len(), 735);
    }

    #[test]
    fn given_every_fixture_when_parsed_should_have_one_value_per_window_in_each_list() {
        for fixture in ALL {
            let result = fixture.result();
            let windows = result.publish_rate.len();

            for len in [
                result.publish_error_rate.len(),
                result.consume_rate.len(),
                result.backlog.len(),
                result.publish_latency_avg.len(),
                result.publish_latency_max.len(),
                result.publish_delay_latency_avg.len(),
                result.publish_delay_latency_9999pct.len(),
                result.end_to_end_latency_50pct.len(),
                result.end_to_end_latency_max.len(),
            ] {
                assert_eq!(len, windows, "{}", fixture.file_name);
            }
        }
    }

    #[test]
    fn given_result_without_error_and_delay_fields_when_parsed_should_default_them() {
        let old = without_fields(RC1_DEFAULT.json, &OPTIONAL_FIELDS_OF_OLD_FILES);

        let result = OmbResult::try_from(old.as_slice()).expect("old files parse");

        assert_eq!(result.publish_rate.len(), 18);
        assert_eq!(result.publish_error_rate, Vec::<f64>::new());
        assert_eq!(result.publish_delay_latency_99pct, Vec::<i64>::new());
        assert_eq!(result.aggregated_publish_delay_latency_99pct, 0);
        assert!(!result.has_publish_delay_latency());
        assert_eq!(result.publish_delay_latency(), None);
        assert_eq!(result.publish_delay_latency_quantiles(), []);
        assert_eq!(result.aggregated_publish_latency_99pct, 1.523);
    }

    #[test]
    fn given_result_without_a_required_field_when_checked_should_not_be_a_result() {
        for field in REQUIRED_FIELDS {
            let json = without_fields(RC1_DEFAULT.json, &[field]);

            assert_eq!(OmbJsonKind::of(&json), OmbJsonKind::Other, "{field}");
            assert!(OmbResult::try_from(json.as_slice()).is_err(), "{field}");
        }
    }

    #[test]
    fn given_files_of_each_kind_when_checked_should_tell_result_other_json_and_broken_apart() {
        let cut_short = &RC1_DEFAULT.json[..RC1_DEFAULT.json.len() / 2];
        let trailing_text = [RC1_DEFAULT.json, b"}".as_slice()].concat();

        for fixture in ALL {
            assert_eq!(
                OmbJsonKind::of(fixture.json),
                OmbJsonKind::Result,
                "{}",
                fixture.file_name
            );
        }
        assert_eq!(OmbJsonKind::of(PHASE_1A_RUN.as_bytes()), OmbJsonKind::Other);
        assert_eq!(OmbJsonKind::of(b"[1, 2, 3]"), OmbJsonKind::Other);
        assert_eq!(OmbJsonKind::of(b"{}"), OmbJsonKind::Other);
        assert_eq!(OmbJsonKind::of(cut_short), OmbJsonKind::Broken);
        assert_eq!(OmbJsonKind::of(&trailing_text), OmbJsonKind::Broken);
        assert_eq!(OmbJsonKind::of(b""), OmbJsonKind::Broken);
        assert_eq!(OmbJsonKind::of(b"not json"), OmbJsonKind::Broken);
    }

    #[test]
    fn given_result_with_wrong_field_type_when_parsed_should_fail_but_look_like_result() {
        let mut json: serde_json::Value =
            serde_json::from_slice(RC1_DEFAULT.json).expect("fixtures are JSON");
        json["topics"] = "twelve".into();
        let json = serde_json::to_vec(&json).expect("JSON serializes");

        assert_eq!(OmbJsonKind::of(&json), OmbJsonKind::Result);
        let error = OmbResult::try_from(json.as_slice()).expect_err("topics is a number");
        assert!(error.to_string().contains("invalid type"), "{error}");
    }

    #[test]
    fn given_quantile_map_when_read_should_parse_keys_sort_by_percentile_and_skip_bad_keys() {
        let mut result = RC1_DEFAULT.result();
        result.aggregated_publish_latency_quantiles = BTreeMap::from([
            ("100.0".to_owned(), 23.339),
            ("3.679712794000773E-6".to_owned(), 0.29),
            ("9.5".to_owned(), 0.5),
            ("50.0".to_owned(), 0.947),
            ("NaN".to_owned(), 1.0),
            ("Infinity".to_owned(), 2.0),
            ("fifty".to_owned(), 3.0),
        ]);

        assert_eq!(
            result.publish_latency_quantiles(),
            [
                OmbQuantile {
                    percentile: 3.679_712_794_000_773e-6,
                    value: 0.29
                },
                OmbQuantile {
                    percentile: 9.5,
                    value: 0.5
                },
                OmbQuantile {
                    percentile: 50.0,
                    value: 0.947
                },
                OmbQuantile {
                    percentile: 100.0,
                    value: 23.339
                },
            ]
        );
    }

    #[test]
    fn given_real_quantile_maps_when_read_should_keep_every_point_in_percentile_order() {
        for fixture in ALL {
            let result = fixture.result();

            for (curve, points) in [
                (
                    result.publish_latency_quantiles(),
                    result.aggregated_publish_latency_quantiles.len(),
                ),
                (
                    result.end_to_end_latency_quantiles(),
                    result.aggregated_end_to_end_latency_quantiles.len(),
                ),
                (
                    result.publish_delay_latency_quantiles(),
                    result.aggregated_publish_delay_latency_quantiles.len(),
                ),
            ] {
                assert_eq!(curve.len(), points, "{}", fixture.file_name);
                assert!(
                    curve
                        .windows(2)
                        .all(|pair| pair[0].percentile < pair[1].percentile
                            && pair[0].value <= pair[1].value),
                    "{}",
                    fixture.file_name
                );
            }
            let publish = result.publish_latency_quantiles();
            assert_eq!(
                publish.last().map(|point| (point.percentile, point.value)),
                Some((100.0, result.aggregated_publish_latency_max))
            );
        }
    }

    #[test]
    fn given_run_without_consumers_when_read_should_not_measure_end_to_end_latency() {
        let result = NO_CONSUMERS.result();

        assert_eq!(result.consumers_per_topic, 0);
        assert!(
            result
                .end_to_end_latency_avg
                .iter()
                .all(|value| *value == 0.0)
        );
        assert!(!result.has_end_to_end_latency());
        assert_eq!(result.end_to_end_latency(), None);
        assert_eq!(result.end_to_end_latency_quantiles(), []);
        assert!(result.has_publish_delay_latency());
    }

    #[test]
    fn given_consumers_but_zero_or_empty_end_to_end_data_when_read_should_not_measure_it() {
        let mut empty = RC1_DEFAULT.result();
        empty.aggregated_end_to_end_latency_quantiles.clear();
        let mut zero = RC1_DEFAULT.result();
        zero.aggregated_end_to_end_latency_max = 0.0;

        assert!(RC1_DEFAULT.result().has_end_to_end_latency());
        assert!(!empty.has_end_to_end_latency());
        assert!(!zero.has_end_to_end_latency());
    }

    #[test]
    fn given_real_results_when_read_should_give_aggregated_percentiles() {
        let errors = PUBLISH_ERRORS.result();
        let partitions = EDGE8_100_PARTITIONS.result();

        assert_eq!(
            errors.publish_latency(),
            OmbPercentiles {
                avg: 5.832_460_578_957_823,
                p50: 4.223,
                p75: 4.713,
                p95: 5.625,
                p99: 7.357,
                p999: 110.34,
                p9999: 5683.999,
                max: 9991.039,
            }
        );
        assert_eq!(
            errors
                .end_to_end_latency()
                .map(|latency| (latency.p99, latency.max)),
            Some((8.0, 10_003.007))
        );
        assert_eq!(
            errors.publish_delay_latency().map(|delay| delay.p99),
            Some(105.0)
        );
        assert_eq!(partitions.publish_latency().p99, 1.89);
        assert_eq!(
            partitions.end_to_end_latency().map(|latency| latency.p99),
            Some(2.0)
        );
    }

    #[test]
    fn given_result_when_serialized_should_round_trip_with_omb_field_names() {
        let result = PUBLISH_ERRORS.result();
        let json = serde_json::to_value(&result).expect("results serialize");

        assert!(json.get("aggregatedPublishLatency9999pct").is_some());
        assert!(json.get("endToEndLatency50pct").is_some());
        assert!(json.get("aggregated_publish_latency_99pct").is_none());
        assert_round_trip(&result);
    }
}
