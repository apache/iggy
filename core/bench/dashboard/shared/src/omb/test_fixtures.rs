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

//! Real OMB results of Iggy, complete, from `core/bench/dashboard/fixtures/omb`. Only the
//! workload names are neutral. The cases that a loader must skip are built in the tests, because
//! that directory holds valid runs only.

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::omb::detail::OmbRunDetail;
use crate::omb::id::OmbRunId;
use crate::omb::result::OmbResult;
use crate::omb::setup::{OmbSetup, OmbSidecar};
use crate::omb::summary::OmbRunSummary;

/// One run directory: the result file and its optional sidecar.
pub(crate) struct Fixture {
    pub(crate) file_name: &'static str,
    pub(crate) json: &'static [u8],
    pub(crate) sidecar: Option<&'static str>,
}

macro_rules! fixture {
    ($directory:literal, $file_name:literal) => {
        Fixture {
            file_name: $file_name,
            json: include_bytes!(concat!(
                "../../../fixtures/omb/",
                $directory,
                "/",
                $file_name
            )),
            sidecar: None,
        }
    };
    ($directory:literal, $file_name:literal, with_sidecar) => {
        Fixture {
            sidecar: Some(include_str!(concat!(
                "../../../fixtures/omb/",
                $directory,
                "/setup.toml"
            ))),
            ..fixture!($directory, $file_name)
        }
    };
}

/// 0.10.0-rc.1 on 3 nodes, 12 topics of 1 partition, 1 producer and 1 consumer per topic, 1000 B,
/// 150 MB/s, driver `Iggy`.
pub(crate) const RC1_DEFAULT: Fixture = fixture!(
    "rc1-12-topics-150mb",
    "12-topics-1-partition-1kb-150mb-Iggy-2026-11-06-13-26-21.json",
    with_sidecar
);
/// The workload and setup of [`RC1_DEFAULT`] with the driver configuration `Iggy-persisted`.
pub(crate) const RC1_PERSISTED: Fixture = fixture!(
    "rc1-12-topics-150mb-persisted",
    "12-topics-1-partition-1kb-150mb-Iggy-persisted-2026-11-06-13-31-22.json",
    with_sidecar
);
/// The workload and machines of [`RC1_DEFAULT`] on 0.9.1-edge.8. Its sidecar has no
/// `data_file_system` property.
pub(crate) const EDGE8_DEFAULT: Fixture = fixture!(
    "edge8-12-topics-150mb",
    "12-topics-1-partition-1kb-150mb-Iggy-2026-11-03-06-24-42.json",
    with_sidecar
);
/// 0.9.1-edge.8 on other machines: 1 topic of 100 partitions, 4 producers and 4 consumers, 1 KiB,
/// 30 windows.
pub(crate) const EDGE8_100_PARTITIONS: Fixture = fixture!(
    "edge8-i7i-100-partitions-125mb",
    "1-topic-100-partitions-1kb-125mb-Iggy-2026-11-02-14-55-18.json",
    with_sidecar
);
/// No consumers, so no end-to-end latency. No sidecar.
pub(crate) const NO_CONSUMERS: Fixture = fixture!(
    "edge-12-topics-75mb-no-consumers",
    "12-topics-1-partition-1kb-75mb-no-consumers-Iggy-2026-10-22-10-21-15.json"
);
/// Publish errors in every window, driver `Iggy-persisted`. No sidecar.
pub(crate) const PUBLISH_ERRORS: Fixture = fixture!(
    "edge-12-topics-75mb-persisted-errors",
    "12-topics-1-partition-1kb-75mb-Iggy-persisted-2026-10-15-13-33-17.json"
);

pub(crate) const ALL: [&Fixture; 6] = [
    &RC1_DEFAULT,
    &RC1_PERSISTED,
    &EDGE8_DEFAULT,
    &EDGE8_100_PARTITIONS,
    &NO_CONSUMERS,
    &PUBLISH_ERRORS,
];

impl Fixture {
    pub(crate) fn result(&self) -> OmbResult {
        OmbResult::try_from(self.json).expect("fixture results parse")
    }

    pub(crate) fn setup(&self) -> Option<OmbSetup> {
        self.sidecar.map(|text| {
            text.parse::<OmbSidecar>()
                .expect("fixture sidecars parse")
                .setup
        })
    }

    pub(crate) fn summary(&self) -> OmbRunSummary {
        OmbRunSummary::new(
            OmbRunId::of_file(self.json),
            self.file_name,
            modified(),
            &self.result(),
            self.setup(),
        )
    }

    pub(crate) fn detail(&self) -> OmbRunDetail {
        OmbRunDetail::new(self.summary(), &self.result())
    }
}

/// A fixed modification time, so no test depends on the checkout.
pub(crate) fn modified() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-11-13T09:30:00Z")
        .expect("the test time parses")
        .with_timezone(&Utc)
}

/// `json` without the top-level `fields`, as an older OMB or another tool would write it.
pub(crate) fn without_fields(json: &[u8], fields: &[&str]) -> Vec<u8> {
    let mut value: serde_json::Value = serde_json::from_slice(json).expect("fixtures are JSON");
    let object = value.as_object_mut().expect("results are JSON objects");
    for field in fields {
        object.remove(*field);
    }
    serde_json::to_vec(&value).expect("JSON serializes")
}

/// Means of real values come from sums, so they can differ in the last bits.
pub(crate) fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() <= expected.abs() * 1e-12,
        "{actual} is not {expected}"
    );
}

/// serde_json parses a float to within one unit in the last place, so the round trip compares the
/// JSON of both values, with that tolerance for numbers.
pub(crate) fn assert_round_trip<T: Serialize + DeserializeOwned>(value: &T) {
    let json = serde_json::to_string(value).expect("the value serializes");
    let back: T = serde_json::from_str(&json).expect("the value deserializes");

    assert_json_close(
        &serde_json::to_value(&back).expect("the value serializes"),
        &serde_json::to_value(value).expect("the value serializes"),
    );
}

fn assert_json_close(actual: &Value, expected: &Value) {
    match (actual, expected) {
        (Value::Number(actual), Value::Number(expected)) => assert_close(
            actual.as_f64().expect("JSON numbers are finite"),
            expected.as_f64().expect("JSON numbers are finite"),
        ),
        (Value::Array(actual), Value::Array(expected)) => {
            assert_eq!(actual.len(), expected.len());
            for (actual, expected) in actual.iter().zip(expected) {
                assert_json_close(actual, expected);
            }
        }
        (Value::Object(actual), Value::Object(expected)) => {
            assert!(actual.keys().eq(expected.keys()), "{actual:?} {expected:?}");
            for (actual, expected) in actual.values().zip(expected.values()) {
                assert_json_close(actual, expected);
            }
        }
        _ => assert_eq!(actual, expected),
    }
}
