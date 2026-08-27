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

//! The `[system.segment] write_io` knob: how segment `.log` and `.index`
//! writes go through the kernel.
//!
//! `buffered` is the kernel default: writes land in the page cache and
//! writeback runs on the kernel's schedule. `uncached` carries
//! `RWF_DONTCACHE` on every segment write, so writeback starts at once and
//! the written pages are dropped from the page cache when it completes.

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

const BUFFERED: &str = "buffered";
const UNCACHED: &str = "uncached";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SegmentIoMode {
    #[default]
    Buffered,
    Uncached,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid segment write_io mode '{0}', expected '{BUFFERED}' or '{UNCACHED}'")]
pub struct InvalidSegmentIoMode(pub String);

impl SegmentIoMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => BUFFERED,
            Self::Uncached => UNCACHED,
        }
    }
}

impl Display for SegmentIoMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SegmentIoMode {
    type Err = InvalidSegmentIoMode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            BUFFERED => Ok(Self::Buffered),
            UNCACHED => Ok(Self::Uncached),
            other => Err(InvalidSegmentIoMode(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::IntoDeserializer;
    use serde::de::value::{Error as DeError, StrDeserializer};

    const ALL_MODES: [SegmentIoMode; 2] = [SegmentIoMode::Buffered, SegmentIoMode::Uncached];

    fn deserialize_str(value: &str) -> Result<SegmentIoMode, DeError> {
        let deserializer: StrDeserializer<'_, DeError> = value.into_deserializer();
        SegmentIoMode::deserialize(deserializer)
    }

    #[test]
    fn display_and_from_str_round_trip_every_mode() {
        for mode in ALL_MODES {
            let parsed: SegmentIoMode = mode.to_string().parse().expect("round trip");
            assert_eq!(parsed, mode);
        }
        assert_eq!(SegmentIoMode::Buffered.to_string(), "buffered");
        assert_eq!(SegmentIoMode::Uncached.to_string(), "uncached");
    }

    #[test]
    fn from_str_rejects_unknown_and_wrong_case_values() {
        for value in ["direct", "", "Buffered", "UNCACHED", " uncached"] {
            let error = SegmentIoMode::from_str(value).expect_err(value);
            assert_eq!(error, InvalidSegmentIoMode(value.to_owned()));
            assert!(error.to_string().contains(value), "{error}");
        }
    }

    #[test]
    fn default_is_buffered() {
        assert_eq!(SegmentIoMode::default(), SegmentIoMode::Buffered);
    }

    // The config file and env override reach the enum through serde, the
    // embedded defaults through FromStr: both spellings must agree.
    #[test]
    fn serde_deserializes_the_same_names_as_from_str() {
        for mode in ALL_MODES {
            assert_eq!(deserialize_str(mode.as_str()).expect("serde"), mode);
        }
        assert!(deserialize_str("Uncached").is_err());
        assert!(deserialize_str("direct").is_err());
    }
}
