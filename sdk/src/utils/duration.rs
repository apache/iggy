/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use humantime::format_duration;
use humantime::Duration as HumanDuration;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fmt::{Display, Formatter},
    ops::Add,
    str::FromStr,
    time::Duration,
};

pub const SEC_IN_MICRO: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct IggyDuration {
    duration: Duration,
}

impl IggyDuration {
    pub const ONE_SECOND: IggyDuration = IggyDuration {
        duration: Duration::from_secs(1),
    };
}

impl IggyDuration {
    pub fn new(duration: Duration) -> IggyDuration {
        IggyDuration { duration }
    }

    pub fn new_from_secs(secs: u64) -> IggyDuration {
        IggyDuration {
            duration: Duration::from_secs(secs),
        }
    }

    pub fn as_human_time_string(&self) -> String {
        format!("{}", format_duration(self.duration))
    }

    pub fn as_secs(&self) -> u32 {
        self.duration.as_secs() as u32
    }

    pub fn as_secs_f64(&self) -> f64 {
        self.duration.as_secs_f64()
    }

    pub fn as_micros(&self) -> u64 {
        self.duration.as_micros() as u64
    }

    pub fn get_duration(&self) -> Duration {
        self.duration
    }

    pub fn is_zero(&self) -> bool {
        self.duration.as_secs() == 0
    }

    pub fn abs_diff(&self, other: IggyDuration) -> IggyDuration {
        let diff = self.duration.abs_diff(other.duration);
        IggyDuration { duration: diff }
    }
}

impl FromStr for IggyDuration {
    type Err = humantime::DurationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = &s.to_lowercase();
        if s == "0" || s == "unlimited" || s == "disabled" || s == "none" {
            Ok(IggyDuration {
                duration: Duration::new(0, 0),
            })
        } else {
            Ok(IggyDuration {
                duration: humantime::parse_duration(s)?,
            })
        }
    }
}

impl From<Option<u64>> for IggyDuration {
    fn from(duration_us: Option<u64>) -> Self {
        match duration_us {
            Some(value) => IggyDuration {
                duration: Duration::from_micros(value),
            },
            None => IggyDuration {
                duration: Duration::new(0, 0),
            },
        }
    }
}

impl From<u64> for IggyDuration {
    fn from(value: u64) -> Self {
        IggyDuration {
            duration: Duration::from_micros(value),
        }
    }
}

impl From<Duration> for IggyDuration {
    fn from(duration: Duration) -> Self {
        IggyDuration { duration }
    }
}

impl From<HumanDuration> for IggyDuration {
    fn from(human_duration: HumanDuration) -> Self {
        Self {
            duration: human_duration.into(),
        }
    }
}

impl From<IggyDuration> for u64 {
    fn from(iggy_duration: IggyDuration) -> u64 {
        iggy_duration.duration.as_micros() as u64
    }
}

impl Default for IggyDuration {
    fn default() -> Self {
        IggyDuration {
            duration: Duration::new(0, 0),
        }
    }
}

impl Display for IggyDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_human_time_string())
    }
}

impl Add for IggyDuration {
    type Output = IggyDuration;

    fn add(self, rhs: Self) -> Self::Output {
        IggyDuration {
            duration: self.duration + rhs.duration,
        }
    }
}

impl Serialize for IggyDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.as_micros())
    }
}

struct IggyDurationVisitor;

impl<'de> Deserialize<'de> for IggyDuration {
    fn deserialize<D>(deserializer: D) -> Result<IggyDuration, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u64(IggyDurationVisitor)
    }
}

impl Visitor<'_> for IggyDurationVisitor {
    type Value = IggyDuration;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a duration in seconds")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(IggyDuration::new(Duration::from_micros(value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_new() {
        let duration = Duration::new(60, 0); // 60 seconds
        let iggy_duration = IggyDuration::new(duration);
        assert_eq!(iggy_duration.as_secs(), 60);
    }

    #[test]
    fn test_as_human_time_string() {
        let duration = Duration::new(3661, 0); // 1 hour, 1 minute and 1 second
        let iggy_duration = IggyDuration::new(duration);
        assert_eq!(iggy_duration.as_human_time_string(), "1h 1m 1s");
    }

    #[test]
    fn test_long_duration_as_human_time_string() {
        let duration = Duration::new(36611233, 0); // 1year 1month 28days 1hour 13minutes 37seconds
        let iggy_duration = IggyDuration::new(duration);
        assert_eq!(
            iggy_duration.as_human_time_string(),
            "1year 1month 28days 1h 13m 37s"
        );
    }

    #[test]
    fn test_from_str() {
        let iggy_duration: IggyDuration = "1h 1m 1s".parse().unwrap();
        assert_eq!(iggy_duration.as_secs(), 3661);
    }

    #[test]
    fn test_display() {
        let duration = Duration::new(3661, 0);
        let iggy_duration = IggyDuration::new(duration);
        let duration_string = format!("{}", iggy_duration);
        assert_eq!(duration_string, "1h 1m 1s");
    }

    #[test]
    fn test_invalid_duration() {
        let result: Result<IggyDuration, _> = "1 hour and 30 minutes".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_seconds_duration() {
        let iggy_duration: IggyDuration = "0s".parse().unwrap();
        assert_eq!(iggy_duration.as_secs(), 0);
    }

    #[test]
    fn test_zero_duration() {
        let iggy_duration: IggyDuration = "0".parse().unwrap();
        assert_eq!(iggy_duration.as_secs(), 0);
    }

    #[test]
    fn test_unlimited() {
        let iggy_duration: IggyDuration = "unlimited".parse().unwrap();
        assert_eq!(iggy_duration.as_secs(), 0);
    }

    #[test]
    fn test_disabled() {
        let iggy_duration: IggyDuration = "disabled".parse().unwrap();
        assert_eq!(iggy_duration.as_secs(), 0);
    }

    #[test]
    fn test_add_duration() {
        let iggy_duration1: IggyDuration = "6s".parse().unwrap();
        let iggy_duration2: IggyDuration = "1m".parse().unwrap();
        let result: IggyDuration = iggy_duration1 + iggy_duration2;
        assert_eq!(result.as_secs(), 66);
    }
}
