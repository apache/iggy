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

use crate::IggyDuration;
use serde::de::{Error as DeError, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    error::Error,
    fmt::{Display, Formatter},
    str::FromStr,
    time::Duration,
};

/// A duration that is guaranteed to be greater than zero.
///
/// Intervals that pace a loop - heartbeats, reconnection and retry delays - turn into
/// a busy loop or a `tokio::time::interval` panic when they are zero. Such fields hold
/// this type so the zero is rejected where the value is built, not where it is awaited.
///
/// `IggyDuration::from_str` maps `0`, `none`, `disabled` and `unlimited` to the same
/// zero, so all four are rejected here.
///
/// # Example
///
/// ```
/// use iggy_common::{IggyDuration, NonZeroIggyDuration, NonZeroDurationError};
/// use std::str::FromStr;
///
/// let interval = NonZeroIggyDuration::from_str("1s").unwrap();
/// assert_eq!(1, interval.as_secs());
/// assert_eq!("1s", format!("{}", interval));
///
/// assert_eq!(Err(NonZeroDurationError::Zero), NonZeroIggyDuration::from_str("none"));
/// assert_eq!(
///     Err(NonZeroDurationError::Zero),
///     NonZeroIggyDuration::try_from(IggyDuration::from(0_u64)),
/// );
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct NonZeroIggyDuration {
    duration: IggyDuration,
}

/// The reason a value could not become a `NonZeroIggyDuration`.
#[derive(Debug, Clone, PartialEq)]
pub enum NonZeroDurationError {
    /// The value parsed or converted to zero.
    Zero,
    /// The text is not a duration `humantime` understands.
    InvalidFormat(humantime::DurationError),
}

impl NonZeroIggyDuration {
    pub const ONE_SECOND: NonZeroIggyDuration = NonZeroIggyDuration {
        duration: IggyDuration::ONE_SECOND,
    };

    pub fn get(&self) -> IggyDuration {
        self.duration
    }

    pub fn get_duration(&self) -> Duration {
        self.duration.get_duration()
    }

    pub fn as_human_time_string(&self) -> String {
        self.duration.as_human_time_string()
    }

    pub fn as_secs(&self) -> u32 {
        self.duration.as_secs()
    }

    pub fn as_micros(&self) -> u64 {
        self.duration.as_micros()
    }
}

impl Display for NonZeroDurationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NonZeroDurationError::Zero => write!(f, "duration must be greater than zero"),
            NonZeroDurationError::InvalidFormat(error) => write!(f, "invalid duration: {error}"),
        }
    }
}

impl Error for NonZeroDurationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            NonZeroDurationError::Zero => None,
            NonZeroDurationError::InvalidFormat(error) => Some(error),
        }
    }
}

impl From<humantime::DurationError> for NonZeroDurationError {
    fn from(error: humantime::DurationError) -> Self {
        NonZeroDurationError::InvalidFormat(error)
    }
}

impl TryFrom<IggyDuration> for NonZeroIggyDuration {
    type Error = NonZeroDurationError;

    fn try_from(duration: IggyDuration) -> Result<Self, Self::Error> {
        if duration.is_zero() {
            return Err(NonZeroDurationError::Zero);
        }

        Ok(NonZeroIggyDuration { duration })
    }
}

impl TryFrom<u64> for NonZeroIggyDuration {
    type Error = NonZeroDurationError;

    fn try_from(duration_us: u64) -> Result<Self, Self::Error> {
        IggyDuration::from(duration_us).try_into()
    }
}

impl From<NonZeroIggyDuration> for IggyDuration {
    fn from(duration: NonZeroIggyDuration) -> Self {
        duration.duration
    }
}

impl FromStr for NonZeroIggyDuration {
    type Err = NonZeroDurationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        IggyDuration::from_str(s)?.try_into()
    }
}

impl Display for NonZeroIggyDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.duration)
    }
}

impl Serialize for NonZeroIggyDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.duration.serialize(serializer)
    }
}

struct NonZeroIggyDurationVisitor;

impl<'de> Deserialize<'de> for NonZeroIggyDuration {
    fn deserialize<D>(deserializer: D) -> Result<NonZeroIggyDuration, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u64(NonZeroIggyDurationVisitor)
    }
}

impl Visitor<'_> for NonZeroIggyDurationVisitor {
    type Value = NonZeroIggyDuration;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a duration in microseconds greater than zero")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        NonZeroIggyDuration::try_from(value).map_err(E::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_positive_duration_should_convert() {
        let duration = NonZeroIggyDuration::try_from(IggyDuration::ONE_SECOND).unwrap();

        assert_eq!(IggyDuration::ONE_SECOND, duration.get());
        assert_eq!(Duration::from_secs(1), duration.get_duration());
    }

    #[test]
    fn given_a_zero_duration_should_fail_to_convert() {
        let error = NonZeroIggyDuration::try_from(IggyDuration::default()).unwrap_err();

        assert_eq!(NonZeroDurationError::Zero, error);
    }

    #[test]
    fn given_a_zero_alias_should_fail_to_parse() {
        for value in ["0", "0s", "none", "disabled", "unlimited"] {
            assert_eq!(
                Err(NonZeroDurationError::Zero),
                NonZeroIggyDuration::from_str(value),
                "expected {value} to be rejected"
            );
        }
    }

    #[test]
    fn given_a_malformed_value_should_report_the_format_error() {
        let error = NonZeroIggyDuration::from_str("1 hour and 30 minutes").unwrap_err();

        assert!(matches!(error, NonZeroDurationError::InvalidFormat(_)));
    }

    #[test]
    fn given_a_human_time_string_should_parse() {
        let duration = NonZeroIggyDuration::from_str("1h 1m 1s").unwrap();

        assert_eq!(3661, duration.as_secs());
        assert_eq!("1h 1m 1s", duration.as_human_time_string());
        assert_eq!("1h 1m 1s", format!("{duration}"));
    }

    #[test]
    fn given_microseconds_should_round_trip_through_serde() {
        let duration = NonZeroIggyDuration::from_str("500ms").unwrap();

        let serialized = serde_json::to_string(&duration).unwrap();

        assert_eq!("500000", serialized);
        assert_eq!(
            duration,
            serde_json::from_str::<NonZeroIggyDuration>(&serialized).unwrap()
        );
    }

    #[test]
    fn given_a_zero_microsecond_value_should_fail_to_deserialize() {
        assert!(serde_json::from_str::<NonZeroIggyDuration>("0").is_err());
    }
}
