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

use humantime::Duration as HumanDuration;
use humantime::format_duration;
use serde::de::{Error as DeError, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    error::Error,
    fmt::{Display, Formatter},
    ops::Add,
    str::FromStr,
    time::Duration,
};

pub const SEC_IN_MICRO: u64 = 1_000_000;

/// A struct for representing time durations with various utility functions.
///
/// This struct wraps `std::time::Duration` and uses the `humantime` crate for parsing and formatting
/// human-readable duration strings. It also implements serialization and deserialization via the `serde` crate.
///
/// # Example
///
/// ```
/// use iggy_common::IggyDuration;
/// use std::str::FromStr;
///
/// let duration = IggyDuration::from(3661_000_000_u64); // 3661 seconds in microseconds
/// assert_eq!(3661, duration.as_secs());
/// assert_eq!("1h 1m 1s", duration.as_human_time_string());
/// assert_eq!("1h 1m 1s", format!("{}", duration));
///
/// let duration = IggyDuration::from(0_u64);
/// assert_eq!(0, duration.as_secs());
/// assert_eq!("0s", duration.as_human_time_string());
/// assert_eq!("0s", format!("{}", duration));
///
/// let duration = IggyDuration::from_str("1h 1m 1s").unwrap();
/// assert_eq!(3661, duration.as_secs());
/// assert_eq!("1h 1m 1s", duration.as_human_time_string());
/// assert_eq!("1h 1m 1s", format!("{}", duration));
///
/// let duration = IggyDuration::from_str("unlimited").unwrap();
/// assert_eq!(0, duration.as_secs());
/// ```
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
        self.duration.is_zero()
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

/// A duration that is guaranteed to be greater than zero.
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

    pub fn new(duration: Duration) -> Result<Self, NonZeroDurationError> {
        IggyDuration::new(duration).try_into()
    }

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

    pub fn as_secs_f64(&self) -> f64 {
        self.duration.as_secs_f64()
    }

    pub fn as_micros(&self) -> u64 {
        self.duration.as_micros()
    }

    /// The gap between two non-zero durations is zero when they are equal, so the
    /// result is an `IggyDuration`.
    pub fn abs_diff(&self, other: NonZeroIggyDuration) -> IggyDuration {
        self.duration.abs_diff(other.duration)
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

impl TryFrom<Duration> for NonZeroIggyDuration {
    type Error = NonZeroDurationError;

    fn try_from(duration: Duration) -> Result<Self, Self::Error> {
        IggyDuration::from(duration).try_into()
    }
}

impl TryFrom<HumanDuration> for NonZeroIggyDuration {
    type Error = NonZeroDurationError;

    fn try_from(duration: HumanDuration) -> Result<Self, Self::Error> {
        IggyDuration::from(duration).try_into()
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

impl Add for NonZeroIggyDuration {
    type Output = NonZeroIggyDuration;

    fn add(self, rhs: Self) -> Self::Output {
        NonZeroIggyDuration {
            duration: self.duration + rhs.duration,
        }
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
        let duration_string = format!("{iggy_duration}");
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
    fn given_a_positive_std_duration_should_build() {
        let duration = NonZeroIggyDuration::new(Duration::from_millis(1500)).unwrap();

        assert_eq!(1.5, duration.as_secs_f64());
    }

    #[test]
    fn given_a_zero_std_duration_should_fail_to_build() {
        assert_eq!(
            Err(NonZeroDurationError::Zero),
            NonZeroIggyDuration::new(Duration::ZERO)
        );
        assert_eq!(
            Err(NonZeroDurationError::Zero),
            NonZeroIggyDuration::try_from(Duration::ZERO)
        );
    }

    #[test]
    fn given_a_human_duration_should_convert() {
        let human_duration = HumanDuration::from_str("1m").unwrap();

        let duration = NonZeroIggyDuration::try_from(human_duration).unwrap();

        assert_eq!(60, duration.as_secs());
    }

    #[test]
    fn given_two_durations_should_report_their_gap() {
        let one_minute = NonZeroIggyDuration::from_str("1m").unwrap();
        let six_seconds = NonZeroIggyDuration::from_str("6s").unwrap();

        assert_eq!(
            IggyDuration::new_from_secs(54),
            one_minute.abs_diff(six_seconds)
        );
        assert_eq!(IggyDuration::default(), one_minute.abs_diff(one_minute));
    }

    #[test]
    fn given_two_durations_should_add_up() {
        let sum = NonZeroIggyDuration::from_str("6s").unwrap()
            + NonZeroIggyDuration::from_str("1m").unwrap();

        assert_eq!(66, sum.as_secs());
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
