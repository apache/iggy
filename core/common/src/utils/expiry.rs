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

use crate::utils::duration::IggyDuration;
use humantime::Duration as HumanDuration;
use humantime::format_duration;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::fmt;
use std::fmt::Display;
use std::iter::Sum;
use std::ops::Add;
use std::str::FromStr;
use std::time::Duration;

/// Helper enum for various time-based expiry related functionalities
#[derive(Debug, Copy, Default, Clone, Eq, PartialEq)]
pub enum IggyExpiry {
    #[default]
    /// Use the default expiry time from the server
    ServerDefault,
    /// Set expiry time to given value
    ExpireDuration(IggyDuration),
    /// Never expire
    NeverExpire,
}

impl IggyExpiry {
    pub fn new(values: Option<Vec<IggyExpiry>>) -> Option<Self> {
        values.map(|items| items.iter().cloned().sum())
    }
}

impl From<&IggyExpiry> for Option<u64> {
    fn from(value: &IggyExpiry) -> Self {
        match value {
            IggyExpiry::ExpireDuration(value) => Some(value.as_micros()),
            IggyExpiry::NeverExpire => Some(u64::MAX),
            IggyExpiry::ServerDefault => None,
        }
    }
}

impl Display for IggyExpiry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NeverExpire => write!(f, "never_expire"),
            Self::ServerDefault => write!(f, "server_default"),
            Self::ExpireDuration(value) => write!(f, "{value}"),
        }
    }
}

impl Sum for IggyExpiry {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.into_iter()
            .fold(IggyExpiry::NeverExpire, |acc, x| acc + x)
    }
}

impl Add for IggyExpiry {
    type Output = IggyExpiry;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (IggyExpiry::NeverExpire, IggyExpiry::NeverExpire) => IggyExpiry::NeverExpire,
            (IggyExpiry::NeverExpire, expiry) => expiry,
            (expiry, IggyExpiry::NeverExpire) => expiry,
            (
                IggyExpiry::ExpireDuration(lhs_duration),
                IggyExpiry::ExpireDuration(rhs_duration),
            ) => IggyExpiry::ExpireDuration(lhs_duration + rhs_duration),
            (IggyExpiry::ServerDefault, IggyExpiry::ExpireDuration(_)) => IggyExpiry::ServerDefault,
            (IggyExpiry::ServerDefault, IggyExpiry::ServerDefault) => IggyExpiry::ServerDefault,
            (IggyExpiry::ExpireDuration(_), IggyExpiry::ServerDefault) => IggyExpiry::ServerDefault,
        }
    }
}

impl FromStr for IggyExpiry {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "unlimited" | "none" | "None" | "Unlimited" | "never_expire" => IggyExpiry::NeverExpire,
            "default" | "server_default" | "Default" | "Server_default" => {
                IggyExpiry::ServerDefault
            }
            value => {
                let duration = value.parse::<HumanDuration>().map_err(|e| format!("{e}"))?;
                if duration.as_secs() > u32::MAX as u64 {
                    return Err(format!(
                        "Value too big for expiry time, maximum value is {}",
                        format_duration(Duration::from_secs(u32::MAX as u64))
                    ));
                }

                IggyExpiry::ExpireDuration(IggyDuration::from(duration))
            }
        };

        Ok(result)
    }
}

impl From<IggyExpiry> for Option<u64> {
    fn from(val: IggyExpiry) -> Self {
        match val {
            IggyExpiry::ExpireDuration(value) => Some(value.as_micros()),
            IggyExpiry::ServerDefault => None,
            IggyExpiry::NeverExpire => Some(u64::MAX),
        }
    }
}

impl From<IggyExpiry> for u64 {
    fn from(val: IggyExpiry) -> Self {
        match val {
            IggyExpiry::ExpireDuration(value) => value.as_micros(),
            IggyExpiry::ServerDefault => 0,
            IggyExpiry::NeverExpire => u64::MAX,
        }
    }
}

impl From<Vec<IggyExpiry>> for IggyExpiry {
    fn from(values: Vec<IggyExpiry>) -> Self {
        let mut result = IggyExpiry::NeverExpire;
        for value in values {
            result = result + value;
        }
        result
    }
}

impl From<u64> for IggyExpiry {
    fn from(value: u64) -> Self {
        match value {
            u64::MAX => IggyExpiry::NeverExpire,
            0 => IggyExpiry::ServerDefault,
            value => IggyExpiry::ExpireDuration(IggyDuration::from(value)),
        }
    }
}

impl From<Option<u64>> for IggyExpiry {
    fn from(value: Option<u64>) -> Self {
        match value {
            Some(value) => match value {
                u64::MAX => IggyExpiry::NeverExpire,
                0 => IggyExpiry::ServerDefault,
                value => IggyExpiry::ExpireDuration(IggyDuration::from(value)),
            },
            None => IggyExpiry::NeverExpire,
        }
    }
}

impl Serialize for IggyExpiry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let expiry = match self {
            IggyExpiry::ExpireDuration(value) => value.as_micros(),
            IggyExpiry::ServerDefault => 0,
            IggyExpiry::NeverExpire => u64::MAX,
        };
        serializer.serialize_u64(expiry)
    }
}

impl<'de> Deserialize<'de> for IggyExpiry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u64(IggyExpiryVisitor)
    }
}

struct IggyExpiryVisitor;

impl Visitor<'_> for IggyExpiryVisitor {
    type Value = IggyExpiry;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a microsecond expiry as a u64")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(IggyExpiry::from(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::duration::SEC_IN_MICRO;

    #[test]
    fn should_parse_expiry() {
        assert_eq!(
            IggyExpiry::from_str("none").unwrap(),
            IggyExpiry::NeverExpire
        );
        assert_eq!(
            IggyExpiry::from_str("15days").unwrap(),
            IggyExpiry::ExpireDuration(IggyDuration::from(SEC_IN_MICRO * 60 * 60 * 24 * 15))
        );
        assert_eq!(
            IggyExpiry::from_str("2min").unwrap(),
            IggyExpiry::ExpireDuration(IggyDuration::from(SEC_IN_MICRO * 60 * 2))
        );
        assert_eq!(
            IggyExpiry::from_str("1ms").unwrap(),
            IggyExpiry::ExpireDuration(IggyDuration::from(1000))
        );
        assert_eq!(
            IggyExpiry::from_str("1s").unwrap(),
            IggyExpiry::ExpireDuration(IggyDuration::ONE_SECOND)
        );
        assert_eq!(
            IggyExpiry::from_str("15days 2min 2s").unwrap(),
            IggyExpiry::ExpireDuration(IggyDuration::from(
                SEC_IN_MICRO * (60 * 60 * 24 * 15 + 60 * 2 + 2)
            ))
        );
    }

    #[test]
    fn should_fail_parsing_expiry() {
        let x = IggyExpiry::from_str("15se");
        assert!(x.is_err());
        assert_eq!(
            x.unwrap_err(),
            "unknown time unit \"se\", supported units: ns, us/µs, ms, sec, min, hours, days, weeks, months, years (and few variations)"
        );
    }

    #[test]
    fn should_sum_expiry() {
        assert_eq!(
            IggyExpiry::NeverExpire + IggyExpiry::NeverExpire,
            IggyExpiry::NeverExpire
        );
        assert_eq!(
            IggyExpiry::NeverExpire + IggyExpiry::ExpireDuration(IggyDuration::from(3)),
            IggyExpiry::ExpireDuration(IggyDuration::from(3))
        );
        assert_eq!(
            IggyExpiry::ExpireDuration(IggyDuration::from(5)) + IggyExpiry::NeverExpire,
            IggyExpiry::ExpireDuration(IggyDuration::from(5))
        );
        assert_eq!(
            IggyExpiry::ExpireDuration(IggyDuration::from(5))
                + IggyExpiry::ExpireDuration(IggyDuration::from(3)),
            IggyExpiry::ExpireDuration(IggyDuration::from(8))
        );
    }

    #[test]
    fn should_sum_expiry_from_vec() {
        assert_eq!(
            vec![IggyExpiry::NeverExpire]
                .into_iter()
                .sum::<IggyExpiry>(),
            IggyExpiry::NeverExpire
        );
        let x = vec![
            IggyExpiry::NeverExpire,
            IggyExpiry::ExpireDuration(IggyDuration::from(333)),
            IggyExpiry::NeverExpire,
            IggyExpiry::ExpireDuration(IggyDuration::from(123)),
        ];
        assert_eq!(
            x.into_iter().sum::<IggyExpiry>(),
            IggyExpiry::ExpireDuration(IggyDuration::from(456))
        );
    }

    #[test]
    fn should_check_display_expiry() {
        assert_eq!(IggyExpiry::NeverExpire.to_string(), "never_expire");
        assert_eq!(
            IggyExpiry::ExpireDuration(IggyDuration::from(333333000000)).to_string(),
            "3days 20h 35m 33s"
        );
    }

    #[test]
    fn should_calculate_none_from_server_default() {
        let expiry = IggyExpiry::ServerDefault;
        let result: Option<u64> = From::from(&expiry);
        assert_eq!(result, None);
    }

    #[test]
    fn should_calculate_u64_max_from_never_expiry() {
        let expiry = IggyExpiry::NeverExpire;
        let result: Option<u64> = From::from(&expiry);
        assert_eq!(result, Some(u64::MAX));
    }

    #[test]
    fn should_calculate_some_seconds_from_message_expire() {
        let duration = IggyDuration::new(Duration::new(42, 0));
        let expiry = IggyExpiry::ExpireDuration(duration);
        let result: Option<u64> = From::from(&expiry);
        assert_eq!(result, Some(42000000));
    }

    #[test]
    fn should_create_new_expiry_from_vec() {
        let some_values = vec![
            IggyExpiry::NeverExpire,
            IggyExpiry::ExpireDuration(IggyDuration::from(3)),
            IggyExpiry::ExpireDuration(IggyDuration::from(2)),
            IggyExpiry::ExpireDuration(IggyDuration::from(1)),
        ];
        assert_eq!(
            IggyExpiry::new(Some(some_values)),
            Some(IggyExpiry::ExpireDuration(IggyDuration::from(6)))
        );
        assert_eq!(IggyExpiry::new(None), None);
        let none_values = vec![IggyExpiry::ServerDefault; 10];

        assert_eq!(
            IggyExpiry::new(Some(none_values)),
            Some(IggyExpiry::ServerDefault)
        );
    }
}
