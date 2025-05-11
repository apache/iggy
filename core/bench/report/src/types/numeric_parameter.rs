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

use rand::Rng;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Display;
use std::str::FromStr;

/// Represents a numeric argument that can be either a single value or a range.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BenchmarkNumericParameter {
    /// Single value
    Value(u32),
    /// Range of values (inclusive)
    Range { min: u32, max: u32 },
}

impl BenchmarkNumericParameter {
    /// Gets the minimum value (for Range) or the single value (for Value)
    pub fn min(&self) -> u32 {
        match self {
            Self::Value(v) => *v,
            Self::Range { min, .. } => *min,
        }
    }

    /// Gets the maximum value (for Range) or the single value (for Value)
    pub fn max(&self) -> u32 {
        match self {
            Self::Value(v) => *v,
            Self::Range { max, .. } => *max,
        }
    }

    /// Gets a value: either single value or random within the range
    pub fn get(&self) -> u32 {
        match self {
            Self::Value(v) => *v,
            Self::Range { min, max } => rand::rng().random_range(*min..=*max),
        }
    }

    /// Checks if the numeric parameter is a single value
    pub fn is_fixed(&self) -> bool {
        matches!(self, Self::Value(_))
    }
}

impl Default for BenchmarkNumericParameter {
    fn default() -> Self {
        Self::Value(0)
    }
}

impl Serialize for BenchmarkNumericParameter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Value(v) => v.serialize(serializer),
            Self::Range { .. } => self.to_string().serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for BenchmarkNumericParameter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let value = serde_json::Value::deserialize(deserializer)?;

        match value {
            serde_json::Value::Number(n) => {
                let num = n
                    .as_u64()
                    .ok_or_else(|| D::Error::custom("Invalid numeric value"))?;
                Ok(BenchmarkNumericParameter::Value(num as u32))
            }
            serde_json::Value::String(s) => s.parse().map_err(D::Error::custom),
            _ => Err(D::Error::custom("Expected number or string")),
        }
    }
}

impl FromStr for BenchmarkNumericParameter {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains("..") {
            let parts: Vec<&str> = s.split("..").collect();
            if parts.len() != 2 {
                return Err("Invalid range format. Expected format: min..max".to_string());
            }

            let min = parts[0]
                .parse::<u32>()
                .map_err(|e| format!("Invalid minimum value: {e}"))?;
            let max = parts[1]
                .parse::<u32>()
                .map_err(|e| format!("Invalid maximum value: {e}"))?;

            if min > max {
                return Err("Minimum value cannot be greater than maximum value".to_string());
            }

            if min == max {
                Ok(BenchmarkNumericParameter::Value(min))
            } else {
                Ok(BenchmarkNumericParameter::Range { min, max })
            }
        } else {
            let value = s
                .parse::<u32>()
                .map_err(|e| format!("Invalid value: {e}"))?;
            Ok(BenchmarkNumericParameter::Value(value))
        }
    }
}

impl Display for BenchmarkNumericParameter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Value(v) => write!(f, "{}", v),
            Self::Range { min, max } => write!(f, "{}..{}", min, max),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_value() {
        let arg = "100".parse::<BenchmarkNumericParameter>().unwrap();
        assert!(matches!(arg, BenchmarkNumericParameter::Value(v) if v == 100));
    }

    #[test]
    fn test_parse_range() {
        let arg = "100..200".parse::<BenchmarkNumericParameter>().unwrap();
        match arg {
            BenchmarkNumericParameter::Range { min, max } => {
                assert_eq!(min, 100);
                assert_eq!(max, 200);
            }
            _ => panic!("Expected Range variant"),
        }
    }

    #[test]
    fn test_parse_equal_range_as_value() {
        let arg = "150..150".parse::<BenchmarkNumericParameter>().unwrap();
        assert!(matches!(arg, BenchmarkNumericParameter::Value(v) if v == 150));
    }

    #[test]
    fn test_invalid_range() {
        assert!("200..100".parse::<BenchmarkNumericParameter>().is_err());
        assert!("invalid..100".parse::<BenchmarkNumericParameter>().is_err());
        assert!("100..invalid".parse::<BenchmarkNumericParameter>().is_err());
    }

    #[test]
    fn test_display() {
        let value = BenchmarkNumericParameter::Value(100);
        assert_eq!(value.to_string(), "100");

        let range = BenchmarkNumericParameter::Range { min: 100, max: 200 };
        assert_eq!(range.to_string(), "100..200");
    }

    #[test]
    fn test_random_value() {
        let value = BenchmarkNumericParameter::Value(100);
        assert_eq!(value.get(), 100);

        let range = BenchmarkNumericParameter::Range { min: 100, max: 200 };
        let random = range.get();
        assert!((100..=200).contains(&random));
    }

    #[test]
    fn test_serialize() {
        let value = BenchmarkNumericParameter::Value(100);
        assert_eq!(serde_json::to_string(&value).unwrap(), "100");

        let range = BenchmarkNumericParameter::Range { min: 100, max: 200 };
        assert_eq!(serde_json::to_string(&range).unwrap(), "\"100..200\"");
    }

    #[test]
    fn test_deserialize() {
        let value: BenchmarkNumericParameter = serde_json::from_str("100").unwrap();
        assert_eq!(value, BenchmarkNumericParameter::Value(100));

        let range: BenchmarkNumericParameter = serde_json::from_str("\"100..200\"").unwrap();
        assert_eq!(
            range,
            BenchmarkNumericParameter::Range { min: 100, max: 200 }
        );

        assert!(serde_json::from_str::<BenchmarkNumericParameter>("\"invalid\"").is_err());
        assert!(serde_json::from_str::<BenchmarkNumericParameter>("\"-5..100\"").is_err());
        assert!(serde_json::from_str::<BenchmarkNumericParameter>("\"100..50\"").is_err());
    }
}
