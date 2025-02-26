use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Display;
use std::str::FromStr;

/// Represents a numeric argument that can be either a single value or a range.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IggyBenchNumericParameter {
    /// Single value
    Value(u32),
    /// Range of values (inclusive)
    Range { min: u32, max: u32 },
}

impl IggyBenchNumericParameter {
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
        use rand::Rng;
        match self {
            Self::Value(v) => *v,
            Self::Range { min, max } => rand::rng().random_range(*min..=*max),
        }
    }
}

impl Default for IggyBenchNumericParameter {
    fn default() -> Self {
        Self::Value(0)
    }
}

impl Serialize for IggyBenchNumericParameter {
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

impl<'de> Deserialize<'de> for IggyBenchNumericParameter {
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
                Ok(IggyBenchNumericParameter::Value(num as u32))
            }
            serde_json::Value::String(s) => s.parse().map_err(D::Error::custom),
            _ => Err(D::Error::custom("Expected number or string")),
        }
    }
}

impl FromStr for IggyBenchNumericParameter {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains("..") {
            let parts: Vec<&str> = s.split("..").collect();
            if parts.len() != 2 {
                return Err("Invalid range format. Expected format: min..max".to_string());
            }

            let min = parts[0]
                .parse::<u32>()
                .map_err(|_| "Invalid minimum value")?;
            let max = parts[1]
                .parse::<u32>()
                .map_err(|_| "Invalid maximum value")?;

            if min > max {
                return Err("Minimum value cannot be greater than maximum value".to_string());
            }

            Ok(IggyBenchNumericParameter::Range { min, max })
        } else {
            let value = s
                .parse::<u32>()
                .map_err(|e| format!("Invalid value: {e}"))?;
            Ok(IggyBenchNumericParameter::Value(value))
        }
    }
}

impl Display for IggyBenchNumericParameter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Value(v) => write!(f, "{}", v),
            Self::Range { min, max } => write!(f, "[{}..{}]", min, max),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_value() {
        let arg = "100".parse::<IggyBenchNumericParameter>().unwrap();
        assert!(matches!(arg, IggyBenchNumericParameter::Value(v) if v == 100));
    }

    #[test]
    fn test_parse_range() {
        let arg = "100..200".parse::<IggyBenchNumericParameter>().unwrap();
        match arg {
            IggyBenchNumericParameter::Range { min, max } => {
                assert_eq!(min, 100);
                assert_eq!(max, 200);
            }
            _ => panic!("Expected Range variant"),
        }
    }

    #[test]
    fn test_invalid_range() {
        assert!("200..100".parse::<IggyBenchNumericParameter>().is_err());
        assert!("invalid..100".parse::<IggyBenchNumericParameter>().is_err());
        assert!("100..invalid".parse::<IggyBenchNumericParameter>().is_err());
    }

    #[test]
    fn test_display() {
        let value = IggyBenchNumericParameter::Value(100);
        assert_eq!(value.to_string(), "100");

        let range = IggyBenchNumericParameter::Range { min: 100, max: 200 };
        assert_eq!(range.to_string(), "100..200");
    }

    #[test]
    fn test_random_value() {
        let value = IggyBenchNumericParameter::Value(100);
        assert_eq!(value.get(), 100);

        let range = IggyBenchNumericParameter::Range { min: 100, max: 200 };
        let random = range.get();
        assert!((100..=200).contains(&random));
    }

    #[test]
    fn test_serialize() {
        let value = IggyBenchNumericParameter::Value(100);
        assert_eq!(serde_json::to_string(&value).unwrap(), "100");

        let range = IggyBenchNumericParameter::Range { min: 100, max: 200 };
        assert_eq!(serde_json::to_string(&range).unwrap(), "\"100..200\"");
    }

    #[test]
    fn test_deserialize() {
        let value: IggyBenchNumericParameter = serde_json::from_str("100").unwrap();
        assert_eq!(value, IggyBenchNumericParameter::Value(100));

        let range: IggyBenchNumericParameter = serde_json::from_str("\"100..200\"").unwrap();
        assert_eq!(
            range,
            IggyBenchNumericParameter::Range { min: 100, max: 200 }
        );

        assert!(serde_json::from_str::<IggyBenchNumericParameter>("\"invalid\"").is_err());
        assert!(serde_json::from_str::<IggyBenchNumericParameter>("\"0..100\"").is_err());
        assert!(serde_json::from_str::<IggyBenchNumericParameter>("\"100..50\"").is_err());
    }
}
