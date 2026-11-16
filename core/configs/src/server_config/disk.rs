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

// The volume holding the data directory.

use super::COMPONENT;
use crate::ConfigurationError;
use configs::ConfigEnv;
use iggy_common::Validatable;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, Copy, ConfigEnv)]
#[serde(default, deny_unknown_fields)]
pub struct DiskConfig {
    /// Whether the server refuses new messages, new consumer offset keys, new
    /// topics and new partitions at or above [`Self::usage_limit_percent`].
    pub usage_limit_enabled: bool,
    /// Percent of the data volume, counted the way the Use% column of `df`
    /// counts it. Space the filesystem reserves for root is not counted, so
    /// 100 is where a server running without root runs out of space.
    pub usage_limit_percent: f64,
}

impl DiskConfig {
    /// Used bytes at which new data is refused on a volume of `usable_bytes`,
    /// or `None` while the limit is disabled or the volume reports no size.
    /// A volume such as ramfs reports no blocks, and `df` shows no Use% for
    /// it. A limit of 0 bytes would refuse every write on it.
    #[must_use]
    pub fn usage_limit_bytes(&self, usable_bytes: u64) -> Option<u64> {
        // Validation keeps the percent within (0, 100], so the result never
        // exceeds `usable_bytes`. The cast truncates, which errs towards a
        // fuller disk.
        (self.usage_limit_enabled && usable_bytes > 0)
            .then(|| (usable_bytes as f64 * self.usage_limit_percent / 100.0) as u64)
    }
}

impl Validatable<ConfigurationError> for DiskConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        // Written so that NaN fails it too. 0 would refuse every write, so it
        // is refused here and `usage_limit_enabled = false` turns the limit off.
        if !(self.usage_limit_percent > 0.0 && self.usage_limit_percent <= 100.0) {
            eprintln!(
                "{COMPONENT} disk.usage_limit_percent ({}) must be > 0 and <= 100. Set \
                 disk.usage_limit_enabled = false to turn the limit off",
                self.usage_limit_percent
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Figment;
    use figment::providers::{Format, Toml};

    fn limited_at(usage_limit_percent: f64) -> DiskConfig {
        DiskConfig {
            usage_limit_enabled: true,
            usage_limit_percent,
        }
    }

    fn from_toml(toml: &str) -> DiskConfig {
        Figment::new()
            .merge(Toml::string(toml))
            .extract()
            .expect("the disk section deserializes")
    }

    #[test]
    fn shipped_default_limits_at_ninety_percent() {
        let config = DiskConfig::default();

        assert!(config.usage_limit_enabled);
        assert_eq!(config.usage_limit_percent, 90.0);
    }

    #[test]
    fn validate_accepts_the_bounds_and_fractions() {
        for usage_limit_percent in [0.0001, 90.5, 100.0] {
            assert!(
                limited_at(usage_limit_percent).validate().is_ok(),
                "usage_limit_percent = {usage_limit_percent} must be accepted"
            );
        }
    }

    #[test]
    fn validate_rejects_out_of_range_and_non_finite_percents() {
        for usage_limit_percent in [
            0.0,
            -0.0,
            -1.0,
            100.000_001,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            assert!(
                limited_at(usage_limit_percent).validate().is_err(),
                "usage_limit_percent = {usage_limit_percent} must be refused"
            );
        }
    }

    #[test]
    fn validate_rejects_an_invalid_percent_while_the_limit_is_disabled() {
        let config = DiskConfig {
            usage_limit_enabled: false,
            usage_limit_percent: f64::NAN,
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn usage_limit_bytes_takes_the_percent_of_the_usable_bytes() {
        // 469.06 GiB, a volume of 491.08 GiB minus 22.02 GiB reserved for root.
        const USABLE: u64 = 503_662_575_616;

        assert_eq!(limited_at(90.0).usage_limit_bytes(1000), Some(900));
        assert_eq!(
            limited_at(90.5).usage_limit_bytes(USABLE),
            Some(455_814_630_932)
        );
        assert_eq!(limited_at(100.0).usage_limit_bytes(USABLE), Some(USABLE));
        assert_eq!(
            limited_at(100.0).usage_limit_bytes(u64::MAX),
            Some(u64::MAX)
        );
    }

    #[test]
    fn usage_limit_bytes_is_none_for_a_volume_without_size() {
        assert_eq!(limited_at(90.0).usage_limit_bytes(0), None);
        assert_eq!(limited_at(100.0).usage_limit_bytes(0), None);
    }

    #[test]
    fn usage_limit_bytes_is_none_while_the_limit_is_disabled() {
        let config = DiskConfig {
            usage_limit_enabled: false,
            ..limited_at(90.0)
        };

        assert_eq!(config.usage_limit_bytes(1000), None);
    }

    #[test]
    fn given_a_disk_section_without_keys_when_deserializing_should_use_the_shipped_defaults() {
        let config = from_toml("");
        let shipped = DiskConfig::default();

        assert_eq!(config.usage_limit_enabled, shipped.usage_limit_enabled);
        assert_eq!(config.usage_limit_percent, shipped.usage_limit_percent);
    }

    #[test]
    fn given_a_whole_number_percent_when_deserializing_should_read_it_as_a_float() {
        let config = from_toml("usage_limit_percent = 85");

        assert_eq!(config.usage_limit_percent, 85.0);
    }
}
