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

use crate::VERSION;
use iggy_common::IggyError;
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct SemanticVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub prerelease: Option<String>,
}

impl FromStr for SemanticVersion {
    type Err = IggyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Split on '+' to separate build metadata (which we ignore)
        let version_core = s.split('+').next().unwrap();

        // Split on '-' to separate prerelease identifier
        let mut parts = version_core.split('-');
        let version_numbers = parts.next().unwrap();
        let prerelease = parts.next().map(String::from);

        // Parse major.minor.patch
        let mut version = version_numbers.split('.');
        let major = version
            .next()
            .ok_or(IggyError::InvalidVersion(s.to_string()))?
            .parse::<u32>()
            .map_err(|_| IggyError::InvalidNumberValue)?;
        let minor = version
            .next()
            .ok_or(IggyError::InvalidVersion(s.to_string()))?
            .parse::<u32>()
            .map_err(|_| IggyError::InvalidNumberValue)?;
        let patch = version
            .next()
            .ok_or(IggyError::InvalidVersion(s.to_string()))?
            .parse::<u32>()
            .map_err(|_| IggyError::InvalidNumberValue)?;

        Ok(SemanticVersion {
            major,
            minor,
            patch,
            prerelease,
        })
    }
}

impl SemanticVersion {
    pub fn current() -> Result<Self, IggyError> {
        if let Ok(version) = VERSION.parse::<SemanticVersion>() {
            return Ok(version);
        }

        Err(IggyError::InvalidVersion(VERSION.into()))
    }

    #[must_use]
    pub fn is_equal_to(&self, other: &SemanticVersion) -> bool {
        self.major == other.major && self.minor == other.minor && self.patch == other.patch
    }

    pub fn is_greater_than(&self, other: &SemanticVersion) -> bool {
        if self.major > other.major {
            return true;
        }
        if self.major < other.major {
            return false;
        }

        if self.minor > other.minor {
            return true;
        }
        if self.minor < other.minor {
            return false;
        }

        if self.patch > other.patch {
            return true;
        }
        if self.patch < other.patch {
            return false;
        }

        false
    }

    pub fn get_numeric_version(&self) -> Result<u32, IggyError> {
        let major = self.major;
        let minor = format!("{:03}", self.minor);
        let patch = format!("{:03}", self.patch);
        if let Ok(version) = format!("{major}{minor}{patch}").parse::<u32>() {
            return Ok(version);
        }

        Err(IggyError::InvalidVersion(self.to_string()))
    }
}

impl Display for SemanticVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if let Some(ref prerelease) = self.prerelease {
            write!(f, "-{prerelease}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_load_the_expected_version_from_package_definition() {
        const CARGO_TOML_VERSION: &str = env!("CARGO_PKG_VERSION");
        assert_eq!(crate::VERSION, CARGO_TOML_VERSION);
    }

    #[test]
    fn should_parse_basic_semantic_version() {
        let version = "1.2.3".parse::<SemanticVersion>().unwrap();
        assert_eq!(version.major, 1);
        assert_eq!(version.minor, 2);
        assert_eq!(version.patch, 3);
        assert_eq!(version.prerelease, None);
        assert_eq!(version.to_string(), "1.2.3");
    }

    #[test]
    fn should_parse_semantic_version_with_prerelease() {
        let version = "0.6.0-rc1".parse::<SemanticVersion>().unwrap();
        assert_eq!(version.major, 0);
        assert_eq!(version.minor, 6);
        assert_eq!(version.patch, 0);
        assert_eq!(version.prerelease, Some("rc1".to_string()));
        assert_eq!(version.to_string(), "0.6.0-rc1");
    }

    #[test]
    fn should_parse_semantic_version_with_alpha() {
        let version = "2.0.0-alpha.1".parse::<SemanticVersion>().unwrap();
        assert_eq!(version.major, 2);
        assert_eq!(version.minor, 0);
        assert_eq!(version.patch, 0);
        assert_eq!(version.prerelease, Some("alpha.1".to_string()));
        assert_eq!(version.to_string(), "2.0.0-alpha.1");
    }

    #[test]
    fn should_parse_semantic_version_with_build_metadata() {
        let version = "1.0.0+20130313144700".parse::<SemanticVersion>().unwrap();
        assert_eq!(version.major, 1);
        assert_eq!(version.minor, 0);
        assert_eq!(version.patch, 0);
        assert_eq!(version.prerelease, None);
        // Build metadata is not included in the display
        assert_eq!(version.to_string(), "1.0.0");
    }

    #[test]
    fn should_parse_semantic_version_with_prerelease_and_build_metadata() {
        let version = "1.0.0-beta+exp.sha.5114f85"
            .parse::<SemanticVersion>()
            .unwrap();
        assert_eq!(version.major, 1);
        assert_eq!(version.minor, 0);
        assert_eq!(version.patch, 0);
        assert_eq!(version.prerelease, Some("beta".to_string()));
        assert_eq!(version.to_string(), "1.0.0-beta");
    }

    #[test]
    fn should_compare_versions_correctly() {
        let v1 = "1.0.0".parse::<SemanticVersion>().unwrap();
        let v2 = "1.0.0-rc1".parse::<SemanticVersion>().unwrap();
        let v3 = "2.0.0".parse::<SemanticVersion>().unwrap();

        assert!(v1.is_equal_to(&v2)); // Prerelease is ignored in comparison
        assert!(!v1.is_equal_to(&v3));
        assert!(v3.is_greater_than(&v1));
        assert!(!v1.is_greater_than(&v3));
    }
}
