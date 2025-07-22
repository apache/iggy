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

#[derive(Debug, Copy, Clone)]
pub struct SemanticVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl FromStr for SemanticVersion {
    type Err = IggyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut version = s.split('.');
        let major = version
            .next()
            .unwrap()
            .parse::<u32>()
            .map_err(|_| IggyError::InvalidNumberValue)?;
        let minor = version
            .next()
            .unwrap()
            .parse::<u32>()
            .map_err(|_| IggyError::InvalidNumberValue)?;
        let patch = version
            .next()
            .unwrap()
            .parse::<u32>()
            .map_err(|_| IggyError::InvalidNumberValue)?;
        Ok(SemanticVersion {
            major,
            minor,
            patch,
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
        write!(
            f,
            "{major}.{minor}.{patch}",
            major = self.major,
            minor = self.minor,
            patch = self.patch
        )
    }
}

mod tests {
    #[test]
    fn should_load_the_expected_version_from_package_definition() {
        const CARGO_TOML_VERSION: &str = env!("CARGO_PKG_VERSION");
        assert_eq!(crate::VERSION, CARGO_TOML_VERSION);
    }
}
