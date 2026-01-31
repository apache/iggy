/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use crate::IggyError;
use std::borrow::Cow;
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct SemanticVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub prerelease: Option<Cow<'static, str>>,
}

/// Parses a string slice to u32 at compile time.
///
/// # Panics
/// - If the string is empty
/// - If the string contains non-digit characters
const fn const_parse_u32_range(bytes: &[u8], start: usize, end: usize) -> u32 {
    if start >= end {
        panic!("Cannot parse empty range as u32");
    }

    let mut result = 0u32;
    let mut i = start;

    if bytes.is_empty() {
        panic!("Can not parse empty string as u32");
    }

    while i < end {
        let byte = bytes[i];

        if !byte.is_ascii_digit() {
            panic!("Invalid digit in version number");
        }

        // ASCII '0' - '9' to 0-9
        let digit = bytes[i] - b'0';
        result = result * 10 + digit as u32;
        i += 1;
    }

    result
}

/// Find pos of a byte in a range, return `end` if not found any
const fn find_byte_in_range(bytes: &[u8], target: u8, start: usize, end: usize) -> usize {
    let mut i = start;

    while i < end {
        if bytes[i] == target {
            return i;
        }
        i += 1;
    }

    end
}

/// Find pos of a byte in a slice, return bytes.len() if not found any
const fn find_byte_pos_or_len(bytes: &[u8], target: u8) -> usize {
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == target {
            return i;
        }
        i += 1;
    }

    bytes.len()
}

/// Extract substring in const context
const fn const_str_slice(s: &str, start: usize, end: usize) -> &str {
    let bytes = s.as_bytes();

    if start > end {
        panic!("Start index must be less than or equal to end index");
    }
    if end > bytes.len() {
        panic!("End index out of bounds");
    }

    // SAFETY: Creating a slice within the bound of original byte slice.
    let slice = unsafe { core::slice::from_raw_parts(bytes.as_ptr().add(start), end - start) };

    match core::str::from_utf8(slice) {
        Ok(substr) => substr,
        Err(_) => panic!("Invalid UTF-8 in version string"),
    }
}

impl FromStr for SemanticVersion {
    type Err = IggyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Split on '+' to separate build metadata (which we ignore)
        let version_core = s.split('+').next().unwrap();

        // Split on '-' to separate prerelease identifier
        let mut parts = version_core.split('-');
        let version_numbers = parts.next().unwrap();
        let prerelease = parts.next().map(|s| Cow::Owned(s.to_string()));

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
    pub const fn parse_const(s: &'static str) -> Self {
        let bytes = s.as_bytes();

        // Split on '+' to ignore build metadata
        let core_end = find_byte_pos_or_len(bytes, b'+');

        // Split on '-' to separate prerelease
        let dash_pos = find_byte_in_range(bytes, b'-', 0, core_end);
        let version_end = if dash_pos < core_end {
            dash_pos
        } else {
            core_end
        };

        // Split version number on '.'
        let first_dot = find_byte_in_range(bytes, b'.', 0, version_end);
        let second_dot = find_byte_in_range(bytes, b'.', first_dot + 1, version_end);

        // Parse major.minor.patch
        let major = const_parse_u32_range(bytes, 0, first_dot);
        let minor = const_parse_u32_range(bytes, first_dot + 1, second_dot);
        let patch = const_parse_u32_range(bytes, second_dot + 1, version_end);

        // Extract prerelease if it present
        let prerelease = if dash_pos < core_end {
            Some(Cow::Borrowed(const_str_slice(s, dash_pos + 1, core_end)))
        } else {
            None
        };

        Self {
            major,
            minor,
            patch,
            prerelease,
        }
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
    fn test_find_byte_pos_or_len() {
        let bytes = b"1.2.3-beta+build";
        assert_eq!(find_byte_pos_or_len(bytes, b'.'), 1);
        assert_eq!(find_byte_pos_or_len(bytes, b'-'), 5);
        assert_eq!(find_byte_pos_or_len(bytes, b'+'), 10);
        assert_eq!(find_byte_pos_or_len(bytes, b'y'), bytes.len()); // Not found
    }

    #[test]
    fn test_find_byte_in_range() {
        let bytes = b"1.2.3-beta";
        assert_eq!(find_byte_in_range(bytes, b'.', 0, 5), 1);
        assert_eq!(find_byte_in_range(bytes, b'.', 2, 5), 3);
        assert_eq!(find_byte_in_range(bytes, b'.', 4, 5), 5); // Not found in range
        assert_eq!(find_byte_in_range(bytes, b'-', 0, 10), 5);
    }

    #[test]
    fn test_const_parse_u32_range() {
        let bytes = b"123";
        assert_eq!(const_parse_u32_range(bytes, 0, 3), 123);

        let bytes = b"1.52.999";
        assert_eq!(const_parse_u32_range(bytes, 0, 1), 1);
        assert_eq!(const_parse_u32_range(bytes, 2, 4), 52);
        assert_eq!(const_parse_u32_range(bytes, 5, 8), 999);
    }

    #[test]
    #[should_panic(expected = "Cannot parse empty range as u32")]
    fn test_const_parse_u32_range_empty() {
        let bytes = b"123";
        const_parse_u32_range(bytes, 1, 1);
    }

    #[test]
    #[should_panic(expected = "Invalid digit in version number")]
    fn test_const_parse_u32_range_invalid() {
        let bytes = b"12a";
        const_parse_u32_range(bytes, 0, 3);
    }

    #[test]
    fn test_const_str_slice() {
        let s = "1.2.3-beta+build";
        assert_eq!(const_str_slice(s, 0, 5), "1.2.3");
        assert_eq!(const_str_slice(s, 6, 10), "beta");
        assert_eq!(const_str_slice(s, 11, 16), "build");
        assert_eq!(const_str_slice(s, 0, 0), "");
    }

    #[test]
    fn should_parse_semver_on_compile_time() {
        const SEMVER: SemanticVersion = SemanticVersion::parse_const("1.0.0-beta");
        assert_eq!(SEMVER.major, 1);
        assert_eq!(SEMVER.minor, 0);
        assert_eq!(SEMVER.patch, 0);
        assert_eq!(SEMVER.prerelease, Some(Cow::Borrowed("beta")));

        const SEMVER_1: SemanticVersion = SemanticVersion::parse_const("2.1.5-rc.1");
        assert_eq!(SEMVER_1.major, 2);
        assert_eq!(SEMVER_1.minor, 1);
        assert_eq!(SEMVER_1.patch, 5);
        assert_eq!(SEMVER_1.prerelease, Some(Cow::Borrowed("rc.1")));

        const SEMVER_2: SemanticVersion = SemanticVersion::parse_const("1.2.3+build.123");
        assert_eq!(SEMVER_2.major, 1);
        assert_eq!(SEMVER_2.minor, 2);
        assert_eq!(SEMVER_2.patch, 3);
        assert_eq!(SEMVER_2.prerelease, None);

        const SEMVER_3: SemanticVersion = SemanticVersion::parse_const("3.2.1-alpha.2+build.456");
        assert_eq!(SEMVER_3.major, 3);
        assert_eq!(SEMVER_3.minor, 2);
        assert_eq!(SEMVER_3.patch, 1);
        assert_eq!(SEMVER_3.prerelease, Some(Cow::Borrowed("alpha.2")));
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
        assert_eq!(version.prerelease, Some(Cow::Borrowed("rc1")));
        assert_eq!(version.to_string(), "0.6.0-rc1");
    }

    #[test]
    fn should_parse_semantic_version_with_alpha() {
        let version = "2.0.0-alpha.1".parse::<SemanticVersion>().unwrap();
        assert_eq!(version.major, 2);
        assert_eq!(version.minor, 0);
        assert_eq!(version.patch, 0);
        assert_eq!(version.prerelease, Some(Cow::Borrowed("alpha.1")));
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
        assert_eq!(version.prerelease, Some(Cow::Borrowed("beta")));
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
