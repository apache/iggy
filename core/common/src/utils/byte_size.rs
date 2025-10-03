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

use super::duration::IggyDuration;
use crate::IggyError;
use byte_unit::{Byte, UnitType};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::{
    iter::Sum,
    ops::{Add, AddAssign, Sub, SubAssign},
    str::FromStr,
};

/// A struct for representing byte sizes with various utility functions.
///
/// This struct uses `Byte` from `byte_unit` crate.
/// It also implements serialization and deserialization via the `serde` crate.
///
/// # Example
///
/// ```
/// use iggy_common::IggyByteSize;
/// use std::str::FromStr;
///
/// let size = IggyByteSize::from(568_000_000_u64);
/// assert_eq!(568_000_000, size.as_bytes_u64());
/// assert_eq!("568.00 MB", size.as_human_string());
/// assert_eq!("568.00 MB", format!("{}", size));
///
/// let size = IggyByteSize::from(0_u64);
/// assert_eq!("unlimited", size.as_human_string_with_zero_as_unlimited());
/// assert_eq!("0 B", size.as_human_string());
/// assert_eq!(0, size.as_bytes_u64());
///
/// let size = IggyByteSize::from_str("1 GB").unwrap();
/// assert_eq!(1_000_000_000, size.as_bytes_u64());
/// assert_eq!("1.00 GB", size.as_human_string());
/// assert_eq!("1.00 GB", format!("{}", size));
/// ```
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct IggyByteSize(Byte);

impl Default for IggyByteSize {
    fn default() -> Self {
        Self(Byte::from_u64(0))
    }
}

impl IggyByteSize {
    pub const fn new(bytes: u64) -> Self {
        Self(Byte::from_u64(bytes))
    }

    /// Returns the byte size as a `u64`.
    pub fn as_bytes_u64(&self) -> u64 {
        self.0.as_u64()
    }

    /// Returns the byte size as a `u32`.
    pub fn as_bytes_u32(&self) -> u32 {
        self.as_bytes_u64() as u32
    }

    /// Returns the byte size as a `usize`.
    pub fn as_bytes_usize(&self) -> usize {
        self.as_bytes_u64() as usize
    }

    /// Returns a human-readable string representation of the byte size using decimal units.
    pub fn as_human_string(&self) -> String {
        format!("{:.2}", self.0.get_appropriate_unit(UnitType::Decimal))
    }

    /// Returns a human-readable string representation of the byte size.
    /// Returns "unlimited" if the size is zero.
    pub fn as_human_string_with_zero_as_unlimited(&self) -> String {
        if self.as_bytes_u64() == 0 {
            return "unlimited".to_string();
        }
        format!("{:.2}", self.0.get_appropriate_unit(UnitType::Decimal))
    }

    /// Calculates the throughput based on the provided duration and returns a human-readable string.
    pub(crate) fn _as_human_throughput_string(&self, duration: &IggyDuration) -> String {
        if duration.is_zero() {
            return "0 B/s".to_string();
        }
        let seconds = duration.as_secs_f64();
        let normalized_bytes_per_second = Self::from((self.as_bytes_u64() as f64 / seconds) as u64);
        format!("{normalized_bytes_per_second}/s")
    }
}

/// Converts a `u64` bytes to `IggyByteSize`.
impl From<u64> for IggyByteSize {
    fn from(byte_size: u64) -> Self {
        IggyByteSize(Byte::from_u64(byte_size))
    }
}

/// Converts an `Option<u64>` bytes to `IggyByteSize`.
impl From<Option<u64>> for IggyByteSize {
    fn from(byte_size: Option<u64>) -> Self {
        match byte_size {
            Some(value) => IggyByteSize(Byte::from_u64(value)),
            None => IggyByteSize(Byte::from_u64(0)),
        }
    }
}

/// Converts a `usize` bytes to `IggyByteSize`.
impl From<usize> for IggyByteSize {
    fn from(byte_size: usize) -> Self {
        IggyByteSize(Byte::from_u64(byte_size as u64))
    }
}

/// Converts an `Option<usize>` to `IggyByteSize` bytes.
impl From<Option<usize>> for IggyByteSize {
    fn from(bytes_size: Option<usize>) -> Self {
        match bytes_size {
            Some(value) => IggyByteSize(Byte::from_u64(value as u64)),
            None => IggyByteSize(Byte::from_u64(0)),
        }
    }
}

impl FromStr for IggyByteSize {
    type Err = IggyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if matches!(s, "0" | "unlimited" | "Unlimited" | "none" | "None") {
            Ok(IggyByteSize(Byte::from_u64(0)))
        } else {
            Ok(IggyByteSize(
                Byte::from_str(s).map_err(|_| IggyError::InvalidSizeBytes)?,
            ))
        }
    }
}

impl PartialEq<u64> for IggyByteSize {
    fn eq(&self, other: &u64) -> bool {
        self.as_bytes_u64() == *other
    }
}

impl PartialOrd<u64> for IggyByteSize {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.as_bytes_u64().partial_cmp(other)
    }
}

impl PartialOrd<IggyByteSize> for IggyByteSize {
    fn partial_cmp(&self, other: &IggyByteSize) -> Option<std::cmp::Ordering> {
        self.as_bytes_u64().partial_cmp(&other.as_bytes_u64())
    }
}

impl fmt::Display for IggyByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_human_string())
    }
}

impl Add for IggyByteSize {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        IggyByteSize(Byte::from_u64(self.as_bytes_u64() + rhs.as_bytes_u64()))
    }
}

impl AddAssign for IggyByteSize {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = Byte::from_u64(self.as_bytes_u64() + rhs.as_bytes_u64());
    }
}

impl Sub for IggyByteSize {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        IggyByteSize(Byte::from_u64(self.as_bytes_u64() - rhs.as_bytes_u64()))
    }
}

impl SubAssign for IggyByteSize {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 = Byte::from_u64(self.as_bytes_u64() - rhs.as_bytes_u64());
    }
}

impl Sum for IggyByteSize {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(IggyByteSize::default(), |acc, ibs| acc + ibs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_u64_ok() {
        let byte_size = IggyByteSize::from(123456789);
        assert_eq!(byte_size.as_bytes_u64(), 123456789);
    }

    #[test]
    fn test_from_u64_zero() {
        let byte_size = IggyByteSize::from(0);
        assert_eq!(byte_size.as_bytes_u64(), 0);
    }

    #[test]
    fn test_from_str_ok() {
        let byte_size = IggyByteSize::from_str("123456789").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 123456789);
    }

    #[test]
    fn test_from_str_zero() {
        let byte_size = IggyByteSize::from_str("0").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 0);
    }

    #[test]
    fn test_from_str_invalid() {
        let byte_size = IggyByteSize::from_str("invalid");
        assert!(byte_size.is_err());
    }

    #[test]
    fn test_from_str_gigabyte() {
        let byte_size = IggyByteSize::from_str("1 GiB").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 1024 * 1024 * 1024);

        let byte_size = IggyByteSize::from_str("1 GB").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 1000 * 1000 * 1000);
    }

    #[test]
    fn test_from_str_megabyte() {
        let byte_size = IggyByteSize::from_str("1 MiB").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 1024 * 1024);

        let byte_size = IggyByteSize::from_str("1 MB").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 1000 * 1000);
    }

    #[test]
    fn test_to_human_string_ok() {
        let byte_size = IggyByteSize::from(1_073_000_000);
        assert_eq!(byte_size.as_human_string(), "1.07 GB");
    }

    #[test]
    fn test_to_human_string_zero() {
        let byte_size = IggyByteSize::from(0);
        assert_eq!(byte_size.as_human_string(), "0 B");
    }

    #[test]
    fn test_to_human_string_special_zero() {
        let byte_size = IggyByteSize::from(0);
        assert_eq!(
            byte_size.as_human_string_with_zero_as_unlimited(),
            "unlimited"
        );
    }

    #[test]
    fn test_throughput_ok() {
        let byte_size = IggyByteSize::from(1_073_000_000);
        let duration = IggyDuration::from_str("10s").unwrap();
        assert_eq!(
            byte_size._as_human_throughput_string(&duration),
            "107.30 MB/s"
        );
    }

    #[test]
    fn test_throughput_zero_size() {
        let byte_size = IggyByteSize::from(0);
        let duration = IggyDuration::from_str("10s").unwrap();
        assert_eq!(byte_size._as_human_throughput_string(&duration), "0 B/s");
    }

    #[test]
    fn test_throughput_zero_duration() {
        let byte_size = IggyByteSize::from(1_073_000_000);
        let duration = IggyDuration::from_str("0s").unwrap();
        assert_eq!(byte_size._as_human_throughput_string(&duration), "0 B/s");
    }

    #[test]
    fn test_throughput_very_low() {
        let byte_size = IggyByteSize::from(8);
        let duration = IggyDuration::from_str("1s").unwrap();
        assert_eq!(byte_size._as_human_throughput_string(&duration), "8 B/s");
    }

    #[test]
    fn test_throughput_very_high() {
        let byte_size = IggyByteSize::from(u64::MAX);
        let duration = IggyDuration::from_str("1s").unwrap();
        assert_eq!(
            byte_size._as_human_throughput_string(&duration),
            "18.45 EB/s"
        );
    }

    #[test]
    fn test_order() {
        assert!(IggyByteSize::from(u64::MAX) > IggyByteSize::from(u64::MIN))
    }

    #[test]
    fn test_sum() {
        let r = 1..10;
        assert_eq!(
            r.clone().sum::<u64>(),
            r.map(IggyByteSize::from)
                .sum::<IggyByteSize>()
                .as_bytes_u64()
        );
    }
}
