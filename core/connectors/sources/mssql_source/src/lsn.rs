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

use serde::{Deserialize, Serialize};
use std::fmt;

const LSN_LENGTH: usize = 10;

/// A `binary(10)` log sequence number or seqval. Derived `Ord` compares bytes big-endian, as SQL Server does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct Lsn([u8; LSN_LENGTH]);

impl Lsn {
    pub(crate) const ZERO: Lsn = Lsn([0; LSN_LENGTH]);
    pub(crate) const MAX: Lsn = Lsn([0xFF; LSN_LENGTH]);

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn is_zero(&self) -> bool {
        self == &Self::ZERO
    }
}

/// A value read as an LSN did not have 10 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InvalidLsnLength(pub(crate) usize);

impl fmt::Display for InvalidLsnLength {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "expected {LSN_LENGTH} bytes, got {}", self.0)
    }
}

impl TryFrom<&[u8]> for Lsn {
    type Error = InvalidLsnLength;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; LSN_LENGTH] = bytes
            .try_into()
            .map_err(|_| InvalidLsnLength(bytes.len()))?;
        Ok(Self(bytes))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("0x")?;
        for byte in self.0 {
            write!(formatter, "{byte:02X}")?;
        }
        Ok(())
    }
}

/// Position of one change. Derived `Ord` gives commit order across tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct Position {
    pub(crate) start_lsn: Lsn,
    pub(crate) seqval: Lsn,
}

impl Position {
    pub(crate) fn new(start_lsn: Lsn, seqval: Lsn) -> Self {
        Self { start_lsn, seqval }
    }
}

impl fmt::Display for Position {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "start_lsn {} seqval {}",
            self.start_lsn, self.seqval
        )
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::{LSN_LENGTH, Lsn, Position};

    /// An LSN whose last byte is `value`.
    pub(crate) fn lsn(value: u8) -> Lsn {
        let mut bytes = [0u8; LSN_LENGTH];
        bytes[LSN_LENGTH - 1] = value;
        Lsn(bytes)
    }

    pub(crate) fn position(start_lsn: u8, seqval: u8) -> Position {
        Position::new(lsn(start_lsn), lsn(seqval))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lsn(bytes: [u8; LSN_LENGTH]) -> Lsn {
        Lsn(bytes)
    }

    #[test]
    fn given_lsn_should_display_as_uppercase_hex() {
        let value = lsn([0x00, 0x00, 0x00, 0x2C, 0x00, 0x00, 0x02, 0x40, 0x00, 0xAB]);

        assert_eq!(value.to_string(), "0x0000002C0000024000AB");
    }

    #[test]
    fn given_wrong_length_should_reject_lsn() {
        assert_eq!(Lsn::try_from([0u8; 9].as_slice()), Err(InvalidLsnLength(9)));
        assert!(Lsn::try_from([0u8; 10].as_slice()).is_ok());
    }

    #[test]
    fn given_bytes_should_compare_big_endian() {
        let mut higher = [0u8; LSN_LENGTH];
        higher[3] = 1;
        let mut lower = [0u8; LSN_LENGTH];
        lower[9] = 0xFF;

        assert!(lsn(higher) > lsn(lower));
        assert!(Lsn::ZERO < Lsn::MAX);
        assert!(Lsn::ZERO.is_zero());
    }

    #[test]
    fn given_interleaved_transactions_should_order_by_start_lsn_before_seqval() {
        // The transaction that started first commits second: higher start LSN, lower seqval.
        let committed_second = Position::new(
            lsn([0, 0, 0, 0x2C, 0, 0, 0x02, 0x58, 0, 0x03]),
            lsn([0, 0, 0, 0x2C, 0, 0, 0x02, 0x40, 0, 0x02]),
        );
        let committed_first = Position::new(
            lsn([0, 0, 0, 0x2C, 0, 0, 0x02, 0x50, 0, 0x03]),
            lsn([0, 0, 0, 0x2C, 0, 0, 0x02, 0x48, 0, 0x02]),
        );

        assert!(committed_first < committed_second);
    }
}
