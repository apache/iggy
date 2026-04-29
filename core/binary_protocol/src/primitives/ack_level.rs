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

use crate::WireError;

/// Acknowledgement policy for consumer-offset write commands.
///
/// Wire format: single `u8` discriminant.
/// - `NoAck(0)`:  leader-local write only; respond as soon as the in-memory
///   and on-disk state have been updated. Matches the fast path used by
///   `PollMessages` auto-commit.
/// - `Quorum(1)`: submit through the partition VSR consensus pipeline and
///   respond only after the write has been committed by a quorum of replicas.
///   This is the default for explicit client writes.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AckLevel {
    NoAck = 0,
    #[default]
    Quorum = 1,
}

impl AckLevel {
    /// Decode an `AckLevel` from its wire discriminant.
    ///
    /// # Errors
    /// Returns `WireError::UnknownDiscriminant` for unrecognised values.
    pub const fn from_code(code: u8) -> Result<Self, WireError> {
        match code {
            0 => Ok(Self::NoAck),
            1 => Ok(Self::Quorum),
            other => Err(WireError::UnknownDiscriminant {
                type_name: "AckLevel",
                value: other,
                offset: 0,
            }),
        }
    }

    /// Encode this `AckLevel` as its wire discriminant.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_no_ack() {
        let level = AckLevel::NoAck;
        let decoded = AckLevel::from_code(level.as_u8()).unwrap();
        assert_eq!(decoded, level);
    }

    #[test]
    fn roundtrip_quorum() {
        let level = AckLevel::Quorum;
        let decoded = AckLevel::from_code(level.as_u8()).unwrap();
        assert_eq!(decoded, level);
    }

    #[test]
    fn default_is_quorum() {
        assert_eq!(AckLevel::default(), AckLevel::Quorum);
    }

    #[test]
    fn discriminant_values() {
        assert_eq!(AckLevel::NoAck.as_u8(), 0);
        assert_eq!(AckLevel::Quorum.as_u8(), 1);
    }

    #[test]
    fn unknown_discriminant_rejected() {
        for code in 2u8..=u8::MAX {
            let err = AckLevel::from_code(code).unwrap_err();
            match err {
                WireError::UnknownDiscriminant {
                    type_name,
                    value,
                    offset,
                } => {
                    assert_eq!(type_name, "AckLevel");
                    assert_eq!(value, code);
                    assert_eq!(offset, 0);
                }
                other => panic!("unexpected error variant: {other:?}"),
            }
        }
    }
}
