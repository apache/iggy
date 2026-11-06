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

use std::fmt;

use super::COMMAND_BYTES_MAX;
use super::registry::CommandKind;

/// Bytes ahead of a command's payload: the kind and reserved padding that
/// keeps the payload 8-byte aligned inside the prepare body.
pub const COMMAND_HEADER_SIZE: usize = 8;

/// A state-machine command: the body of a prepare with
/// [`Operation::StateMachineCommand`], borrowing its payload.
///
/// [`Operation::StateMachineCommand`]: iggy_binary_protocol::Operation::StateMachineCommand
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MachineCommand<'a> {
    pub kind: CommandKind,
    pub payload: &'a [u8],
}

impl<'a> MachineCommand<'a> {
    /// # Errors
    /// Returns an error for a body shorter than its header, padding that is not
    /// zero, or a body above [`COMMAND_BYTES_MAX`].
    pub fn decode(body: &'a [u8]) -> Result<Self, CommandDecodeError> {
        if body.len() > COMMAND_BYTES_MAX {
            return Err(CommandDecodeError::TooLarge { length: body.len() });
        }
        let Some((header, payload)) = body.split_at_checked(COMMAND_HEADER_SIZE) else {
            return Err(CommandDecodeError::Short { length: body.len() });
        };
        let [low, high, reserved @ ..] = header else {
            return Err(CommandDecodeError::Short { length: body.len() });
        };
        if reserved.iter().any(|byte| *byte != 0) {
            return Err(CommandDecodeError::ReservedBytes);
        }
        Ok(Self {
            kind: CommandKind(u16::from_le_bytes([*low, *high])),
            payload,
        })
    }

    /// The prepare body of a command of `kind` carrying `payload`.
    ///
    /// # Errors
    /// Returns an error when the body would exceed [`COMMAND_BYTES_MAX`].
    pub fn encode(kind: CommandKind, payload: &[u8]) -> Result<Vec<u8>, CommandEncodeError> {
        let length = COMMAND_HEADER_SIZE + payload.len();
        if length > COMMAND_BYTES_MAX {
            return Err(CommandEncodeError::TooLarge { length });
        }
        let mut body = Vec::with_capacity(length);
        body.extend_from_slice(&kind.0.to_le_bytes());
        body.resize(COMMAND_HEADER_SIZE, 0);
        body.extend_from_slice(payload);
        Ok(body)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandDecodeError {
    Short { length: usize },
    TooLarge { length: usize },
    ReservedBytes,
}

impl fmt::Display for CommandDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Short { length } => {
                write!(
                    f,
                    "command body of {length} bytes is shorter than its header"
                )
            }
            Self::TooLarge { length } => write!(
                f,
                "command body of {length} bytes exceeds the {COMMAND_BYTES_MAX} byte limit"
            ),
            Self::ReservedBytes => f.write_str("command header padding is not zero"),
        }
    }
}

impl std::error::Error for CommandDecodeError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandEncodeError {
    TooLarge { length: usize },
}

impl fmt::Display for CommandEncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { length } => write!(
                f,
                "command of {length} bytes exceeds the {COMMAND_BYTES_MAX} byte limit"
            ),
        }
    }
}

impl std::error::Error for CommandEncodeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_payload_when_round_tripping_should_keep_kind_and_bytes() {
        let body = MachineCommand::encode(CommandKind(7), b"floor").unwrap();
        assert_eq!(body.len(), COMMAND_HEADER_SIZE + 5);
        let command = MachineCommand::decode(&body).unwrap();
        assert_eq!(command.kind, CommandKind(7));
        assert_eq!(command.payload, b"floor");
    }

    #[test]
    fn given_an_empty_payload_when_decoding_should_accept_the_bare_header() {
        let body = MachineCommand::encode(CommandKind(u16::MAX), &[]).unwrap();
        let command = MachineCommand::decode(&body).unwrap();
        assert_eq!(command.kind, CommandKind(u16::MAX));
        assert_eq!(command.payload, b"");
    }

    #[test]
    fn given_malformed_bodies_when_decoding_should_refuse_them() {
        assert_eq!(
            MachineCommand::decode(&[1, 0, 0]),
            Err(CommandDecodeError::Short { length: 3 })
        );
        let mut body = MachineCommand::encode(CommandKind(1), b"x").unwrap();
        body[size_of::<u16>()] = 1;
        assert_eq!(
            MachineCommand::decode(&body),
            Err(CommandDecodeError::ReservedBytes)
        );
        let oversized = vec![0; COMMAND_BYTES_MAX + 1];
        assert_eq!(
            MachineCommand::decode(&oversized),
            Err(CommandDecodeError::TooLarge {
                length: COMMAND_BYTES_MAX + 1
            })
        );
        assert_eq!(
            MachineCommand::encode(CommandKind(1), &oversized[COMMAND_HEADER_SIZE..]),
            Err(CommandEncodeError::TooLarge {
                length: COMMAND_BYTES_MAX + 1
            })
        );
    }
}
