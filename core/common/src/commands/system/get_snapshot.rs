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

use crate::error::IggyError;
use crate::BytesSerializable;
use crate::Validatable;
use crate::{Command, GET_SNAPSHOT_FILE_CODE};
use crate::{SnapshotCompression, SystemSnapshotType};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use tracing::error;

/// `GetSnapshot` command is used to get snapshot information.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct GetSnapshot {
    pub snapshot_types: Vec<SystemSnapshotType>,
    pub compression: SnapshotCompression,
}

impl Default for GetSnapshot {
    fn default() -> Self {
        let types = vec![
            SystemSnapshotType::FilesystemOverview,
            SystemSnapshotType::ProcessList,
            SystemSnapshotType::ResourceUsage,
            SystemSnapshotType::ServerLogs,
        ];
        Self {
            compression: SnapshotCompression::Deflated,
            snapshot_types: types,
        }
    }
}

impl Command for GetSnapshot {
    fn code(&self) -> u32 {
        GET_SNAPSHOT_FILE_CODE
    }
}

impl Validatable<IggyError> for GetSnapshot {
    fn validate(&self) -> Result<(), IggyError> {
        if self.snapshot_types.contains(&SystemSnapshotType::All) && self.snapshot_types.len() > 1 {
            error!("When using 'All' snapshot type, no other types can be specified");
            return Err(IggyError::InvalidCommand);
        }
        Ok(())
    }
}

impl BytesSerializable for GetSnapshot {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(self.compression.as_code());

        bytes.put_u8(self.snapshot_types.len() as u8);
        for snapshot_type in &self.snapshot_types {
            bytes.put_u8(snapshot_type.as_code());
        }

        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<GetSnapshot, IggyError> {
        let mut index = 0;

        let compression =
            SnapshotCompression::from_code(*bytes.get(index).ok_or(IggyError::InvalidCommand)?)?;
        index += 1;

        let types_count = *bytes.get(index).ok_or(IggyError::InvalidCommand)? as usize;
        index += 1;

        let mut snapshot_types = Vec::with_capacity(types_count);

        for _ in 0..types_count {
            let tool =
                SystemSnapshotType::from_code(*bytes.get(index).ok_or(IggyError::InvalidCommand)?)?;
            index += 1;

            snapshot_types.push(tool);
        }

        Ok(GetSnapshot {
            compression,
            snapshot_types,
        })
    }
}

impl Display for GetSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GetSnapshotFile {{\n  snapshot_types: [\n")?;
        for snapshot_type in &self.snapshot_types {
            writeln!(f, "    {}", snapshot_type)?;
        }
        write!(f, "  ],\n  Compression: {}\n}}", self.compression)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let get_snapshot_file_command = GetSnapshot {
            compression: SnapshotCompression::Deflated,
            snapshot_types: vec![SystemSnapshotType::FilesystemOverview],
        };

        let bytes = get_snapshot_file_command.to_bytes();

        let deserialized = GetSnapshot::from_bytes(bytes.clone()).unwrap();

        assert!(!bytes.is_empty());

        assert_eq!(
            deserialized.compression,
            get_snapshot_file_command.compression
        );
        assert_eq!(
            deserialized.snapshot_types,
            get_snapshot_file_command.snapshot_types
        );
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let types = vec![SystemSnapshotType::FilesystemOverview];

        let mut bytes = BytesMut::new();
        bytes.put_u8(SnapshotCompression::Deflated.as_code());
        bytes.put_u8(1);

        bytes.put_u8(types.len() as u8);
        for t in &types {
            bytes.put_u8(t.as_code());
        }

        let get_snapshot = GetSnapshot::from_bytes(bytes.freeze());
        assert!(get_snapshot.is_ok());

        let get_snapshot = get_snapshot.unwrap();
        assert_eq!(get_snapshot.snapshot_types.len(), 1);
        assert_eq!(get_snapshot.compression, SnapshotCompression::Deflated);
        assert_eq!(
            get_snapshot.snapshot_types[0],
            SystemSnapshotType::FilesystemOverview
        );
    }
}
