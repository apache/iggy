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
use crate::{BytesSerializable, Command, GET_CLUSTER_METADATA_CODE, Validatable};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `GetClusterMetadata` command is used to retrieve cluster metadata including
/// available nodes, their roles, and connection information.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct GetClusterMetadata {}

impl Command for GetClusterMetadata {
    fn code(&self) -> u32 {
        GET_CLUSTER_METADATA_CODE
    }
}

impl Validatable<IggyError> for GetClusterMetadata {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for GetClusterMetadata {
    fn to_bytes(&self) -> Bytes {
        Bytes::new()
    }

    fn from_bytes(bytes: Bytes) -> Result<GetClusterMetadata, IggyError> {
        if !bytes.is_empty() {
            return Err(IggyError::InvalidCommand);
        }
        Ok(GetClusterMetadata {})
    }
}

impl Display for GetClusterMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GetClusterMetadata")
    }
}
