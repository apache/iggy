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

use crate::{
    BytesSerializable, IggyError,
    types::cluster::{
        role::ClusterNodeRole, status::ClusterNodeStatus, transport_endpoints::TransportEndpoints,
    },
};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ClusterNode {
    pub name: String,
    pub ip: String,
    pub endpoints: TransportEndpoints,
    pub role: ClusterNodeRole,
    pub status: ClusterNodeStatus,
}

impl BytesSerializable for ClusterNode {
    fn to_bytes(&self) -> Bytes {
        let size = self.get_buffer_size();
        let mut bytes = BytesMut::with_capacity(size);
        self.write_to_buffer(&mut bytes);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        if bytes.len() < 10 {
            // Minimum: 4 (name_len) + 4 (ip_len) + 1 (role) + 1 (status)
            return Err(IggyError::InvalidCommand);
        }

        let mut position = 0;

        // Read name length
        let name_len = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        ) as usize;
        position += 4;

        // Read name
        if bytes.len() < position + name_len {
            return Err(IggyError::InvalidCommand);
        }
        let name = String::from_utf8(bytes[position..position + name_len].to_vec())
            .map_err(|_| IggyError::InvalidCommand)?;
        position += name_len;

        // Read IP length
        let ip_len = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        ) as usize;
        position += 4;

        // Read IP
        if bytes.len() < position + ip_len {
            return Err(IggyError::InvalidCommand);
        }
        let ip = String::from_utf8(bytes[position..position + ip_len].to_vec())
            .map_err(|_| IggyError::InvalidCommand)?;
        position += ip_len;

        // Read transport endpoints
        let endpoints_bytes = bytes.slice(position..);
        let endpoints = TransportEndpoints::from_bytes(endpoints_bytes)?;
        position += endpoints.get_buffer_size();

        // Read role
        if bytes.len() < position + 1 {
            return Err(IggyError::InvalidCommand);
        }
        let role = ClusterNodeRole::try_from(bytes[position])?;
        position += 1;

        // Read status
        if bytes.len() < position + 1 {
            return Err(IggyError::InvalidCommand);
        }
        let status = ClusterNodeStatus::try_from(bytes[position])?;

        Ok(ClusterNode {
            name,
            ip,
            endpoints,
            role,
            status,
        })
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.name.len() as u32);
        buf.put_slice(self.name.as_bytes());
        buf.put_u32_le(self.ip.len() as u32);
        buf.put_slice(self.ip.as_bytes());
        self.endpoints.write_to_buffer(buf);
        self.role.write_to_buffer(buf);
        self.status.write_to_buffer(buf);
    }

    fn get_buffer_size(&self) -> usize {
        4 + self.name.len() + 4 + self.ip.len() + self.endpoints.get_buffer_size() + 1 + 1 // name_len + name + ip_len + ip + endpoints + role + status
    }
}

impl Display for ClusterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClusterNode {{ name: {}, ip: {}, endpoints: {}, role: {}, status: {} }}",
            self.name, self.ip, self.endpoints, self.role, self.status
        )
    }
}
