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

use crate::{BytesSerializable, IggyError, TransportProtocol, types::cluster::node::ClusterNode};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Represents metadata of all nodes in the cluster.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ClusterMetadata {
    /// Name of the cluster.
    pub name: String,
    /// Unique identifier of the cluster.
    pub id: u32,
    /// Transport used for cluster communication (for binary protocol it's u8, 1=TCP, 2=QUIC, 3=HTTP).
    /// For HTTP it's a string "tcp", "quic", "http".
    pub transport: TransportProtocol,
    /// List of all nodes in the cluster.
    pub nodes: Vec<ClusterNode>,
}

impl BytesSerializable for ClusterMetadata {
    fn to_bytes(&self) -> Bytes {
        let name_bytes = self.name.as_bytes();
        let transport = self.transport as u8;

        // Calculate size for each node
        let nodes_size: usize = self.nodes.iter().map(|node| node.get_buffer_size()).sum();
        let size = 4 + name_bytes.len() + 4 + 1 + 4 + nodes_size; // name_len + name + id + transport + nodes_len + nodes

        let mut bytes = BytesMut::with_capacity(size);

        // Write name length and name
        bytes.put_u32_le(name_bytes.len() as u32);
        bytes.put_slice(name_bytes);

        // Write cluster id
        bytes.put_u32_le(self.id);

        // Write transport
        bytes.put_u8(transport);

        // Write nodes count
        bytes.put_u32_le(self.nodes.len() as u32);

        // Write each node
        for node in &self.nodes {
            node.write_to_buffer(&mut bytes);
        }

        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<ClusterMetadata, IggyError> {
        if bytes.len() < 12 {
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

        // Read cluster id
        if bytes.len() < position + 4 {
            return Err(IggyError::InvalidCommand);
        }
        let id = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 4;

        // Read transport
        if bytes.len() < position + 1 {
            return Err(IggyError::InvalidCommand);
        }
        let transport_byte = bytes[position];
        let transport = TransportProtocol::try_from(transport_byte)?;
        position += 1;

        // Read nodes count
        if bytes.len() < position + 4 {
            return Err(IggyError::InvalidCommand);
        }
        let nodes_count = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        ) as usize;
        position += 4;

        // Read nodes
        let mut nodes = Vec::with_capacity(nodes_count);
        for _ in 0..nodes_count {
            let node = ClusterNode::from_bytes(bytes.slice(position..))?;
            position += node.get_buffer_size();
            nodes.push(node);
        }

        Ok(ClusterMetadata {
            name,
            id,
            transport,
            nodes,
        })
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        let name_bytes = self.name.as_bytes();
        let transport = self.transport as u8;

        // Write name length and name
        buf.put_u32_le(name_bytes.len() as u32);
        buf.put_slice(name_bytes);

        // Write cluster id
        buf.put_u32_le(self.id);

        // Write transport as u8
        buf.put_u8(transport);

        // Write nodes count
        buf.put_u32_le(self.nodes.len() as u32);

        // Write each node
        for node in &self.nodes {
            node.write_to_buffer(buf);
        }
    }

    fn get_buffer_size(&self) -> usize {
        let nodes_size: usize = self.nodes.iter().map(|node| node.get_buffer_size()).sum();
        4 + self.name.len() + 4 + 1 + 4 + nodes_size // name_len + name + id + transport + nodes_len + nodes
    }
}

impl Display for ClusterMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let nodes = self
            .nodes
            .iter()
            .map(|node| node.to_string())
            .collect::<Vec<_>>();

        write!(
            f,
            "ClusterMetadata {{ name: {}, id: {}, transport: {}, nodes: {:?} }}",
            self.name, self.id, self.transport, nodes
        )
    }
}
