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
use crate::codec::{WireDecode, WireEncode, read_str, read_u8, read_u16_le, read_u32_le};
use bytes::{BufMut, BytesMut};

/// `GetClusterMetadata` response: cluster name and list of nodes.
///
/// Wire format:
/// ```text
/// [name_len:4 LE][name:N][nodes_count:4 LE][ClusterNodeResponse]*
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMetadataResponse {
    pub name: String,
    pub nodes: Vec<ClusterNodeResponse>,
}

/// A single node within the cluster metadata.
///
/// Wire format:
/// ```text
/// [name_len:4 LE][name:N][ip_len:4 LE][ip:N]
/// [tcp:2 LE][quic:2 LE][http:2 LE][websocket:2 LE]
/// [role:1][status:1]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNodeResponse {
    pub name: String,
    pub ip: String,
    pub tcp_port: u16,
    pub quic_port: u16,
    pub http_port: u16,
    pub websocket_port: u16,
    pub role: u8,
    pub status: u8,
}

const NODE_FIXED_SIZE: usize = 4 + 4 + 8 + 1 + 1; // name_len + ip_len + ports + role + status

impl WireEncode for ClusterNodeResponse {
    fn encoded_size(&self) -> usize {
        NODE_FIXED_SIZE + self.name.len() + self.ip.len()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.name.len() as u32);
        buf.put_slice(self.name.as_bytes());
        buf.put_u32_le(self.ip.len() as u32);
        buf.put_slice(self.ip.as_bytes());
        buf.put_u16_le(self.tcp_port);
        buf.put_u16_le(self.quic_port);
        buf.put_u16_le(self.http_port);
        buf.put_u16_le(self.websocket_port);
        buf.put_u8(self.role);
        buf.put_u8(self.status);
    }
}

impl WireDecode for ClusterNodeResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let name_len = read_u32_le(buf, 0)? as usize;
        let name = read_str(buf, 4, name_len)?;
        let pos = 4 + name_len;

        let ip_len = read_u32_le(buf, pos)? as usize;
        let ip = read_str(buf, pos + 4, ip_len)?;
        let pos = pos + 4 + ip_len;

        let tcp_port = read_u16_le(buf, pos)?;
        let quic_port = read_u16_le(buf, pos + 2)?;
        let http_port = read_u16_le(buf, pos + 4)?;
        let websocket_port = read_u16_le(buf, pos + 6)?;
        let role = read_u8(buf, pos + 8)?;
        let status = read_u8(buf, pos + 9)?;

        Ok((
            Self {
                name,
                ip,
                tcp_port,
                quic_port,
                http_port,
                websocket_port,
                role,
                status,
            },
            pos + 10,
        ))
    }
}

impl WireEncode for ClusterMetadataResponse {
    fn encoded_size(&self) -> usize {
        4 + self.name.len()
            + 4
            + self
                .nodes
                .iter()
                .map(WireEncode::encoded_size)
                .sum::<usize>()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.name.len() as u32);
        buf.put_slice(self.name.as_bytes());
        buf.put_u32_le(self.nodes.len() as u32);
        for node in &self.nodes {
            node.encode(buf);
        }
    }
}

impl WireDecode for ClusterMetadataResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let name_len = read_u32_le(buf, 0)? as usize;
        let name = read_str(buf, 4, name_len)?;
        let mut pos = 4 + name_len;

        let nodes_count = read_u32_le(buf, pos)? as usize;
        pos += 4;

        let remaining = buf.len().saturating_sub(pos);
        let mut nodes = Vec::with_capacity(crate::codec::capped_capacity(
            nodes_count,
            remaining,
            NODE_FIXED_SIZE,
        ));
        for _ in 0..nodes_count {
            let (node, consumed) = ClusterNodeResponse::decode(&buf[pos..])?;
            pos += consumed;
            nodes.push(node);
        }

        Ok((Self { name, nodes }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_node(name: &str, ip: &str) -> ClusterNodeResponse {
        ClusterNodeResponse {
            name: name.to_string(),
            ip: ip.to_string(),
            tcp_port: 8090,
            quic_port: 8091,
            http_port: 3000,
            websocket_port: 3001,
            role: 1,
            status: 1,
        }
    }

    #[test]
    fn node_roundtrip() {
        let node = sample_node("node-1", "192.168.1.1");
        let bytes = node.to_bytes();
        let (decoded, consumed) = ClusterNodeResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, node);
    }

    #[test]
    fn roundtrip_no_nodes() {
        let resp = ClusterMetadataResponse {
            name: "test-cluster".to_string(),
            nodes: vec![],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = ClusterMetadataResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn roundtrip_with_nodes() {
        let resp = ClusterMetadataResponse {
            name: "prod-cluster".to_string(),
            nodes: vec![
                sample_node("node-1", "10.0.0.1"),
                sample_node("node-2", "10.0.0.2"),
                sample_node("node-3", "10.0.0.3"),
            ],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = ClusterMetadataResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn truncated_returns_error() {
        let resp = ClusterMetadataResponse {
            name: "c".to_string(),
            nodes: vec![sample_node("n", "1.2.3.4")],
        };
        let bytes = resp.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                ClusterMetadataResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn empty_cluster_name() {
        let resp = ClusterMetadataResponse {
            name: String::new(),
            nodes: vec![],
        };
        let bytes = resp.to_bytes();
        let (decoded, _) = ClusterMetadataResponse::decode(&bytes).unwrap();
        assert_eq!(decoded.name, "");
    }

    #[test]
    fn bogus_node_count_does_not_oom() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(4);
        buf.put_slice(b"test");
        buf.put_u32_le(u32::MAX);
        assert!(ClusterMetadataResponse::decode(&buf).is_err());
    }
}
