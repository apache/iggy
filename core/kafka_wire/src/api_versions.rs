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

//! `ApiVersions` requests up to v5, one version past what `kafka-protocol` decodes.
//!
//! `kafka-protocol` 0.18 stops at v4. v5 (KIP-1242) adds `ClusterId` and `NodeId` to the request:
//! the broker that the client means to reach. The response gains no field, so the v4 encoder
//! writes a v5 response body.

use bytes::Bytes;
use kafka_protocol::messages::ApiVersionsRequest;
use kafka_protocol::protocol::Decodable;
use kafka_protocol::protocol::buf::ByteBuf;

use crate::bounds_guard::validate_api_versions_shape;
use crate::codec::decode_guarded;
use crate::error::Result;

/// Highest `ApiVersions` version that `kafka-protocol` decodes and encodes.
pub const CODEC_MAX_VERSION: i16 = 4;

/// The broker that an `ApiVersions` v5 request names (KIP-1242).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntendedBroker {
    pub cluster_id: String,
    pub node_id: i32,
}

/// Decodes an `ApiVersions` request at `version`, and returns the broker that it names.
///
/// Only v5 names one, and only with a `ClusterId`. A Java client from 4.4 sends both fields when
/// its metadata knows the cluster, and neither on a connection to a bootstrap server.
///
/// # Errors
///
/// [`crate::KafkaWireError::Malformed`] when the body does not decode at `version`.
pub fn decode_request(version: i16, body: Bytes) -> Result<Option<IntendedBroker>> {
    if version <= CODEC_MAX_VERSION {
        decode_guarded::<ApiVersionsRequest>(version, body, validate_api_versions_shape)?;
        return Ok(None);
    }
    let request = decode_guarded::<RequestV5>(version, body, validate_api_versions_shape)?;
    Ok(request.cluster_id.map(|cluster_id| IntendedBroker {
        cluster_id,
        node_id: request.node_id,
    }))
}

/// The v5 fields after the client software name and version.
struct RequestV5 {
    cluster_id: Option<String>,
    node_id: i32,
}

impl Decodable for RequestV5 {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> anyhow::Result<Self> {
        anyhow::ensure!(version == 5, "ApiVersions v{version} has no v5 fields");
        anyhow::ensure!(compact_string(buf)?.is_some(), "ClientSoftwareName is null");
        anyhow::ensure!(
            compact_string(buf)?.is_some(),
            "ClientSoftwareVersion is null"
        );
        let cluster_id = compact_string(buf)?;
        let node_id = buf.try_get_i32()?;
        skip_tagged_fields(buf)?;
        Ok(Self {
            cluster_id,
            node_id,
        })
    }
}

/// A compact nullable string: a varint of the byte count plus one, where 0 means null.
fn compact_string<B: ByteBuf>(buf: &mut B) -> anyhow::Result<Option<String>> {
    let Some(len) = unsigned_varint(buf)?.checked_sub(1) else {
        return Ok(None);
    };
    let bytes = buf.try_get_bytes(usize::try_from(len)?)?;
    Ok(Some(String::from_utf8(bytes.to_vec())?))
}

fn skip_tagged_fields<B: ByteBuf>(buf: &mut B) -> anyhow::Result<()> {
    for _ in 0..unsigned_varint(buf)? {
        let _tag = unsigned_varint(buf)?;
        let size = unsigned_varint(buf)?;
        buf.try_get_bytes(usize::try_from(size)?)?;
    }
    Ok(())
}

/// The varint reader of `kafka-protocol`: at most 5 bytes, in `u32`. The bounds guard reads a
/// varint the same way, so its walk and this one stop at the same byte.
fn unsigned_varint<B: ByteBuf>(buf: &mut B) -> anyhow::Result<u32> {
    let mut value = 0_u32;
    for shift in [0, 7, 14, 21, 28] {
        let byte = buf.try_get_u8()?;
        value |= u32::from(byte & 0x7F) << shift;
        if byte < 0x80 {
            break;
        }
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use super::*;

    fn put_compact_string(body: &mut BytesMut, value: Option<&str>) {
        let Some(value) = value else {
            body.put_u8(0);
            return;
        };
        body.put_u8(u8::try_from(value.len() + 1).expect("a short test string"));
        body.put_slice(value.as_bytes());
    }

    fn v5_body(cluster_id: Option<&str>, node_id: i32, tagged_fields: &[u8]) -> Bytes {
        let mut body = BytesMut::new();
        put_compact_string(&mut body, Some("client"));
        put_compact_string(&mut body, Some("1"));
        put_compact_string(&mut body, cluster_id);
        body.put_i32(node_id);
        body.put_slice(tagged_fields);
        body.freeze()
    }

    #[test]
    fn given_v5_with_a_cluster_id_should_return_the_broker_it_names() {
        let named = decode_request(5, v5_body(Some("cluster-a"), 2, &[0])).unwrap();
        assert_eq!(
            named,
            Some(IntendedBroker {
                cluster_id: "cluster-a".to_owned(),
                node_id: 2,
            })
        );
    }

    #[test]
    fn given_v5_without_a_cluster_id_should_name_no_broker() {
        assert_eq!(decode_request(5, v5_body(None, -1, &[0])).unwrap(), None);
    }

    #[test]
    fn given_v5_with_an_unknown_tagged_field_should_skip_it() {
        // One tagged field: tag 7, two bytes of data.
        let body = v5_body(Some("cluster-a"), 2, &[1, 7, 2, 0xAB, 0xCD]);
        assert!(decode_request(5, body).unwrap().is_some());
    }

    #[test]
    fn given_a_version_below_v5_should_name_no_broker() {
        let mut v3 = BytesMut::new();
        put_compact_string(&mut v3, Some("client"));
        put_compact_string(&mut v3, Some("1"));
        v3.put_u8(0);
        let v3 = v3.freeze();
        assert_eq!(decode_request(3, v3.clone()).unwrap(), None);
        assert_eq!(decode_request(4, v3).unwrap(), None);
        assert_eq!(decode_request(0, Bytes::new()).unwrap(), None);
    }

    #[test]
    fn given_a_v5_body_with_trailing_bytes_should_be_malformed() {
        assert!(decode_request(5, v5_body(Some("cluster-a"), 2, &[0, 0])).is_err());
    }

    #[test]
    fn given_a_v4_body_at_v5_should_be_malformed() {
        let mut v4 = BytesMut::new();
        put_compact_string(&mut v4, Some("client"));
        put_compact_string(&mut v4, Some("1"));
        v4.put_u8(0);
        assert!(decode_request(5, v4.freeze()).is_err());
    }

    #[test]
    fn given_a_cluster_id_that_is_not_utf8_should_be_malformed() {
        let mut body = BytesMut::new();
        put_compact_string(&mut body, Some("client"));
        put_compact_string(&mut body, Some("1"));
        body.put_slice(&[3, 0xFF, 0xFE]);
        body.put_i32(2);
        body.put_u8(0);
        assert!(decode_request(5, body.freeze()).is_err());
    }

    #[test]
    fn given_v6_should_be_refused() {
        assert!(decode_request(6, v5_body(Some("cluster-a"), 2, &[0])).is_err());
    }
}
