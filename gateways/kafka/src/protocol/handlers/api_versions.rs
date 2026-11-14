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

//! `ApiVersions` (API key 18).

use bytes::Bytes;
use iggy_kafka_wire::api_versions::{CODEC_MAX_VERSION, IntendedBroker, decode_request};
use kafka_protocol::messages::ApiVersionsResponse;
use kafka_protocol::messages::api_versions_response::ApiVersion;

use crate::error::Result;
use crate::protocol::api::{
    API_KEY_API_VERSIONS, ApiVersionRange, ConnectionState, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_REBOOTSTRAP_REQUIRED, ERROR_UNSUPPORTED_VERSION, GatewayState, HandleOutcome,
    acl_advertised_ranges, advertised_min_version, is_supported_version, sasl_advertised_ranges,
    supported_api_ranges,
};
use crate::protocol::handlers::{encode_message, respond_or_close};

/// Java clients 4.0 to 4.3 open at v4, and 4.4 clients at v5, so both get an answer without a
/// KIP-511 retry.
///
/// v4 changes only how a broker reports a feature at version 0, and this gateway reports no
/// features. v5 names the broker that the client means to reach (KIP-1242).
pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_API_VERSIONS,
    min_version: 0,
    max_version: 5,
};

/// Which of the listener-dependent keys one connection is told about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Advertisement {
    /// `SaslHandshake` and `SaslAuthenticate`.
    pub sasl: bool,
    /// `DescribeAcls`.
    pub acls: bool,
}

impl Advertisement {
    /// Nothing listener-dependent: a plaintext listener with SASL off.
    pub const NONE: Self = Self {
        sasl: false,
        acls: false,
    };

    /// Both: a listener that runs SASL always names a principal.
    pub const SASL: Self = Self {
        sasl: true,
        acls: true,
    };
}

#[expect(
    clippy::unused_async,
    reason = "the shared handler signature, kept until a handler awaits the bridge"
)]
pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    if !is_supported_version(API_KEY_API_VERSIONS, api_version) {
        // KIP-511: reply with v0 when the requested version is not understood.
        return respond_or_close(
            encode_response(0, ERROR_UNSUPPORTED_VERSION, connection.advertisement()),
            "ApiVersions",
        );
    }
    let error_code = match decode_request(api_version, body) {
        Ok(Some(intended)) if names_another_broker(state, connection, &intended) => {
            tracing::debug!(
                cluster_id = intended.cluster_id,
                node_id = intended.node_id,
                gateway_cluster_id = %state.cluster_id,
                gateway_node_id = state.node_id,
                "ApiVersions names another broker, so the client must rebootstrap"
            );
            ERROR_REBOOTSTRAP_REQUIRED
        }
        Ok(_) => ERROR_NONE,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable (see the same
            // note on the Produce decode-failure arm).
            tracing::debug!(%error, "failed to decode ApiVersions request");
            ERROR_INVALID_REQUEST
        }
    };
    respond_or_close(
        encode_response(api_version, error_code, connection.advertisement()),
        "ApiVersions",
    )
}

/// Advertise exactly what the firewall allows, so a client never negotiates a version this
/// gateway then refuses.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `api_version`.
pub fn encode_response(
    api_version: i16,
    error_code: i16,
    advertisement: Advertisement,
) -> Result<Bytes> {
    // The SASL and ACL keys are advertised only on a listener that runs the exchange or names a
    // principal, and are deliberately kept out of `SUPPORTED_RANGES` so dispatch never serves
    // them. Elsewhere the state machine still answers them with `ILLEGAL_SASL_STATE` and keeps
    // the connection open.
    let sasl = if advertisement.sasl {
        sasl_advertised_ranges()
    } else {
        &[][..]
    };
    let acls = if advertisement.acls {
        acl_advertised_ranges()
    } else {
        &[][..]
    };
    // Ascending by api_key, as every real broker emits it. Chaining the SASL rows onto the end
    // would put key 17 after key 19, which no broker does and some clients do not expect.
    let mut rows: Vec<&ApiVersionRange> = supported_api_ranges()
        .iter()
        .chain(sasl)
        .chain(acls)
        .collect();
    rows.sort_unstable_by_key(|r| r.api_key);
    let api_keys = rows
        .into_iter()
        .map(|r| {
            ApiVersion::default()
                .with_api_key(r.api_key)
                .with_min_version(advertised_min_version(r.api_key, r.min_version))
                .with_max_version(r.max_version)
        })
        .collect();
    let resp = ApiVersionsResponse::default()
        .with_error_code(error_code)
        .with_api_keys(api_keys);
    // The v5 response has the fields of v4, and `kafka-protocol` encodes only up to v4.
    encode_message(&resp, api_version.min(CODEC_MAX_VERSION), 128)
}

/// KIP-1242: the client's metadata names another node or another cluster for this address. That
/// follows an address that moved to another gateway, or a gateway without the bridge that
/// restarted with a new random cluster id.
///
/// Gateways without the bridge each draw their own cluster id, so with several of them listed, a
/// client that learned the cluster from one names a cluster that the others do not share. Only
/// the node is compared there: comparing the cluster would answer 129 on every connection to the
/// other gateways, and the client would rebootstrap into the same answer.
fn names_another_broker(
    state: &GatewayState,
    connection: &ConnectionState,
    intended: &IntendedBroker,
) -> bool {
    if intended.node_id != state.node_id {
        return true;
    }
    let cluster_id_shared = state.bridge.is_some() || connection.brokers.peers.is_empty();
    cluster_id_shared && intended.cluster_id != state.cluster_id.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_v3_requires_software_fields() {
        assert!(decode_request(3, Bytes::new()).is_err());
    }

    #[test]
    fn decode_v3_accepts_valid_body() {
        // Hand-encoded rather than round-tripped through `ApiVersionsRequest::encode`: encoding
        // is gated behind the crate's "client" feature, which this broker-only binary doesn't
        // enable.
        let body = Bytes::from_static(&[
            0x0a, b'i', b'g', b'g', b'y', b'-', b't', b'e', b's',
            b't', // compact string (len 9)
            0x06, b'0', b'.', b'1', b'.', b'0', // compact string (len 5)
            0x00, // empty tagged fields
        ]);
        assert_eq!(decode_request(3, body).unwrap(), None);
    }
}
