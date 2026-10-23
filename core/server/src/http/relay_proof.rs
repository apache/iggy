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

//! Proof that a relayed partition request comes from a cluster node.
//!
//! A hop target acts on the route a relay carries: it takes a relay-target
//! slot, holds the request until it applied the route's metadata op, and
//! serves the route's partition under the route's context. A client could set
//! those fields itself, so the contacted node signs them with HMAC-SHA256
//! under a key every node derives from the cluster key material its bearers
//! already depend on, and the hop target refuses relay fields without a
//! valid proof made within one [`RELAY_PROOF_LIFETIME`] of its own clock.
//!
//! A proof covers the method, the path and query, the route and its own
//! expiry. It leaves out the bearer and the body: the hop target
//! authenticates and authorizes the request again, as for any caller.

use std::str::FromStr;
use std::time::Duration;

use axum::http::header::InvalidHeaderValue;
use axum::http::{HeaderValue, Method};
use iggy_common::{IggyDuration, IggyTimestamp};
use ring::hmac;

/// How long a relay proof stays valid, which is also the deadline of the
/// partition request it relays. Each attempt signs anew, the hop target checks
/// the proof when the request head arrives, and no attempt outlives the
/// request deadline, so an older proof could only serve a replay.
///
/// Expiry compares the wall clocks of two nodes, which are assumed to agree
/// within a few seconds (NTP). A hop target refuses a proof made more than
/// this lifetime before or after its own clock reading: one whose clock runs
/// ahead of the signer's by more than the lifetime, less the transit time,
/// refuses every relay of the signer as expired, and one whose clock lags by
/// more than the lifetime refuses them as early. Within that, a lagging hop
/// target accepts a proof for as much longer as it lags, two lifetimes at
/// most.
pub(in crate::http) const RELAY_PROOF_LIFETIME: Duration = Duration::from_secs(10);

/// BLAKE3 `derive_key` context that turns the cluster key material into the
/// relay proof key, independent of every other key derived from the same
/// material (the JWT signing key, the replica-auth MAC key).
///
/// Frozen string: it is KDF domain separation, not a label. Editing it derives
/// another key, and nodes on either side of the edit refuse each other's
/// relays.
const RELAY_KEY_CONTEXT: &str = "apache-iggy server http-relay v1 key->hmac-sha256-key";

/// HMAC key every node derives alike, so a proof one node makes verifies on
/// every node.
pub(in crate::http) struct RelayKey(hmac::Key);

/// Why a hop target refused relay fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::http) enum UnprovenRelay {
    /// No proof, an unreadable one, or one made for other fields or under
    /// another key.
    Invalid,
    /// A proof made for these fields under this key, expired this long ago
    /// by this node's clock.
    Expired(Duration),
    /// A proof made for these fields under this key, this far ahead of this
    /// node's clock, more than [`RELAY_PROOF_LIFETIME`].
    Early(Duration),
}

impl UnprovenRelay {
    /// The refusal as a hop target names it to the node whose relay it
    /// refused.
    pub(in crate::http) const fn as_str(self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::Expired(_) => "expired",
            Self::Early(_) => "early",
        }
    }
}

impl RelayKey {
    pub(in crate::http) fn derive(material: &str) -> Self {
        let key = blake3::derive_key(RELAY_KEY_CONTEXT, material.as_bytes());
        Self(hmac::Key::new(hmac::HMAC_SHA256, &key))
    }

    /// The proof of these relay fields, made at `now` and valid for
    /// [`RELAY_PROOF_LIFETIME`], as `<expiry in unix microseconds>,<hex HMAC>`.
    pub(in crate::http) fn prove(
        &self,
        method: &Method,
        path_and_query: &str,
        route: &HeaderValue,
        now: IggyTimestamp,
    ) -> Result<HeaderValue, InvalidHeaderValue> {
        let expires_at = (now + IggyDuration::from(RELAY_PROOF_LIFETIME)).as_micros();
        let tag = hmac::sign(
            &self.0,
            &signed_bytes(method, path_and_query, route, expires_at),
        );
        HeaderValue::try_from(format!("{expires_at},{}", hex::encode(tag)))
    }

    /// Accept `proof` only when it was made for these fields under this key,
    /// within one [`RELAY_PROOF_LIFETIME`] of `now` either way. Without the
    /// bound ahead, a signer whose clock runs ahead would make proofs that
    /// hold for as long as it runs ahead. The MAC is compared in constant
    /// time, and before the expiry, so a proof is reported expired or early
    /// only when it is genuine.
    pub(in crate::http) fn verify(
        &self,
        method: &Method,
        path_and_query: &str,
        route: &HeaderValue,
        proof: Option<&HeaderValue>,
        now: IggyTimestamp,
    ) -> Result<(), UnprovenRelay> {
        let (expires_at, tag) = proof.and_then(parse_proof).ok_or(UnprovenRelay::Invalid)?;
        hmac::verify(
            &self.0,
            &signed_bytes(method, path_and_query, route, expires_at),
            &tag,
        )
        .map_err(|_| UnprovenRelay::Invalid)?;
        let now = now.as_micros();
        if now > expires_at {
            return Err(UnprovenRelay::Expired(Duration::from_micros(
                now - expires_at,
            )));
        }
        let lifetime = IggyDuration::from(RELAY_PROOF_LIFETIME).as_micros();
        let ahead = expires_at.saturating_sub(lifetime).saturating_sub(now);
        if ahead > lifetime {
            return Err(UnprovenRelay::Early(Duration::from_micros(ahead)));
        }
        Ok(())
    }
}

/// A number of a relay header in the one form a node writes it: ASCII digits,
/// no sign and no leading zero. Integer `FromStr` also takes a leading `+` and
/// leading zeros, so one value would read from several strings.
pub(in crate::http) fn parse_decimal<T: FromStr>(field: &str) -> Option<T> {
    match field.as_bytes() {
        [] | [b'0', _, ..] => None,
        digits if digits.iter().all(u8::is_ascii_digit) => field.parse().ok(),
        _ => None,
    }
}

fn parse_proof(proof: &HeaderValue) -> Option<(u64, Vec<u8>)> {
    let (expires_at, tag) = proof.to_str().ok()?.split_once(',')?;
    if tag.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return None;
    }
    Some((parse_decimal(expires_at)?, hex::decode(tag).ok()?))
}

/// Every variable-length field goes in behind its length, so no two sets of
/// fields sign the same bytes.
fn signed_bytes(
    method: &Method,
    path_and_query: &str,
    route: &HeaderValue,
    expires_at: u64,
) -> Vec<u8> {
    let fields = [
        method.as_str().as_bytes(),
        path_and_query.as_bytes(),
        route.as_bytes(),
    ];
    let mut bytes = Vec::with_capacity(
        fields
            .iter()
            .map(|field| size_of::<u64>() + field.len())
            .sum::<usize>()
            + size_of::<u64>(),
    );
    for field in fields {
        bytes.extend_from_slice(&(field.len() as u64).to_le_bytes());
        bytes.extend_from_slice(field);
    }
    bytes.extend_from_slice(&expires_at.to_le_bytes());
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    const MATERIAL: &str = "0123456789abcdef0123456789abcdef";
    const PATH: &str = "/streams/1/topics/1/messages?partition_id=2";
    const NOW: u64 = 1_800_000_000_000_000;

    fn lifetime_micros() -> u64 {
        IggyDuration::from(RELAY_PROOF_LIFETIME).as_micros()
    }

    fn route() -> HeaderValue {
        HeaderValue::from_static("2,1,0,41,9900")
    }

    fn proof_at(key: &RelayKey, now: u64) -> HeaderValue {
        key.prove(&Method::POST, PATH, &route(), IggyTimestamp::from(now))
            .expect("a proof is a valid header value")
    }

    fn verify_at(key: &RelayKey, proof: &HeaderValue, now: u64) -> Result<(), UnprovenRelay> {
        key.verify(
            &Method::POST,
            PATH,
            &route(),
            Some(proof),
            IggyTimestamp::from(now),
        )
    }

    #[test]
    fn given_proof_when_verified_before_its_expiry_should_accept_and_after_it_should_refuse() {
        let key = RelayKey::derive(MATERIAL);
        let expires_at = NOW + lifetime_micros();
        let proof = proof_at(&key, NOW);
        assert_eq!(verify_at(&key, &proof, NOW), Ok(()));
        assert_eq!(verify_at(&key, &proof, expires_at), Ok(()));
        assert_eq!(
            verify_at(&key, &proof, expires_at + 1),
            Err(UnprovenRelay::Expired(Duration::from_micros(1)))
        );
    }

    #[test]
    fn given_proof_made_ahead_of_this_clock_when_verifying_should_refuse_it_beyond_one_lifetime() {
        let key = RelayKey::derive(MATERIAL);
        let lifetime = lifetime_micros();
        assert_eq!(
            verify_at(&key, &proof_at(&key, NOW + lifetime), NOW),
            Ok(())
        );
        assert_eq!(
            verify_at(&key, &proof_at(&key, NOW + lifetime + 1), NOW),
            Err(UnprovenRelay::Early(Duration::from_micros(lifetime + 1)))
        );
    }

    #[test]
    fn given_proof_when_any_relay_field_changes_should_refuse() {
        let key = RelayKey::derive(MATERIAL);
        let proof = proof_at(&key, NOW);
        let expires_at = NOW + lifetime_micros();
        let now = IggyTimestamp::from(NOW);
        let forged_frontier = HeaderValue::from_static("2,1,0,18446744073709551615,9900");
        let longer_budget = HeaderValue::from_static("2,1,0,41,99000");
        for (method, path_and_query, relayed_route) in [
            (Method::GET, PATH, route()),
            (
                Method::POST,
                "/streams/1/topics/1/messages?partition_id=3",
                route(),
            ),
            (Method::POST, PATH, forged_frontier),
            (Method::POST, PATH, longer_budget),
        ] {
            assert_eq!(
                key.verify(&method, path_and_query, &relayed_route, Some(&proof), now),
                Err(UnprovenRelay::Invalid),
                "{method} {path_and_query} {relayed_route:?}"
            );
        }
        let extended = proof.to_str().expect("an ASCII proof").replacen(
            &expires_at.to_string(),
            &(expires_at + 1).to_string(),
            1,
        );
        let extended = HeaderValue::try_from(extended).expect("a valid header value");
        assert_eq!(verify_at(&key, &extended, NOW), Err(UnprovenRelay::Invalid));
    }

    #[test]
    fn given_proof_under_other_key_material_when_verifying_should_refuse() {
        let proof = proof_at(&RelayKey::derive(MATERIAL), NOW);
        let other = RelayKey::derive("fedcba9876543210fedcba9876543210");
        assert_eq!(verify_at(&other, &proof, NOW), Err(UnprovenRelay::Invalid));
    }

    #[test]
    fn given_missing_or_malformed_proof_when_verifying_should_refuse() {
        let key = RelayKey::derive(MATERIAL);
        let now = IggyTimestamp::from(NOW);
        assert_eq!(
            key.verify(&Method::POST, PATH, &route(), None, now),
            Err(UnprovenRelay::Invalid)
        );
        for malformed in ["", "1800000000000001", "soon,00", "1800000000000001,zz"] {
            assert_eq!(
                verify_at(&key, &HeaderValue::from_static(malformed), NOW),
                Err(UnprovenRelay::Invalid),
                "{malformed:?}"
            );
        }
    }

    #[test]
    fn given_genuine_proof_with_a_non_canonical_expiry_when_verifying_should_refuse() {
        let key = RelayKey::derive(MATERIAL);
        let proof = proof_at(&key, NOW);
        assert_eq!(verify_at(&key, &proof, NOW), Ok(()));
        let proof = proof.to_str().expect("an ASCII proof");
        for padded in [format!("+{proof}"), format!("0{proof}")] {
            let padded = HeaderValue::try_from(padded).expect("a valid header value");
            assert_eq!(
                verify_at(&key, &padded, NOW),
                Err(UnprovenRelay::Invalid),
                "{padded:?}"
            );
        }
    }

    #[test]
    fn given_genuine_proof_with_uppercase_tag_when_verifying_should_refuse() {
        let key = RelayKey::derive(MATERIAL);
        let proof = proof_at(&key, NOW);
        assert_eq!(verify_at(&key, &proof, NOW), Ok(()));
        let proof = proof.to_str().unwrap();
        let (expiry, tag) = proof.split_once(',').unwrap();
        assert_ne!(tag, tag.to_ascii_uppercase());
        let uppercase =
            HeaderValue::try_from(format!("{expiry},{}", tag.to_ascii_uppercase())).unwrap();
        assert_eq!(
            verify_at(&key, &uppercase, NOW),
            Err(UnprovenRelay::Invalid)
        );
    }

    #[test]
    fn given_fields_split_differently_when_signing_should_sign_different_bytes() {
        let route = HeaderValue::from_static("1");
        let shifted = HeaderValue::from_static("");
        assert_ne!(
            signed_bytes(&Method::GET, "/a", &route, NOW),
            signed_bytes(&Method::GET, "/a1", &shifted, NOW)
        );
    }
}
