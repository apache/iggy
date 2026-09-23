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

//! Turning SASL credentials into a verified Iggy identity.
//!
//! The gateway keeps no credential store and no user mapping. A Kafka principal's username and
//! password are an Iggy username and password, so authenticating is forwarding them to an Iggy
//! login and seeing whether it succeeds. `docs/AUTHENTICATION.md` has the reasoning.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use iggy::prelude::{AutoLogin, Client, Credentials, IggyClientBuilder, IggyError};
use tracing::{debug, warn};

use crate::bridge::config::DEFAULT_IGGY_ADDR;
use crate::env::parse_bool;
use crate::protocol::sasl::PlainCredentials;

/// Bound on one credential verification.
///
/// Covers the dial and the login. Teardown has its own, much smaller budget
/// ([`TEARDOWN_TIMEOUT`]) so that one attempt cannot hold an authentication permit for twice this
/// long. The caller bounds the whole thing again from outside, against its pre-authentication
/// budget, because a permit wait is not covered here at all.
///
/// A verification that has not answered inside this is indistinguishable, from the Kafka client's
/// side, from one that failed, and the client is holding a connection open waiting for it. Shorter
/// than the bridge's own 15s request budget because a login is a bounded handshake against a
/// server that is either reachable or not, not an arbitrary data operation.
///
/// "Bounded" is not "one round trip". `establish_session` also performs a cluster-metadata lookup
/// for leader settlement, and on a redirect it reconnects and logs in a second time, so a single
/// verification can cost two logins against a clustered deployment. The measurements in
/// `docs/MANUAL_TESTING.md` were taken against one node and therefore never exercise that path.
const VERIFY_TIMEOUT: Duration = Duration::from_secs(10);

/// Retries the dial makes before giving up, not the SDK's unlimited default.
///
/// The SDK counts passes after the first, so zero still makes one full pass over the endpoints.
/// Any retry adds a second pass plus a `reconnection.interval` sleep while an authentication slot
/// is held, and a Kafka client retries the whole authentication itself anyway, so an inner loop
/// would only hide the failure underneath one the client cannot see.
const VERIFY_RECONNECTION_RETRIES: u32 = 0;

/// Budget for tearing the verification client down again.
///
/// Deliberately far shorter than [`VERIFY_TIMEOUT`]. Teardown happens while the caller still holds
/// an authentication permit, so giving it the full verify budget would let one attempt occupy a
/// slot for twice as long as the doc on [`VERIFY_TIMEOUT`] claims the whole operation can take.
/// Nothing is lost by cutting it short: `Drop` aborts the heartbeat task regardless.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Delay imposed on a peer after its first rejected login, doubled on every further rejection.
const THROTTLE_BASE_DELAY: Duration = Duration::from_millis(500);

/// Ceiling on the doubling, and how long a peer stays remembered once its delay has run out.
const THROTTLE_MAX_DELAY: Duration = Duration::from_secs(30);

/// Peers remembered at once.
///
/// Bounds the table against a sweep of source addresses. Once it is full of peers still inside
/// their delay, new peers go untracked, which is no worse than having no throttle at all.
const THROTTLE_MAX_PEERS: usize = 4096;

/// Refuses verifications from a peer whose last login was rejected, for an escalating delay.
///
/// Every guess costs a full Argon2id hash on an Iggy shard thread, and reconnecting is free, so
/// without this a single peer can keep every authentication slot busy with wrong passwords.
/// Keyed on the IP rather than the socket address because the port changes on every reconnect,
/// and on the /64 for IPv6 because a single host is routinely handed a whole /64 to draw from.
/// Peers sharing a NAT share a delay, which is why it starts short.
#[derive(Debug, Default)]
pub struct FailedLoginThrottle {
    peers: Mutex<HashMap<IpAddr, Strikes>>,
}

#[derive(Debug, Clone, Copy)]
struct Strikes {
    rejections: u32,
    blocked_until: Instant,
}

impl Strikes {
    fn is_forgotten(&self, now: Instant) -> bool {
        now >= self.blocked_until + THROTTLE_MAX_DELAY
    }
}

impl FailedLoginThrottle {
    /// Whether `peer` is still inside the delay its last rejection earned.
    #[must_use]
    pub fn is_blocked(&self, peer: IpAddr) -> bool {
        self.is_blocked_at(peer, Instant::now())
    }

    /// Records a rejected login from `peer` and extends its delay.
    pub fn record_rejection(&self, peer: IpAddr) {
        self.record_rejection_at(peer, Instant::now());
    }

    /// Forgets `peer` once it has proven a credential.
    pub fn record_success(&self, peer: IpAddr) {
        self.peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&throttle_key(peer));
    }

    fn is_blocked_at(&self, peer: IpAddr, now: Instant) -> bool {
        let peers = self.peers.lock().unwrap_or_else(PoisonError::into_inner);
        peers
            .get(&throttle_key(peer))
            .is_some_and(|strikes| now < strikes.blocked_until)
    }

    fn record_rejection_at(&self, peer: IpAddr, now: Instant) {
        let peer = throttle_key(peer);
        let mut peers = self.peers.lock().unwrap_or_else(PoisonError::into_inner);
        if !peers.contains_key(&peer) && peers.len() >= THROTTLE_MAX_PEERS {
            peers.retain(|_, strikes| !strikes.is_forgotten(now));
            if peers.len() >= THROTTLE_MAX_PEERS {
                return;
            }
        }
        let rejections = peers
            .get(&peer)
            .filter(|strikes| !strikes.is_forgotten(now))
            .map_or(1, |strikes| strikes.rejections.saturating_add(1));
        let doublings = (rejections - 1).min(16);
        let delay = THROTTLE_BASE_DELAY
            .saturating_mul(1 << doublings)
            .min(THROTTLE_MAX_DELAY);
        peers.insert(
            peer,
            Strikes {
                rejections,
                blocked_until: now + delay,
            },
        );
    }
}

/// The address a peer is throttled under: IPv4 as is, IPv6 truncated to its /64.
///
/// An IPv4-mapped IPv6 address is unwrapped first, so a dual-stack listener throttles the same
/// client under the same key whichever socket family it arrived on.
fn throttle_key(peer: IpAddr) -> IpAddr {
    match peer {
        IpAddr::V4(_) => peer,
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or_else(
            || {
                let prefix = v6.to_bits() & !u128::from(u64::MAX);
                IpAddr::V6(Ipv6Addr::from_bits(prefix))
            },
            IpAddr::V4,
        ),
    }
}

/// Why a SASL exchange did not produce a verified identity.
///
/// Both variants reach the client as the same `SASL_AUTHENTICATION_FAILED` with the same generic
/// message. They are distinct here only so the gateway's own log can tell an operator whether
/// their Iggy server is unreachable or their user typed the wrong password.
#[derive(Debug)]
pub enum AuthError {
    /// Iggy rejected the credentials.
    Rejected,
    /// Iggy could not be reached, or did not answer inside the verification timeout.
    Unavailable,
}

/// Verifies Kafka-supplied credentials.
///
/// A trait rather than a concrete type so the protocol tests can drive the whole SASL exchange
/// over a socket without an Iggy server behind it.
#[async_trait]
pub trait SaslAuthenticator: Send + Sync + std::fmt::Debug {
    /// Returns `Ok(())` when `credentials` name a real, active Iggy user.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Rejected`] when Iggy refuses the credentials and
    /// [`AuthError::Unavailable`] when it cannot be asked.
    async fn authenticate(&self, credentials: &PlainCredentials) -> Result<(), AuthError>;
}

/// How the verifier reaches Iggy.
///
/// Separate from the gateway's own listener security. A deployment can terminate TLS on the Kafka
/// side and still speak plain TCP to a co-located Iggy, or the reverse, and the two are configured
/// independently because they protect different hops.
#[derive(Debug, Clone, Default)]
pub struct IggyTls {
    pub enabled: bool,
    /// Name checked against the server certificate. Empty means derive it from the address.
    pub domain: String,
    /// PEM roots to trust. Unset uses the SDK's bundled roots, not the system trust store.
    pub ca_file: Option<String>,
}

/// Verifies credentials by logging into a real Iggy server with them.
///
/// Every verification opens its own connection, logs in, and shuts down again. That is one Argon2
/// verify and one replicated `Register` per authenticated Kafka connection, which is the cost
/// `docs/AUTHENTICATION.md` describes and does not hide.
///
/// It is also why there is no credential cache here. Caching a verification keyed on the username
/// alone would let a second connection present any password for a principal already seen, which
/// is an authentication bypass rather than an optimisation. Caching it keyed on the credential
/// means storing something password-equivalent in gateway memory. Neither is worth doing before
/// there is a handler whose throughput the login cost actually limits, and today Produce and Fetch
/// are still stubs, so no verified session has a consumer to be held for.
#[derive(Debug)]
pub struct IggyAuthenticator {
    address: String,
    tls: IggyTls,
}

impl IggyAuthenticator {
    /// The complete set of `IGGY_KAFKA_*` vars this type reads, for `main`'s unknown-var guard.
    ///
    /// `IGGY_KAFKA_IGGY_ADDR` is listed here even though `bridge::config` also carries it. This
    /// type reads it directly, and relying on the bridge's list to cover it is the cross-list
    /// coupling the guard exists to avoid: a bridge rename would then break SASL startup on a
    /// variable SASL reads itself.
    pub const KNOWN_ENV_VARS: &'static [&'static str] = &[
        "IGGY_KAFKA_IGGY_ADDR",
        "IGGY_KAFKA_IGGY_TLS_ENABLED",
        "IGGY_KAFKA_IGGY_TLS_DOMAIN",
        "IGGY_KAFKA_IGGY_TLS_CA_FILE",
    ];

    #[must_use]
    pub const fn new(address: String) -> Self {
        Self {
            address,
            tls: IggyTls {
                enabled: false,
                domain: String::new(),
                ca_file: None,
            },
        }
    }

    #[must_use]
    pub fn with_tls(mut self, tls: IggyTls) -> Self {
        self.tls = tls;
        self
    }

    #[must_use]
    pub const fn is_tls_enabled(&self) -> bool {
        self.tls.enabled
    }

    /// Reads the Iggy address and transport security from the environment.
    ///
    /// No credentials of its own: every verification uses the credentials the Kafka client
    /// presented, which is the whole point of forwarding them rather than mapping them.
    ///
    /// # Errors
    ///
    /// Returns a message naming the offending variable when `IGGY_KAFKA_IGGY_TLS_ENABLED` is set
    /// to anything but `true` or `false`. Defaulting a mistyped security switch to off is how a
    /// deployment ends up sending passwords in the clear while believing it does not.
    pub fn from_env() -> Result<Self, String> {
        let address =
            std::env::var("IGGY_KAFKA_IGGY_ADDR").unwrap_or_else(|_| DEFAULT_IGGY_ADDR.to_string());
        // Unset reads as off; only a value that is set and unrecognised is an error, which is
        // what `parse_bool` decides for every switch in this gateway.
        let enabled = match std::env::var("IGGY_KAFKA_IGGY_TLS_ENABLED") {
            Ok(raw) => parse_bool("IGGY_KAFKA_IGGY_TLS_ENABLED", &raw)?,
            Err(_) => false,
        };
        let domain = std::env::var("IGGY_KAFKA_IGGY_TLS_DOMAIN").unwrap_or_default();
        let ca_file = std::env::var("IGGY_KAFKA_IGGY_TLS_CA_FILE").ok();
        // Configuring TLS material and leaving the switch off is the same "believes TLS is on"
        // mistake the value check above exists to catch, reached through the unset path instead of
        // a typo. Silently ignoring it would send credentials in the clear.
        if !enabled && (!domain.is_empty() || ca_file.is_some()) {
            return Err(
                "IGGY_KAFKA_IGGY_TLS_DOMAIN or IGGY_KAFKA_IGGY_TLS_CA_FILE is set but \
                 IGGY_KAFKA_IGGY_TLS_ENABLED is not true; the connection to Iggy would be in the \
                 clear despite the TLS configuration"
                    .to_string(),
            );
        }
        Ok(Self::new(address).with_tls(IggyTls {
            enabled,
            domain,
            ca_file,
        }))
    }
}

impl std::fmt::Display for IggyAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.tls.enabled {
            write!(f, "{} over TLS", self.address)
        } else {
            write!(f, "{} in the clear", self.address)
        }
    }
}

#[async_trait]
impl SaslAuthenticator for IggyAuthenticator {
    async fn authenticate(&self, credentials: &PlainCredentials) -> Result<(), AuthError> {
        let auto_login = AutoLogin::Enabled(Credentials::UsernamePassword(
            credentials.username.clone(),
            credentials.password.clone(),
        ));
        let mut builder = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(self.address.clone())
            .with_auto_sign_in(auto_login)
            .with_reconnection_max_retries(Some(VERIFY_RECONNECTION_RETRIES));
        if self.tls.enabled {
            builder = builder.with_tls_enabled(true);
            if !self.tls.domain.is_empty() {
                builder = builder.with_tls_domain(self.tls.domain.clone());
            }
            if let Some(ref ca_file) = self.tls.ca_file {
                builder = builder.with_tls_ca_file(ca_file.clone());
            }
        }
        let client = builder.build().map_err(|error| classify(&error))?;

        let connected = tokio::time::timeout(VERIFY_TIMEOUT, client.connect()).await;
        let outcome = match connected {
            Err(_elapsed) => Err(AuthError::Unavailable),
            Ok(Err(error)) => Err(classify(&error)),
            Ok(Ok(())) => Ok(()),
        };

        // Shut down on both paths, and not `disconnect`: only `shutdown` stops the heartbeat task,
        // which would otherwise keep pinging, observe the dropped transport, and reconnect using
        // the very credentials this call was only meant to check.
        //
        // Both failure shapes are reported, and separately: the outer `Elapsed` and the inner
        // `IggyError` mean different things, and folding them together would log "timed out" for a
        // shutdown that failed immediately. Neither leaks the client, since `Drop` aborts the
        // heartbeat regardless, but an invisible failure here would hide a real one.
        match tokio::time::timeout(TEARDOWN_TIMEOUT, client.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                warn!(%error, "failed to shut down a credential-verification client");
            }
            Err(_elapsed) => {
                warn!("timed out shutting down a credential-verification client");
            }
        }

        outcome
    }
}

/// Splits Iggy's errors into "the credentials are wrong" and "we could not ask".
///
/// Everything unrecognised is treated as unavailable rather than rejected. A gateway that reports
/// an unfamiliar server-side failure as a bad password sends the user to change a password that
/// was never the problem, and it hides a real outage behind what looks like user error.
fn classify(error: &IggyError) -> AuthError {
    match error {
        IggyError::InvalidCredentials
        | IggyError::InvalidUsername
        | IggyError::InvalidPassword
        | IggyError::UserInactive
        | IggyError::Unauthorized => {
            debug!("Iggy rejected the presented credentials");
            AuthError::Rejected
        }
        other => {
            warn!(%other, "could not verify credentials against Iggy");
            AuthError::Unavailable
        }
    }
}

#[cfg(test)]
mod tests {
    use secrecy::SecretString;
    use serial_test::serial;

    use super::*;

    /// Leaves every variable [`IggyAuthenticator::from_env`] reads unset, so one case's value
    /// cannot decide another's result.
    fn clear_authenticator_env() {
        for key in IggyAuthenticator::KNOWN_ENV_VARS {
            // SAFETY: every test that calls this is `#[serial]`, which is what makes an env
            // mutation sound under edition 2024 - see the note on the tests below.
            unsafe {
                std::env::remove_var(key);
            }
        }
    }

    fn credentials() -> PlainCredentials {
        PlainCredentials {
            username: "alice".to_string(),
            password: SecretString::from("s3cret".to_string()),
        }
    }

    #[test]
    fn given_a_credential_rejection_when_classified_should_not_look_like_an_outage() {
        assert!(matches!(
            classify(&IggyError::InvalidCredentials),
            AuthError::Rejected
        ));
        assert!(matches!(
            classify(&IggyError::InvalidPassword),
            AuthError::Rejected
        ));
        assert!(matches!(
            classify(&IggyError::UserInactive),
            AuthError::Rejected
        ));
    }

    #[test]
    fn given_an_unfamiliar_error_when_classified_should_be_unavailable_not_rejected() {
        // Reporting an unrecognised server-side failure as a bad password sends the user to change
        // a password that was never wrong, and buries the real outage.
        assert!(matches!(
            classify(&IggyError::CannotEstablishConnection),
            AuthError::Unavailable
        ));
        assert!(matches!(
            classify(&IggyError::Disconnected),
            AuthError::Unavailable
        ));
        assert!(matches!(
            classify(&IggyError::TransientNotCommitted),
            AuthError::Unavailable
        ));
    }

    /// `#[serial]`, unkeyed: this crate's lib test binary shares one default group, and
    /// `bridge::config`'s and `server`'s env-touching tests are in it too. Edition 2024's
    /// `env::set_var`/`remove_var` are unsound against *any* concurrent env read on another
    /// thread, whichever key either side happens to touch.
    #[test]
    #[serial]
    fn given_an_empty_environment_when_read_should_reach_the_shared_default_in_the_clear() {
        clear_authenticator_env();
        let authenticator = IggyAuthenticator::from_env().expect("an empty environment is valid");
        assert!(!authenticator.is_tls_enabled());
        assert_eq!(
            authenticator.to_string(),
            format!("{DEFAULT_IGGY_ADDR} in the clear"),
            "the address falls back to the same constant the bridge defaults to"
        );
    }

    #[test]
    #[serial]
    fn given_a_mistyped_tls_switch_when_read_should_refuse_to_start() {
        clear_authenticator_env();
        // SAFETY: `#[serial]` excludes every other env-touching test in this binary.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_TLS_ENABLED", "TRUE");
        }
        let result = IggyAuthenticator::from_env();
        clear_authenticator_env();
        assert!(
            result.is_err(),
            "defaulting a mistyped security switch to off is how a deployment sends passwords in \
             the clear while believing it does not"
        );
    }

    #[test]
    #[serial]
    fn given_tls_material_without_the_switch_when_read_should_refuse_to_start() {
        clear_authenticator_env();
        // SAFETY: `#[serial]` excludes every other env-touching test in this binary.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_TLS_CA_FILE", "/etc/iggy/ca.pem");
        }
        let result = IggyAuthenticator::from_env();
        clear_authenticator_env();
        assert!(
            result.is_err(),
            "configured TLS material with the switch unset is the same mistake reached through \
             the unset path instead of a typo"
        );
    }

    #[test]
    #[serial]
    fn given_a_complete_tls_configuration_when_read_should_carry_it_through() {
        clear_authenticator_env();
        // SAFETY: `#[serial]` excludes every other env-touching test in this binary.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_ADDR", "iggy.internal:8090");
            std::env::set_var("IGGY_KAFKA_IGGY_TLS_ENABLED", "true");
            std::env::set_var("IGGY_KAFKA_IGGY_TLS_DOMAIN", "iggy.internal");
        }
        let result = IggyAuthenticator::from_env();
        clear_authenticator_env();

        let authenticator = result.expect("a complete TLS configuration is valid");
        assert!(authenticator.is_tls_enabled());
        assert_eq!(authenticator.to_string(), "iggy.internal:8090 over TLS");
    }

    #[test]
    fn given_a_rejected_peer_when_throttled_should_block_until_the_delay_runs_out() {
        let throttle = FailedLoginThrottle::default();
        let peer: IpAddr = [192, 0, 2, 1].into();
        let other: IpAddr = [192, 0, 2, 2].into();
        let start = Instant::now();

        throttle.record_rejection_at(peer, start);
        assert!(throttle.is_blocked_at(peer, start));
        assert!(
            !throttle.is_blocked_at(other, start),
            "the delay is per peer"
        );
        assert!(!throttle.is_blocked_at(peer, start + THROTTLE_BASE_DELAY));

        // A second rejection before the peer is forgotten doubles the delay.
        let second = start + THROTTLE_BASE_DELAY;
        throttle.record_rejection_at(peer, second);
        assert!(throttle.is_blocked_at(peer, second + THROTTLE_BASE_DELAY));
        assert!(!throttle.is_blocked_at(peer, second + THROTTLE_BASE_DELAY * 2));
    }

    #[test]
    fn given_a_throttled_peer_when_it_authenticates_should_be_forgotten() {
        let throttle = FailedLoginThrottle::default();
        let peer: IpAddr = [192, 0, 2, 1].into();
        let start = Instant::now();
        throttle.record_rejection_at(peer, start);
        throttle.record_rejection_at(peer, start);
        throttle.record_success(peer);
        assert!(!throttle.is_blocked_at(peer, start));

        // The escalation restarts from the base delay rather than where it left off.
        throttle.record_rejection_at(peer, start);
        assert!(!throttle.is_blocked_at(peer, start + THROTTLE_BASE_DELAY));
    }

    #[test]
    fn given_an_ipv6_peer_should_share_a_delay_across_its_slash_64() {
        let throttle = FailedLoginThrottle::default();
        let start = Instant::now();
        let first: IpAddr = "2001:db8:1:2::1".parse().expect("valid address");
        let same_prefix: IpAddr = "2001:db8:1:2:ffff::9".parse().expect("valid address");
        let other_prefix: IpAddr = "2001:db8:1:3::1".parse().expect("valid address");

        throttle.record_rejection_at(first, start);
        assert!(throttle.is_blocked_at(same_prefix, start));
        assert!(!throttle.is_blocked_at(other_prefix, start));

        let mapped: IpAddr = "::ffff:192.0.2.7".parse().expect("valid address");
        let plain: IpAddr = [192, 0, 2, 7].into();
        throttle.record_rejection_at(mapped, start);
        assert!(throttle.is_blocked_at(plain, start));
    }

    #[test]
    fn given_repeated_rejections_should_cap_the_delay() {
        let throttle = FailedLoginThrottle::default();
        let peer: IpAddr = [192, 0, 2, 1].into();
        let start = Instant::now();
        for _ in 0..64 {
            throttle.record_rejection_at(peer, start);
        }
        assert!(throttle.is_blocked_at(peer, start + THROTTLE_MAX_DELAY / 2));
        assert!(!throttle.is_blocked_at(peer, start + THROTTLE_MAX_DELAY));
    }

    #[tokio::test]
    async fn given_an_unreachable_iggy_when_authenticating_should_report_unavailable() {
        // Port 1 on loopback refuses immediately, so this exercises the failure path without
        // waiting out VERIFY_TIMEOUT.
        let authenticator = IggyAuthenticator::new("127.0.0.1:1".to_string());
        let result = authenticator.authenticate(&credentials()).await;
        assert!(
            matches!(result, Err(AuthError::Unavailable)),
            "an unreachable server must never be reported as a credential rejection: {result:?}"
        );
    }
}
