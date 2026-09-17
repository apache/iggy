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

use std::time::Duration;

use async_trait::async_trait;
use iggy::prelude::{AutoLogin, Client, Credentials, IggyClientBuilder, IggyError};
use tracing::{debug, warn};

use crate::protocol::sasl::PlainCredentials;

/// Bound on one credential verification, covering the dial, the login, and the teardown.
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
/// A Kafka client retries the whole authentication itself, so an unbounded inner loop would only
/// hide the failure underneath one the client cannot see.
const VERIFY_RECONNECTION_RETRIES: u32 = 1;

/// Budget for tearing the verification client down again.
///
/// Deliberately far shorter than [`VERIFY_TIMEOUT`]. Teardown happens while the caller still holds
/// an authentication permit, so giving it the full verify budget would let one attempt occupy a
/// slot for twice as long as the doc on [`VERIFY_TIMEOUT`] claims the whole operation can take.
/// Nothing is lost by cutting it short: `Drop` aborts the heartbeat task regardless.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Why a SASL exchange did not produce a verified identity.
///
/// Both variants reach the client as the same `SASL_AUTHENTICATION_FAILED` with the same generic
/// message. They are distinct here only so the gateway's own log can tell an operator whether
/// their Iggy server is unreachable or their user typed the wrong password.
#[derive(Debug)]
pub enum AuthError {
    /// Iggy rejected the credentials.
    Rejected,
    /// Iggy could not be reached, or did not answer inside [`VERIFY_TIMEOUT`].
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

#[derive(Debug)]
pub struct IggyAuthenticator {
    address: String,
    tls: IggyTls,
}

/// Iggy address used when `IGGY_KAFKA_IGGY_ADDR` is unset.
///
/// Matches `bridge::config`'s own default. The two read the same variable for the same purpose, so
/// they must not disagree about what it falls back to.
const DEFAULT_IGGY_ADDR: &str = "127.0.0.1:8090";

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
        let enabled = match std::env::var("IGGY_KAFKA_IGGY_TLS_ENABLED").as_deref() {
            Ok("true") => true,
            Ok("false") | Err(_) => false,
            Ok(other) => {
                return Err(format!(
                    "invalid IGGY_KAFKA_IGGY_TLS_ENABLED `{other}`: expected `true` or `false`"
                ));
            }
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

    use super::*;

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
