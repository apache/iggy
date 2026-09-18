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
use iggy::prelude::{
    AutoLogin, Client, Credentials, Identifier, IggyClientBuilder, IggyError, Permissions,
};
use tracing::{debug, warn};

use crate::protocol::acl::PrincipalPermissions;
use crate::protocol::sasl::PlainCredentials;

/// Bound on one credential verification.
///
/// Covers the dial and the login only. The permission read and the teardown have their own, much
/// smaller budgets ([`PERMISSION_READ_TIMEOUT`] and [`TEARDOWN_TIMEOUT`]), so the three together
/// stay inside the caller's pre-authentication budget rather than exceeding it. The caller bounds
/// the whole thing again from outside, because a permit wait is not covered here at all.
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

/// Budget for the permission read that follows a successful login.
///
/// Much smaller than [`VERIFY_TIMEOUT`] on purpose, and deliberately small in absolute terms. The
/// caller bounds the whole exchange at its pre-authentication budget, which must also absorb the
/// wait for an authentication slot, so every second spent here is a second that wait does not get.
/// At one second the inner worst case is 12s against a 15s outer budget, leaving the queue three
/// seconds rather than one.
///
/// It also bounds only *this* future, not the SDK's work. A cancelled call leaves the SDK's own
/// read running on a detached task that holds its connection lock until that task's own deadline,
/// so a shorter budget here does not stop that work, it only stops the caller waiting on it. The
/// login has already succeeded by this point, so abandoning the read costs an ACL view and nothing
/// else.
const PERMISSION_READ_TIMEOUT: Duration = Duration::from_secs(1);

/// Budget for tearing the verification client down again.
///
/// Deliberately far shorter than [`VERIFY_TIMEOUT`]. Teardown happens while the caller still holds
/// an authentication permit, so every second here is a second some other connection waits. Nothing
/// is lost by cutting it short: `Drop` aborts the heartbeat task regardless.
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
    /// Iggy could not be reached, or did not answer inside the verification timeout.
    Unavailable,
}

/// Who authenticated, and what Iggy already allows them to do.
///
/// The permissions are a snapshot taken during the login that verified the credentials, not a live
/// view. Refreshing them would mean either holding an Iggy session open per connection, which the
/// authentication design rejects on cost, or keeping the password, which it rejects outright. A
/// permission changed mid-connection is therefore invisible until the client reconnects, the same
/// way Iggy's own data plane lags a revocation until the owning shard applies it.
#[derive(Debug, Clone)]
pub struct AuthenticatedPrincipal {
    pub username: String,
    pub permissions: PrincipalPermissions,
    /// Whether [`Self::permissions`] is a real answer or a fallback.
    ///
    /// A read that failed after a successful login degrades to an empty set, which is
    /// indistinguishable on the wire from a principal that genuinely holds nothing. Silently
    /// reporting "no access" for "we could not tell" is the wrong answer to give an operator
    /// debugging access, so the two are kept apart here and answered differently.
    ///
    /// This matters more once Produce and Fetch consume the snapshot: nothing may authorize off a
    /// value that was never read.
    pub permissions_known: bool,
}

/// Verifies Kafka-supplied credentials.
///
/// A trait rather than a concrete type so the protocol tests can drive the whole SASL exchange
/// over a socket without an Iggy server behind it.
#[async_trait]
pub trait SaslAuthenticator: Send + Sync + std::fmt::Debug {
    /// Returns the principal when `credentials` name a real, active Iggy user.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Rejected`] when Iggy refuses the credentials and
    /// [`AuthError::Unavailable`] when it cannot be asked.
    async fn authenticate(
        &self,
        credentials: &PlainCredentials,
    ) -> Result<AuthenticatedPrincipal, AuthError>;
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
    async fn authenticate(
        &self,
        credentials: &PlainCredentials,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
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
            // The login already proved the credentials. Reading the principal's own record on the
            // same session is the one extra round trip that lets `DescribeAcls` answer later
            // without a second login or a stored password. A user may always read itself, with no
            // permission required (`dispatch/authz.rs` exempts a self-targeted read), so this
            // cannot fail for want of a grant.
            Ok(Ok(())) => fetch_permissions(&client, &credentials.username).await,
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

        outcome.map(|(permissions, permissions_known)| AuthenticatedPrincipal {
            username: credentials.username.clone(),
            permissions,
            permissions_known,
        })
    }
}

/// Reads the just-authenticated user's own record and projects its global permissions.
///
/// A missing record or absent permissions both yield an empty set rather than an error: the
/// credentials were already accepted, so refusing the connection here would reject a valid login
/// over an authorization view it never asked for.
async fn fetch_permissions(
    client: &impl Client,
    username: &str,
) -> Result<(PrincipalPermissions, bool), AuthError> {
    // Unreachable in practice: a username that reached a successful login is already inside
    // Identifier's own length bounds. Propagating rather than degrading is still the wrong shape
    // for this function, so it degrades like every other failure below.
    let Ok(identifier) = Identifier::named(username) else {
        warn!("authenticated, but the principal's name is not a valid Iggy identifier");
        return Ok((PrincipalPermissions::default(), false));
    };
    // Deliberately not propagated as a failure. The credentials were already accepted by the login
    // above, so turning a stumble on this second round trip into a rejection would answer a correct
    // password with `SASL_AUTHENTICATION_FAILED`, which a Kafka client treats as fatal and raises
    // to the application. Losing the ACL view is the lesser harm, and it degrades to an empty one.
    let fetched = tokio::time::timeout(PERMISSION_READ_TIMEOUT, client.get_user(&identifier)).await;
    let mut known = true;
    let user = match fetched {
        Ok(Ok(None)) => {
            known = false;
            // Distinct from an error: the login succeeded, so the account exists. A record that
            // resolves to nothing here means the read raced a deletion, and silently reporting an
            // empty ACL view for it would look identical to a principal with no grants.
            warn!("authenticated, but the principal's own record resolved to nothing");
            None
        }
        Ok(Ok(user)) => user,
        Ok(Err(error)) => {
            known = false;
            warn!(%error, "authenticated, but could not read the principal's permissions");
            None
        }
        Err(_elapsed) => {
            known = false;
            warn!("authenticated, but timed out reading the principal's permissions");
            None
        }
    };

    // A record that resolved but carries no permissions is a real answer: the principal holds
    // nothing. Only a read that did not resolve is unknown.
    Ok((
        user.and_then(|user| user.permissions)
            .as_ref()
            .map(PrincipalPermissions::from)
            .unwrap_or_default(),
        known,
    ))
}

/// Projects Iggy's global permissions onto the subset that has a Kafka meaning.
///
/// Stream flags fold into the topic ones because Kafka has no resource above a topic and every
/// Kafka topic lives inside an Iggy stream, so a stream grant is in practice a grant over the
/// topics a Kafka client can reach. `docs/ACL_MAPPING.md` has the full table.
impl From<&Permissions> for PrincipalPermissions {
    fn from(permissions: &Permissions) -> Self {
        let global = &permissions.global;
        // Iggy's enforcement is hierarchical, so a flag-for-flag copy describes a principal that
        // cannot do things Iggy will in fact let it do. The rules are in
        // `core/metadata/src/permissioner/permissioner_rules/`: polling is granted by any of the
        // four read/manage flags on topics or streams, appending by either manage flag, and
        // reading server state by either server flag. Mirroring that here is the difference
        // between describing a consumer and describing a principal that appears unable to consume.
        let manages = global.manage_topics || global.manage_streams;
        let reads = global.read_topics || global.read_streams;
        Self {
            read_servers: global.read_servers || global.manage_servers,
            read_topics: reads || manages,
            manage_topics: manages,
            poll_messages: global.poll_messages || reads || manages,
            send_messages: global.send_messages || manages,
        }
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

    /// Iggy grants polling to anyone holding any of the four read/manage flags on topics or
    /// streams, not only to the explicit `poll_messages` flag. Copying flags one for one described
    /// such a principal as unable to consume, which is the exact falsehood the derived group
    /// binding exists to avoid.
    #[test]
    fn given_only_read_topics_when_projected_should_still_be_able_to_poll() {
        let mut permissions = Permissions::default();
        permissions.global.read_topics = true;
        let projected = PrincipalPermissions::from(&permissions);
        assert!(
            projected.poll_messages,
            "read_topics grants polling in Iggy"
        );
        assert!(projected.read_topics);
        assert!(!projected.send_messages, "it does not grant appending");
    }

    #[test]
    fn given_only_manage_streams_when_projected_should_grant_both_directions() {
        // Managing streams grants appending and polling, and implies the topic-level reads.
        let mut permissions = Permissions::default();
        permissions.global.manage_streams = true;
        let projected = PrincipalPermissions::from(&permissions);
        assert!(projected.send_messages);
        assert!(projected.poll_messages);
        assert!(projected.manage_topics);
    }

    #[test]
    fn given_only_manage_servers_when_projected_should_also_allow_describing_the_cluster() {
        let mut permissions = Permissions::default();
        permissions.global.manage_servers = true;
        let projected = PrincipalPermissions::from(&permissions);
        assert!(
            projected.read_servers,
            "managing implies reading, as the permissioner's own rule does"
        );
    }

    #[test]
    fn given_no_grants_when_projected_should_stay_empty() {
        let projected = PrincipalPermissions::from(&Permissions::default());
        assert_eq!(projected, PrincipalPermissions::default());
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
