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

use crate::binary::impl_binary_client;
use crate::driver::OperationDriver;
use crate::links::{Dialer, ReconnectPolicy, ReplicaLinks};
use crate::session_owner::SessionOwner;
use iggy_common::{
    AutoLogin, ConnectionString, ConnectionStringUtils, IggyError, NonZeroIggyDuration,
    TcpClientConfig, TcpConnectionStringOptions, TransportProtocol,
};
use std::str::FromStr;
use std::sync::Arc;

/// TCP client whose replica links share one session and operation driver.
#[derive(Debug)]
pub struct TcpClient {
    pub(crate) config: Arc<TcpClientConfig>,
    pub(crate) driver: OperationDriver,
}

impl Default for TcpClient {
    fn default() -> Self {
        Self::create(Arc::new(TcpClientConfig::default())).unwrap()
    }
}

impl_binary_client!(TcpClient);

impl TcpClient {
    /// Creates a TCP client for the endpoint, credentials and heartbeat interval.
    pub fn new(
        server_address: &str,
        auto_sign_in: AutoLogin,
        heartbeat_interval: NonZeroIggyDuration,
    ) -> Result<Self, IggyError> {
        Self::create(Arc::new(TcpClientConfig {
            heartbeat_interval,
            server_address: server_address.to_string(),
            auto_login: auto_sign_in,
            ..Default::default()
        }))
    }

    /// Creates a TLS client that validates the endpoint against the supplied domain.
    pub fn new_tls(
        server_address: &str,
        domain: &str,
        auto_sign_in: AutoLogin,
        heartbeat_interval: NonZeroIggyDuration,
    ) -> Result<Self, IggyError> {
        Self::create(Arc::new(TcpClientConfig {
            heartbeat_interval,
            server_address: server_address.to_string(),
            tls_enabled: true,
            tls_domain: domain.to_string(),
            auto_login: auto_sign_in,
            ..Default::default()
        }))
    }

    /// Creates a TCP client from an Iggy connection string.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        if ConnectionStringUtils::parse_protocol(connection_string)? != TransportProtocol::Tcp {
            return Err(IggyError::InvalidConnectionString);
        }

        Self::create(Arc::new(
            ConnectionString::<TcpConnectionStringOptions>::from_str(connection_string)?.into(),
        ))
    }

    /// Creates the client without spawning tasks or opening sockets.
    pub fn create(config: Arc<TcpClientConfig>) -> Result<Self, IggyError> {
        let session = Arc::new(SessionOwner::with_pending_operations(
            config.auto_login.clone(),
            config.pending_operations,
        ));
        let links = ReplicaLinks::new(
            Dialer::Tcp(Arc::clone(&config)),
            config.server_address.clone(),
            ReconnectPolicy::from(&config.reconnection),
            session,
        );
        Ok(Self {
            config,
            driver: OperationDriver::new(links),
        })
    }

    #[cfg(test)]
    pub(crate) fn from_links(links: ReplicaLinks) -> Self {
        Self {
            config: Arc::new(TcpClientConfig::default()),
            driver: OperationDriver::new(links),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::links::OPERATION_TIMEOUT;
    use crate::links::fake_cluster::{
        Answer, FakeCluster, TEST_BUDGET, endpoint, eventually, links, policy,
    };
    use iggy_binary_protocol::codes::{
        BIND_SESSION_CODE, CHANGE_PASSWORD_CODE, LOGIN_REGISTER_CODE, LOGOUT_USER_CODE, PING_CODE,
        UPDATE_USER_CODE,
    };
    use iggy_binary_protocol::consensus::EvictionReason;
    use iggy_binary_protocol::requests::users::{
        ChangePasswordRequest, LoginRegisterRequest, LoginRegisterWithPatRequest,
    };
    use iggy_binary_protocol::{WireDecode, WireEncode, WireIdentifier};
    use iggy_common::{
        BinaryTransport, Client, ClientState, Credentials, Identifier, IggyDuration,
        PersonalAccessTokenClient, SystemClient, TcpClientReconnectionConfig, UserClient,
        UserUpdateOptions, with_binary_deadline,
    };
    use secrecy::{ExposeSecret, SecretString};
    use std::time::Duration;
    use tokio::time::{Instant, sleep};

    const USERNAME: &str = "iggy";
    const PASSWORD: &str = "iggy";
    const WRONG_PASSWORD: &str = "wrong";
    const NEW_USERNAME: &str = "renamed";
    const NEW_PASSWORD: &str = "rotated";
    const COMMIT: u64 = 20;

    fn manual_client(cluster: &Arc<FakeCluster>) -> TcpClient {
        let session = Arc::new(SessionOwner::new(AutoLogin::Disabled));
        TcpClient::from_links(ReplicaLinks::new(
            cluster.dialer(),
            endpoint(0),
            policy(),
            session,
        ))
    }

    async fn signed_in_client(cluster: &Arc<FakeCluster>) -> TcpClient {
        let client = manual_client(cluster);
        client.login_user(USERNAME, PASSWORD).await.unwrap();
        client
    }

    /// Ends the next call of `code` with an unknown outcome, and answers its
    /// replay with `replay`.
    fn end_uncertain(cluster: &FakeCluster, code: u32, replay: Answer) {
        cluster.script(
            &endpoint(0),
            code,
            [
                Answer::Refuse(IggyError::TransientNotCommitted),
                Answer::Refuse(IggyError::RequestTooOld),
                replay,
            ],
        );
    }

    /// The outcome of `call`, after a resolve proved it if the call ended
    /// uncertain.
    async fn proved(client: &TcpClient, call: Result<(), IggyError>) -> Result<(), IggyError> {
        match call {
            Err(IggyError::OperationUncertain(op)) => {
                client
                    .resolve_operation(op, (Instant::now() + OPERATION_TIMEOUT).into_std())
                    .await
            }
            outcome => outcome,
        }
    }

    /// Ends the session on the server, so the client signs in again, and
    /// returns that sign-in.
    async fn sign_in_again(cluster: &FakeCluster) -> LoginRegisterRequest {
        let sign_ins = cluster.count(LOGIN_REGISTER_CODE);
        *cluster.session.lock().unwrap() = None;
        cluster.drop_connections(&endpoint(0));
        eventually("the client signs in again", || {
            cluster.count(LOGIN_REGISTER_CODE) > sign_ins
        })
        .await;
        let sign_ins = cluster.received(LOGIN_REGISTER_CODE);
        LoginRegisterRequest::decode_from(&sign_ins.last().unwrap().body).unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_connection_when_explicitly_signing_in_should_bootstrap_before_register() {
        for token in [false, true] {
            let cluster = FakeCluster::new(1);
            let client = manual_client(&cluster);
            let identity = if token {
                client.login_with_personal_access_token("test-token").await
            } else {
                client.login_user("iggy", "iggy").await
            };
            assert!(identity.is_ok(), "token={token}: {identity:?}");
            assert_eq!(client.get_state().await, ClientState::Authenticated);
            assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
            client.disconnect().await.unwrap();
            assert_eq!(client.ping().await.unwrap_err(), IggyError::NotConnected);
            assert_eq!(cluster.count(PING_CODE), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_invalid_token_when_relogging_should_preserve_the_live_session() {
        let cluster = FakeCluster::new(1);
        let client = manual_client(&cluster);
        client.login_user("iggy", "iggy").await.unwrap();
        let identity = client.driver.session().identity();

        assert_eq!(
            client
                .login_with_personal_access_token("")
                .await
                .unwrap_err(),
            IggyError::InvalidFormat
        );
        assert_eq!(client.driver.session().identity(), identity);
        assert_eq!(client.get_state().await, ClientState::Authenticated);
        assert_eq!(cluster.count(LOGOUT_USER_CODE), 0);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_automatic_sign_in_when_explicitly_signing_in_should_use_explicit_credentials() {
        let cluster = FakeCluster::new(1);
        let client = TcpClient::from_links(links(&cluster, &endpoint(0), policy()));
        client.connect().await.unwrap();
        client.login_user("other", "password").await.unwrap();
        let requests = cluster.received(LOGIN_REGISTER_CODE);
        assert_eq!(requests.len(), 2);
        let automatic = LoginRegisterRequest::decode_from(&requests[0].body).unwrap();
        let explicit = LoginRegisterRequest::decode_from(&requests[1].body).unwrap();
        assert_eq!(automatic.username.as_str(), "iggy");
        assert_eq!(explicit.username.as_str(), "other");
        assert_eq!(cluster.count(LOGOUT_USER_CODE), 1);
        assert_eq!(client.get_state().await, ClientState::Authenticated);
    }

    #[tokio::test(start_paused = true)]
    async fn given_configured_credentials_when_explicitly_signing_in_should_skip_auto_login() {
        for configured_token in [false, true] {
            for explicit_token in [false, true] {
                let cluster = FakeCluster::new(1);
                let credentials = if configured_token {
                    Credentials::PersonalAccessToken(SecretString::from("wrong-token"))
                } else {
                    Credentials::UsernamePassword(
                        "configured".to_owned(),
                        SecretString::from("wrong"),
                    )
                };
                let session = Arc::new(SessionOwner::new(AutoLogin::Enabled(credentials)));
                let client = TcpClient::from_links(ReplicaLinks::new(
                    cluster.dialer(),
                    endpoint(0),
                    policy(),
                    session,
                ));
                let identity = if explicit_token {
                    client.login_with_personal_access_token("valid-token").await
                } else {
                    client.login_user("explicit", "password").await
                };
                assert!(
                    identity.is_ok(),
                    "configured_token={configured_token}, explicit_token={explicit_token}: {identity:?}"
                );
                let requests = cluster.received(LOGIN_REGISTER_CODE);
                assert_eq!(
                    requests.len(),
                    1,
                    "only explicit credentials reach the server"
                );
                if explicit_token {
                    let request =
                        LoginRegisterWithPatRequest::decode_from(&requests[0].body).unwrap();
                    assert_eq!(request.token.expose_secret(), "valid-token");
                } else {
                    let request = LoginRegisterRequest::decode_from(&requests[0].body).unwrap();
                    assert_eq!(request.username.as_str(), "explicit");
                }
                assert_eq!(cluster.count(LOGOUT_USER_CODE), 0);
                assert_eq!(client.get_state().await, ClientState::Authenticated);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_invalid_auto_login_when_explicitly_signing_in_should_ignore_its_failure() {
        for token in [false, true] {
            let cluster = FakeCluster::new(1);
            let credentials = if token {
                Credentials::PersonalAccessToken(SecretString::from(""))
            } else {
                Credentials::UsernamePassword("iggy".to_owned(), SecretString::from(""))
            };
            let client = || {
                TcpClient::from_links(ReplicaLinks::new(
                    cluster.dialer(),
                    endpoint(0),
                    policy(),
                    Arc::new(SessionOwner::new(AutoLogin::Enabled(credentials.clone()))),
                ))
            };
            let automatic = client();
            let error = automatic.connect().await.unwrap_err();
            assert_eq!(
                error,
                if token {
                    IggyError::InvalidFormat
                } else {
                    IggyError::InvalidPassword
                }
            );
            automatic.disconnect().await.unwrap();
            assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 0);

            let explicit = client();
            if token {
                explicit
                    .login_with_personal_access_token("valid-token")
                    .await
                    .unwrap();
            } else {
                explicit.login_user("iggy", "iggy").await.unwrap();
            }
            assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
            assert_eq!(explicit.get_state().await, ClientState::Authenticated);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_concurrent_connect_when_signing_in_should_use_explicit_credentials() {
        for rejection in [
            None,
            Some(EvictionReason::InvalidCredentials),
            Some(EvictionReason::InvalidToken),
        ] {
            let cluster = FakeCluster::new(1);
            if let Some(reason) = rejection {
                cluster.script(&endpoint(0), LOGIN_REGISTER_CODE, [Answer::Evict(reason)]);
            }
            let client = TcpClient::from_links(links(&cluster, &endpoint(0), policy()));
            let (automatic, explicit) =
                tokio::join!(client.connect(), client.login_user("other", "password"));
            assert_eq!(
                automatic,
                match rejection {
                    Some(EvictionReason::InvalidCredentials) => Err(IggyError::InvalidCredentials),
                    Some(EvictionReason::InvalidToken) =>
                        Err(IggyError::InvalidPersonalAccessToken),
                    None => Ok(()),
                    _ => unreachable!(),
                }
            );
            assert!(explicit.is_ok(), "rejection={rejection:?}: {explicit:?}");
            let requests = cluster.received(LOGIN_REGISTER_CODE);
            assert_eq!(requests.len(), 2);
            let request = LoginRegisterRequest::decode_from(&requests[1].body).unwrap();
            assert_eq!(request.username.as_str(), "other");
            assert_eq!(
                cluster.count(LOGOUT_USER_CODE),
                usize::from(rejection.is_none())
            );
            assert_eq!(client.get_state().await, ClientState::Authenticated);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_lost_link_with_a_bound_session_when_signing_in_should_release_old_identity() {
        for expired in [false, true] {
            let cluster = FakeCluster::new(1);
            let client = manual_client(&cluster);
            client.login_user("iggy", "iggy").await.unwrap();
            // A transport loss preserves the session; public Disconnect resets it.
            client.driver.links().disconnect();
            if expired {
                *cluster.session.lock().unwrap() = None;
            }
            client.login_user("other", "password").await.unwrap();
            assert_eq!(client.get_state().await, ClientState::Authenticated);
            let requests = cluster.received(LOGIN_REGISTER_CODE);
            assert_eq!(requests.len(), 2);
            let request = LoginRegisterRequest::decode_from(&requests[1].body).unwrap();
            assert_eq!(request.username.as_str(), "other");
            assert_eq!(cluster.count(BIND_SESSION_CODE), 1);
            assert_eq!(cluster.count(LOGOUT_USER_CODE), usize::from(!expired));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_stop_during_connect_when_explicit_login_waits_should_not_redial() {
        for shutdown in [false, true] {
            let cluster = FakeCluster::new(1);
            cluster.script(&endpoint(0), LOGIN_REGISTER_CODE, [Answer::Silence]);
            let client = TcpClient::from_links(links(&cluster, &endpoint(0), policy()));
            let (automatic, explicit, ()) = tokio::join!(
                client.connect(),
                client.login_user("other", "password"),
                async {
                    eventually("automatic register reaches the server", || {
                        cluster.count(LOGIN_REGISTER_CODE) == 1
                    })
                    .await;
                    if shutdown {
                        client.shutdown().await.unwrap();
                    } else {
                        client.disconnect().await.unwrap();
                    }
                }
            );
            assert!(automatic.is_err());
            assert!(explicit.is_err());
            assert_eq!(
                client.get_state().await,
                if shutdown {
                    ClientState::Shutdown
                } else {
                    ClientState::Disconnected
                }
            );
            assert!(!client.driver.session().epoch().bound);
            assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
            assert_eq!(cluster.dials_to(&endpoint(0)).len(), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_rejected_credentials_when_signing_in_again_should_reconnect_without_retaining() {
        for (replicas, previously_authenticated) in [(1, false), (2, true), (1, true)] {
            let cluster = FakeCluster::new(replicas);
            let client = manual_client(&cluster);
            client.connect().await.unwrap();
            if previously_authenticated {
                client.login_user("iggy", "iggy").await.unwrap();
                eventually("all replica links are ready", || {
                    client.driver.links().ready_replicas().len() == usize::from(replicas)
                })
                .await;
            }
            let previous_identity = client.driver.session().identity();
            cluster.script(
                &endpoint(0),
                LOGIN_REGISTER_CODE,
                [Answer::Evict(EvictionReason::InvalidCredentials)],
            );
            assert_eq!(
                client.login_user("iggy", "wrong").await.unwrap_err(),
                IggyError::InvalidCredentials
            );
            if replicas > 1 {
                assert!(client.driver.links().ready_replicas().contains(&1));
            }
            client.login_user("other", "password").await.unwrap();
            assert_eq!(client.get_state().await, ClientState::Authenticated);
            assert_ne!(client.driver.session().identity(), previous_identity);
            let requests = cluster.received(LOGIN_REGISTER_CODE);
            assert_eq!(requests.len(), 2 + usize::from(previously_authenticated));
            let request =
                LoginRegisterRequest::decode_from(&requests.last().unwrap().body).unwrap();
            assert_eq!(request.username.as_str(), "other");
            assert_eq!(
                cluster.count(LOGOUT_USER_CODE),
                usize::from(previously_authenticated)
            );
            assert_eq!(cluster.dials_to(&endpoint(0)).len(), 2);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_silent_endpoint_when_login_deadline_passes_should_never_send_register() {
        let cluster = FakeCluster::new(1);
        cluster.silent.lock().unwrap().insert(endpoint(0));
        let client = manual_client(&cluster);
        let deadline = Instant::now() + Duration::from_millis(10);
        let result = with_binary_deadline(deadline, client.login_user("iggy", "iggy")).await;
        assert_eq!(result.unwrap_err(), IggyError::TransientNotAccepted);
        assert_eq!(Instant::now(), deadline);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 0);
        assert!(!client.driver.session().epoch().bound);
    }

    #[tokio::test(start_paused = true)]
    async fn given_stop_during_explicit_login_when_reply_is_pending_should_not_restore_session() {
        for shutdown in [false, true] {
            let cluster = FakeCluster::new(1);
            cluster.script(&endpoint(0), LOGIN_REGISTER_CODE, [Answer::Silence]);
            let client = manual_client(&cluster);
            let (result, ()) = tokio::join!(client.login_user("iggy", "iggy"), async {
                eventually("register reaches the server", || {
                    cluster.count(LOGIN_REGISTER_CODE) == 1
                })
                .await;
                if shutdown {
                    client.shutdown().await.unwrap();
                } else {
                    client.disconnect().await.unwrap();
                }
            });
            assert!(result.is_err());
            assert_eq!(
                client.get_state().await,
                if shutdown {
                    ClientState::Shutdown
                } else {
                    ClientState::Disconnected
                }
            );
            assert!(!client.driver.session().epoch().bound);
            assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_committed_password_change_when_signing_in_again_should_send_the_new_password()
    {
        for (raw, resolved) in [(false, false), (true, false), (false, true), (true, true)] {
            let cluster = FakeCluster::new(1);
            let client = signed_in_client(&cluster).await;
            if resolved {
                end_uncertain(&cluster, CHANGE_PASSWORD_CODE, Answer::Commit(COMMIT));
            }

            let changed = if raw {
                let request = ChangePasswordRequest {
                    user_id: WireIdentifier::named(USERNAME).unwrap(),
                    current_password: PASSWORD.to_owned(),
                    new_password: NEW_PASSWORD.to_owned(),
                };
                client
                    .send_raw_with_response(CHANGE_PASSWORD_CODE, request.to_bytes())
                    .await
                    .map(drop)
            } else {
                let user = Identifier::named(USERNAME).unwrap();
                client.change_password(&user, PASSWORD, NEW_PASSWORD).await
            };
            proved(&client, changed).await.unwrap();
            let sign_in = sign_in_again(&cluster).await;

            let case = format!("raw={raw}, resolved={resolved}");
            let attempts = if resolved { 3 } else { 1 };
            assert_eq!(cluster.count(CHANGE_PASSWORD_CODE), attempts, "{case}");
            assert_eq!(sign_in.password.expose_secret(), NEW_PASSWORD, "{case}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_committed_rename_of_the_session_user_when_signing_in_again_should_send_the_new_username()
     {
        for resolved in [false, true] {
            let cluster = FakeCluster::new(1);
            let client = signed_in_client(&cluster).await;
            if resolved {
                end_uncertain(&cluster, UPDATE_USER_CODE, Answer::Commit(COMMIT));
            }

            let renamed = client
                .update_user(
                    &Identifier::named(USERNAME).unwrap(),
                    Some(NEW_USERNAME),
                    None,
                    &UserUpdateOptions::default(),
                )
                .await;
            proved(&client, renamed).await.unwrap();
            let sign_in = sign_in_again(&cluster).await;

            let case = format!("resolved={resolved}");
            let attempts = if resolved { 3 } else { 1 };
            assert_eq!(cluster.count(UPDATE_USER_CODE), attempts, "{case}");
            assert_eq!(sign_in.username.as_str(), NEW_USERNAME, "{case}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_committed_rejection_of_a_password_change_when_signing_in_again_should_send_the_old_password()
     {
        for resolved in [false, true] {
            let cluster = FakeCluster::new(1);
            let client = signed_in_client(&cluster).await;
            let rejected = Answer::CommitRejected(COMMIT, IggyError::InvalidCredentials);
            if resolved {
                end_uncertain(&cluster, CHANGE_PASSWORD_CODE, rejected);
            } else {
                cluster.script(&endpoint(0), CHANGE_PASSWORD_CODE, [rejected]);
            }

            let changed = client
                .change_password(
                    &Identifier::named(USERNAME).unwrap(),
                    WRONG_PASSWORD,
                    NEW_PASSWORD,
                )
                .await;

            assert_eq!(
                proved(&client, changed).await,
                Err(IggyError::InvalidCredentials),
                "resolved={resolved}"
            );
            let sign_in = sign_in_again(&cluster).await;
            assert_eq!(
                sign_in.password.expose_secret(),
                PASSWORD,
                "resolved={resolved}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_explicit_disconnect_when_keepalive_ticks_should_not_reconnect() {
        let cluster = FakeCluster::new(1);
        let client = TcpClient::from_links(links(&cluster, &endpoint(0), policy()));
        client.connect().await.unwrap();

        client.disconnect().await.unwrap();

        assert!(!client.keepalive().await.unwrap());
        assert_eq!(client.get_state().await, ClientState::Disconnected);
        assert_eq!(cluster.dials_to(&endpoint(0)).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_logout_when_keepalive_ticks_should_not_sign_in_again() {
        let cluster = FakeCluster::new(1);
        let client = TcpClient::from_links(links(&cluster, &endpoint(0), policy()));
        client.connect().await.unwrap();
        client.logout_user().await.unwrap();

        // Every link stops, as after an outage longer than the retry budget.
        client.driver.links().disconnect();

        assert!(!client.keepalive().await.unwrap());
        assert_eq!(client.get_state().await, ClientState::Disconnected);
        assert!(!client.driver.session().epoch().bound);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_rejected_explicit_sign_in_after_the_session_was_lost_when_keepalive_ticks_should_not_sign_in_again()
     {
        for owed in [true, false] {
            let cluster = FakeCluster::new(1);
            let limited = ReconnectPolicy::from(&TcpClientReconnectionConfig {
                max_retries: Some(1),
                ..TcpClientReconnectionConfig::default()
            });
            let client = TcpClient::from_links(links(&cluster, &endpoint(0), limited));
            client.connect().await.unwrap();
            *cluster.session.lock().unwrap() = None;
            if owed {
                // The slot finds the session gone and owes the sign-in, which
                // loses its link until the slot runs out of retries.
                cluster
                    .hang_up_on_register
                    .lock()
                    .unwrap()
                    .insert(endpoint(0));
                cluster.drop_connections(&endpoint(0));
                eventually("the slot stops", || {
                    client.driver.links().state() == ClientState::Disconnected
                })
                .await;
                cluster.hang_up_on_register.lock().unwrap().clear();
            } else {
                // The explicit sign-in is the first to find the session gone.
                client.driver.links().disconnect();
            }
            assert_eq!(client.driver.session().epoch().sign_in_owed, owed);
            *cluster.reject_credentials.lock().unwrap() = true;
            assert_eq!(
                client.login_user("other", "password").await.unwrap_err(),
                IggyError::InvalidCredentials
            );
            *cluster.reject_credentials.lock().unwrap() = false;
            let registers = cluster.count(LOGIN_REGISTER_CODE);
            cluster.drop_connections(&endpoint(0));
            eventually("the client is disconnected", || {
                client.driver.links().state() == ClientState::Disconnected
            })
            .await;

            let pinged = client.keepalive().await.unwrap();
            sleep(TEST_BUDGET).await;

            let signed_in_as: Vec<String> = cluster.received(LOGIN_REGISTER_CODE)[registers..]
                .iter()
                .map(|request| {
                    let request = LoginRegisterRequest::decode_from(&request.body).unwrap();
                    request.username.as_str().to_owned()
                })
                .collect();
            assert!(
                signed_in_as.is_empty(),
                "owed={owed}: signed in again as {signed_in_as:?}"
            );
            assert!(!pinged, "owed={owed}");
            assert_eq!(
                client.get_state().await,
                ClientState::Disconnected,
                "owed={owed}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_late_automatic_sign_in_when_signing_in_as_another_user_should_keep_that_user()
    {
        for replicas in [1, 2] {
            let cluster = FakeCluster::new(replicas);
            let client = TcpClient::from_links(links(&cluster, &endpoint(0), policy()));
            client.connect().await.unwrap();
            eventually("every slot is ready", || {
                client.driver.links().ready_replicas().len() == usize::from(replicas)
            })
            .await;
            // The last slot finds the session gone and signs in again as the
            // last user. The server registers it, but the reply waits. With
            // two replicas, that slot is not on the link of the explicit
            // Register, so the explicit reply can come first.
            cluster
                .held_registers
                .lock()
                .unwrap()
                .insert("iggy".to_owned());
            *cluster.session.lock().unwrap() = None;
            cluster.drop_connections(&endpoint(replicas - 1));
            eventually("the slot signs in again", || {
                cluster.count(LOGIN_REGISTER_CODE) == 2
            })
            .await;
            assert!(
                client.driver.session().epoch().sign_in_owed,
                "replicas={replicas}"
            );
            let registers = cluster.count(LOGIN_REGISTER_CODE);

            let signed_in = client.login_user("other", "password").await;
            assert!(signed_in.is_ok(), "replicas={replicas}: {signed_in:?}");
            cluster.held_registers.lock().unwrap().clear();
            sleep(TEST_BUDGET).await;

            let requests = cluster.received(LOGIN_REGISTER_CODE);
            let signed_in_as: Vec<String> = requests[registers..]
                .iter()
                .map(|request| {
                    let request = LoginRegisterRequest::decode_from(&request.body).unwrap();
                    request.username.as_str().to_owned()
                })
                .collect();
            assert_eq!(signed_in_as, ["other"], "replicas={replicas}");
            let (owed, explicit) = (&requests[registers - 1], &requests[registers]);
            assert_ne!(
                explicit.header.client, owed.header.client,
                "replicas={replicas}"
            );
            assert_eq!(
                client.get_state().await,
                ClientState::Authenticated,
                "replicas={replicas}"
            );
            let registered = cluster.session.lock().unwrap().unwrap();
            assert_eq!(
                client
                    .driver
                    .session()
                    .identity()
                    .map(|identity| identity.session),
                Ok(registered),
                "replicas={replicas}"
            );
        }
    }

    #[test]
    fn should_fail_with_a_zero_heartbeat_interval() {
        let value = "iggy+tcp://user:secret@127.0.0.1:1234?heartbeat_interval=none";

        let error = TcpClient::from_connection_string(value).err();

        assert!(matches!(error, Some(IggyError::InvalidConnectionString)));
    }

    #[test]
    fn should_succeed_with_a_zero_reestablish_after() {
        let value = "iggy+tcp://user:secret@127.0.0.1:1234?reestablish_after=0";

        let client = TcpClient::from_connection_string(value).unwrap();

        assert!(client.config.reconnection.reestablish_after.is_zero());
    }

    #[test]
    fn should_fail_with_a_zero_reconnection_interval() {
        let value = "iggy+tcp://user:secret@127.0.0.1:1234?reconnection_interval=0";

        let error = TcpClient::from_connection_string(value).err();

        assert!(matches!(error, Some(IggyError::InvalidConnectionString)));
    }

    #[test]
    fn should_fail_with_empty_connection_string() {
        let value = "";
        let tcp_client = TcpClient::from_connection_string(value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_fail_without_username() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_fail_without_password() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_fail_without_server_address() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_fail_without_port() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_fail_with_invalid_prefix() {
        let connection_string_prefix = "invalid+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_fail_with_unmatch_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_succeed_with_default_prefix() {
        let default_connection_string_prefix = "iggy://";
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{default_connection_string_prefix}{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_ok());
    }

    #[test]
    fn should_fail_with_invalid_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?invalid_option=invalid"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_succeed_without_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_ok());

        let tcp_client_config = tcp_client.unwrap().config;
        assert_eq!(
            tcp_client_config.server_address,
            format!("{server_address}:{port}")
        );
        match &tcp_client_config.auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(u, p)) => {
                assert_eq!(u, &username.to_string());
                assert_eq!(p.expose_secret(), password);
            }
            other => panic!("expected UsernamePassword auto_login, got {other:?}"),
        }

        assert!(!tcp_client_config.tls_enabled);
        assert!(tcp_client_config.tls_domain.is_empty());
        assert!(tcp_client_config.tls_ca_file.is_none());
        assert_eq!(
            tcp_client_config.heartbeat_interval,
            NonZeroIggyDuration::from_str("5s").unwrap()
        );

        assert!(tcp_client_config.reconnection.enabled);
        assert!(tcp_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            tcp_client_config.reconnection.interval,
            NonZeroIggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            tcp_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[test]
    fn should_succeed_with_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let heartbeat_interval = "10s";
        let reconnection_retries = "10";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?heartbeat_interval={heartbeat_interval}&reconnection_retries={reconnection_retries}"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_ok());

        let tcp_client_config = tcp_client.unwrap().config;
        assert_eq!(
            tcp_client_config.server_address,
            format!("{server_address}:{port}")
        );
        match &tcp_client_config.auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(u, p)) => {
                assert_eq!(u, &username.to_string());
                assert_eq!(p.expose_secret(), password);
            }
            other => panic!("expected UsernamePassword auto_login, got {other:?}"),
        }

        assert!(!tcp_client_config.tls_enabled);
        assert!(tcp_client_config.tls_domain.is_empty());
        assert!(tcp_client_config.tls_ca_file.is_none());
        assert_eq!(
            tcp_client_config.heartbeat_interval,
            NonZeroIggyDuration::from_str(heartbeat_interval).unwrap()
        );

        assert!(tcp_client_config.reconnection.enabled);
        assert_eq!(
            tcp_client_config.reconnection.max_retries.unwrap(),
            reconnection_retries.parse::<u32>().unwrap()
        );
        assert_eq!(
            tcp_client_config.reconnection.interval,
            NonZeroIggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            tcp_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[test]
    fn should_succeed_with_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_ok());

        let tcp_client_config = tcp_client.unwrap().config;
        assert_eq!(
            tcp_client_config.server_address,
            format!("{server_address}:{port}")
        );
        match &tcp_client_config.auto_login {
            AutoLogin::Enabled(Credentials::PersonalAccessToken(t)) => {
                assert_eq!(t.expose_secret(), pat);
            }
            other => panic!("expected PersonalAccessToken auto_login, got {other:?}"),
        }

        assert!(!tcp_client_config.tls_enabled);
        assert!(tcp_client_config.tls_domain.is_empty());
        assert!(tcp_client_config.tls_ca_file.is_none());
        assert_eq!(
            tcp_client_config.heartbeat_interval,
            NonZeroIggyDuration::from_str("5s").unwrap()
        );

        assert!(tcp_client_config.reconnection.enabled);
        assert!(tcp_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            tcp_client_config.reconnection.interval,
            NonZeroIggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            tcp_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }
}
