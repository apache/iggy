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
    use crate::links::fake_cluster::{Answer, FakeCluster, endpoint, eventually, links, policy};
    use iggy_binary_protocol::WireDecode;
    use iggy_binary_protocol::codes::{
        BIND_SESSION_CODE, LOGIN_REGISTER_CODE, LOGOUT_USER_CODE, PING_CODE,
    };
    use iggy_binary_protocol::consensus::EvictionReason;
    use iggy_binary_protocol::requests::users::{
        LoginRegisterRequest, LoginRegisterWithPatRequest,
    };
    use iggy_common::{
        BinaryTransport, Client, ClientState, Credentials, IggyDuration, PersonalAccessTokenClient,
        SystemClient, UserClient, with_binary_deadline,
    };
    use secrecy::{ExposeSecret, SecretString};
    use std::time::Duration;
    use tokio::time::Instant;

    fn manual_client(cluster: &Arc<FakeCluster>) -> TcpClient {
        let session = Arc::new(SessionOwner::new(AutoLogin::Disabled));
        TcpClient::from_links(ReplicaLinks::new(
            cluster.dialer(),
            endpoint(0),
            policy(),
            session,
        ))
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
    async fn given_disconnect_during_connect_when_explicit_login_waits_should_not_redial() {
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
                client.disconnect().await.unwrap();
            }
        );
        assert!(automatic.is_err());
        assert!(explicit.is_err());
        assert_eq!(client.get_state().await, ClientState::Disconnected);
        assert!(!client.driver.session().epoch().bound);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
        assert_eq!(cluster.dials_to(&endpoint(0)).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_rejected_credentials_when_signing_in_again_should_reconnect_without_retaining() {
        let cluster = FakeCluster::new(1);
        cluster.script(
            &endpoint(0),
            LOGIN_REGISTER_CODE,
            [Answer::Evict(EvictionReason::InvalidCredentials)],
        );
        let client = manual_client(&cluster);
        client.connect().await.unwrap();
        assert_eq!(
            client.login_user("iggy", "wrong").await.unwrap_err(),
            IggyError::InvalidCredentials
        );
        client.login_user("iggy", "iggy").await.unwrap();
        assert_eq!(client.get_state().await, ClientState::Authenticated);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 2);
        assert_eq!(cluster.dials_to(&endpoint(0)).len(), 2);
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
    async fn given_disconnect_during_explicit_login_when_reply_is_pending_should_not_restore_session()
     {
        let cluster = FakeCluster::new(1);
        cluster.script(&endpoint(0), LOGIN_REGISTER_CODE, [Answer::Silence]);
        let client = manual_client(&cluster);
        let (result, ()) = tokio::join!(client.login_user("iggy", "iggy"), async {
            eventually("register reaches the server", || {
                cluster.count(LOGIN_REGISTER_CODE) == 1
            })
            .await;
            client.disconnect().await.unwrap();
        });
        assert!(result.is_err());
        assert_eq!(client.get_state().await, ClientState::Disconnected);
        assert!(!client.driver.session().epoch().bound);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
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
