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
use iggy_common::{ConnectionString, WebSocketConnectionStringOptions};
use iggy_common::{IggyError, WebSocketClientConfig};
use std::sync::Arc;
use tracing::error;

/// WebSocket client whose replica links share one session and operation driver.
#[derive(Debug)]
pub struct WebSocketClient {
    pub(crate) config: Arc<WebSocketClientConfig>,
    pub(crate) driver: OperationDriver,
}

impl Default for WebSocketClient {
    fn default() -> Self {
        Self::create(Arc::new(WebSocketClientConfig::default())).unwrap()
    }
}

impl_binary_client!(WebSocketClient);

impl WebSocketClient {
    /// Creates a WebSocket client from an Iggy connection string.
    ///
    /// # Errors
    ///
    /// The error of parsing a malformed connection string, else an error of [`Self::create`].
    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        let parsed_connection_string =
            ConnectionString::<WebSocketConnectionStringOptions>::new(connection_string)?;
        let config = WebSocketClientConfig::from(parsed_connection_string);
        Self::create(Arc::new(config))
    }

    /// Creates the client without spawning tasks or opening sockets.
    ///
    /// # Errors
    ///
    /// [`IggyError::InvalidConfiguration`] when the WebSocket `max_write_buffer_size` is not
    /// greater than its `write_buffer_size`.
    pub fn create(config: Arc<WebSocketClientConfig>) -> Result<Self, IggyError> {
        let websocket = config.ws_config.to_tungstenite_config();
        if websocket.max_write_buffer_size <= websocket.write_buffer_size {
            error!(
                "WebSocket max_write_buffer_size: {} must be greater than write_buffer_size: {}",
                websocket.max_write_buffer_size, websocket.write_buffer_size
            );
            return Err(IggyError::InvalidConfiguration);
        }
        let session = Arc::new(SessionOwner::with_pending_operations(
            config.auto_login.clone(),
            config.pending_operations,
        ));
        let links = ReplicaLinks::new(
            Dialer::WebSocket(Arc::clone(&config)),
            config.server_address.clone(),
            ReconnectPolicy::from(&config.reconnection),
            session,
        );
        Ok(Self {
            config,
            driver: OperationDriver::new(links),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{AutoLogin, BinaryTransport, ClientState, NonZeroIggyDuration};
    use std::str::FromStr;

    #[test]
    fn should_be_created_with_default_config() {
        let client = WebSocketClient::default();
        assert_eq!(client.config.server_address, "127.0.0.1:8092");
        assert_eq!(
            client.config.heartbeat_interval,
            NonZeroIggyDuration::from_str("5s").unwrap()
        );
        assert!(matches!(client.config.auto_login, AutoLogin::Disabled));
        assert!(client.config.reconnection.enabled);
    }

    #[tokio::test]
    async fn should_be_disconnected_by_default() {
        let client = WebSocketClient::default();
        assert_eq!(client.get_state().await, ClientState::Disconnected);
    }

    #[test]
    fn should_succeed_from_connection_string() {
        let connection_string = "iggy+ws://user:secret@127.0.0.1:8092";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_ok());
    }

    #[test]
    fn should_reject_invalid_write_buffer_limits() {
        for options in [
            "max_write_buffer_size=1",
            "write_buffer_size=0&max_write_buffer_size=0",
            "write_buffer_size=1024&max_write_buffer_size=1024",
            "write_buffer_size=1024&max_write_buffer_size=1023",
        ] {
            let connection_string = format!("iggy+ws://user:secret@127.0.0.1:8092?{options}");
            let error = WebSocketClient::from_connection_string(&connection_string).err();

            assert_eq!(error, Some(IggyError::InvalidConfiguration), "{options}");
        }
    }

    #[test]
    fn should_accept_write_buffer_limits_above_the_write_buffer() {
        for options in [
            "write_buffer_size=0&max_write_buffer_size=1",
            "write_buffer_size=1024&max_write_buffer_size=1025",
        ] {
            let connection_string = format!("iggy+ws://user:secret@127.0.0.1:8092?{options}");
            let error = WebSocketClient::from_connection_string(&connection_string).err();

            assert_eq!(error, None, "{options}");
        }
    }

    #[test]
    fn should_create_with_custom_config() {
        let config = WebSocketClientConfig {
            server_address: "localhost:9090".to_string(),
            heartbeat_interval: NonZeroIggyDuration::from_str("10s").unwrap(),
            ..Default::default()
        };

        let client = WebSocketClient::create(Arc::new(config));
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.config.server_address, "localhost:9090");
        assert_eq!(
            client.config.heartbeat_interval,
            NonZeroIggyDuration::from_str("10s").unwrap()
        );
    }

    #[test]
    fn should_fail_with_a_zero_heartbeat_interval() {
        let value = "iggy+ws://user:secret@127.0.0.1:1234?heartbeat_interval=none";

        let error = WebSocketClient::from_connection_string(value).err();

        assert!(matches!(error, Some(IggyError::InvalidConnectionString)));
    }

    #[test]
    fn should_fail_with_a_zero_reconnection_interval() {
        let value = "iggy+ws://user:secret@127.0.0.1:1234?reconnection_interval=0";

        let error = WebSocketClient::from_connection_string(value).err();

        assert!(matches!(error, Some(IggyError::InvalidConnectionString)));
    }

    #[test]
    fn should_fail_with_empty_connection_string() {
        let value = "";
        let client = WebSocketClient::from_connection_string(value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_username() {
        let connection_string = "iggy+ws://:secret@127.0.0.1:8080";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_password() {
        let connection_string = "iggy+ws://user:@127.0.0.1:8080";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_server_address() {
        let connection_string = "iggy+ws://user:secret@:8080";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_with_invalid_options() {
        let connection_string = "iggy+ws://user:secret@127.0.0.1:8080?invalid_option=invalid";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_err());
    }

    #[test]
    fn should_succeed_from_connection_string_with_hostname() {
        let connection_string = "iggy+ws://user:secret@localhost:8092";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.config.server_address, "localhost:8092");
    }
}
