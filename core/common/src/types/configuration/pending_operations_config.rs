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

use std::num::NonZeroUsize;

/// Client-wide limits for active and retained replicated mutations.
/// Queued writers keep their frame bytes charged after completion or abandonment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingOperationsConfig {
    /// Active or retained mutations sharing this client session.
    pub max_operations: NonZeroUsize,
    /// Frozen headers and bodies, including frames still held by a writer.
    /// A useful limit must fit the 256-byte header plus the largest mutation body.
    /// Smaller positive limits are valid but reject every mutation that cannot fit.
    pub max_bytes: NonZeroUsize,
}

impl PendingOperationsConfig {
    pub const DEFAULT_MAX_OPERATIONS: NonZeroUsize = NonZeroUsize::new(256).unwrap();
    pub const DEFAULT_MAX_BYTES: NonZeroUsize = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
}

impl Default for PendingOperationsConfig {
    fn default() -> Self {
        Self {
            max_operations: Self::DEFAULT_MAX_OPERATIONS,
            max_bytes: Self::DEFAULT_MAX_BYTES,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConnectionString, IggyError, QuicClientConfig, QuicClientConfigBuilder,
        QuicConnectionStringOptions, TcpClientConfig, TcpClientConfigBuilder,
        TcpConnectionStringOptions, WebSocketClientConfig, WebSocketClientConfigBuilder,
        WebSocketConnectionStringOptions,
    };
    use std::str::FromStr;

    #[test]
    fn given_custom_mutation_limits_when_building_transport_configs_should_preserve_them() {
        let limits = PendingOperationsConfig {
            max_operations: NonZeroUsize::new(3).unwrap(),
            max_bytes: NonZeroUsize::new(4096).unwrap(),
        };
        assert_eq!(
            TcpClientConfigBuilder::new()
                .with_pending_operations(limits)
                .build()
                .unwrap()
                .pending_operations,
            limits
        );
        assert_eq!(
            QuicClientConfigBuilder::new()
                .with_pending_operations(limits)
                .build()
                .unwrap()
                .pending_operations,
            limits
        );
        assert_eq!(
            WebSocketClientConfigBuilder::new()
                .with_pending_operations(limits)
                .build()
                .unwrap()
                .pending_operations,
            limits
        );
    }

    #[test]
    fn given_connection_string_limits_when_building_all_binary_transports_should_preserve_them() {
        let query = "max_pending_operations=3&max_pending_operation_bytes=4096";
        let tcp = TcpClientConfig::from(
            ConnectionString::<TcpConnectionStringOptions>::from_str(&format!(
                "iggy+tcp://iggy:iggy@localhost:8090?{query}"
            ))
            .unwrap(),
        );
        let quic = QuicClientConfig::from(
            ConnectionString::<QuicConnectionStringOptions>::from_str(&format!(
                "iggy+quic://iggy:iggy@localhost:8080?{query}"
            ))
            .unwrap(),
        );
        let ws = WebSocketClientConfig::from(
            ConnectionString::<WebSocketConnectionStringOptions>::from_str(&format!(
                "iggy+ws://iggy:iggy@localhost:8092?{query}"
            ))
            .unwrap(),
        );
        let expected = PendingOperationsConfig {
            max_operations: NonZeroUsize::new(3).unwrap(),
            max_bytes: NonZeroUsize::new(4096).unwrap(),
        };
        for actual in [
            tcp.pending_operations,
            quic.pending_operations,
            ws.pending_operations,
        ] {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn given_invalid_connection_string_limits_when_parsing_should_fail_before_client_creation() {
        for field in ["max_pending_operations", "max_pending_operation_bytes"] {
            for value in ["0", "-1", "1.5", "18446744073709551616", ""] {
                let address = format!("iggy+tcp://iggy:iggy@localhost:8090?{field}={value}");
                assert!(matches!(
                    ConnectionString::<TcpConnectionStringOptions>::from_str(&address),
                    Err(IggyError::InvalidConnectionString)
                ));
                let address = address.replace("iggy+tcp:", "iggy+quic:");
                assert!(matches!(
                    ConnectionString::<QuicConnectionStringOptions>::from_str(&address),
                    Err(IggyError::InvalidConnectionString)
                ));
                let address = address.replace("iggy+quic:", "iggy+ws:");
                assert!(matches!(
                    ConnectionString::<WebSocketConnectionStringOptions>::from_str(&address),
                    Err(IggyError::InvalidConnectionString)
                ));
            }
        }
    }
}
