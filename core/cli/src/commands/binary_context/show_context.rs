/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use anyhow::bail;
use async_trait::async_trait;
use comfy_table::Table;
use tracing::{Level, event};

use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use iggy_common::Client;

use super::common::{ContextConfig, ContextManager};

const MASKED_VALUE: &str = "********";

pub struct ShowContextCmd {
    context_name: String,
}

impl ShowContextCmd {
    pub fn new(context_name: String) -> Self {
        Self { context_name }
    }

    fn add_opt_str(table: &mut Table, label: &str, value: &Option<String>) {
        if let Some(ref v) = *value {
            table.add_row(vec![label, v]);
        }
    }

    fn add_opt_bool(table: &mut Table, label: &str, value: Option<bool>) {
        if let Some(v) = value {
            table.add_row(vec![label, &v.to_string()]);
        }
    }

    fn add_opt_u16(table: &mut Table, label: &str, value: Option<u16>) {
        if let Some(v) = value {
            table.add_row(vec![label, &v.to_string()]);
        }
    }

    fn add_opt_u32(table: &mut Table, label: &str, value: Option<u32>) {
        if let Some(v) = value {
            table.add_row(vec![label, &v.to_string()]);
        }
    }

    fn add_opt_u64(table: &mut Table, label: &str, value: Option<u64>) {
        if let Some(v) = value {
            table.add_row(vec![label, &v.to_string()]);
        }
    }

    fn add_masked(table: &mut Table, label: &str, value: &Option<String>) {
        if value.is_some() {
            table.add_row(vec![label, MASKED_VALUE]);
        }
    }

    fn build_table(name: &str, is_active: bool, config: &ContextConfig) -> Table {
        let mut table = Table::new();
        table.set_header(vec!["Property", "Value"]);

        let display_name = if is_active {
            format!("{name}*")
        } else {
            name.to_string()
        };
        table.add_row(vec!["Name", &display_name]);

        let iggy = &config.iggy;

        Self::add_opt_str(&mut table, "Transport", &iggy.transport);
        Self::add_masked(&mut table, "Encryption Key", &iggy.encryption_key);
        Self::add_opt_str(
            &mut table,
            "Credentials Username",
            &iggy.credentials_username,
        );
        Self::add_masked(
            &mut table,
            "Credentials Password",
            &iggy.credentials_password,
        );

        Self::add_opt_str(&mut table, "HTTP API URL", &iggy.http_api_url);
        Self::add_opt_u32(&mut table, "HTTP Retries", iggy.http_retries);

        Self::add_opt_str(&mut table, "TCP Server Address", &iggy.tcp_server_address);
        Self::add_opt_u32(
            &mut table,
            "TCP Reconnection Max Retries",
            iggy.tcp_reconnection_max_retries,
        );
        Self::add_opt_str(
            &mut table,
            "TCP Reconnection Interval",
            &iggy.tcp_reconnection_interval,
        );
        Self::add_opt_bool(&mut table, "TCP TLS Enabled", iggy.tcp_tls_enabled);
        Self::add_opt_str(&mut table, "TCP TLS Domain", &iggy.tcp_tls_domain);

        Self::add_opt_str(&mut table, "QUIC Client Address", &iggy.quic_client_address);
        Self::add_opt_str(&mut table, "QUIC Server Address", &iggy.quic_server_address);
        Self::add_opt_str(&mut table, "QUIC Server Name", &iggy.quic_server_name);
        Self::add_opt_u32(
            &mut table,
            "QUIC Reconnection Max Retries",
            iggy.quic_reconnection_max_retries,
        );
        Self::add_opt_str(
            &mut table,
            "QUIC Reconnection Interval",
            &iggy.quic_reconnection_interval,
        );
        Self::add_opt_u64(
            &mut table,
            "QUIC Max Concurrent Bidi Streams",
            iggy.quic_max_concurrent_bidi_streams,
        );
        Self::add_opt_u64(
            &mut table,
            "QUIC Datagram Send Buffer Size",
            iggy.quic_datagram_send_buffer_size,
        );
        Self::add_opt_u16(&mut table, "QUIC Initial MTU", iggy.quic_initial_mtu);
        Self::add_opt_u64(&mut table, "QUIC Send Window", iggy.quic_send_window);
        Self::add_opt_u64(&mut table, "QUIC Receive Window", iggy.quic_receive_window);
        Self::add_opt_u64(
            &mut table,
            "QUIC Response Buffer Size",
            iggy.quic_response_buffer_size,
        );
        Self::add_opt_u64(
            &mut table,
            "QUIC Keep Alive Interval",
            iggy.quic_keep_alive_interval,
        );
        Self::add_opt_u64(
            &mut table,
            "QUIC Max Idle Timeout",
            iggy.quic_max_idle_timeout,
        );
        Self::add_opt_bool(
            &mut table,
            "QUIC Validate Certificate",
            iggy.quic_validate_certificate,
        );

        Self::add_opt_str(
            &mut table,
            "WebSocket Server Address",
            &iggy.websocket_server_address,
        );
        Self::add_opt_u32(
            &mut table,
            "WebSocket Reconnection Max Retries",
            iggy.websocket_reconnection_max_retries,
        );
        Self::add_opt_str(
            &mut table,
            "WebSocket Reconnection Interval",
            &iggy.websocket_reconnection_interval,
        );

        if let Some(ref username) = config.username {
            table.add_row(vec!["Username", username]);
        }
        Self::add_masked(&mut table, "Password", &config.password);
        Self::add_masked(&mut table, "Token", &config.token);
        Self::add_opt_str(&mut table, "Token Name", &config.token_name);

        for (key, value) in &config.extra {
            table.add_row(vec![key.as_str(), &value.to_string()]);
        }

        table
    }
}

#[async_trait]
impl CliCommand for ShowContextCmd {
    fn explain(&self) -> String {
        let context_name = &self.context_name;
        format!("show context {context_name}")
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut context_mgr = ContextManager::default();
        let contexts_map = context_mgr.get_contexts().await?;
        let active_context_key = context_mgr.get_active_context_key().await?;

        let config = match contexts_map.get(&self.context_name) {
            Some(config) => config,
            None => bail!("context '{}' not found", self.context_name),
        };

        let is_active = self.context_name == active_context_key;
        let table = Self::build_table(&self.context_name, is_active, config);

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_explain_message() {
        let cmd = ShowContextCmd::new("production".to_string());
        assert_eq!(cmd.explain(), "show context production");
    }

    #[test]
    fn should_not_require_login() {
        let cmd = ShowContextCmd::new("test".to_string());
        assert!(!cmd.login_required());
    }

    #[test]
    fn should_not_require_connection() {
        let cmd = ShowContextCmd::new("test".to_string());
        assert!(!cmd.connection_required());
    }
}
