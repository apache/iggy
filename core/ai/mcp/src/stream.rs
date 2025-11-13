/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use crate::{configs::IggyConfig, error::McpRuntimeError};
use iggy::prelude::{Client, IggyClient, IggyClientBuilder};
use tracing::{error, info};

pub async fn init(config: IggyConfig) -> Result<IggyClient, McpRuntimeError> {
    let address = config.address;
    let username = config.username;
    let password = config.password;
    let token = config.token;

    let connection_string = if !token.is_empty() {
        let redacted_token = token.chars().take(3).collect::<String>();
        info!("Using token: {redacted_token}*** for Iggy authentication");
        format!("iggy://{token}@{address}")
    } else {
        info!("Using username and password for Iggy authentication");
        if username.is_empty() {
            error!("Iggy username cannot be empty (if token is not provided)");
            return Err(McpRuntimeError::MissingIggyCredentials);
        }

        if password.is_empty() {
            error!("Iggy password cannot be empty (if token is not provided)");
            return Err(McpRuntimeError::MissingIggyCredentials);
        }

        let redacted_username = username.chars().take(3).collect::<String>();
        let redacted_password = password.chars().take(3).collect::<String>();
        info!(
            "Using username: {redacted_username}***, password: {redacted_password}*** for Iggy authentication"
        );
        format!("iggy://{username}:{password}@{address}")
    };

    let connection_string = if config.tls.enabled {
        let ca_file = &config.tls.ca_file;
        if ca_file.is_empty() {
            error!("TLS CA file must be provided when TLS is enabled.");
            return Err(McpRuntimeError::MissingTlsCertificateFile);
        }
        let domain = config
            .tls
            .domain
            .clone()
            .filter(|domain| !domain.is_empty())
            .map(|domain| format!("&tls_domain={domain}"))
            .unwrap_or_default();
        format!("{connection_string}?tls=true&tls_ca_file={ca_file}{domain}")
    } else {
        connection_string
    };

    let client = IggyClientBuilder::from_connection_string(&connection_string)?.build()?;
    client.connect().await?;
    Ok(client)
}
