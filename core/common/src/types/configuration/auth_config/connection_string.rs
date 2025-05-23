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

use crate::{AutoLogin, ConnectionStringOptions, Credentials, IggyError};
use std::str::FromStr;

const DEFAULT_CONNECTION_STRING_PREFIX: &str = "iggy://";
const CONNECTION_STRING_PREFIX: &str = "iggy+";

#[derive(Debug)]
pub struct ConnectionString<T: ConnectionStringOptions + Default> {
    server_address: String,
    auto_login: AutoLogin,
    options: T,
}

impl<T: ConnectionStringOptions + Default> ConnectionString<T> {
    pub fn server_address(&self) -> &str {
        &self.server_address
    }

    pub fn auto_login(&self) -> &AutoLogin {
        &self.auto_login
    }

    pub fn options(&self) -> &T {
        &self.options
    }

    pub fn new(connection_string: &str) -> Result<Self, IggyError> {
        let connection_string = connection_string.split("://").collect::<Vec<&str>>()[1];
        let parts = connection_string.split('@').collect::<Vec<&str>>();

        if parts.len() != 2 {
            return Err(IggyError::InvalidConnectionString);
        }

        let credentials = parts[0].split(':').collect::<Vec<&str>>();
        if credentials.len() != 2 {
            return Err(IggyError::InvalidConnectionString);
        }

        let username = credentials[0];
        let password = credentials[1];
        if username.is_empty() || password.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        let server_and_options = parts[1].split('?').collect::<Vec<&str>>();
        if server_and_options.len() > 2 {
            return Err(IggyError::InvalidConnectionString);
        }

        let server_address = server_and_options[0];
        if server_address.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        if !server_address.contains(':') {
            return Err(IggyError::InvalidConnectionString);
        }

        let port = server_address.split(':').collect::<Vec<&str>>()[1];
        if port.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        if port.parse::<u16>().is_err() {
            return Err(IggyError::InvalidConnectionString);
        }

        let connection_string_options;
        if let Some(options) = server_and_options.get(1) {
            connection_string_options = T::parse_options(options)?;
        } else {
            connection_string_options = T::default();
        }

        Ok(ConnectionString {
            server_address: server_address.to_owned(),
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_owned(),
                password.to_owned(),
            )),
            options: connection_string_options,
        })
    }
}

impl<T: ConnectionStringOptions + Default> FromStr for ConnectionString<T> {
    type Err = IggyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ConnectionString::<T>::new(s)
    }
}

/// ConnectionStringUtils is a utility struct for connection strings.
pub struct ConnectionStringUtils;

impl ConnectionStringUtils {
    pub fn parse_protocol(connection_string: &str) -> Result<String, IggyError> {
        if connection_string.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        if connection_string.starts_with(DEFAULT_CONNECTION_STRING_PREFIX) {
            return Ok("TCP".to_string());
        }

        if !connection_string.starts_with(CONNECTION_STRING_PREFIX) {
            return Err(IggyError::InvalidConnectionString);
        }

        let connection_string = connection_string.replace(CONNECTION_STRING_PREFIX, "");
        let protocol = connection_string.split("://").collect::<Vec<&str>>()[0];

        match protocol {
            "" => {
                return Ok("TCP".to_string());
            }
            "tcp" => {
                return Ok("TCP".to_string());
            }
            "quic" => {
                return Ok("QUIC".to_string());
            }
            "http" => {
                return Ok("HTTP".to_string());
            }
            _ => {
                return Err(IggyError::InvalidConnectionString);
            }
        };
    }
}
