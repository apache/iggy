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

use iggy::prelude::{Client, IggyClient, IggyClientBuilder};
use std::path::PathBuf;
use tracing::{error, info};

use crate::configs::runtime::IggyConfig;
use crate::error::RuntimeError;

const TOKEN_FILE_PREFIX: &str = "file:";

fn append_query_parameters(connection_string: &str, parameters: &str) -> String {
    let separator = if connection_string.contains('?') {
        '&'
    } else {
        '?'
    };
    format!("{connection_string}{separator}{parameters}")
}

fn expand_home(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    } else if path == "~"
        && let Some(home) = dirs::home_dir()
    {
        return home;
    }
    PathBuf::from(path)
}

fn resolve_token(token: &str) -> Result<String, RuntimeError> {
    if let Some(path) = token.strip_prefix(TOKEN_FILE_PREFIX) {
        let file_path = expand_home(path);

        if !file_path.exists() {
            error!("Token file does not exist: {}", path);
            return Err(RuntimeError::TokenFileNotFound(path.to_string()));
        }

        let content = std::fs::read_to_string(&file_path).map_err(|e| {
            error!("Failed to read token file '{}': {}", path, e);
            RuntimeError::TokenFileReadError(path.to_string(), e.to_string())
        })?;

        let trimmed = content.trim().to_string();

        if trimmed.is_empty() {
            error!("Token file is empty: {}", path);
            return Err(RuntimeError::TokenFileEmpty(path.to_string()));
        }

        Ok(trimmed)
    } else {
        Ok(token.to_string())
    }
}

pub struct IggyClients {
    pub producer: IggyClient,
    pub consumer: IggyClient,
}

pub async fn init(config: IggyConfig) -> Result<IggyClients, RuntimeError> {
    let consumer = create_client(&config).await?;
    let producer = create_client(&config).await?;
    let iggy_clients = IggyClients { producer, consumer };
    Ok(iggy_clients)
}

/// Builds the authenticated connection string for `config`, resolving a
/// `file:`-prefixed token first. Every client of the configured Iggy server
/// goes through this, so all of them authenticate identically.
pub(crate) fn connection_string(config: &IggyConfig) -> Result<String, RuntimeError> {
    let token = if config.token.is_empty() {
        None
    } else {
        Some(resolve_token(&config.token)?)
    };
    connection_string_with_token(config, token.as_deref())
}

fn connection_string_with_token(
    config: &IggyConfig,
    token: Option<&str>,
) -> Result<String, RuntimeError> {
    let address = config.address.to_owned();
    let username = config.username.to_owned();
    let password = config.password.to_owned();

    let connection_string = if let Some(token) = token {
        let redacted_token = token.chars().take(3).collect::<String>();
        info!("Using token: {redacted_token}*** for Iggy authentication");
        format!("iggy://{token}@{address}")
    } else {
        info!("Using username and password for Iggy authentication");
        if username.is_empty() {
            error!("Iggy username cannot be empty (if token is not provided)");
            return Err(RuntimeError::MissingIggyCredentials);
        }

        if password.is_empty() {
            error!("Iggy password cannot be empty (if token is not provided)");
            return Err(RuntimeError::MissingIggyCredentials);
        }

        let redacted_username = username.chars().take(3).collect::<String>();
        let redacted_password = password.chars().take(3).collect::<String>();
        info!(
            "Using username: {redacted_username}***, password: {redacted_password}*** for Iggy authentication"
        );
        format!("iggy://{username}:{password}@{address}")
    };

    if config.tls.enabled {
        let ca_file = &config.tls.ca_file;
        if ca_file.is_empty() {
            error!("TLS CA file must be provided when TLS is enabled.");
            return Err(RuntimeError::MissingTlsCertificateFile);
        }
        let domain = config
            .tls
            .domain
            .clone()
            .filter(|domain| !domain.is_empty())
            .map(|domain| format!("&tls_domain={domain}"))
            .unwrap_or_default();
        Ok(append_query_parameters(
            &connection_string,
            &format!("tls=true&tls_ca_file={ca_file}{domain}"),
        ))
    } else {
        Ok(connection_string)
    }
}

async fn create_client(config: &IggyConfig) -> Result<IggyClient, RuntimeError> {
    let connection_string = connection_string(config)?;
    let client = IggyClientBuilder::from_connection_string(&connection_string)?.build()?;
    client.connect().await?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_expand_home_with_tilde_prefix() {
        let path = "~/some/path";
        let result = expand_home(path);

        if let Some(home) = dirs::home_dir() {
            assert_eq!(result, home.join("some/path"));
        } else {
            assert_eq!(result, PathBuf::from(path));
        }
    }

    #[test]
    fn test_expand_home_with_only_tilde() {
        let path = "~";
        let result = expand_home(path);

        if let Some(home) = dirs::home_dir() {
            assert_eq!(result, home);
        } else {
            assert_eq!(result, PathBuf::from(path));
        }
    }

    #[test]
    fn test_expand_home_without_tilde() {
        let path = "/absolute/path";
        let result = expand_home(path);
        assert_eq!(result, PathBuf::from("/absolute/path"));
    }

    #[test]
    fn test_expand_home_relative_path() {
        let path = "relative/path";
        let result = expand_home(path);
        assert_eq!(result, PathBuf::from("relative/path"));
    }

    #[test]
    fn given_existing_query_when_appending_parameters_should_use_ampersand() {
        let connection_string = "iggy://user:password@127.0.0.1:8090?reconnection_retries=0";

        let result = append_query_parameters(connection_string, "tls=true");

        assert_eq!(
            result,
            "iggy://user:password@127.0.0.1:8090?reconnection_retries=0&tls=true"
        );
    }

    #[test]
    fn given_no_query_when_appending_parameters_should_use_question_mark() {
        let connection_string = "iggy://user:password@127.0.0.1:8090";

        let result = append_query_parameters(connection_string, "tls=true");

        assert_eq!(result, "iggy://user:password@127.0.0.1:8090?tls=true");
    }

    #[test]
    fn test_resolve_token_direct_value() {
        let token = "my-secret-token";
        let result = resolve_token(token).unwrap();
        assert_eq!(result, "my-secret-token");
    }

    #[test]
    fn test_resolve_token_from_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "token-from-file").unwrap();

        let token = format!("file:{}", temp_file.path().display());
        let result = resolve_token(&token).unwrap();
        assert_eq!(result, "token-from-file");
    }

    #[test]
    fn test_resolve_token_from_file_trims_whitespace() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "  token-with-spaces  \n\n").unwrap();

        let token = format!("file:{}", temp_file.path().display());
        let result = resolve_token(&token).unwrap();
        assert_eq!(result, "token-with-spaces");
    }

    #[test]
    fn test_resolve_token_file_not_found() {
        let token = "file:/nonexistent/path/to/token.txt";
        let result = resolve_token(token);

        assert!(result.is_err());
        assert!(matches!(result, Err(RuntimeError::TokenFileNotFound(_))));
    }

    #[test]
    fn test_resolve_token_empty_file() {
        let temp_file = NamedTempFile::new().unwrap();

        let token = format!("file:{}", temp_file.path().display());
        let result = resolve_token(&token);

        assert!(result.is_err());
        assert!(matches!(result, Err(RuntimeError::TokenFileEmpty(_))));
    }

    #[test]
    fn test_resolve_token_file_with_only_whitespace() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "   \n\t\n   ").unwrap();

        let token = format!("file:{}", temp_file.path().display());
        let result = resolve_token(&token);

        assert!(result.is_err());
        assert!(matches!(result, Err(RuntimeError::TokenFileEmpty(_))));
    }

    #[test]
    fn test_connection_string_with_username_and_password() {
        let config = IggyConfig::default();
        let result = connection_string(&config).unwrap();
        assert_eq!(
            result,
            format!(
                "iggy://{}:{}@{}",
                config.username, config.password, config.address
            )
        );
    }

    #[test]
    fn test_connection_string_with_token() {
        let config = IggyConfig {
            token: "my-secret-token".to_owned(),
            ..IggyConfig::default()
        };
        let result = connection_string(&config).unwrap();
        assert_eq!(result, format!("iggy://my-secret-token@{}", config.address));
    }

    #[test]
    fn test_connection_string_resolves_token_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "token-from-file").unwrap();
        let config = IggyConfig {
            token: format!("file:{}", temp_file.path().display()),
            ..IggyConfig::default()
        };
        let result = connection_string(&config).unwrap();
        assert_eq!(result, format!("iggy://token-from-file@{}", config.address));
    }

    #[test]
    fn test_connection_string_without_credentials_fails() {
        let config = IggyConfig {
            username: String::new(),
            ..IggyConfig::default()
        };
        let result = connection_string(&config);
        assert!(matches!(result, Err(RuntimeError::MissingIggyCredentials)));
    }

    #[test]
    fn test_connection_string_with_tls_appends_parameters() {
        let mut config = IggyConfig::default();
        config.tls.enabled = true;
        config.tls.ca_file = "/certs/ca.pem".to_owned();
        config.tls.domain = Some("iggy.internal".to_owned());
        let result = connection_string(&config).unwrap();
        assert!(
            result.ends_with("?tls=true&tls_ca_file=/certs/ca.pem&tls_domain=iggy.internal"),
            "unexpected connection string: {result}"
        );
    }

    #[test]
    fn test_connection_string_with_tls_without_ca_file_fails() {
        let mut config = IggyConfig::default();
        config.tls.enabled = true;
        let result = connection_string(&config);
        assert!(matches!(
            result,
            Err(RuntimeError::MissingTlsCertificateFile)
        ));
    }
}
