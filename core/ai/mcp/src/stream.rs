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
use std::path::PathBuf;
use tracing::{error, info};

const TOKEN_FILE_PREFIX: &str = "file:";

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

fn resolve_token(token: &str) -> Result<String, McpRuntimeError> {
    if let Some(path) = token.strip_prefix(TOKEN_FILE_PREFIX) {
        let file_path = expand_home(path);

        if !file_path.exists() {
            error!("Token file does not exist: {}", path);
            return Err(McpRuntimeError::TokenFileNotFound(path.to_string()));
        }

        let content = std::fs::read_to_string(&file_path).map_err(|e| {
            error!("Failed to read token file '{}': {}", path, e);
            McpRuntimeError::TokenFileReadError(path.to_string(), e.to_string())
        })?;

        let trimmed = content.trim().to_string();

        if trimmed.is_empty() {
            error!("Token file is empty: {}", path);
            return Err(McpRuntimeError::TokenFileEmpty(path.to_string()));
        }

        Ok(trimmed)
    } else {
        Ok(token.to_string())
    }
}

pub async fn init(config: IggyConfig) -> Result<IggyClient, McpRuntimeError> {
    let address = config.address;
    let username = config.username;
    let password = config.password;
    let token = if config.token.is_empty() {
        None
    } else {
        Some(resolve_token(&config.token)?)
    };

    let connection_string = if let Some(token) = token {
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
        assert!(matches!(result, Err(McpRuntimeError::TokenFileNotFound(_))));
    }

    #[test]
    fn test_resolve_token_empty_file() {
        let temp_file = NamedTempFile::new().unwrap();

        let token = format!("file:{}", temp_file.path().display());
        let result = resolve_token(&token);

        assert!(result.is_err());
        assert!(matches!(result, Err(McpRuntimeError::TokenFileEmpty(_))));
    }

    #[test]
    fn test_resolve_token_file_with_only_whitespace() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "   \n\t\n   ").unwrap();

        let token = format!("file:{}", temp_file.path().display());
        let result = resolve_token(&token);

        assert!(result.is_err());
        assert!(matches!(result, Err(McpRuntimeError::TokenFileEmpty(_))));
    }
}
