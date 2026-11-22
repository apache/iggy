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

use crate::args::CliOptions;
use crate::error::{CmdToolError, IggyCmdError};
use anyhow::{Context, bail};
use iggy::clients::client::IggyClient;
use iggy::prelude::{IggyError, PersonalAccessTokenClient, UserClient};
use iggy_cli::commands::binary_system::session::{ServerSession, ensure_default_store};
use passterm::{Stream, isatty, prompt_password_tty};
use secrecy::{ExposeSecret, SecretString};
use std::env::var;

#[cfg(feature = "login-session")]
mod credentials_login_session {
    pub(crate) use iggy_cli::commands::cli_command::PRINT_TARGET;
    pub(crate) use keyring_core::Entry;
    pub(crate) use tracing::{Level, event};
}

#[cfg(feature = "login-session")]
use credentials_login_session::*;

static ENV_IGGY_USERNAME: &str = "IGGY_USERNAME";
static ENV_IGGY_PASSWORD: &str = "IGGY_PASSWORD";
static ENV_IGGY_TOKEN: &str = "IGGY_TOKEN";

struct IggyUserClient {
    username: String,
    password: SecretString,
}

enum Credentials {
    UserNameAndPassword(IggyUserClient),
    /// A token, with its keyring name when it was read by name.
    PersonalAccessToken(SecretString, Option<String>),
    SessionWithToken(SecretString, String),
}

pub(crate) struct IggyCredentials<'a> {
    credentials: Option<Credentials>,
    iggy_client: Option<&'a IggyClient>,
    login_required: bool,
}

impl<'a> IggyCredentials<'a> {
    pub(crate) fn new(
        cli_options: &CliOptions,
        server_address: &str,
        login_required: bool,
        prefer_explicit_credentials: bool,
    ) -> anyhow::Result<Self, anyhow::Error> {
        if !login_required {
            return Ok(Self {
                credentials: None,
                iggy_client: None,
                login_required,
            });
        }

        let session_credentials = || -> Option<Credentials> {
            let token = ServerSession::new(server_address.to_string()).get_token()?;
            Some(Credentials::SessionWithToken(
                SecretString::from(token),
                server_address.to_string(),
            ))
        };

        // Non-login commands reuse the cached session token first. `iggy login`
        // sets `prefer_explicit_credentials` so freshly supplied credentials win
        // over a possibly-expired cached token; it falls back to the cached
        // token below only when no explicit credentials were provided.
        if !prefer_explicit_credentials && let Some(credentials) = session_credentials() {
            return Ok(Self {
                credentials: Some(credentials),
                iggy_client: None,
                login_required,
            });
        }

        #[cfg(feature = "login-session")]
        if let Some(token_name) = &cli_options.token_name {
            let server_address = format!("iggy:{server_address}");
            event!(target: PRINT_TARGET, Level::DEBUG,"Checking token presence under service: {} and name: {}",
            server_address, token_name);
            ensure_default_store()?;
            let entry = Entry::new(&server_address, token_name)?;
            let token = entry.get_password()?;

            return Ok(Self {
                credentials: Some(Credentials::PersonalAccessToken(
                    SecretString::from(token),
                    Some(token_name.clone()),
                )),
                iggy_client: None,
                login_required,
            });
        }

        if let Some(token) = &cli_options.token {
            Ok(Self {
                credentials: Some(Credentials::PersonalAccessToken(
                    SecretString::from(token.clone()),
                    None,
                )),
                iggy_client: None,
                login_required,
            })
        } else if let Some(username) = &cli_options.username {
            let password = match &cli_options.password {
                Some(password) => SecretString::from(password.clone()),
                None => {
                    let pwd = if isatty(Stream::Stdin) {
                        prompt_password_tty(Some("Password: "))?
                    } else {
                        let mut pwd = String::new();
                        std::io::stdin().read_line(&mut pwd)?;
                        pwd.trim_end_matches(['\n', '\r']).to_string()
                    };
                    SecretString::from(pwd)
                }
            };

            Ok(Self {
                credentials: Some(Credentials::UserNameAndPassword(IggyUserClient {
                    username: username.clone(),
                    password,
                })),
                iggy_client: None,
                login_required,
            })
        } else if let Some(token) = var(ENV_IGGY_TOKEN).ok().filter(|token| !token.is_empty()) {
            // An environment token outranks the username and password pair
            // below it, so a CI job can authenticate with IGGY_TOKEN alone.
            // An empty value counts as unset. This is read here rather than
            // as a clap `env` attribute: clap would treat the value like the
            // --token flag and fail on the credentials group conflict rules
            // whenever -u/-p are also present.
            Ok(Self {
                credentials: Some(Credentials::PersonalAccessToken(
                    SecretString::from(token),
                    None,
                )),
                iggy_client: None,
                login_required,
            })
        } else if var(ENV_IGGY_USERNAME).is_ok() && var(ENV_IGGY_PASSWORD).is_ok() {
            Ok(Self {
                credentials: Some(Credentials::UserNameAndPassword(IggyUserClient {
                    username: var(ENV_IGGY_USERNAME)?,
                    password: SecretString::from(var(ENV_IGGY_PASSWORD)?),
                })),
                iggy_client: None,
                login_required,
            })
        } else if prefer_explicit_credentials && let Some(credentials) = session_credentials() {
            // `iggy login` with no explicit credentials: reuse the cached
            // session token so a still-valid session reports "already logged in"
            // instead of demanding credentials for a no-op.
            Ok(Self {
                credentials: Some(credentials),
                iggy_client: None,
                login_required,
            })
        } else {
            Err(IggyCmdError::CmdToolError(CmdToolError::MissingCredentials).into())
        }
    }

    pub(crate) fn set_iggy_client(&mut self, iggy_client: &'a IggyClient) {
        self.iggy_client = Some(iggy_client);
    }

    pub(crate) async fn login_user(&self) -> anyhow::Result<(), anyhow::Error> {
        if let Some(client) = self.iggy_client
            && self.login_required
        {
            let credentials = self.credentials.as_ref().unwrap();
            match credentials {
                Credentials::UserNameAndPassword(username_and_password) => {
                    let _ = client
                        .login_user(
                            &username_and_password.username,
                            username_and_password.password.expose_secret(),
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "Problem with server login for username: {}",
                                username_and_password.username
                            )
                        })?;
                }
                Credentials::PersonalAccessToken(token_value, _) => {
                    let _ = client
                        .login_with_personal_access_token(token_value.expose_secret())
                        .await
                        .with_context(|| "Problem with server login with token".to_string())?;
                }
                Credentials::SessionWithToken(token_value, server_address) => {
                    let login_result = client
                        .login_with_personal_access_token(token_value.expose_secret())
                        .await;
                    if let Err(err) = login_result {
                        if matches!(
                            err,
                            IggyError::Unauthenticated
                                | IggyError::InvalidPersonalAccessToken
                                | IggyError::ResourceNotFound(_)
                                | IggyError::PersonalAccessTokenExpired(_, _)
                        ) {
                            let server_session = ServerSession::new(server_address.clone());
                            server_session.delete()?;
                            bail!(
                                "Login session expired for Iggy server: {server_address}, please login again or use other authentication method"
                            );
                        } else {
                            bail!("Problem with server login with token");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn logout_user(
        &self,
        deleted_token: Option<&str>,
    ) -> anyhow::Result<(), anyhow::Error> {
        if let Some(client) = self.iggy_client
            && self.login_required
        {
            let session_may_have_ended =
                deleted_token.is_some_and(|token_name| self.signed_in_with(token_name));
            match client.logout_user().await {
                // A deleted token ends the sessions it opened, and the client may already know.
                Err(IggyError::Unauthenticated | IggyError::Disconnected)
                    if session_may_have_ended => {}
                Err(error @ (IggyError::Unauthenticated | IggyError::Disconnected))
                    if deleted_token.is_some()
                        && let Some(Credentials::PersonalAccessToken(token, None)) =
                            &self.credentials =>
                {
                    // A raw token has no name. Only a credential refusal confirms that it is gone.
                    match client
                        .login_with_personal_access_token(token.expose_secret())
                        .await
                    {
                        Err(IggyError::InvalidPersonalAccessToken) => {}
                        Ok(_) => {
                            client
                                .logout_user()
                                .await
                                .context("Problem with server logout")?;
                            return Err(error).context("Problem with server logout");
                        }
                        Err(_) => return Err(error).context("Problem with server logout"),
                    }
                }
                result => result.with_context(|| "Problem with server logout".to_string())?,
            }
        }

        Ok(())
    }

    /// Whether the sign-in credential has the given token name.
    fn signed_in_with(&self, token_name: &str) -> bool {
        match &self.credentials {
            Some(Credentials::SessionWithToken(_, server_address)) => {
                ServerSession::new(server_address.clone()).get_token_name() == token_name
            }
            Some(Credentials::PersonalAccessToken(_, name)) => name.as_deref() == Some(token_name),
            Some(Credentials::UserNameAndPassword(_)) | None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_the_credentials_of_a_command_when_it_deletes_a_token_should_relax_the_sign_out_only_for_its_own()
     {
        let with = |credentials| IggyCredentials {
            credentials: Some(credentials),
            iggy_client: None,
            login_required: true,
        };
        let token = || SecretString::from("token");
        let session = with(Credentials::SessionWithToken(
            token(),
            "127.0.0.1:8090".to_owned(),
        ));
        let named = with(Credentials::PersonalAccessToken(
            token(),
            Some("ci".to_owned()),
        ));
        let raw = with(Credentials::PersonalAccessToken(token(), None));
        let password = with(Credentials::UserNameAndPassword(IggyUserClient {
            username: "iggy".to_owned(),
            password: token(),
        }));
        let session_token = ServerSession::new("127.0.0.1:8090".to_owned()).get_token_name();

        assert!(session.signed_in_with(&session_token));
        assert!(!session.signed_in_with("ci"));
        assert!(named.signed_in_with("ci"));
        assert!(!named.signed_in_with("other"));
        assert!(!raw.signed_in_with("other"));
        assert!(!password.signed_in_with("ci"));
    }
}
