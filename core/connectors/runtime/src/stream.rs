use iggy::prelude::{Client, IggyClient, IggyClientBuilder};
use tracing::{error, info};

use crate::{configs::IggyConfig, error::RuntimeError};

pub async fn init(config: IggyConfig) -> Result<(IggyClient, IggyClient), RuntimeError> {
    let iggy_address = config.address;
    let iggy_username = config.username;
    let iggy_password = config.password;
    let iggy_token = config.token;
    let consumer_client = create_client(
        &iggy_address,
        iggy_username.as_deref(),
        iggy_password.as_deref(),
        iggy_token.as_deref(),
    )
    .await?;
    consumer_client.connect().await?;
    let producer_client = create_client(
        &iggy_address,
        iggy_username.as_deref(),
        iggy_password.as_deref(),
        iggy_token.as_deref(),
    )
    .await?;
    producer_client.connect().await?;
    Ok((consumer_client, producer_client))
}

async fn create_client(
    address: &str,
    username: Option<&str>,
    password: Option<&str>,
    token: Option<&str>,
) -> Result<IggyClient, RuntimeError> {
    let connection_string = if let Some(token) = token {
        if token.is_empty() {
            error!("Iggy token cannot be empty (if username and password are not provided)");
            return Err(RuntimeError::MissingIggyCredentials);
        }

        let redacted_token = token.chars().take(3).collect::<String>();
        info!("Using token: {redacted_token}*** for Iggy authentication");
        format!("iggy://{token}@{address}")
    } else {
        info!("Using username and password for Iggy authentication");
        let username = username.ok_or(RuntimeError::MissingIggyCredentials)?;
        if username.is_empty() {
            error!("Iggy password cannot be empty (if token is not provided)");
            return Err(RuntimeError::MissingIggyCredentials);
        }

        let password = password.ok_or(RuntimeError::MissingIggyCredentials)?;
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

    let client = IggyClientBuilder::from_connection_string(&connection_string)?.build()?;
    client.connect().await?;
    Ok(client)
}
