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

mod seeder;

use anyhow::Result;
use clap::Parser;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::IggyClient;
use iggy::prelude::{Aes256GcmEncryptor, Args, ArgsOptional, Client, EncryptorKind, UserClient};
use std::error::Error;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct DataSeederArgs {
    #[clap(flatten)]
    pub(crate) iggy: ArgsOptional,

    #[arg(long, default_value = "iggy")]
    pub username: String,

    #[arg(long, default_value = "iggy")]
    pub password: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = DataSeederArgs::parse();
    let iggy_args = Args::from(vec![args.iggy.clone()]);

    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .init();
    let encryptor: Option<Arc<EncryptorKind>> = match iggy_args.encryption_key.is_empty() {
        true => None,
        false => Some(Arc::new(EncryptorKind::Aes256Gcm(
            Aes256GcmEncryptor::from_base64_key(&iggy_args.encryption_key).unwrap(),
        ))),
    };
    info!("Selected transport: {}", iggy_args.transport);
    let username = args.username.clone();
    let password = args.password.clone();
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(iggy_args)?);
    let client = client_provider::get_raw_client(client_provider_config, false).await?;
    let client = IggyClient::create(client, None, encryptor);
    client.connect().await?;
    client.login_user(&username, &password).await.unwrap();
    info!("Data seeder has started...");
    seeder::seed(&client).await.unwrap();
    info!("Data seeder has finished.");
    Ok(())
}
