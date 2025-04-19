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

use anyhow::Result;
use clap::Parser;
use iggy::prelude::*;
use iggy_examples::shared::args::Args;
use iggy_examples::shared::messages::*;
use iggy_examples::shared::system;
use std::error::Error;
use std::sync::Arc;
use tracing::{info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .init();
    info!(
        "Message headers consumer has started, selected transport: {}",
        args.transport
    );
    let client_provider_config = Arc::new(ClientProviderConfig::from_args(args.to_sdk_args())?);
    let client = client_provider::get_raw_client(client_provider_config, false).await?;
    let client = IggyClient::new(client);
    client.connect().await?;
    system::init_by_consumer(&args, &client).await;
    system::consume_messages(&args, &client, &handle_message).await
}

fn handle_message(message: &IggyMessage) -> Result<(), Box<dyn Error>> {
    // The payload can be of any type as it is a raw byte array. In this case it's a JSON string.
    let payload = std::str::from_utf8(&message.payload)?;
    // The message type is stored in the custom message header.
    let header_key = HeaderKey::new("message_type").unwrap();
    let headers_map = message.user_headers_map()?.unwrap();
    let message_type = headers_map.get(&header_key).unwrap().as_str()?;
    info!(
        "Handling message type: {} at offset: {}...",
        message_type, message.header.offset
    );
    match message_type {
        ORDER_CREATED_TYPE => {
            let order_created = serde_json::from_str::<OrderCreated>(payload)?;
            info!("{:#?}", order_created);
        }
        ORDER_CONFIRMED_TYPE => {
            let order_confirmed = serde_json::from_str::<OrderConfirmed>(payload)?;
            info!("{:#?}", order_confirmed);
        }
        ORDER_REJECTED_TYPE => {
            let order_rejected = serde_json::from_str::<OrderRejected>(payload)?;
            info!("{:#?}", order_rejected);
        }
        _ => {
            warn!("Received unknown message type: {}", message_type);
        }
    }
    Ok(())
}
