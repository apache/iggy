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

use crate::shared::args::Args;
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::IggyClient;
use iggy::error::IggyError;
use std::sync::Arc;

/// Builds an Iggy client using the provided stream and topic identifiers.
///
/// # Arguments
///
/// * `stream_id` - The identifier of the stream.
/// * `topic_id` - The identifier of the topic.
///
/// # Returns
///
/// A `Result` wrapping the `IggyClient` instance or an `IggyError`.
///
pub async fn build_client(
    stream_id: &str,
    topic_id: &str,
    connect: bool,
) -> Result<IggyClient, IggyError> {
    let args = Args::new(stream_id.to_string(), topic_id.to_string());
    build_client_from_args(args.to_sdk_args(), connect).await
}

/// Builds an Iggy client using the provided `Args`.
///
/// # Arguments
///
/// * `args` - The `Args` to use to build the client.
///
/// # Returns
///
/// A `Result` wrapping the `IggyClient` instance or an `IggyError`.
///
pub async fn build_client_from_args(
    args: iggy::args::Args,
    connect: bool,
) -> Result<IggyClient, IggyError> {
    // Build client provider configuration
    let client_provider_config = Arc::new(
        ClientProviderConfig::from_args(args).expect("Failed to create client provider config"),
    );

    // Build client_provider
    let client = client_provider::get_raw_client(client_provider_config, connect)
        .await
        .expect("Failed to build client provider");

    // Build client
    let client = match IggyClient::builder().with_client(client).build() {
        Ok(client) => client,
        Err(e) => return Err(e),
    };

    Ok(client)
}
