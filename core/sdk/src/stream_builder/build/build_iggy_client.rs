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
use crate::prelude::{IggyClient, IggyError};
use iggy_common::Client;

/// Creates an [`IggyClient`] from `connection_string` and connects it.
///
/// The client comes back connected, so the caller can build a producer or a consumer on it.
///
/// # Errors
///
/// - [`IggyError::InvalidConnectionString`] when `connection_string` cannot be parsed.
/// - Any error raised while connecting to the server.
///
/// [`IggyClient`]: crate::prelude::IggyClient
pub(crate) async fn build_iggy_client(connection_string: &str) -> Result<IggyClient, IggyError> {
    let client = IggyClient::from_connection_string(connection_string)?;
    client.connect().await?;
    Ok(client)
}
