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
use iggy_binary_protocol::Client;

/// Builds an `IggyClient` from the given connection string.
///
/// # Arguments
///
/// * `connection_string` - The connection string to use.
///
/// # Errors
///
/// * `IggyError` - If the connection string is invalid or the client cannot be initialized.
///
/// # Details
///
/// This function will create a new `IggyClient` with the given `connection_string`.
/// It will then connect to the server using the provided connection string.
/// If the connection string is invalid or the client cannot be initialized,
/// an `IggyError` will be returned.
///
pub(crate) async fn build_iggy_client(connection_string: &str) -> Result<IggyClient, IggyError> {
    let client = IggyClient::from_connection_string(connection_string)?;
    client.connect().await?;
    Ok(client)
}
