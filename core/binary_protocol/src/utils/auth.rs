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

use crate::BinaryTransport;
use iggy_common::{ClientState, IggyError};

pub(crate) async fn fail_if_not_authenticated<T: BinaryTransport>(
    transport: &T,
) -> Result<(), IggyError> {
    match transport.get_state().await {
        ClientState::Shutdown => Err(IggyError::ClientShutdown),
        ClientState::Disconnected | ClientState::Connecting | ClientState::Authenticating => {
            Err(IggyError::Disconnected)
        }
        ClientState::Connected => Err(IggyError::Unauthenticated),
        ClientState::Authenticated => Ok(()),
    }
}
