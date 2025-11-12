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

use crate::binary::{command::ServerCommand, sender::SenderKind};
use bytes::BytesMut;
use iggy_common::IggyError;

pub async fn receive_and_validate(
    sender: &mut SenderKind,
    code: u32,
    length: u32,
) -> Result<ServerCommand, IggyError> {
    let mut buffer = BytesMut::with_capacity(length as usize);
    unsafe {
        buffer.set_len(length as usize);
    }
    let buffer = if length == 0 {
        buffer
    } else {
        let (result, buffer) = sender.read(buffer).await;
        result?;
        buffer
    };

    let command = ServerCommand::from_code_and_payload(code, buffer.freeze())?;
    command.validate()?;
    Ok(command)
}
