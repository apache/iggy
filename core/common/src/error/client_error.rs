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

use thiserror::Error;
use tokio::io;

use crate::IggyError;

/// The error type for the client.
#[derive(Debug, Error)]
pub enum ClientError {
    /// Command is invalid and cannot be sent.
    #[error("Invalid command")]
    InvalidCommand,
    /// Transport is invalid and cannot be used.
    #[error("Invalid transport {0}")]
    InvalidTransport(String),
    /// IO error.
    #[error("IO error")]
    IoError(#[from] io::Error),
    /// SDK error.
    #[error("SDK error")]
    SdkError(#[from] IggyError),
}
