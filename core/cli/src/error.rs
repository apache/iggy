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

use std::fmt::{Display, Formatter, Result};
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum CmdToolError {
    MissingCredentials,
    InvalidEncryptionKey,
    #[cfg(feature = "login-session")]
    MissingServerAddress,
}

impl Display for CmdToolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::MissingCredentials => {
                write!(f, "Missing iggy server credentials")
            }
            Self::InvalidEncryptionKey => {
                write!(f, "Invalid encryption key provided")
            }
            #[cfg(feature = "login-session")]
            Self::MissingServerAddress => {
                write!(f, "Missing iggy server address")
            }
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum IggyCmdError {
    #[error("Iggy client error")]
    IggyClient(#[from] iggy::prelude::ClientError),

    #[error("Iggy sdk or command error")]
    CommandError(#[from] anyhow::Error),

    #[error("Iggy password prompt error")]
    PasswordPrompt(#[from] passterm::PromptError),

    #[error("Iggy command line tool error")]
    CmdToolError(#[from] CmdToolError),
}
