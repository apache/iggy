/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

//! Login/register failure taxonomy.
//!
//! The login/register flow itself lives in `dispatch` + `auth`; this module
//! owns the error type those handlers return and the terminal-vs-transient
//! split that decides a fast-fail reply versus a silent close (recoverable
//! failures stay silent so the SDK replays).

use crate::session_manager::SessionError;
use metadata::RegisterSubmitError;

/// Login/register failure.
///
/// `#[non_exhaustive]`: external matchers need a wildcard arm.
#[derive(Debug)]
#[non_exhaustive]
pub enum LoginRegisterError {
    InvalidCredentials,
    InvalidToken,
    UserInactive,
    Session(SessionError),
    /// Recoverable consensus failure. The connection stays `Connected`; the
    /// SDK read-timeout replays.
    Transient(RegisterSubmitError),
}

impl LoginRegisterError {
    /// `true` for a terminal failure the client cannot fix by retrying (bad
    /// credentials / token / inactive user / session error); `false` for a
    /// transient consensus failure the SDK replays. The handler fast-fails
    /// terminal errors with an empty reply and stays silent on transient ones.
    #[must_use]
    pub(crate) const fn is_terminal(&self) -> bool {
        !matches!(self, Self::Transient(_))
    }
}

impl std::fmt::Display for LoginRegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCredentials => write!(f, "invalid username or password"),
            Self::InvalidToken => write!(f, "invalid or expired personal access token"),
            Self::UserInactive => write!(f, "user account is inactive"),
            Self::Session(e) => write!(f, "session error: {e}"),
            Self::Transient(e) => write!(f, "transient consensus failure: {e}"),
        }
    }
}

impl std::error::Error for LoginRegisterError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_vs_transient() {
        assert!(LoginRegisterError::InvalidCredentials.is_terminal());
        assert!(LoginRegisterError::InvalidToken.is_terminal());
        assert!(LoginRegisterError::UserInactive.is_terminal());
        assert!(LoginRegisterError::Session(SessionError::ConnectionNotFound(0)).is_terminal());
        // Transient is the only recoverable variant: never terminal.
        assert!(!LoginRegisterError::Transient(RegisterSubmitError::PipelineFull).is_terminal());
    }
}
