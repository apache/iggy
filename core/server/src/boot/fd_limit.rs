// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The process's own `RLIMIT_NOFILE`, raised once at startup, and the client
//! connection cap sized from it.

use message_bus::ConnectionCap;
use nix::errno::Errno;
use nix::sys::resource::{Resource, getrlimit, setrlimit};
use thiserror::Error;
use tracing::{info, warn};

/// `OPEN_MAX` from `<sys/syslimits.h>`, which `libc` does not export. The
/// macOS hard limit is usually `RLIM_INFINITY`, and `setrlimit(2)` rejects
/// that as a soft `RLIMIT_NOFILE` with `EINVAL`, so the man page's recipe is
/// `min(OPEN_MAX, rlim_max)`.
#[cfg(target_vendor = "apple")]
const APPLE_OPEN_MAX: u64 = 10_240;

/// `RLIMIT_NOFILE` around [`raise_open_file_limit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenFileLimit {
    pub soft_before: u64,
    pub soft: u64,
    pub hard: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum OpenFileLimitError {
    #[error("cannot read RLIMIT_NOFILE: {0}")]
    Read(Errno),
    #[error(
        "cannot raise the RLIMIT_NOFILE soft limit from {soft} to {target} (hard {hard}): {errno}"
    )]
    Raise {
        errno: Errno,
        soft: u64,
        hard: u64,
        target: u64,
    },
}

/// Raise the soft `RLIMIT_NOFILE` to the hard limit (clamped on macOS).
/// A soft limit already at or above the target is left as it is.
///
/// # Errors
///
/// [`OpenFileLimitError::Read`] if the limit cannot be read, and
/// [`OpenFileLimitError::Raise`] if `setrlimit` rejects the new soft limit.
pub fn raise_open_file_limit() -> Result<OpenFileLimit, OpenFileLimitError> {
    let (soft_before, hard) =
        getrlimit(Resource::RLIMIT_NOFILE).map_err(OpenFileLimitError::Read)?;
    let target = soft_target(hard);
    if soft_before >= target {
        return Ok(OpenFileLimit {
            soft_before,
            soft: soft_before,
            hard,
        });
    }
    setrlimit(Resource::RLIMIT_NOFILE, target, hard).map_err(|errno| {
        OpenFileLimitError::Raise {
            errno,
            soft: soft_before,
            hard,
            target,
        }
    })?;
    Ok(OpenFileLimit {
        soft_before,
        soft: target,
        hard,
    })
}

/// Build shard 0's cap on client sockets from `[message_bus] connections_max`
/// and log the value in effect.
///
/// Call it after [`raise_open_file_limit`], because an unset cap is half the
/// soft limit in effect.
#[must_use]
pub fn client_connection_cap(connections_max: Option<u32>) -> ConnectionCap {
    let soft_limit = getrlimit(Resource::RLIMIT_NOFILE).map(|(soft, _)| soft);
    let max = resolve_connections_max(connections_max, soft_limit.ok());
    match (connections_max, soft_limit) {
        (Some(0), _) => {
            info!("client connections are not capped, message_bus.connections_max is 0");
        }
        (None, Err(errno)) => {
            warn!(
                %errno,
                "cannot read RLIMIT_NOFILE, so client connections are not capped; set message_bus.connections_max"
            );
        }
        (None, Ok(soft_limit)) => {
            info!(
                connections_max = max,
                soft_limit, "client connections are capped at half the open-file limit"
            );
        }
        (Some(configured), Ok(soft_limit)) if u64::from(configured) >= soft_limit => {
            warn!(
                connections_max = configured,
                soft_limit,
                "message_bus.connections_max is not below the open-file limit, so clients can take the descriptors that storage writes need"
            );
        }
        (Some(configured), soft_limit) => {
            info!(
                connections_max = configured,
                soft_limit = soft_limit.ok(),
                "client connections are capped by message_bus.connections_max"
            );
        }
    }
    ConnectionCap::new(max)
}

/// The cap for `[message_bus] connections_max`, or `None` for no cap. Unset
/// is half of `soft_limit`, so clients cannot take the descriptors that
/// storage writes need, and no cap when the limit is unknown. Zero is no cap.
fn resolve_connections_max(connections_max: Option<u32>, soft_limit: Option<u64>) -> Option<usize> {
    match connections_max {
        Some(0) => None,
        Some(max) => Some(usize::try_from(max).unwrap_or(usize::MAX)),
        None => soft_limit
            .map(|soft| soft / 2)
            .filter(|&max| max > 0)
            .map(|max| usize::try_from(max).unwrap_or(usize::MAX)),
    }
}

#[cfg(target_vendor = "apple")]
const fn soft_target(hard: u64) -> u64 {
    if hard < APPLE_OPEN_MAX {
        hard
    } else {
        APPLE_OPEN_MAX
    }
}

#[cfg(not(target_vendor = "apple"))]
const fn soft_target(hard: u64) -> u64 {
    hard
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_process_limit_when_raising_should_leave_reported_soft_limit_in_effect() {
        let limit = raise_open_file_limit().expect("RLIMIT_NOFILE must be raisable in tests");

        let (soft, hard) = getrlimit(Resource::RLIMIT_NOFILE).expect("RLIMIT_NOFILE readable");
        assert_eq!(soft, limit.soft);
        assert_eq!(hard, limit.hard);
        assert!(limit.soft >= limit.soft_before);
        #[cfg(target_os = "linux")]
        assert_eq!(soft, hard);
    }

    #[test]
    fn given_connections_max_when_resolving_should_apply_the_default_and_zero_rules() {
        assert_eq!(resolve_connections_max(None, Some(10_240)), Some(5_120));
        assert_eq!(resolve_connections_max(None, Some(1)), None);
        assert_eq!(resolve_connections_max(None, None), None);
        assert_eq!(resolve_connections_max(Some(0), Some(10_240)), None);
        assert_eq!(resolve_connections_max(Some(0), None), None);
        assert_eq!(resolve_connections_max(Some(64), Some(10_240)), Some(64));
        assert_eq!(resolve_connections_max(Some(64), None), Some(64));
        assert_eq!(
            resolve_connections_max(Some(20_000), Some(10_240)),
            Some(20_000)
        );
    }
}
