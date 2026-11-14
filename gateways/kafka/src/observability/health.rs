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

//! Whether the gateway should get new clients, as `/ready` answers it.

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Mutex, PoisonError};

use tokio::sync::watch;

use crate::bridge::Reach;

const STARTING: u8 = 0;
const SERVING: u8 = 1;
const DRAINING: u8 = 2;

/// What `/ready` answers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ReadyState {
    /// The listeners accept, and the bridge, when there is one, reaches Iggy.
    Ready,
    /// The gateway has not started to serve yet: it waits for Iggy or binds its listeners.
    #[default]
    Starting,
    /// The gateway shuts down and drains its connections.
    Draining,
    /// The listeners accept, but the bridge does not reach Iggy. Why, as the last check found.
    IggyUnreachable(String),
}

impl fmt::Display for ReadyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready => f.write_str("ready"),
            Self::Starting => f.write_str("not ready: starting"),
            Self::Draining => f.write_str("not ready: shutting down"),
            Self::IggyUnreachable(reason) => write!(f, "not ready: Iggy is unreachable: {reason}"),
        }
    }
}

/// Where the gateway is in its life, and the bridge's last word on Iggy.
#[derive(Debug, Default)]
pub struct Readiness {
    phase: AtomicU8,
    /// `None` without a bridge, or before the gateway serves.
    iggy: Mutex<Option<watch::Receiver<Reach>>>,
}

impl Readiness {
    /// What `/ready` answers now.
    #[must_use]
    pub fn state(&self) -> ReadyState {
        match self.phase.load(Ordering::Acquire) {
            STARTING => return ReadyState::Starting,
            DRAINING => return ReadyState::Draining,
            _ => {}
        }
        let reach = self
            .iggy
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|iggy| iggy.borrow().clone());
        match reach {
            None | Some(Reach::Reachable) => ReadyState::Ready,
            Some(Reach::Unknown) => ReadyState::IggyUnreachable("not checked yet".to_string()),
            Some(Reach::Unreachable(reason)) => ReadyState::IggyUnreachable(reason),
        }
    }

    /// Whether the bridge reaches Iggy, as its last check found. `None` without a bridge, or
    /// before the gateway serves.
    #[must_use]
    pub fn iggy_reachable(&self) -> Option<bool> {
        self.iggy
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|iggy| *iggy.borrow() == Reach::Reachable)
    }

    pub(super) fn serving(&self, iggy: Option<watch::Receiver<Reach>>) {
        *self.iggy.lock().unwrap_or_else(PoisonError::into_inner) = iggy;
        // A shutdown that began first stays: a drain never turns back into serving.
        let _ = self
            .phase
            .compare_exchange(STARTING, SERVING, Ordering::AcqRel, Ordering::Acquire);
    }

    pub(super) fn draining(&self) {
        self.phase.store(DRAINING, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_gateway_that_serves_without_a_bridge_should_be_ready() {
        let readiness = Readiness::default();
        assert_eq!(readiness.state(), ReadyState::Starting);

        readiness.serving(None);
        assert_eq!(readiness.state(), ReadyState::Ready);
        assert_eq!(
            readiness.iggy_reachable(),
            None,
            "no bridge, no Iggy to reach"
        );

        readiness.draining();
        assert_eq!(readiness.state(), ReadyState::Draining);
        readiness.serving(None);
        assert_eq!(
            readiness.state(),
            ReadyState::Draining,
            "a drain never turns back into serving"
        );
    }

    #[test]
    fn given_a_bridge_should_follow_what_it_says_of_iggy() {
        let (reach, receiver) = watch::channel(Reach::Unknown);
        let readiness = Readiness::default();
        readiness.serving(Some(receiver));
        assert!(matches!(readiness.state(), ReadyState::IggyUnreachable(_)));

        reach.send_replace(Reach::Reachable);
        assert_eq!(readiness.state(), ReadyState::Ready);
        assert_eq!(readiness.iggy_reachable(), Some(true));

        reach.send_replace(Reach::Unreachable("connection refused".to_string()));
        assert_eq!(
            readiness.state(),
            ReadyState::IggyUnreachable("connection refused".to_string())
        );
        assert_eq!(
            readiness.state().to_string(),
            "not ready: Iggy is unreachable: connection refused"
        );
    }
}
