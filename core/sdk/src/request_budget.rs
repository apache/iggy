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

use std::future::Future;
use std::time::Duration;

use iggy_common::{IggyError, NonZeroIggyDuration};
use tokio::time::{Instant, timeout_at};

/// One logical request's absolute deadline, shared by its attempts.
#[derive(Clone, Copy)]
pub(crate) struct RequestBudget {
    deadline: Instant,
}

impl RequestBudget {
    pub(crate) fn new(timeout: NonZeroIggyDuration) -> Self {
        Self {
            deadline: Instant::now() + timeout.get_duration(),
        }
    }

    pub(crate) fn deadline(self) -> Instant {
        self.deadline
    }

    pub(crate) fn remaining(self) -> Result<Duration, IggyError> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            Err(IggyError::RequestTimeout)
        } else {
            Ok(remaining)
        }
    }

    pub(crate) async fn run<T>(
        self,
        future: impl Future<Output = Result<T, IggyError>>,
    ) -> Result<T, IggyError> {
        self.remaining()?;
        timeout_at(self.deadline, future)
            .await
            .map_err(|_| IggyError::RequestTimeout)?
    }
}
