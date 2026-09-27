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

use crate::{IggyError, NonZeroIggyDuration};
use tokio::time::{Instant, timeout_at};

tokio::task_local! {
    static ACTIVE_REQUEST_BUDGET: RequestBudget;
}

pub fn active_request_budget() -> Option<RequestBudget> {
    ACTIVE_REQUEST_BUDGET.try_with(|budget| *budget).ok()
}

pub async fn run_with_request_budget<T>(
    timeout: Option<NonZeroIggyDuration>,
    on_expire: impl FnOnce(),
    future: impl Future<Output = Result<T, IggyError>>,
) -> Result<T, IggyError> {
    let budget = active_request_budget().or_else(|| timeout.map(RequestBudget::new));
    let Some(budget) = budget else {
        return future.await;
    };
    let result = if active_request_budget().is_some() {
        budget.run(future).await
    } else {
        ACTIVE_REQUEST_BUDGET
            .scope(budget, budget.run(future))
            .await
    };
    if matches!(result, Err(IggyError::RequestTimeoutOutcomeUnknown)) {
        on_expire();
    }
    result
}

/// One logical request's absolute deadline, shared by its attempts.
#[derive(Clone, Copy)]
pub struct RequestBudget {
    deadline: Instant,
}

impl RequestBudget {
    pub fn new(timeout: NonZeroIggyDuration) -> Self {
        Self {
            deadline: Instant::now() + timeout.get_duration(),
        }
    }

    pub fn deadline(self) -> Instant {
        self.deadline
    }

    pub fn remaining(self) -> Result<Duration, IggyError> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            Err(IggyError::RequestTimeoutOutcomeUnknown)
        } else {
            Ok(remaining)
        }
    }

    pub async fn run<T>(
        self,
        future: impl Future<Output = Result<T, IggyError>>,
    ) -> Result<T, IggyError> {
        self.remaining()?;
        timeout_at(self.deadline, future)
            .await
            .map_err(|_| IggyError::RequestTimeoutOutcomeUnknown)?
    }
}
