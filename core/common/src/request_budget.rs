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

use tokio::time::{Instant, timeout_at};

use crate::{IggyError, NonZeroIggyDuration};

tokio::task_local! {
    static REQUEST_BUDGET: RequestBudget;
}

#[derive(Clone, Copy)]
pub struct RequestBudget {
    deadline: Instant,
}

impl RequestBudget {
    pub fn deadline() -> Option<Instant> {
        REQUEST_BUDGET.try_with(|budget| budget.deadline).ok()
    }

    /// Runs the operation under its caller's deadline, or starts a new one.
    pub async fn run<T>(
        timeout: Option<NonZeroIggyDuration>,
        on_expire: impl FnOnce(),
        future: impl Future<Output = Result<T, IggyError>>,
    ) -> Result<T, IggyError> {
        let existing = REQUEST_BUDGET.try_with(|budget| *budget).ok();
        let budget = existing.or_else(|| {
            timeout.map(|timeout| Self {
                deadline: Instant::now() + timeout.get_duration(),
            })
        });
        let Some(budget) = budget else {
            return future.await;
        };
        // Login reconnects nest budgeted futures; boxing keeps their stack use bounded.
        let future = Box::pin(future);
        let run = async {
            if budget.deadline <= Instant::now() {
                return Err(IggyError::RequestTimeoutOutcomeUnknown);
            }
            timeout_at(budget.deadline, future)
                .await
                .map_err(|_| IggyError::RequestTimeoutOutcomeUnknown)?
        };
        let result = if existing.is_some() {
            run.await
        } else {
            REQUEST_BUDGET.scope(budget, run).await
        };
        if matches!(result, Err(IggyError::RequestTimeoutOutcomeUnknown)) {
            on_expire();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn nested_operation_inherits_request_deadline() {
        let timeout = NonZeroIggyDuration::new(Duration::from_secs(1)).unwrap();
        let result = RequestBudget::run(Some(timeout), || {}, async {
            let outer_deadline = RequestBudget::deadline();
            let inner_deadline = RequestBudget::run(Some(timeout), || {}, async {
                Ok(RequestBudget::deadline())
            })
            .await?;
            assert_eq!(inner_deadline, outer_deadline);
            Ok(())
        })
        .await;

        assert!(result.is_ok());
        assert!(RequestBudget::deadline().is_none());
    }
}
