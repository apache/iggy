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
    static REQUEST_DEADLINE: Instant;
}

/// Returns the active request deadline, if this task has a budget.
pub fn request_budget_deadline() -> Option<Instant> {
    REQUEST_DEADLINE.try_with(|deadline| *deadline).ok()
}

/// Runs a request under its existing deadline, or starts one from `timeout`.
pub async fn with_request_budget<T>(
    timeout: Option<NonZeroIggyDuration>,
    on_expire: impl FnOnce(),
    future: impl Future<Output = Result<T, IggyError>>,
) -> Result<T, IggyError> {
    let existing_deadline = request_budget_deadline();
    let deadline = existing_deadline
        .or_else(|| timeout.map(|timeout| Instant::now() + timeout.get_duration()));
    let Some(deadline) = deadline else {
        return future.await;
    };
    // Login reconnects nest budgeted futures; boxing keeps their stack use bounded.
    let future = Box::pin(future);
    let run = async {
        if deadline <= Instant::now() {
            return Err(IggyError::RequestTimeoutOutcomeUnknown);
        }
        timeout_at(deadline, future)
            .await
            .map_err(|_| IggyError::RequestTimeoutOutcomeUnknown)?
    };
    let result = if existing_deadline.is_some() {
        run.await
    } else {
        REQUEST_DEADLINE.scope(deadline, run).await
    };
    if matches!(result, Err(IggyError::RequestTimeoutOutcomeUnknown)) {
        on_expire();
    }
    result
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn nested_operation_inherits_request_deadline() {
        let timeout = NonZeroIggyDuration::new(Duration::from_secs(1)).unwrap();
        let result = with_request_budget(Some(timeout), || {}, async {
            let outer_deadline = request_budget_deadline();
            let inner_deadline = with_request_budget(Some(timeout), || {}, async {
                Ok(request_budget_deadline())
            })
            .await?;
            assert_eq!(inner_deadline, outer_deadline);
            Ok(())
        })
        .await;

        assert!(result.is_ok());
        assert!(request_budget_deadline().is_none());
    }
}
