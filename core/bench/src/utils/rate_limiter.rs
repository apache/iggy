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

use governor::{
    Quota, RateLimiter as GovernorRateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use iggy::prelude::IggyByteSize;
use std::num::NonZeroU32;

pub struct BenchmarkRateLimiter {
    rate_limiter: GovernorRateLimiter<NotKeyed, InMemoryState, DefaultClock>,
}

impl BenchmarkRateLimiter {
    pub fn new(bytes_per_second: IggyByteSize) -> Self {
        let bytes_per_second =
            NonZeroU32::new(u32::try_from(bytes_per_second.as_bytes_u64()).unwrap_or(u32::MAX))
                .unwrap();
        let rate_limiter = GovernorRateLimiter::direct(Quota::per_second(bytes_per_second));

        // Fill the bucket to avoid burst
        let _ = rate_limiter.check_n(bytes_per_second);

        Self { rate_limiter }
    }

    pub async fn wait_until_necessary(&self, bytes: u64) {
        self.rate_limiter
            .until_n_ready(NonZeroU32::new(u32::try_from(bytes).unwrap_or(u32::MAX)).unwrap())
            .await
            .unwrap();
    }
}
