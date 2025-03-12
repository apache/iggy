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

use atomic_time::AtomicInstant;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Thread-safe rate limiter using linger-based algorithm
pub struct RateLimiter {
    bytes_per_second: u64,
    last_operation: AtomicInstant,
}

impl RateLimiter {
    pub fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second,
            last_operation: AtomicInstant::now(),
        }
    }

    /// Throttles the caller based on the configured rate limit
    pub async fn throttle(&self, bytes: u64) {
        let now = Instant::now();
        let last_op = self.last_operation.load(Ordering::Relaxed);

        let time_per_byte = 1.0 / self.bytes_per_second as f64;

        let target_duration = Duration::from_secs_f64(bytes as f64 * time_per_byte);

        let elapsed = now.duration_since(last_op);

        if elapsed < target_duration {
            let sleep_duration = target_duration - elapsed;
            self.last_operation
                .store(now + sleep_duration, Ordering::Relaxed);
            sleep(sleep_duration).await;
        } else {
            self.last_operation.store(now, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter() {
        let limiter = RateLimiter::new(1000); // 1000 bytes per second
        let start = Instant::now();

        // Try to send 100 bytes 5 times
        for _ in 0..5 {
            limiter.throttle(100).await;
        }

        // Should take approximately 0.5 seconds (500ms) to send 500 bytes at 1000 bytes/sec
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(450)); // Allow some wiggle room
        assert!(elapsed <= Duration::from_millis(550));
    }
}
