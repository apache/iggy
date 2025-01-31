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
