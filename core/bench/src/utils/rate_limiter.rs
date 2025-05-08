use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter as GovernorRateLimiter,
};
use iggy::prelude::IggyByteSize;
use std::num::NonZeroU32;

pub struct BenchmarkRateLimiter {
    rate_limiter: GovernorRateLimiter<NotKeyed, InMemoryState, DefaultClock>,
}

impl BenchmarkRateLimiter {
    pub fn new(bytes_per_second: IggyByteSize) -> Self {
        let bytes_per_second = NonZeroU32::new(bytes_per_second.as_bytes_u64() as u32).unwrap();
        let rate_limiter = GovernorRateLimiter::direct(Quota::per_second(bytes_per_second));

        // Fill the bucket to avoid burst
        let _ = rate_limiter.check_n(bytes_per_second);

        Self { rate_limiter }
    }

    pub async fn wait_until_necessary(&self, bytes: u64) {
        self.rate_limiter
            .until_n_ready(NonZeroU32::new(bytes as u32).unwrap())
            .await
            .unwrap();
    }
}
