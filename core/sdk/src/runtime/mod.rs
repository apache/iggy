use std::{fmt::Debug, time::Instant};
use iggy_common::IggyDuration;

#[cfg(feature = "async")]
use std::pin::Pin;

#[cfg(feature = "async")]
pub type Job = Box<
    dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
    + Send
    + 'static,
>;

#[cfg(feature = "sync")]
pub type Job = Box<dyn FnOnce() + Send + 'static>;

#[maybe_async::maybe_async(AFIT)]
pub trait Interval {
    async fn tick(&self) -> Instant;
}

#[maybe_async::maybe_async(AFIT)]
pub trait Runtime: Send + Sync + Debug {
    type Join: Send + 'static;
    type Interval: Interval + Send + 'static;

    async fn sleep(&self, dur: IggyDuration);
    fn spawn(&self, job: Job);
    fn interval(&self, dur: IggyDuration) -> Self::Interval;
}
