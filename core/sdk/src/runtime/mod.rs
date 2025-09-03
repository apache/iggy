use iggy_common::IggyDuration;
use std::fmt::Debug;
use tokio::{task::JoinHandle, time::Sleep};

pub trait Runtime: Send + Sync + Debug {
    type Join: Send + 'static;
    type Sleep: Future<Output = ()> + Send + 'static;

    fn spawn(&self, fut: impl Future<Output = ()> + Send + 'static) -> Self::Join;
    fn sleep(&self, dur: IggyDuration) -> Self::Sleep;
}

#[derive(Debug)]
pub struct TokioRuntime {}

impl Runtime for TokioRuntime {
    type Join = JoinHandle<()>;
    type Sleep = Sleep;

    fn spawn(&self, fut: impl Future<Output = ()> + Send + 'static) -> Self::Join {
        tokio::spawn(fut)
    }

    fn sleep(&self, dur: IggyDuration) -> Sleep {
        tokio::time::sleep(dur.get_duration())
    }
}
