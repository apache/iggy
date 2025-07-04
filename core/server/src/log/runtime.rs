use std::{pin::Pin, time::Duration};

use futures::{FutureExt, SinkExt, Stream};
use opentelemetry_sdk::runtime::{Runtime, RuntimeChannel, TrySend};

#[derive(Clone)]
pub struct MonoioRuntime;

impl Runtime for MonoioRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // TODO: This wont' work, we init Opentelemetry in the main thread, when there is no instance of monoio runtime
        // running yet....
        monoio::spawn(future);
    }

    fn delay(&self, duration: Duration) -> impl Future<Output = ()> + Send + 'static {
        let sleep = Sleep::new(duration);
        sleep
    }
}


pub struct Sleep {
    pub inner: Pin<Box<monoio::time::Sleep>>,
}

impl Sleep {
    pub fn new(duration: Duration) -> Self {
        Self {
            inner: Box::pin(monoio::time::sleep(duration)),
        }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.inner.as_mut().poll_unpin(cx)
    }
}

// Safety: There is no way for `Sleep` future to be flipped like a burger to another thread,
// because we create instance of OpenTelemetry SDK runtime in the main thread, and monoio futures don't require Send & Sync bounds.
unsafe impl Send for Sleep {}