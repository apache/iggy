use futures::{Stream, StreamExt, channel::mpsc};
use opentelemetry_sdk::runtime::{Runtime, RuntimeChannel, TrySend};
use std::{pin::Pin, time::Duration};

#[derive(Clone)]
pub struct CompioRuntime;

impl Runtime for CompioRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // This is mainly used to run batch span processing in the background. Note, that the function
        // does not return a handle. OpenTelemetry will use a different way to wait for the future to
        // finish when the caller shuts down.
        compio::runtime::spawn(future).detach();
    }

    fn delay(&self, duration: Duration) -> impl Future<Output = ()> + Send + 'static {
        compio::time::sleep(duration)
    }
}

#[derive(Debug)]
pub struct CompioSender<T> {
    sender: mpsc::UnboundedSender<T>,
}

impl<T> CompioSender<T> {
    pub fn new(sender: mpsc::UnboundedSender<T>) -> Self {
        Self { sender }
    }
}

// Safety: Since we use compio runtime which is single-threaded, or rather the Future: !Send + !Sync,
// we can implement those traits, to satisfy the trait bounds from `Runtime` and `RuntimeChannel` traits.
unsafe impl<T> Send for CompioSender<T> {}
unsafe impl<T> Sync for CompioSender<T> {}

impl<T: std::fmt::Debug + Send> TrySend for CompioSender<T> {
    type Message = T;

    fn try_send(
        &self,
        item: Self::Message,
    ) -> Result<(), opentelemetry_sdk::runtime::TrySendError> {
        self.sender.unbounded_send(item).map_err(|_err| {
            // Unbounded channels can only fail if disconnected, never full
            opentelemetry_sdk::runtime::TrySendError::ChannelClosed
        })
    }
}

pub struct CompioReceiver<T> {
    receiver: mpsc::UnboundedReceiver<T>,
}

impl<T> CompioReceiver<T> {
    pub fn new(receiver: mpsc::UnboundedReceiver<T>) -> Self {
        Self { receiver }
    }
}

impl<T: std::fmt::Debug + Send> Stream for CompioReceiver<T> {
    type Item = T;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.receiver.poll_next_unpin(cx)
    }
}

impl RuntimeChannel for CompioRuntime {
    type Receiver<T: std::fmt::Debug + Send> = CompioReceiver<T>;
    type Sender<T: std::fmt::Debug + Send> = CompioSender<T>;

    fn batch_message_channel<T: std::fmt::Debug + Send>(
        &self,
        _capacity: usize,
    ) -> (Self::Sender<T>, Self::Receiver<T>) {
        // Use the unbounded channel, this trait is used for batch processing, which naturally will limit the number of messages.
        let (sender, receiver) = mpsc::unbounded();
        (CompioSender::new(sender), CompioReceiver::new(receiver))
    }
}
