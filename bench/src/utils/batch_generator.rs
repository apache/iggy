use super::rate_limiter::BenchmarkRateLimiter;
use flume::{bounded, Receiver};
use futures::stream::{unfold, Stream};
use iggy::{
    messages::send_messages::Message,
    utils::{byte_size::IggyByteSize, sizeable::Sizeable},
};
use iggy_bench_report::numeric_parameter::IggyBenchNumericParameter;
use std::str::FromStr;

pub struct IggyMessageBatch {
    pub messages: Vec<Message>,
    pub user_data_bytes: u64,
    pub total_bytes: u64,
}

pub struct IggyBatchGenerator {
    rx: Receiver<IggyMessageBatch>,
    rate_limiter: Option<BenchmarkRateLimiter>,
}

impl IggyBatchGenerator {
    /// Spawns a background thread that perpetually generates batches of messages.
    ///
    /// * `message_size` – fixed or random size specification (`Value` or `Range`).
    /// * `messages_per_batch` – how many messages in each batch.
    /// * `channel_capacity` – how many batches can be buffered before the generator blocks.
    /// * `rate_limit` – optional rate limit in bytes per second.
    pub async fn new(
        message_size: IggyBenchNumericParameter,
        messages_per_batch: IggyBenchNumericParameter,
        channel_capacity: usize,
        rate_limit: Option<IggyByteSize>,
    ) -> Self {
        let (tx, rx) = bounded::<IggyMessageBatch>(channel_capacity);

        tokio::spawn(async move {
            loop {
                let batch = Self::generate_batch(&message_size, &messages_per_batch);
                if tx.send(batch).is_err() {
                    break;
                }
            }
        });

        let rate_limiter = rate_limit.map(BenchmarkRateLimiter::new);

        IggyBatchGenerator { rx, rate_limiter }
    }

    /// Consumes this generator and returns a `Stream` of `IggyMessageBatch`.
    pub fn into_stream(self) -> impl Stream<Item = IggyMessageBatch> {
        unfold(self, |gen| async move {
            let batch = match gen.rx.recv_async().await {
                Ok(b) => b,
                Err(_) => return None,
            };

            if let Some(limiter) = &gen.rate_limiter {
                limiter.wait_until_necessary(batch.user_data_bytes).await;
            }

            Some((batch, gen))
        })
    }

    /// Actually builds one batch of messages.
    fn generate_batch(
        message_size: &IggyBenchNumericParameter,
        messages_per_batch: &IggyBenchNumericParameter,
    ) -> IggyMessageBatch {
        let mut user_data_bytes = 0u64;
        let mut total_bytes = 0u64;
        let mut messages = Vec::with_capacity(messages_per_batch.max() as usize);

        for _ in 0..messages_per_batch.get() {
            let random_msg_payload_size = message_size.get();
            let payload = Self::create_payload(random_msg_payload_size);

            let message = Message::from_str(&payload).unwrap();
            user_data_bytes += message.payload.len() as u64;
            total_bytes += message.get_size_bytes().as_bytes_u64();
            messages.push(message);
        }

        IggyMessageBatch {
            messages,
            user_data_bytes,
            total_bytes,
        }
    }

    /// Utility method for generating a payload of the requested size.
    /// Cycles through `'a'..='z'`.
    fn create_payload(size: u32) -> String {
        let mut payload = String::with_capacity(size as usize);
        for i in 0..size {
            let c = (97 + (i % 26)) as u8 as char;
            payload.push(c);
        }
        payload
    }
}
