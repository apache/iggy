use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use iggy_common::{
    Identifier, IggyByteSize, IggyDuration, IggyError, IggyMessage, Partitioning, Sizeable,
};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::error;

use super::{producer::ProducerCore, send_mode::BackgroundConfig};

#[derive(Debug)]
pub struct ShardMessage {
    pub stream: Arc<Identifier>,
    pub topic: Arc<Identifier>,
    pub messages: Vec<IggyMessage>,
    pub partitioning: Option<Arc<Partitioning>>,
}

impl Sizeable for ShardMessage {
    fn get_size_bytes(&self) -> IggyByteSize {
        let mut total = IggyByteSize::new(0);

        total += self.stream.get_size_bytes();
        total += self.topic.get_size_bytes();
        for msg in &self.messages {
            total += msg.get_size_bytes();
        }
        total
    }
}

pub struct Shard {
    tx: flume::Sender<ShardMessage>,
    _handle: JoinHandle<()>,
}

impl Shard {
    pub fn new(
        core: Arc<ProducerCore>,
        current_buffered: Arc<AtomicUsize>,
        notify: Arc<Notify>,
    ) -> Self {
        let (tx, rx) = flume::bounded::<ShardMessage>(10); // use from config
        let handle = tokio::spawn(async move {
            while let Ok(msg) = rx.recv_async().await {
                let size = msg.get_size_bytes();
                if let Err(e) = core
                    .send_internal(&msg.stream, &msg.topic, msg.messages, msg.partitioning)
                    .await
                {
                    // send to err chan
                    // error!("{:?}", e);
                }
                current_buffered.fetch_sub(size.as_bytes_usize(), Ordering::Relaxed);
                notify.notify_waiters();
            }
        });
        Self {
            tx,
            _handle: handle,
        }
    }

    async fn send_with_block(&self, message: ShardMessage) -> Result<(), IggyError> {
        self.tx.send_async(message).await.map_err(|e| {
            error!("Failed to send_with_block: {e}");
            IggyError::BackgroundSendError
        })
    }

    async fn send_with_timeout(
        &self,
        message: ShardMessage,
        timeout: IggyDuration,
    ) -> Result<(), IggyError> {
        match tokio::time::timeout(timeout.get_duration(), self.tx.send_async(message)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                error!("Channel send failed during timeout: {e}");
                Err(IggyError::BackgroundSendTimeout)
            }
            Err(_) => {
                error!("Timeout elapsed before sending message batch");
                Err(IggyError::BackgroundSendTimeout)
            }
        }
    }

    fn send_with_fail(&self, message: ShardMessage) -> Result<(), IggyError> {
        self.tx.try_send(message).map_err(|_| {
            error!("Channel is full, dropping message batch");
            IggyError::BackgroundSendError
        })
    }
}

pub trait Sharding {
    fn pick_shard(
        &self,
        shards: &[Shard],
        messages: &[IggyMessage],
        stream: &Identifier,
        topic: &Identifier,
    ) -> usize;
}

pub struct RoundRobinSharding {
    counter: AtomicUsize,
}

impl Default for RoundRobinSharding {
    fn default() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Sharding for RoundRobinSharding {
    fn pick_shard(
        &self,
        shards: &[Shard],
        _: &[IggyMessage],
        _: &Identifier,
        _: &Identifier,
    ) -> usize {
        self.counter.fetch_add(1, Ordering::Relaxed) % shards.len()
    }
}

pub enum Backpressure {
    /// Block until the send succeeds
    Block,
    /// Block with a timeout, after which the send fails
    BlockWithTimeout(IggyDuration),
    /// Fail immediately without retrying
    FailImmediately,
}

struct ProducerDispatcher<S: Sharding> {
    core: Arc<ProducerCore>,
    backpressure: Backpressure,
    sharding: S,
    shards: Vec<Shard>,
    current_buffered: Arc<AtomicUsize>,
    notify: Arc<Notify>,
    config: Arc<BackgroundConfig>,
}

impl<S> ProducerDispatcher<S>
where
    S: Sharding,
{
    pub fn new(
        core: Arc<ProducerCore>,
        backpressure: Backpressure,
        config: Arc<BackgroundConfig>,
        sharding: S,
    ) -> Self {
        let mut shards = Vec::with_capacity(config.max_in_flight);
        let current_buffered = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());

        for _ in 0..config.max_in_flight {
            shards.push(Shard::new(
                core.clone(),
                current_buffered.clone(),
                notify.clone(),
            ));
        }

        Self {
            core,
            backpressure,
            sharding,
            shards,
            current_buffered,
            config,
            notify,
        }
    }

    pub async fn dispatch(
        &self,
        messages: Vec<IggyMessage>,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        let shard_message = ShardMessage {
            messages,
            stream,
            topic,
            partitioning,
        };
        let batch_bytes = shard_message.get_size_bytes();

        let mut reserved = self.current_buffered.load(Ordering::Relaxed);
        if let Some(buffer_size) = &self.config.buffer_size {
            if batch_bytes.as_bytes_usize() > buffer_size.as_bytes_usize() {
                return Err(IggyError::BackgroundSendBufferOverflow);
            }
            loop {
                if buffer_size.as_bytes_usize() != 0
                    && reserved + batch_bytes.as_bytes_usize() > buffer_size.as_bytes_usize()
                {
                    match self.backpressure {
                        Backpressure::Block => {
                            self.notify.notified().await;
                            continue;
                        }
                        Backpressure::BlockWithTimeout(t) => {
                            if tokio::time::timeout(t.get_duration(), self.notify.notified())
                                .await
                                .is_err()
                            {
                                return Err(IggyError::BackgroundSendTimeout);
                            }
                            continue;
                        }
                        Backpressure::FailImmediately => {
                            return Err(IggyError::BackgroundSendBufferOverflow);
                        }
                    };
                }
                match self.current_buffered.compare_exchange(
                    reserved,
                    reserved + batch_bytes.as_bytes_usize(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(v) => reserved = v,
                }
            }
        }

        let shard_ix = self.sharding.pick_shard(
            &self.shards,
            &shard_message.messages,
            &shard_message.stream,
            &shard_message.topic,
        );
        let shard = self.shards.get(shard_ix).unwrap();

        let result = match self.backpressure {
            Backpressure::Block => shard.send_with_block(shard_message).await,
            Backpressure::BlockWithTimeout(t) => shard.send_with_timeout(shard_message, t).await,
            Backpressure::FailImmediately => shard.send_with_fail(shard_message),
        };
        if result.is_err() {
            self.current_buffered
                .fetch_sub(batch_bytes.as_bytes_usize(), Ordering::Relaxed);
        }
        result
    }
}
