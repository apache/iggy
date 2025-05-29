use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};

use iggy_common::{Identifier, IggyByteSize, IggyDuration, IggyError, IggyMessage, Partitioning};
use tokio::task::JoinHandle;
use tracing::error;

use super::producer::ProducerCore;

#[derive(Debug, Clone, Default, PartialEq)]
pub enum SendMode {
    #[default]
    Sync,
    Background,
}

#[derive(Debug, Clone)]
/// Determines how the `send_messages` API should behave when problem is encountered
pub enum BackpressureMode {
    /// Block until the send succeeds
    Block,
    /// Block with a timeout, after which the send fails
    BlockWithTimeout(IggyDuration),
    /// Fail immediately without retrying
    FailImmediately,
}

#[derive(Debug, Clone)]
pub struct BackgroundConfig {
    pub max_in_flight: usize,
    pub in_flight_timeout: Option<IggyDuration>,
    pub batch_size: Option<usize>,
    pub failure_mode: BackpressureMode,
    pub buffer_size: Option<IggyByteSize>, // rename: maximum_buffer_size
}

pub struct shardMessage {
    pub stream: Arc<Identifier>,
    pub topic: Arc<Identifier>,
    pub messages: Vec<IggyMessage>,
    pub partitioning: Option<Arc<Partitioning>>,
}

struct shard {
    core: Arc<ProducerCore>,
    tx: flume::Sender<shardMessage>,
    _join_handle: JoinHandle<()>,
}

impl shard {
    fn new(id: usize, producer_core: Arc<ProducerCore>) -> Self {
        let (tx, rx) = flume::bounded::<shardMessage>(10); // todo добавить размер в конфигурацию
        let core = producer_core.clone();
        let handle = tokio::spawn(async move {
            while let Ok(message) = rx.recv_async().await {
                // todo поменять на match
                core.send_internal(&message.stream, &message.topic, message.messages, message.partitioning).await.map_err(|e| {
                    error!("{e}");
                }).unwrap();
            }
        });
        Self {
            core: producer_core,
            tx,
            _join_handle: handle,
        }
    }

    async fn send(
        &self,
        message: shardMessage,
    ) -> Result<(), IggyError> {
        self.tx.send_async(message).await.map_err(|_| IggyError::BackgroundSendError)
    }

    async fn send_timeout(&self, message: shardMessage, timeout: IggyDuration) -> Result<(), IggyError> {
        match tokio::time::timeout(timeout.get_duration(), self.tx.send_async(message)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(IggyError::BackgroundSendTimeout),
            Err(_) => Err(IggyError::BackgroundSendTimeout)
        }
    }

    fn send_with_fail(&self, message: shardMessage) -> Result<(), IggyError> {
        self.tx.try_send(message).map_err(|_| IggyError::BackgroundSendError)
    }
}

pub struct Dispatcher {
    config: Arc<BackgroundConfig>,
    sender: flume::Sender<shardMessage>,
    _join_handle: JoinHandle<()>,
}

impl Dispatcher {
    pub fn new(core: Arc<ProducerCore>, num_shards: usize, config: Arc<BackgroundConfig>) -> Self {
        let mut shards = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            shards.push(shard::new(i, core.clone()));
        }

        let (tx, rx) = flume::bounded::<shardMessage>(0);
        let sent = AtomicUsize::new(0);
        let inner_config = config.clone();
        let handle = tokio::spawn(async move {
            loop {
                if let Ok(msg) = rx.recv_async().await {
                    let ix = sent.fetch_add(1, Ordering::SeqCst) % num_shards;
                    let shard = shards.get(ix).unwrap();
                    let result = match inner_config.failure_mode {
                        BackpressureMode::Block => shard.send(msg).await,
                        BackpressureMode::BlockWithTimeout(t) => shard.send_timeout(msg, t).await,
                        BackpressureMode::FailImmediately => shard.send_with_fail(msg),
                    };
                    if let Err(e) = result {
                        // todo добавить канал для ошибок
                        error!("{}", e);
                    }
                }
            }
        });
        Self {
            config,
            sender: tx,
            _join_handle: handle,
        }
    }

    pub async fn dispatch(&self, msg: shardMessage) -> Result<(), flume::SendError<shardMessage>> {
        self.sender.send_async(msg).await
    }
}
