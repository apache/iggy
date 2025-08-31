use std::sync::Arc;

use iggy_common::TcpClientConfig;

use crate::connection::transport::Transport;

pub struct TcpClientSync<T> {
    pub(crate) stream_factory: Arc<T>, 
    pub(crate) config: Arc<TcpClientConfig>,
}

impl<T: Transport> TcpClientSync<T> {
    fn new(stream_factory: Arc<T>, config: Arc<TcpClientConfig>) -> Self {
        Self { stream_factory, config }
    }   
}

