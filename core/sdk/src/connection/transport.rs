use std::{io, net::SocketAddr, pin::Pin, str::FromStr, sync::Arc};

use futures::{AsyncRead, AsyncWrite};
use iggy_common::{AutoLogin, IggyDuration, TcpClientConfig};
use tokio::net::TcpSocket;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub trait ClientConfig {
    fn server_address(&self) -> SocketAddr;
    fn auto_login(&self) -> AutoLogin;
    fn reconnection_reestablish_after(&self) -> IggyDuration;
    fn reconnection_max_retries(&self) -> Option<u32>;
    fn heartbeat_interval(&self) -> IggyDuration;
}

impl ClientConfig for TcpClientConfig {
    fn auto_login(&self) -> AutoLogin {
        self.auto_login.clone()
    }

    fn heartbeat_interval(&self) -> IggyDuration {
        self.heartbeat_interval
    }

    fn reconnection_max_retries(&self) -> Option<u32> {
        self.reconnection.max_retries
    }

    fn reconnection_reestablish_after(&self) -> IggyDuration {
        self.reconnection.reestablish_after
    }

    fn server_address(&self) -> SocketAddr {
        SocketAddr::from_str(&self.server_address).unwrap()
    }
}

pub trait Transport: Send + Sync + 'static {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    type Config: ClientConfig + Clone + Send + Sync + 'static;

    fn connect(
        cfg: Arc<Self::Config>,
        server_address: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<Self::Stream>> + Send>>;
}

#[derive(Debug)]
pub struct TokioTcpTransport;

impl Transport for TokioTcpTransport {
    type Stream = Compat<tokio::net::TcpStream>;
    type Config = TcpClientConfig;

    fn connect(
        cfg: Arc<Self::Config>,
        server_address: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<Self::Stream>> + Send>> {
        let nodelay = cfg.nodelay;

        Box::pin(async move {
            let sock = match server_address {
                std::net::SocketAddr::V4(_) => TcpSocket::new_v4()?,
                _ => TcpSocket::new_v6()?,
            };
            if nodelay {
                sock.set_nodelay(true).ok();
            }
            let s = sock.connect(server_address).await?;
            Ok(s.compat())
        })
    }
}
