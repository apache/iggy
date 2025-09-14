use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::str::FromStr;
use std::sync::Arc;

use iggy_common::{AutoLogin, IggyDuration, TcpClientConfig};

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

pub trait Transport {
    type Stream: Read + Write;
    type Config: ClientConfig;

    fn connect(
        cfg: Arc<Self::Config>, // TODO maybe should remove arc
        server_address: SocketAddr,
    ) -> io::Result<Self::Stream>;
}

pub struct TcpTransport;

impl Transport for TcpTransport {
    type Stream = TcpStream;
    type Config = TcpClientConfig;

    fn connect(
            cfg: Arc<Self::Config>, // TODO maybe should remove arc
            server_address: SocketAddr,
        ) -> io::Result<Self::Stream> {
            let stream = TcpStream::connect(server_address)?;
            stream.set_nodelay(cfg.nodelay)?;
            Ok(stream)
        }
}
