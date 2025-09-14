use std::{
    fmt::Debug, io::{self, IoSlice, Read, Write}, net::SocketAddr, sync::{Arc, Mutex}
};

use bytes::BytesMut;
use iggy_binary_protocol::Client;
use iggy_common::{ClientState, IggyError, TcpClientConfig};
use tokio::runtime::Runtime;
use tracing::{debug, error, trace};

use crate::{
    connection::transport::{ClientConfig, Transport},
    protocol::core::ProtocolCore,
};

pub struct TcpClientSync<T>
where
    T: Transport,
    T::Config: ClientConfig,
{
    pub(crate) stream_factory: Arc<T>,
    pub(crate) config: Arc<T::Config>,
    inner: Mutex<ProtocolCore>,
    rt: Runtime,
    stream: Option<T::Stream>,
}

impl<T: Transport> TcpClientSync<T> {
    fn new(stream_factory: Arc<T>, config: Arc<TcpClientConfig>) -> Self {
        Self {
            stream_factory,
            config,
        }
    }
}

#[async_trait]
impl<T> Client for TcpClientSync<T>
where
    T: Transport + Debug,
{
    // async fn connect(&self) -> Result<(), IggyError> {
    //     let address = self.config.server_address();
    //     let core = self.inner.lock().unwrap();
    //     let cfg = self.config.clone();
    //     let mut stream = self.stream.take();

    //     self.rt.block_on(async move {
    //         let current_state = core.state;
    //             if matches!(
    //                 current_state,
    //                 ClientState::Connected
    //                     | ClientState::Authenticating
    //                     | ClientState::Authenticated
    //             ) {
    //                 debug!(
    //                     "ConnectionDriver: Already connected (state: {:?}), completing waiter immediately",
    //                     current_state
    //                 );
    //                 return
    //             }
    //         if matches!(current_state, ClientState::Connecting) {
    //             debug!("ConnectionDriver: Already connecting, adding to waiters");
    //             return
    //         }

    //         core.desire_connect(address).map_err(|e| {
    //             error!("ConnectionDriver: desire_connect failed: {}", e.as_string());
    //             io::Error::new(io::ErrorKind::ConnectionAborted, e.as_string())
    //         })?;

    //         stream = Some(T::connect(cfg, address)?);

    //         core.on_connected().map_err(|e| {
    //             io::Error::new(io::ErrorKind::ConnectionRefused, e.as_string())
    //         })?;
    //         debug!("ConnectionDriver: Connection established");
    //         if !core.should_wait_auth() {
    //             return
    //         }

    //     });

    //     Ok(())
    // }

    // async  fn disconnect(&self) -> Result<(), IggyError> {
    //     let address = self.config.server_address();
    //     let core = self.inner.lock().unwrap();
    //     let cfg = self.config.clone();
    //     let mut stream = self.stream.take();

    //     self.rt.block_on(async move {

    //     });
    // }
}

impl<T> TcpClientSync<T>
where
    T: Transport + Debug,
{
    fn connect(core: &mut ProtocolCore, address: SocketAddr, config: Arc<T::Config>) -> Result<(), IggyError> {
        let current_state = core.state;
        if matches!(
            current_state,
            ClientState::Connected | ClientState::Authenticating | ClientState::Authenticated
        ) {
            debug!(
                "TcpClientSync: Already connected (state: {:?}), completing waiter immediately",
                current_state
            );
            return Ok(());
        }
        if matches!(current_state, ClientState::Connecting) {
            debug!("TcpClientSync: Already connecting, adding to waiters");
            return Ok(());
        }

        core.desire_connect(address).map_err(|e| {
            error!("TcpClientSync: desire_connect failed: {}", e.as_string());
            e
        })?;

        let mut stream = T::connect(config, address).map_err(|e| {
            error!("TcpClientSync: failed to establish tcp connection: {}", e.to_string());
            IggyError::CannotEstablishConnection
        })?;

        core.on_connected()?;

        debug!("TcpClientSync: Connection established");
        if !core.should_wait_auth() {
            return Ok(());
        }

        match core.poll_transmit() {
            None => {
                error!("TcpClientSync: incorrect core state, want auth command, got none");
                return Err(IggyError::IncorrectConnectionState)
            }
            Some(buf) => {
                let data = [IoSlice::new(&buf.header), IoSlice::new(&buf.payload)];
                stream.write_vectored(&data);
                stream.flush();
                trace!("Sent a TCP request, waiting for a response...");

                for _ in 0..16 {
                    let mut recv_buffer = BytesMut::with_capacity(8192);
                    let n = {
                        match stream.read(&mut recv_buffer) {
                            Ok(0) => {
                                core.disconnect();
                                return Err(IggyError::CannotEstablishConnection);
                            }
                            Ok(n) => n,
                            Err(e) => {
                                error!("TcpClientSync: got error on command: {}", e);
                                // TODO change error
                                return Err(IggyError::CannotEstablishConnection);
                            }
                        }

                    };
                    let mut status;
                    core.process_incoming_with(&mut recv_buffer, |req_id, status, payload| {
                        if status == 0 {
                            return Err(IggyError::from_code(status));
                        }
                        return Ok(());
                    });
                }


                return Ok(())
            }
        }
    }

    // fn drive_command()
}
