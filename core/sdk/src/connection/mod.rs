use std::{
    collections::VecDeque,
    fmt::Debug,
    io::{self, IoSlice},
    mem::MaybeUninit,
    net::SocketAddr,
    ops::Deref,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
};

use crate::{
    connection::transport::{ClientConfig, TokioTcpTransport, Transport},
    protocol::{ControlAction, ProtocolCore, ProtocolCoreConfig, TxBuf},
    runtime::{Runtime, TokioRuntime},
};
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{AsyncRead, AsyncWrite, task::AtomicWaker};
use iggy_binary_protocol::{BinaryClient, BinaryTransport, Client};
use iggy_common::{ClientState, Command, DiagnosticEvent, IggyDuration, IggyError};
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use tracing::{debug, error};

mod transport;

pub type NewTokioTcpClient = NewTcpClient<TokioTcpTransport, TokioRuntime>;

pub enum ClientCommand {
    Connect(SocketAddr),
    Disconnect,
    Shutdown,
}

#[derive(Debug)]
pub struct ConnectionInner<T: Transport, R: Runtime> {
    pub(crate) state: Mutex<State<T, R>>,
}

#[derive(Debug)]
pub struct ConnectionRef<T: Transport, R: Runtime>(Arc<ConnectionInner<T, R>>);

impl<T: Transport, R: Runtime> ConnectionRef<T, R> {
    fn new(core: ProtocolCore, cfg: Arc<T::Config>, rt: Arc<R>) -> Self {
        Self(Arc::new(ConnectionInner {
            state: Mutex::new(State {
                rt,
                inner: core,
                driver: None,
                stream: None,
                current_send: None,
                send_offset: 0,
                cfg,
                recv_buffer: BytesMut::with_capacity(16 * 1024),
                wait_timer: None,
                waiters: Arc::new(Waiters {
                    map: Mutex::new(FxHashMap::with_capacity_and_hasher(256, Default::default())),
                    next_id: AtomicU64::new(0),
                }),
                requests_to_wait: FxHashMap::with_capacity_and_hasher(256, Default::default()),
                pending_commands: VecDeque::new(),
                connect_waiters: Vec::new(),
                pending_connect: None,
            }),
        }))
    }
}

impl<T: Transport, R: Runtime> ConnectionRef<T, R> {
    fn state(&self) -> ClientState {
        let state = self.0.state.lock();
        state.inner.state
    }

    fn set_state(&self, client_state: ClientState) {
        let mut state = self.0.state.lock();
        state.inner.state = client_state
    }
}

impl<T: Transport, R: Runtime> Deref for ConnectionRef<T, R> {
    type Target = ConnectionInner<T, R>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Transport, R: Runtime> Clone for ConnectionRef<T, R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

struct WaitEntry<T> {
    waker: AtomicWaker,
    result: Option<T>,
}

struct Waiters<T> {
    map: Mutex<FxHashMap<u64, WaitEntry<T>>>,
    next_id: AtomicU64,
}

impl<T> Waiters<T> {
    fn alloc(&self) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.map.lock().insert(
            id,
            WaitEntry {
                waker: AtomicWaker::new(),
                result: None,
            },
        );
        id
    }

    fn complete(&self, id: u64, val: T) -> bool {
        if let Some(entry) = self.map.lock().get_mut(&id) {
            entry.result = Some(val);
            entry.waker.wake();
            true
        } else {
            false
        }
    }
}

struct WaitFuture<T> {
    waiters: Arc<Waiters<T>>,
    id: u64,
}

impl<T> Future for WaitFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut map = self.waiters.map.lock();

        let ready: Option<T> = {
            if let Some(entry) = map.get_mut(&self.id) {
                if let Some(val) = entry.result.take() {
                    Some(val)
                } else {
                    entry.waker.register(cx.waker());
                    entry.result.take()
                }
            } else {
                None
            }
        };

        if let Some(val) = ready {
            map.remove(&self.id);
            Poll::Ready(val)
        } else {
            Poll::Pending
        }
    }
}

pub struct State<T: Transport, R: Runtime> {
    rt: Arc<R>,
    inner: ProtocolCore,
    driver: Option<Waker>,
    stream: Option<T::Stream>,
    current_send: Option<TxBuf>,
    send_offset: usize,
    recv_buffer: BytesMut,
    cfg: Arc<T::Config>,

    wait_timer: Option<Pin<Box<R::Sleep>>>,

    waiters: Arc<Waiters<Result<Bytes, IggyError>>>,
    requests_to_wait: FxHashMap<u64, u64>,
    pending_commands: VecDeque<(u64, ClientCommand)>,
    connect_waiters: Vec<u64>,
    pending_connect: Option<Pin<Box<dyn Future<Output = io::Result<T::Stream>> + Send>>>,
}

impl<T: Transport, R: Runtime> Debug for State<T, R> {
    // todo implement debug
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "test")
    }
}

impl<T: Transport, R: Runtime> State<T, R> {
    fn take_waker(&mut self) -> Option<Waker> {
        self.driver.take()
    }

    fn complete_all_waiters_with_error(&mut self, error: IggyError) {
        for id in self.connect_waiters.drain(..) {
            let _ = self.waiters.complete(id, Err(error.clone()));
        }

        for (_request_id, wait_id) in self.requests_to_wait.drain() {
            let _ = self.waiters.complete(wait_id, Err(error.clone()));
        }

        for (wait_id, _cmd) in self.pending_commands.drain(..) {
            let _ = self.waiters.complete(wait_id, Err(error.clone()));
        }
    }

    fn enqueu_command(
        &mut self,
        command: ClientCommand,
    ) -> (WaitFuture<Result<Bytes, IggyError>>, Option<Waker>) {
        let id = self.waiters.alloc();
        self.pending_commands.push_back((id, command));
        let waker = self.take_waker();
        (
            WaitFuture {
                waiters: self.waiters.clone(),
                id,
            },
            waker,
        )
    }

    fn enqueue_message(
        &mut self,
        code: u32,
        payload: Bytes,
    ) -> (WaitFuture<Result<Bytes, IggyError>>, Option<Waker>) {
        let wait_id = self.waiters.alloc();
        let waker = match self.inner.send(code, payload) {
            Ok(protocol_id) => {
                self.requests_to_wait.insert(protocol_id, wait_id);
                self.take_waker()
            }
            Err(e) => {
                self.waiters.complete(wait_id, Err(e));
                None
            }
        };
        (
            WaitFuture {
                waiters: self.waiters.clone(),
                id: wait_id,
            },
            waker,
        )
    }

    fn drive_client_commands(&mut self) -> io::Result<bool> {
        let mut made_progress = false;
        for (request_id, cmd) in self.pending_commands.drain(..) {
            made_progress = true;
            match cmd {
                ClientCommand::Connect(server_address) => {
                    debug!(
                        "ConnectionDriver: Processing Connect command to {}",
                        server_address
                    );
                    let current_state = self.inner.state;

                    if matches!(
                        current_state,
                        ClientState::Connected
                            | ClientState::Authenticating
                            | ClientState::Authenticated
                    ) {
                        debug!(
                            "ConnectionDriver: Already connected (state: {:?}), completing waiter immediately",
                            current_state
                        );
                        let _ = self.waiters.complete(request_id, Ok(Bytes::new()));
                        continue;
                    }

                    if matches!(current_state, ClientState::Connecting) {
                        debug!("ConnectionDriver: Already connecting, adding to waiters");
                        self.connect_waiters.push(request_id);
                        continue;
                    }

                    self.connect_waiters.push(request_id);
                    self.inner.desire_connect(server_address).map_err(|e| {
                        error!("ConnectionDriver: desire_connect failed: {}", e.as_string());
                        io::Error::new(io::ErrorKind::ConnectionAborted, e.as_string())
                    })?;
                    debug!(
                        "ConnectionDriver: desire_connect successful, state: {:?}",
                        self.inner.state
                    );
                }
                ClientCommand::Disconnect => {
                    self.inner.disconnect();
                    self.waiters.complete(request_id, Ok(Bytes::new()));
                }
                ClientCommand::Shutdown => {
                    self.inner.shutdown();
                    self.waiters.complete(request_id, Ok(Bytes::new()));
                }
            }
        }
        Ok(made_progress)
    }

    fn drive_connect(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        if let Some(fut) = self.pending_connect.as_mut() {
            match fut.as_mut().poll(cx) {
                Poll::Pending => return Ok(false),
                Poll::Ready(Ok(stream)) => {
                    self.stream = Some(stream);
                    self.pending_connect = None;
                    self.inner.on_connected().map_err(|e| {
                        io::Error::new(io::ErrorKind::ConnectionRefused, e.as_string())
                    })?;
                    debug!(
                        "ConnectionDriver: Connection established, completing {} waiters",
                        self.connect_waiters.len()
                    );
                    if !self.inner.should_wait_auth() {
                        for id in self.connect_waiters.drain(..) {
                            debug!("ConnectionDriver: Completing connect waiter {}", id);
                            let _ = self.waiters.complete(id, Ok(Bytes::new()));
                        }
                    }
                    return Ok(true);
                }
                Poll::Ready(Err(_e)) => {
                    self.pending_connect = None;
                    self.inner.disconnect();
                    for id in self.connect_waiters.drain(..) {
                        let _ = self
                            .waiters
                            .complete(id, Err(IggyError::CannotEstablishConnection));
                    }
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn drive_timer(&mut self, cx: &mut Context<'_>) -> bool {
        if let Some(t) = &mut self.wait_timer {
            if t.as_mut().poll(cx).is_pending() {
                return false;
            }
            self.wait_timer = None;
            return true;
        }
        false
    }

    fn drive_transmit(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        if self.current_send.is_none() {
            if let Some(tx) = self.inner.poll_transmit() {
                self.current_send = Some(tx);
                self.send_offset = 0;
            } else {
                return Ok(false);
            }
        }

        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "No stream"))?;

        let buf = self.current_send.as_ref().unwrap();
        let mut offset = self.send_offset;

        while offset < buf.total_len() {
            let mut storage = [IoSlice::new(&[]), IoSlice::new(&[])];

            let iov = if offset < 8 {
                storage[0] = IoSlice::new(&buf.header[offset..]);
                if !buf.payload.is_empty() {
                    storage[1] = IoSlice::new(&buf.payload);
                    &storage[..2]
                } else {
                    &storage[..1]
                }
            } else {
                let body_off = offset - 8;
                storage[0] = IoSlice::new(&buf.payload[body_off..]);
                &storage[..1]
            };

            let written = match Pin::new(&mut *stream).poll_write_vectored(cx, iov)? {
                Poll::Ready(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "write returned 0 bytes",
                    ));
                }
                Poll::Ready(n) => n,
                Poll::Pending => return Ok(false),
            };

            offset += written;
            self.send_offset += written;
        }

        match Pin::new(stream).poll_flush(cx)? {
            Poll::Pending => return Ok(false),
            Poll::Ready(()) => {}
        }

        self.send_offset = 0;
        self.current_send = None;
        Ok(true)
    }

    fn drive_receive(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        let mut progress = false;

        // todo add some const var
        for _ in 0..16 {
            if self.recv_buffer.spare_capacity_mut().is_empty() {
                self.recv_buffer.reserve(8192);
            }

            let spare: &mut [MaybeUninit<u8>] = self.recv_buffer.spare_capacity_mut();

            let buf: &mut [u8] = unsafe { &mut *(spare as *mut [MaybeUninit<u8>] as *mut [u8]) };

            let n = {
                let stream = self
                    .stream
                    .as_mut()
                    .ok_or(io::Error::new(io::ErrorKind::NotConnected, "No stream"))?;
                match Pin::new(&mut *stream).poll_read(cx, buf)? {
                    Poll::Pending => return Ok(progress),
                    Poll::Ready(0) => {
                        self.inner.disconnect();
                        self.stream = None;
                        self.complete_all_waiters_with_error(IggyError::CannotEstablishConnection);
                        return Ok(true);
                    }
                    Poll::Ready(n) => n,
                }
            };

            unsafe {
                self.recv_buffer.advance_mut(n);
            }

            self.inner
                .process_incoming_with(&mut self.recv_buffer, |req_id, status, payload| {
                    if let Some(wait_id) = self.requests_to_wait.remove(&req_id) {
                        let res = if status == 0 {
                            Ok(payload)
                        } else {
                            Err(IggyError::from_code(status))
                        };
                        let _ = self.waiters.complete(wait_id, res);
                    }
                });
            progress = true;
        }

        if let Some(auth_res) = self.inner.take_auth_result() {
            match auth_res {
                Ok(()) => {
                    for id in self.connect_waiters.drain(..) {
                        let _ = self.waiters.complete(id, Ok(Bytes::new()));
                    }
                }
                Err(e) => {
                    for id in self.connect_waiters.drain(..) {
                        let _ = self.waiters.complete(id, Err(e.clone()));
                    }
                }
            }
            progress = true;
        }

        Ok(progress)
    }
}

struct ConnectionDriver<T: Transport, R: Runtime>(ConnectionRef<T, R>);

impl<T: Transport, R: Runtime> Future for ConnectionDriver<T, R> {
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let st = &mut *self.0.state.lock();
        let mut keep_going = false;

        keep_going |= st.drive_client_commands()?;

        let order = st.inner.poll();
        match order {
            ControlAction::Wait(dur) => {
                if st.wait_timer.is_none() {
                    st.wait_timer = Some(Box::pin(st.rt.sleep(dur)));
                }
                keep_going |= st.drive_timer(cx);
            }
            ControlAction::Connect(server_adress) => {
                if st.pending_connect.is_none() {
                    st.pending_connect = Some(T::connect(st.cfg.clone(), server_adress));
                }
                keep_going |= st.drive_connect(cx)?;
            }
            ControlAction::Noop | ControlAction::Authenticate { .. } => {}
            ControlAction::Error(e) => {
                if matches!(e, IggyError::ClientShutdown) {
                    debug!("ConnectionDriver: Received ClientShutdown, terminating gracefully");
                    return Poll::Ready(Ok(()));
                }
                error!("ConnectionDriver: Error - {e:?}");
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, format!("{e:?}"))));
            }
        }

        if st.stream.is_some() {
            match st.drive_transmit(cx) {
                Ok(progress) => keep_going |= progress,
                Err(e) => {
                    debug!("Transmit error, disconnecting: {:?}", e);
                    st.inner.disconnect();
                    st.stream = None;
                    st.complete_all_waiters_with_error(IggyError::CannotEstablishConnection);
                    keep_going = true;
                }
            }

            if st.stream.is_some() {
                match st.drive_receive(cx) {
                    Ok(progress) => keep_going |= progress,
                    Err(e) => {
                        debug!("Receive error, disconnecting: {:?}", e);
                        st.inner.disconnect();
                        st.stream = None;
                        st.complete_all_waiters_with_error(IggyError::CannotEstablishConnection);
                        keep_going = true;
                    }
                }
            }
        }

        let current_state = st.inner.state;
        if current_state == ClientState::Shutdown {
            if st.stream.is_some() {
                debug!("ConnectionDriver: Closing stream due to Shutdown");
                st.stream = None;
            }
            return Poll::Ready(Ok(()));
        }

        if current_state == ClientState::Disconnected && st.stream.is_some() {
            debug!("ConnectionDriver: Closing stream due to Disconnected but keeping driver alive");
            st.stream = None;
        }

        st.driver = Some(cx.waker().clone());

        if keep_going {
            cx.waker().wake_by_ref();
        }

        Poll::Pending
    }
}

#[derive(Debug)]
pub struct NewTcpClient<T: Transport, R: Runtime> {
    state: ConnectionRef<T, R>,
    config: Arc<T::Config>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
    _driver_handle: R::Join,
}

impl<T: Transport, R: Runtime + 'static> NewTcpClient<T, R> {
    pub fn create(config: Arc<T::Config>, rt: Arc<R>) -> Result<Self, IggyError> {
        let (tx, rx) = broadcast(1000);

        let proto_config = ProtocolCoreConfig {
            auto_login: config.auto_login(),
            reestablish_after: config.reconnection_reestablish_after(),
            max_retries: config.reconnection_max_retries(),
        };

        let conn = ConnectionRef::new(ProtocolCore::new(proto_config), config.clone(), rt.clone());
        let driver = ConnectionDriver(conn.clone());
        let driver_handle = rt.spawn(async move {
            if let Err(e) = driver.await {
                error!("I/O error: {e}");
            }
        });

        Ok(Self {
            state: conn,
            config,
            events: (tx, rx),
            _driver_handle: driver_handle,
        })
    }

    async fn send_raw(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        let (wait_future, waker) = {
            let mut state = self.state.0.state.lock();
            state.enqueue_message(code, payload)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        wait_future.await
    }
}

#[async_trait]
impl<T, R> Client for NewTcpClient<T, R>
where
    T: Transport + Debug,
    R: Runtime + Debug + Send + Sync + 'static,
    T::Config: ClientConfig + Debug + Send + Sync + 'static,
    R::Join: Debug + Send + Sync + 'static,
{
    async fn connect(&self) -> Result<(), IggyError> {
        let address = self.config.server_address();
        let (fut, waker) = {
            let mut state = self.state.0.state.lock();
            state.enqueu_command(ClientCommand::Connect(address))
        };
        if let Some(waker) = waker {
            waker.wake();
        }

        match fut.await {
            Ok(_) => {
                self.publish_event(DiagnosticEvent::Connected).await;
                Ok(())
            }
            Err(IggyError::CannotEstablishConnection) => {
                self.publish_event(DiagnosticEvent::Disconnected).await;
                Err(IggyError::CannotEstablishConnection)
            }
            Err(e) => {
                error!("Got error: {e} on connect");
                Err(e)
            }
        }
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        let (fut, waker) = {
            let mut state = self.state.0.state.lock();
            state.enqueu_command(ClientCommand::Disconnect)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        fut.await?;
        self.publish_event(DiagnosticEvent::Disconnected).await;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        let (fut, waker) = {
            let mut state = self.state.0.state.lock();
            state.enqueu_command(ClientCommand::Shutdown)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        fut.await?;
        self.publish_event(DiagnosticEvent::Shutdown).await;
        Ok(())
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.events.1.clone()
    }
}

#[async_trait]
impl<T, R> BinaryTransport for NewTcpClient<T, R>
where
    T: Transport + Debug,
    R: Runtime + Debug + Send + Sync + 'static,
    T::Config: ClientConfig + Debug + Send + Sync + 'static,
    R::Join: Debug + Send + Sync + 'static,
{
    async fn get_state(&self) -> ClientState {
        self.state.state()
    }

    async fn set_state(&self, state: ClientState) {
        self.state.set_state(state);
    }

    async fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.broadcast(event).await {
            error!("Failed to send a TCP diagnostic event: {error}");
        }
    }

    async fn send_with_response<C: Command>(&self, command: &C) -> Result<Bytes, IggyError> {
        command.validate()?;
        self.send_raw_with_response(command.code(), command.to_bytes())
            .await
    }

    async fn send_raw_with_response(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        self.send_raw(code, payload).await
    }

    fn get_heartbeat_interval(&self) -> IggyDuration {
        self.config.heartbeat_interval()
    }
}

impl<T, R> BinaryClient for NewTcpClient<T, R>
where
    T: Transport + Debug,
    R: Runtime + Debug + Send + Sync + 'static,
    T::Config: ClientConfig + Debug + Send + Sync + 'static,
    R::Join: Debug + Send + Sync + 'static,
{
}

impl<T: Transport, R: Runtime> Drop for NewTcpClient<T, R> {
    fn drop(&mut self) {
        let mut state = self.state.0.state.lock();
        state.inner.disconnect();
        let waker = state.take_waker();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
