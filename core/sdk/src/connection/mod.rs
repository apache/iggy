use std::{
    collections::{HashMap, VecDeque}, fmt::Debug, io, net::SocketAddr, ops::Deref, pin::Pin, str::FromStr, sync::{
        atomic::{AtomicBool, AtomicU64, Ordering}, Arc, Mutex
    }, task::{ready, Context, Poll, Waker}, time::Duration
};

use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use futures::{future::poll_fn, task::AtomicWaker, AsyncRead, AsyncWrite, FutureExt};
use iggy_binary_protocol::{BinaryClient, BinaryTransport, Client};
use iggy_common::{
    ClientState, Command, DiagnosticEvent, IggyDuration, IggyError, TcpClientConfig,
};
use tokio::{
    net::TcpStream,
    sync::Mutex as TokioMutex,
    time::{Sleep, sleep},
};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{debug, error, info, trace, warn};
use crate::protocol::{Order, ProtocolCore, ProtocolCoreConfig, Response, TxBuf};

pub trait StreamBuilder {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    fn connect(self, addr: SocketAddr) -> Pin<Box<dyn Future<Output = io::Result<Self::Stream>> + Send>>;
}

pub struct TokioTcpBuilder {
    inner: tokio::net::TcpSocket,
}

impl TokioTcpBuilder {
    pub fn new_for(addr: &SocketAddr) -> io::Result<Self> {
        let sock = match addr {
            SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        sock.set_nodelay(true)?;
        Ok(Self { inner: sock })
    }
}

impl StreamBuilder for TokioTcpBuilder {
    type Stream = Compat<TcpStream>;

    fn connect(self, addr: SocketAddr) -> Pin<Box<dyn Future<Output = io::Result<Self::Stream>> + Send>> {
        Box::pin(async move {
            let s = self.inner.connect(addr).await?;
            Ok(s.compat())
        })    
    }
}

pub type TokioCompat = Compat<TcpStream>;
pub type NewTokioTcpClient = NewTcpClient<TokioCompat>;

pub trait SocketFactory<S>: Fn(&SocketAddr) -> io::Result<S> + Send + Sync + 'static {}
impl<F, S> SocketFactory<S> for F where F: Fn(&SocketAddr) -> io::Result<S> + Send + Sync + 'static {}

#[derive(Debug, Clone)]
pub struct ConnectionStats {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub pending_sends: usize,
    pub pending_receives: usize,
}

pub enum ClientCommand {
    Connect(SocketAddr),
    Disconnect,
    Shutdown,
}

pub trait AsyncIO: AsyncWrite + AsyncRead + Unpin {}
impl<T: AsyncRead + AsyncWrite + Unpin> AsyncIO for T {}

pub trait Runtime: Send + Sync + Debug {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>);
}

#[derive(Debug)]
pub struct TokioRuntime {}

impl Runtime for TokioRuntime {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(future);
    }
}

#[derive(Debug)]
struct SendState {
    request_id: u64,
    encoded: BytesMut,
    offset: usize,
}

// sans-io state, который будет находиться отдкельно
#[derive(Debug)]
pub struct ProtoConnectionState {
    pub core: ProtocolCore,
    error: Option<IggyError>,
}

#[derive(Debug)]
pub struct ConnectionInner<S: AsyncIO> {
    pub(crate) state: Mutex<State<S>>,
}

#[derive(Debug)]
pub struct ConnectionRef<S: AsyncIO>(Arc<ConnectionInner<S>>);

impl<S: AsyncIO> ConnectionRef<S> {
    fn new(state: ProtoConnectionState, factory: Box<dyn SocketFactory<S>>, config: Arc<TcpClientConfig>) -> Self {
        Self(Arc::new(ConnectionInner {
            state: Mutex::new(State {
                inner: state,
                driver: None,
                socket_factory: factory,
                socket: None,
                current_send: None,
                send_offset: 0,
                recv_buffer: BytesMut::new(),
                wait_timer: None,
                ready_responses: HashMap::new(),
                recv_waiters: HashMap::new(),
                config,
                waiters: Arc::new(Waiters { map: Mutex::new(HashMap::new()), next_id: AtomicU64::new(0) }),
                pending_commands: VecDeque::new(),
                ready_commands: VecDeque::new(),
            })
        }))
    }
}

impl<S: AsyncIO> ConnectionRef<S> {
    fn state(&self) -> ClientState {
        let state = self.0.state.lock().unwrap();
        state.inner.core.state()
    }
}

impl<S: AsyncIO> Deref for ConnectionRef<S> {
    type Target = ConnectionInner<S>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S: AsyncIO> Clone for ConnectionRef<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

type RequestId = u64;

struct WaitEntry<T> {
    waker: AtomicWaker,
    result: Option<T>,
}

struct Waiters<T> {
    map: Mutex<HashMap<RequestId, WaitEntry<T>>>,
    next_id: AtomicU64,
}

impl<T> Waiters<T> {
    fn new() -> Self {
        Self { map: Mutex::new(HashMap::new()), next_id: AtomicU64::new(1) }
    }
    fn alloc(&self) -> RequestId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.map.lock().unwrap().insert(id, WaitEntry { waker: AtomicWaker::new(), result: None });
        id
    }
    fn complete(&self, id: RequestId, val: T) -> bool {
        if let Some(entry) = self.map.lock().unwrap().get_mut(&id) {
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
    id: RequestId,
}

impl<T> Future for WaitFuture<T> {
    type Output = T;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut waiters = self.waiters.map.lock().unwrap();
        if let Some(val) = {
            if let Some(e) = waiters.get_mut(&self.id) {
                e.result.take()
            } else { None }
        } {
            waiters.remove(&self.id);
            return Poll::Ready(val);
        }

        {
            if let Some(e) = waiters.get(&self.id) {
                e.waker.register(cx.waker());
            }
        }

        if let Some(val) = {
            if let Some(e) = waiters.get_mut(&self.id) {
                e.result.take()
            } else { None }
        } {
            waiters.remove(&self.id);
            return Poll::Ready(val);
        }

        Poll::Pending
    }
}

pub struct State<S, B>
where
    S: AsyncIO,
    B: StreamBuilder<Stream = S>
{
    inner: ProtoConnectionState,
    driver: Option<Waker>,
    socket_factory: B,
    socket: Option<S>,
    current_send: Option<TxBuf>,
    send_offset: usize,
    recv_buffer: BytesMut,
    wait_timer: Option<Pin<Box<Sleep>>>,
    ready_responses: HashMap<u64, Result<Bytes, IggyError>>,
    pub recv_waiters: HashMap<u64, Waker>,
    config: Arc<TcpClientConfig>,

    waiters: Arc<Waiters<Result<Bytes, IggyError>>>,
    pending_commands: VecDeque<(u64, ClientCommand)>,
    ready_commands: VecDeque<u64>,
    connect_waiters: Vec<u64>,
}

impl<S, B> Debug for State<S, B>
where
    S: AsyncIO,
    B: StreamBuilder<Stream = S>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "test"
        )
    }
}

impl<S, B> State<S, B>
where
    S: AsyncIO,
    B: StreamBuilder<Stream = S>
{
    fn wake(&mut self) {
        if let Some(waker) = self.driver.take() {
            waker.wake();
        }
    }

    fn enqueu_command(&mut self, command: ClientCommand) -> WaitFuture<Result<Bytes, IggyError>> {
        let id = self.waiters.alloc();
        // TODO перетащить в sans io ядро inner.core
        self.pending_commands.push_back((id, command));
        WaitFuture { waiters: self.waiters.clone(), id }
    }

    fn enqueue_message(&mut self, code: u32, payload: Bytes) -> Result<u64, IggyError> {
        self.inner.core.send(code, payload)
    }

    fn drive_client_commands(&mut self) -> bool {
        for (request_id, cmd) in self.pending_commands.drain(..) {
            match cmd {
                ClientCommand::Connect(server_addr) => {
                    self.connect_waiters.push(request_id);
                    self.inner.core.
                    // let socket = self.socket_factory.connect(server_addr);

                    // self.socket = Some(socket);
                    // self.ready_commands.push_back(request_id);
                    // self.waiters.complete(request_id, Ok(Bytes::new()));
                }
                ClientCommand::Disconnect => {
                    // TODO add processing
                    self.ready_commands.push_back(request_id);
                    self.waiters.complete(request_id, Ok(Bytes::new()));
                }
                ClientCommand::Shutdown => {
                    // TODO add processing
                    self.ready_commands.push_back(request_id);
                    self.waiters.complete(request_id, Ok(Bytes::new()));
                }
            }
        }
        true
    }

    fn drive_timer(&mut self, cx: &mut Context<'_>) -> bool {
        if let Some(t) = &mut self.wait_timer {
            if t.as_mut().poll(cx).is_pending() {
                return false;
            }
            self.wait_timer = None;
        }
        true
    }

    fn drive_transmit(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        let mut offset = self.send_offset;
        let socket = self.socket.as_mut().ok_or(io::Error::new(
            io::ErrorKind::NotConnected, 
            "No socket"
        ))?;

        if let Some(buf) = self.current_send.take() {
            while self.send_offset < buf.data.len() {
                match Pin::new(&mut *socket).poll_write(cx, &buf.data[offset..])? {
                    Poll::Ready(n) => {
                        offset += n;
                        self.send_offset += n;
                    }
                    Poll::Pending => return Ok(false),
                }
            }
            match Pin::new(socket).poll_flush(cx)? {
                Poll::Pending => return Ok(false),
                Poll::Ready(()) => {}
            }
            self.current_send = None;
        }
        Ok(true)
    }

    fn drive_receive(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        if self.socket.is_none() {
            // TODO add return error
            panic!("socket is none")
        }

        let mut recv_scratch = vec![0u8; 4096];

        loop {
            let n = {
                let socket = self.socket.as_mut().ok_or(io::Error::new(
                    io::ErrorKind::NotConnected, 
                    "No socket"
                ))?;
                let mut pinned = Pin::new(&mut *socket);
                match pinned.as_mut().poll_read(cx, &mut recv_scratch)? {
                    Poll::Pending => return Ok(false),
                    Poll::Ready(n) => n,
                }
            };
            self.recv_buffer.extend_from_slice(&recv_scratch[..n]);
            self.process_incoming()?;
        }
    }

    // TODO перенкести в sans io ядро
    fn process_incoming(&mut self) -> Result<(), io::Error> {
        loop {
            if self.recv_buffer.len() < 8 {
                break;
            }
            let status = u32::from_le_bytes(self.recv_buffer[0..4].try_into().unwrap());
            let length = u32::from_le_bytes(self.recv_buffer[4..8].try_into().unwrap());
            let total = 8usize + length as usize;
            if self.recv_buffer.len() < total {
                break;
            }

            self.recv_buffer.advance(8);
            let payload = if length > 0 {
                Bytes::copy_from_slice(&self.recv_buffer.split_to(length as usize))
            } else {
                Bytes::new()
            };

            if let Some(request_id) = self.inner.core.on_response(status, &payload) {
                let result = if status == 0 {
                    Ok(payload)
                } else {
                    Err(IggyError::from_code(status))
                };
                self.ready_responses.insert(request_id, result);
                if let Some(waker) = self.recv_waiters.remove(&request_id) {
                    waker.wake();
                }
            }
        }
        Ok(())
    }
}

struct ConnectionDriver<S: AsyncIO>(ConnectionRef<S>);

impl<S: AsyncIO + Send> ConnectionDriver<S> {
    // fn new(state: Arc<Mutex<ProtoConnectionState>>, socket: S) -> Self {
    //     Self {
    //         state,
    //         socket: Box::pin(socket),
    //         current_send: None,
    //         send_offset: 0,
    //         recv_buffer: BytesMut::with_capacity(4096),
    //         wait_timer: None,
    //         send_buf: Mutex::new(VecDeque::new())
    //     }
    // }
}

impl<S: AsyncIO> Future for ConnectionDriver<S> {
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let st = &mut *self.0.state.lock().unwrap();

        let mut keep_going = st.drive_timer(cx);

        let order = st.inner.core.poll();

        match order {
            Order::Wait(dur) => {
                st.wait_timer = Some(Box::pin(tokio::time::sleep(dur.get_duration())));
                return Poll::Pending;
            }
            Order::Transmit(tx) => {
                st.current_send = Some(tx);
                st.send_offset = 0;
            }
            Order::Error(e) => {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, format!("{e:?}"))));
            }
            Order::Noop | Order::Authenticate { .. } => {}
            Order::Connect => {todo!("добавить вызов метода из state")}
        }

        keep_going |= st.drive_client_commands();
        keep_going |= st.drive_transmit(cx)?;
        keep_going |= st.drive_receive(cx)?;

        if keep_going {
            cx.waker().wake_by_ref();
        } else {
            st.driver = Some(cx.waker().clone());
        }

        Poll::Pending
    }
}

#[derive(Debug)]
pub struct NewTcpClient<S: AsyncIO> {
    // transport: Arc<TokioMutex<Option<TokioTcpTransport>>>,
    state: ConnectionRef<S>,
    // state: Arc<Mutex<ProtoConnectionState>>,
    config: Arc<TcpClientConfig>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
    // runtime: Arc<dyn Runtime>,
}

impl<S: AsyncIO + Send + Sync + 'static> NewTcpClient<S> {
    pub fn create(config: Arc<TcpClientConfig>, factory:Box<dyn SocketFactory<S>>) -> Result<Self, IggyError> {
        // let runtime = Arc::new(TokioRuntime {});
        // let transport = TokioTcpTransport::new(config.clone(), runtime.clone());
        // let state = transport.state.clone();

        let (tx, rx) = broadcast(1000);
        let (client_tx, client_rx) = flume::unbounded::<ClientCommand>();
        
        let proto_config = ProtocolCoreConfig{
            auto_login: iggy_common::AutoLogin::Disabled,
            reestablish_after: IggyDuration::new_from_secs(5),
            max_retries: None,
        };

        let conn = ConnectionRef::new(ProtoConnectionState { core: ProtocolCore::new(proto_config), error: None }, factory, config.clone());
        let driver = ConnectionDriver(conn.clone());
        tokio::spawn(async move {
            if let Err(e) = driver.await {
                tracing::error!("I/O error: {e}");
            }
        });

        Ok(Self {
            // transport: Arc::new(TokioMutex::new(Some(transport))),
            state: conn,
            config,
            events: (tx, rx),
            // runtime,
        })
    }

    async fn send_raw(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        let id = {
            // TODO вынести в отдельный метод для быстрого деструктурирования
            let mut state = self.state.0.state.lock().unwrap();
            state.enqueue_message(code, payload)
        }?;

        poll_fn(move |cx| {
            let mut state = self.state.0.state.lock().unwrap();

            // if let Some(ref error) = state.error {
            //     return Poll::Ready(Err(error.clone()));
            // }

            if let Some(result) = state.ready_responses.remove(&id) {
                return Poll::Ready(result);
            }

            state.recv_waiters.insert(id, cx.waker().clone());
            state.wake();

            Poll::Pending
        }).await
    }
}

#[async_trait]
impl<S: AsyncIO + Debug + Send + Sync + 'static> Client for NewTcpClient<S> {
    async fn connect(&self) -> Result<(), IggyError> {
        let address = SocketAddr::from_str(&self.config.server_address).unwrap();
        let fut = {
            let mut state = self.state.0.state.lock().unwrap();
            state.enqueu_command(ClientCommand::Connect(address))
        };
        fut.await?;
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        let fut = {
            let mut state = self.state.0.state.lock().unwrap();
            state.enqueu_command(ClientCommand::Disconnect)
        };
        fut.await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        let fut = {
            let mut state = self.state.0.state.lock().unwrap();
            state.enqueu_command(ClientCommand::Shutdown)
        };
        fut.await?;
        Ok(())
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.events.1.clone()
    }
}

#[async_trait]
impl<S: AsyncIO + Debug + Send + Sync + 'static> BinaryTransport for NewTcpClient<S> {
    async fn get_state(&self) -> ClientState {
        self.state.state()
    }

    async fn set_state(&self, _state: ClientState) {
        // State is managed by core, this is for compatibility
    }

    async fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.broadcast(event).await {
            error!("Failed to send a TCP diagnostic event: {error}");
        }
    }

    async fn send_with_response<T: Command>(&self, command: &T) -> Result<Bytes, IggyError> {
        command.validate()?;
        self.send_raw_with_response(command.code(), command.to_bytes())
            .await
    }

    async fn send_raw_with_response(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        self.send_raw(code, payload).await
    }

    fn get_heartbeat_interval(&self) -> IggyDuration {
        self.config.heartbeat_interval
    }
}

impl<S: AsyncIO + Debug + Send + Sync + 'static> BinaryClient for NewTcpClient<S> {}
