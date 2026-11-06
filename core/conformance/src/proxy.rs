// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The fault proxy between an SDK client and the server.
//!
//! Each client entity that a test routes through the proxy gets a listener of
//! its own, so every frame on it belongs to that client. The proxy splits the
//! byte stream at the 256-byte consensus headers, forwards each frame
//! unchanged, applies the `fail_frames` rules of the client and records every
//! frame for `expect_frames`. Frames keep their bytes, so checksums stay valid.

use crate::format::Direction;
use iggy_binary_protocol::codes::{LOGIN_REGISTER_CODE, LOGOUT_USER_CODE, command_name};
use iggy_binary_protocol::{
    Command, GenericHeader, HEADER_SIZE, Operation, ReplyHeader, RequestHeader, read_size_field,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Above the largest frame that an SDK sends, so that garbage cannot make
/// the proxy allocate without bound.
const MAX_FRAME_SIZE: usize = 256 * 1024 * 1024;

/// Frames of keep-alive traffic, which `expect_frames` leaves out by default.
const PING_COMMANDS: [&str; 3] = ["ping", "ping_client", "pong_client"];

/// What a rule does to the frame that it matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// The server never sees the request. The connection stays open.
    DropRequest,
    /// The server applies the request, and the reply never arrives.
    DropReply,
    /// The proxy closes the connection instead of forwarding the frame.
    CloseBeforeForward,
    /// The proxy forwards the frame, then closes the connection.
    CloseAfterForward,
    /// The proxy holds the frame for `delay_ms`, then forwards it.
    Delay,
}

/// What happened to a recorded frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Forwarded,
    Dropped,
    Delayed,
    ClosedBefore,
    ClosedAfter,
}

/// One recorded frame, in the form that `expect_frames` matches.
#[derive(Debug, Clone, Serialize)]
pub struct Frame {
    /// The proxy's number of the TCP connection, from 1.
    pub connection: u64,
    pub direction: Direction,
    /// The protocol command name, such as `consumer_group.join`, or the
    /// consensus command for a frame that carries no request, such as
    /// `ping_client`.
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<u32>,
    /// The request number, which a reply repeats.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<u64>,
    /// The client id as decimal text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<u64>,
    /// The status of a reply. Zero means success.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u32>,
    pub size: u32,
    /// Milliseconds since the listener started.
    pub at_ms: u64,
    pub outcome: Outcome,
}

impl Frame {
    #[must_use]
    pub fn is_ping(&self) -> bool {
        PING_COMMANDS.contains(&self.command.as_str())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailFramesArgs {
    pub client: String,
    #[serde(rename = "match", default)]
    pub frame: FrameMatch,
    /// Matching frames that pass before the rule applies.
    #[serde(default)]
    pub skip: u32,
    /// Matching frames that the rule applies to.
    #[serde(default = "one")]
    pub times: u32,
    pub action: Action,
    pub delay_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameMatch {
    pub direction: Option<Direction>,
    pub command: Option<String>,
    pub code: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefuseConnectionsArgs {
    pub client: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientArgs {
    pub client: String,
}

fn one() -> u32 {
    1
}

/// The listeners of one test.
#[derive(Default)]
pub struct ProxyHub {
    listeners: HashMap<String, Listener>,
}

struct Listener {
    state: Arc<ListenerState>,
    accept: JoinHandle<()>,
}

struct ListenerState {
    upstream: String,
    started: Instant,
    rules: Mutex<Vec<Rule>>,
    frames: Mutex<Vec<Frame>>,
    connections: AtomicU64,
    refuse_until: Mutex<Option<Instant>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

struct Rule {
    args: FailFramesArgs,
    matched: u32,
    applied: u32,
}

impl ProxyHub {
    /// Opens a listener for `client` that forwards to `upstream`, and returns
    /// the address that the client dials.
    ///
    /// # Errors
    ///
    /// Fails when no local port is free.
    pub async fn listen(&mut self, client: &str, upstream: &str) -> io::Result<String> {
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?.to_string();
        let state = Arc::new(ListenerState {
            upstream: upstream.to_owned(),
            started: Instant::now(),
            rules: Mutex::default(),
            frames: Mutex::default(),
            connections: AtomicU64::new(0),
            refuse_until: Mutex::default(),
            tasks: Mutex::default(),
        });
        let accept = tokio::spawn(accept_loop(socket, Arc::clone(&state)));
        if let Some(previous) = self
            .listeners
            .insert(client.to_owned(), Listener { state, accept })
        {
            previous.close();
        }
        Ok(address)
    }

    /// # Errors
    ///
    /// Fails for a client without a listener, or a delay without `delay_ms`.
    pub fn fail_frames(&self, args: FailFramesArgs) -> Result<(), String> {
        if args.action == Action::Delay && args.delay_ms.is_none() {
            return Err("the delay action needs delay_ms".to_owned());
        }
        let state = self.state(&args.client)?;
        lock(&state.rules).push(Rule {
            args,
            matched: 0,
            applied: 0,
        });
        Ok(())
    }

    /// # Errors
    ///
    /// Fails for a client without a listener.
    pub fn refuse_connections(&self, args: &RefuseConnectionsArgs) -> Result<(), String> {
        let state = self.state(&args.client)?;
        *lock(&state.refuse_until) = Some(Instant::now() + Duration::from_millis(args.duration_ms));
        Ok(())
    }

    /// # Errors
    ///
    /// Fails for a client without a listener.
    pub fn clear_rules(&self, client: &str) -> Result<(), String> {
        let state = self.state(client)?;
        lock(&state.rules).clear();
        *lock(&state.refuse_until) = None;
        Ok(())
    }

    /// The frames of `client`, in the order in which the proxy read them.
    ///
    /// # Errors
    ///
    /// Fails for a client without a listener.
    pub fn frames(&self, client: &str) -> Result<Vec<Frame>, String> {
        Ok(lock(&self.state(client)?.frames).clone())
    }

    /// Closes every listener and every connection.
    pub fn close_all(&mut self) {
        for (_, listener) in self.listeners.drain() {
            listener.close();
        }
    }

    fn state(&self, client: &str) -> Result<&Arc<ListenerState>, String> {
        self.listeners
            .get(client)
            .map(|listener| &listener.state)
            .ok_or_else(|| format!("{client:?} does not go through the proxy"))
    }
}

impl Listener {
    fn close(self) {
        self.accept.abort();
        for task in lock(&self.state.tasks).drain(..) {
            task.abort();
        }
    }
}

impl ListenerState {
    /// The action of the first rule that applies to `frame`, which also
    /// counts the frame for each rule that it matches.
    fn action(&self, frame: &Frame) -> Option<(Action, Option<u64>)> {
        let mut rules = lock(&self.rules);
        for rule in rules.iter_mut() {
            if !rule.matches(frame) {
                continue;
            }
            rule.matched += 1;
            if rule.matched > rule.args.skip && rule.applied < rule.args.times {
                rule.applied += 1;
                return Some((rule.args.action, rule.args.delay_ms));
            }
        }
        None
    }

    fn refusing(&self) -> bool {
        lock(&self.refuse_until).is_some_and(|until| Instant::now() < until)
    }
}

impl Rule {
    fn matches(&self, frame: &Frame) -> bool {
        let implied = match self.args.action {
            Action::DropRequest => Some(Direction::Request),
            Action::DropReply => Some(Direction::Reply),
            _ => None,
        };
        let direction = self.args.frame.direction.or(implied);
        direction.is_none_or(|direction| direction == frame.direction)
            && self
                .args
                .frame
                .command
                .as_ref()
                .is_none_or(|command| *command == frame.command)
            && self
                .args
                .frame
                .code
                .is_none_or(|code| Some(code) == frame.code)
    }
}

async fn accept_loop(socket: TcpListener, state: Arc<ListenerState>) {
    loop {
        let client = match socket.accept().await {
            Ok((client, _)) => client,
            Err(error) => {
                tracing::warn!("The proxy cannot accept: {error}");
                continue;
            }
        };
        if state.refusing() {
            drop(client);
            continue;
        }
        let connection = state.connections.fetch_add(1, Ordering::Relaxed) + 1;
        let task = tokio::spawn(serve(client, connection, Arc::clone(&state)));
        lock(&state.tasks).push(task);
    }
}

async fn serve(client: TcpStream, connection: u64, state: Arc<ListenerState>) {
    let server = match TcpStream::connect(&state.upstream).await {
        Ok(server) => server,
        Err(error) => {
            tracing::warn!("The proxy cannot reach {}: {error}", state.upstream);
            return;
        }
    };
    if let Err(error) = client.set_nodelay(true).and(server.set_nodelay(true)) {
        tracing::warn!("The proxy cannot set TCP_NODELAY: {error}");
    }
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    let (close, closed) = watch::channel(false);
    let link = Link {
        connection,
        state,
        requests: Arc::default(),
        close,
    };
    let upstream = pump(
        &link,
        Direction::Request,
        client_read,
        server_write,
        closed.clone(),
    );
    let downstream = pump(&link, Direction::Reply, server_read, client_write, closed);
    // The first side to end tears the connection down, as a lost network
    // link does.
    tokio::select! {
        result = upstream => log_end(result),
        result = downstream => log_end(result),
    }
}

/// The state that both directions of one connection share.
struct Link {
    connection: u64,
    state: Arc<ListenerState>,
    /// The command code of each request number, to name the replies.
    requests: Arc<Mutex<HashMap<u64, u32>>>,
    close: watch::Sender<bool>,
}

async fn pump(
    link: &Link,
    direction: Direction,
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    mut closed: watch::Receiver<bool>,
) -> io::Result<()> {
    loop {
        let bytes = tokio::select! {
            read = read_frame(&mut from) => match read? {
                Some(bytes) => bytes,
                None => return Ok(()),
            },
            _ = closed.changed() => return Ok(()),
        };
        let mut frame = describe(link, direction, &bytes);
        let action = link.state.action(&frame);
        frame.outcome = match action {
            None => Outcome::Forwarded,
            Some((Action::DropRequest | Action::DropReply, _)) => Outcome::Dropped,
            Some((Action::Delay, _)) => Outcome::Delayed,
            Some((Action::CloseBeforeForward, _)) => Outcome::ClosedBefore,
            Some((Action::CloseAfterForward, _)) => Outcome::ClosedAfter,
        };
        // Recorded before the forward, so that a request always precedes its
        // reply in the record.
        lock(&link.state.frames).push(frame);
        match action {
            None => forward(&mut to, &bytes).await?,
            Some((Action::DropRequest | Action::DropReply, _)) => {}
            Some((Action::Delay, delay_ms)) => {
                tokio::time::sleep(Duration::from_millis(delay_ms.unwrap_or_default())).await;
                forward(&mut to, &bytes).await?;
            }
            Some((Action::CloseBeforeForward, _)) => {
                let _ = link.close.send(true);
                return Ok(());
            }
            Some((Action::CloseAfterForward, _)) => {
                forward(&mut to, &bytes).await?;
                let _ = link.close.send(true);
                return Ok(());
            }
        }
    }
}

async fn read_frame(from: &mut OwnedReadHalf) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0u8; HEADER_SIZE];
    match from.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let size = read_size_field(&header).map_or(0, |size| size as usize);
    if !(HEADER_SIZE..=MAX_FRAME_SIZE).contains(&size) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a frame of {size} bytes"),
        ));
    }
    let mut frame = vec![0u8; size];
    frame[..HEADER_SIZE].copy_from_slice(&header);
    from.read_exact(&mut frame[HEADER_SIZE..]).await?;
    Ok(Some(frame))
}

async fn forward(to: &mut OwnedWriteHalf, bytes: &[u8]) -> io::Result<()> {
    to.write_all(bytes).await?;
    to.flush().await
}

fn describe(link: &Link, direction: Direction, bytes: &[u8]) -> Frame {
    let header = &bytes[..HEADER_SIZE];
    let mut frame = Frame {
        connection: link.connection,
        direction,
        command: "unknown".to_owned(),
        code: None,
        request: None,
        client: None,
        session: None,
        status: None,
        size: read_size_field(header).unwrap_or_default(),
        at_ms: u64::try_from(link.state.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        outcome: Outcome::Forwarded,
    };
    let Ok(generic) = bytemuck::checked::try_pod_read_unaligned::<GenericHeader>(header) else {
        return frame;
    };
    match generic.command {
        Command::Request => {
            if let Ok(request) = bytemuck::checked::try_pod_read_unaligned::<RequestHeader>(header)
            {
                let code = request_code(&request);
                lock(&link.requests).insert(request.request, code);
                frame.command = name_of(code);
                frame.code = Some(code);
                frame.request = Some(request.request);
                frame.client = Some(request.client.to_string());
                frame.session = Some(request.session);
            }
        }
        Command::Reply => {
            if let Ok(reply) = bytemuck::checked::try_pod_read_unaligned::<ReplyHeader>(header) {
                let code = lock(&link.requests)
                    .get(&reply.request)
                    .copied()
                    .or_else(|| reply.operation.to_command_code());
                frame.command = code.map_or_else(|| "reply".to_owned(), name_of);
                frame.code = code;
                frame.request = Some(reply.request);
                frame.client = Some(reply.client.to_string());
                frame.status = Some(reply.status);
            }
        }
        other => frame.command = snake_case(&format!("{other:?}")),
    }
    frame
}

/// The command code of a request, as the server dispatches it.
fn request_code(request: &RequestHeader) -> u32 {
    match request.operation {
        Operation::Register => LOGIN_REGISTER_CODE,
        Operation::Logout => LOGOUT_USER_CODE,
        Operation::NonReplicated => u32::from_le_bytes(request.reserved),
        operation => operation.to_command_code().unwrap_or_default(),
    }
}

fn name_of(code: u32) -> String {
    command_name(code).map_or_else(|_| format!("code_{code}"), str::to_owned)
}

fn snake_case(name: &str) -> String {
    let mut snake = String::with_capacity(name.len() + 4);
    for (index, character) in name.chars().enumerate() {
        if character.is_uppercase() {
            if index > 0 {
                snake.push('_');
            }
            snake.extend(character.to_lowercase());
        } else {
            snake.push(character);
        }
    }
    snake
}

fn log_end(result: io::Result<()>) {
    if let Err(error) = result {
        tracing::debug!("A proxied connection ended: {error}");
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(direction: Direction, command: &str) -> Frame {
        Frame {
            connection: 1,
            direction,
            command: command.to_owned(),
            code: None,
            request: None,
            client: None,
            session: None,
            status: None,
            size: 256,
            at_ms: 0,
            outcome: Outcome::Forwarded,
        }
    }

    fn rule(action: Action, command: &str, skip: u32, times: u32) -> Rule {
        Rule {
            args: FailFramesArgs {
                client: "client0".to_owned(),
                frame: FrameMatch {
                    direction: None,
                    command: Some(command.to_owned()),
                    code: None,
                },
                skip,
                times,
                action,
                delay_ms: None,
            },
            matched: 0,
            applied: 0,
        }
    }

    #[test]
    fn given_a_drop_request_rule_when_a_reply_matches_the_command_should_not_apply() {
        let rule = rule(Action::DropRequest, "message.send", 0, 1);
        assert!(rule.matches(&frame(Direction::Request, "message.send")));
        assert!(!rule.matches(&frame(Direction::Reply, "message.send")));
    }

    #[test]
    fn given_skip_and_times_when_frames_match_should_apply_only_in_between() {
        let state = ListenerState {
            upstream: String::new(),
            started: Instant::now(),
            rules: Mutex::new(vec![rule(Action::DropRequest, "message.send", 1, 2)]),
            frames: Mutex::default(),
            connections: AtomicU64::new(0),
            refuse_until: Mutex::default(),
            tasks: Mutex::default(),
        };
        let send = frame(Direction::Request, "message.send");
        let actions: Vec<bool> = (0..4).map(|_| state.action(&send).is_some()).collect();
        assert_eq!(actions, [false, true, true, false]);
    }

    #[test]
    fn given_consensus_command_names_when_converting_should_use_snake_case() {
        assert_eq!(snake_case("PingClient"), "ping_client");
        assert_eq!(snake_case("Eviction"), "eviction");
    }
}
