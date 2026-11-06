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

//! The agent of the Rust SDK, `iggy-conformance agent`.
//!
//! It is also the admin client of the orchestrator, which calls it in process
//! instead of through stdio.

mod calls;

use crate::format::Transport;
use crate::protocol::{
    AgentError, CreateClientArgs, EntityKind, Hello, HelloLine, PROTOCOL_VERSION, Reply, Request,
    RequestLine,
};
use iggy::prelude::{
    Client, ClientWrapper, Identifier, IggyClient, IggyConsumer, IggyError, PartitionReader,
    StreamClient, TcpClient, TcpClientConfig, UserClient,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as SyncMutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

/// What this agent implements beyond the methods every agent has.
pub const CAPABILITIES: [&str; 9] = [
    "reader",
    "consumer",
    "resolve_operation",
    "abandon_operation",
    "raw_command",
    "raw_command_replica",
    "describe_options",
    "metadata_changes",
    "connection_info",
];

/// Serves the protocol on stdin and stdout until stdin closes.
///
/// # Errors
///
/// Fails when stdout cannot be written.
pub async fn serve_stdio() -> std::io::Result<()> {
    let (answers, mut outbox) = mpsc::unbounded_channel::<String>();
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(line) = outbox.recv().await {
            stdout.write_all(line.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
        Ok::<(), std::io::Error>(())
    });
    let hello = serde_json::to_string(&HelloLine { hello: hello() })
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    send(&answers, hello);

    let agent = RustAgent::default();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let request: RequestLine = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                tracing::error!("Cannot read the request {line}: {error}");
                continue;
            }
        };
        // Each request runs on its own task, so a long call never delays a
        // reset or a cancellation behind it.
        let agent = agent.clone();
        let answers = answers.clone();
        tokio::spawn(async move {
            let reply = agent.handle(request.request).await;
            match serde_json::to_string(&reply.into_line(request.id)) {
                Ok(text) => send(&answers, text),
                Err(error) => tracing::error!("Cannot encode an answer: {error}"),
            }
        });
    }
    agent.reset().await;
    drop(answers);
    writer
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?
}

fn send(answers: &mpsc::UnboundedSender<String>, line: String) {
    if answers.send(line).is_err() {
        tracing::error!("The answer writer stopped");
    }
}

#[must_use]
pub fn hello() -> Hello {
    Hello {
        sdk: "rust".to_owned(),
        sdk_version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol: PROTOCOL_VERSION,
        transports: vec![Transport::Tcp],
        capabilities: CAPABILITIES.iter().map(|name| (*name).to_owned()).collect(),
    }
}

/// The entities and tasks of one agent. Clones share them.
#[derive(Clone, Default)]
pub struct RustAgent {
    state: Arc<State>,
}

#[derive(Default)]
struct State {
    clients: SyncMutex<HashMap<String, Arc<IggyClient>>>,
    readers: SyncMutex<HashMap<String, Arc<Mutex<PartitionReader>>>>,
    consumers: SyncMutex<HashMap<String, Arc<Mutex<IggyConsumer>>>>,
    tasks: SyncMutex<HashMap<String, JoinHandle<Reply>>>,
}

impl RustAgent {
    pub async fn handle(&self, request: Request) -> Reply {
        match request {
            Request::Create {
                kind: EntityKind::Client,
                entity,
                args,
            } => self.create_client(entity, args).await,
            Request::Create { kind, .. } => {
                Reply::Unsupported(format!("creating a {kind:?} with create"))
            }
            Request::Call {
                entity,
                method,
                args,
                save_as,
            } => self.call(&entity, &method, args, save_as).await,
            Request::Start {
                task,
                entity,
                method,
                args,
            } => {
                let agent = self.clone();
                let handle =
                    tokio::spawn(async move { agent.call(&entity, &method, args, None).await });
                if let Some(previous) = lock(&self.state.tasks).insert(task.clone(), handle) {
                    previous.abort();
                }
                Reply::Result(Value::Null)
            }
            Request::Await { task } => {
                let Some(handle) = lock(&self.state.tasks).remove(&task) else {
                    return Reply::Failure(format!("no task {task:?}"));
                };
                handle
                    .await
                    .unwrap_or_else(|error| Reply::Failure(format!("task {task:?}: {error}")))
            }
            Request::Cancel { task } => {
                let Some(handle) = lock(&self.state.tasks).remove(&task) else {
                    return Reply::Failure(format!("no task {task:?}"));
                };
                handle.abort();
                Reply::Result(Value::Null)
            }
            Request::Close { entity } => {
                self.close(&entity).await;
                Reply::Result(Value::Null)
            }
            Request::Reset => {
                self.reset().await;
                Reply::Result(Value::Null)
            }
        }
    }

    /// Calls a method of an entity directly, as the orchestrator's admin does.
    pub async fn call(
        &self,
        entity: &str,
        method: &str,
        args: Value,
        save_as: Option<String>,
    ) -> Reply {
        calls::call(self, entity, method, args, save_as).await
    }

    async fn create_client(&self, entity: String, args: Value) -> Reply {
        let args: CreateClientArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => return Reply::Failure(format!("create arguments: {error}")),
        };
        if args.transport != Transport::Tcp {
            return Reply::Unsupported(format!("the {:?} transport", args.transport));
        }
        match connect(args).await {
            Ok(client) => {
                lock(&self.state.clients).insert(entity, Arc::new(client));
                Reply::Result(Value::Null)
            }
            Err(error) => sdk_error(&error),
        }
    }

    fn client(&self, entity: &str) -> Option<Arc<IggyClient>> {
        lock(&self.state.clients).get(entity).cloned()
    }

    fn reader(&self, entity: &str) -> Option<Arc<Mutex<PartitionReader>>> {
        lock(&self.state.readers).get(entity).cloned()
    }

    fn keep_reader(&self, entity: String, reader: PartitionReader) {
        lock(&self.state.readers).insert(entity, Arc::new(Mutex::new(reader)));
    }

    fn consumer(&self, entity: &str) -> Option<Arc<Mutex<IggyConsumer>>> {
        lock(&self.state.consumers).get(entity).cloned()
    }

    fn keep_consumer(&self, entity: String, consumer: IggyConsumer) {
        lock(&self.state.consumers).insert(entity, Arc::new(Mutex::new(consumer)));
    }

    async fn close(&self, entity: &str) {
        lock(&self.state.readers).remove(entity);
        let consumer = lock(&self.state.consumers).remove(entity);
        if let Some(consumer) = consumer {
            shut_down(entity, &consumer).await;
        }
        let client = lock(&self.state.clients).remove(entity);
        if let Some(client) = client
            && let Err(error) = client.shutdown().await
        {
            tracing::warn!("Cannot shut down the client {entity}: {error}");
        }
    }

    /// Deletes every stream and every user but `keep_user`, with the client
    /// `entity`, so that each test starts from an empty server.
    ///
    /// # Errors
    ///
    /// Returns the first SDK error.
    pub async fn purge(&self, entity: &str, keep_user: &str) -> Result<(), IggyError> {
        let client = self.client(entity).ok_or(IggyError::NotConnected)?;
        for stream in client.get_streams().await? {
            client
                .delete_stream(&Identifier::numeric(stream.id)?)
                .await?;
        }
        for user in client.get_users().await? {
            if user.username != keep_user {
                client.delete_user(&Identifier::numeric(user.id)?).await?;
            }
        }
        Ok(())
    }

    /// Cancels every task and closes every entity.
    pub async fn reset(&self) {
        let tasks: Vec<JoinHandle<Reply>> = lock(&self.state.tasks)
            .drain()
            .map(|(_, handle)| handle)
            .collect();
        for task in tasks {
            task.abort();
        }
        lock(&self.state.readers).clear();
        let consumers: Vec<(String, Arc<Mutex<IggyConsumer>>)> =
            lock(&self.state.consumers).drain().collect();
        for (entity, consumer) in consumers {
            shut_down(&entity, &consumer).await;
        }
        let clients: Vec<(String, Arc<IggyClient>)> = lock(&self.state.clients).drain().collect();
        for (entity, client) in clients {
            if let Err(error) = client.shutdown().await {
                tracing::warn!("Cannot shut down the client {entity}: {error}");
            }
        }
    }
}

async fn shut_down(entity: &str, consumer: &Mutex<IggyConsumer>) {
    if let Err(error) = consumer.lock().await.shutdown().await {
        tracing::warn!("Cannot shut down the consumer {entity}: {error}");
    }
}

async fn connect(args: CreateClientArgs) -> Result<IggyClient, IggyError> {
    let config = TcpClientConfig {
        server_address: args.address,
        ..TcpClientConfig::default()
    };
    let tcp = TcpClient::create(Arc::new(config))?;
    let client = IggyClient::create(ClientWrapper::Tcp(tcp), None, None);
    client.connect().await?;
    if let Some(credentials) = args.login {
        client
            .login_user(&credentials.username, &credentials.password)
            .await?;
    }
    Ok(client)
}

/// The answer for an SDK error, with the operation that a retained-operation
/// error names.
#[must_use]
pub fn sdk_error(error: &IggyError) -> Reply {
    let partition_id = match error {
        IggyError::PartitionLaneBlocked(partition, _) => Some(*partition),
        _ => None,
    };
    Reply::Error(AgentError {
        code: error.as_code(),
        operation_id: error.operation_id(),
        partition_id,
        message: Some(error.to_string()),
    })
}

/// Locks a map that no holder leaves poisoned: no lock is held across a
/// panic point.
fn lock<T>(mutex: &SyncMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
