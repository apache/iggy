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

//! Starts an SDK agent as a child process and exchanges JSON lines with it.

use crate::protocol::{
    Hello, HelloLine, PROTOCOL_VERSION, Reply, Request, RequestLine, ResponseLine,
};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;

/// The agent program: `rust` for the built-in Rust agent, or a shell command.
#[derive(Debug, Clone)]
pub enum AgentCommand {
    BuiltIn,
    Shell(String),
}

#[derive(Debug, thiserror::Error)]
pub enum HostError {
    #[error("cannot start the agent: {0}")]
    Spawn(std::io::Error),
    #[error("the agent sent no hello within {0:?}")]
    HelloTimeout(Duration),
    #[error("the agent closed its stdout before its hello")]
    NoHello,
    #[error("the agent's hello is invalid: {0}")]
    BadHello(String),
    #[error("the agent speaks protocol {0}, this runner speaks {PROTOCOL_VERSION}")]
    Protocol(u32),
    #[error("the agent did not answer within {0:?}")]
    Timeout(Duration),
    #[error("the agent exited")]
    Exited,
    #[error("cannot write to the agent: {0}")]
    Write(std::io::Error),
    #[error("the agent's answer is invalid: {0}")]
    BadAnswer(String),
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<ResponseLine>>>>;

pub struct AgentHost {
    child: Child,
    stdin: AsyncMutex<ChildStdin>,
    pending: Pending,
    next_id: AtomicU64,
    hello: Hello,
    reader: JoinHandle<()>,
}

impl AgentHost {
    /// Starts the agent and waits for its hello.
    ///
    /// # Errors
    ///
    /// Fails when the agent does not start, or sends no valid hello in time.
    pub async fn spawn(command: &AgentCommand, hello_timeout: Duration) -> Result<Self, HostError> {
        let mut process = match command {
            AgentCommand::BuiltIn => {
                let program = std::env::current_exe().map_err(HostError::Spawn)?;
                let mut process = Command::new(program);
                process.arg("agent");
                process
            }
            AgentCommand::Shell(line) => {
                let mut process = Command::new("sh");
                process.arg("-c").arg(format!("exec {line}"));
                process
            }
        };
        let mut child = process
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(HostError::Spawn)?;
        let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
            return Err(HostError::Spawn(std::io::Error::other(
                "the agent has no piped stdio",
            )));
        };
        let mut lines = BufReader::new(stdout).lines();
        let hello_line = match timeout(hello_timeout, lines.next_line()).await {
            Err(_) => return Err(HostError::HelloTimeout(hello_timeout)),
            Ok(Err(_) | Ok(None)) => return Err(HostError::NoHello),
            Ok(Ok(Some(line))) => line,
        };
        let hello = serde_json::from_str::<HelloLine>(&hello_line)
            .map_err(|error| HostError::BadHello(format!("{error}: {hello_line}")))?
            .hello;
        if hello.protocol != PROTOCOL_VERSION {
            return Err(HostError::Protocol(hello.protocol));
        }
        let pending: Pending = Arc::default();
        let reader = tokio::spawn(read_answers(lines, Arc::clone(&pending)));
        Ok(Self {
            child,
            stdin: AsyncMutex::new(stdin),
            pending,
            next_id: AtomicU64::new(1),
            hello,
            reader,
        })
    }

    #[must_use]
    pub fn hello(&self) -> &Hello {
        &self.hello
    }

    /// Sends a request and waits for its answer.
    ///
    /// # Errors
    ///
    /// Fails when the agent does not answer in time, exits, or answers with an
    /// invalid line.
    pub async fn request(&self, request: Request, limit: Duration) -> Result<Reply, HostError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (answer, answered) = oneshot::channel();
        lock(&self.pending).insert(id, answer);
        let mut line = serde_json::to_string(&RequestLine { id, request })
            .map_err(|error| HostError::BadAnswer(error.to_string()))?;
        line.push('\n');
        {
            let mut stdin = self.stdin.lock().await;
            let written = async {
                stdin.write_all(line.as_bytes()).await?;
                stdin.flush().await
            };
            if let Err(error) = written.await {
                lock(&self.pending).remove(&id);
                return Err(HostError::Write(error));
            }
        }
        match timeout(limit, answered).await {
            Err(_) => {
                lock(&self.pending).remove(&id);
                Err(HostError::Timeout(limit))
            }
            Ok(Err(_)) => Err(HostError::Exited),
            Ok(Ok(line)) => Reply::try_from(line).map_err(HostError::BadAnswer),
        }
    }

    /// Stops the agent.
    pub async fn stop(mut self) {
        self.reader.abort();
        if let Err(error) = self.child.kill().await {
            tracing::warn!("Cannot stop the agent: {error}");
        }
    }
}

async fn read_answers(
    mut lines: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    pending: Pending,
) {
    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(error) => {
                tracing::error!("Cannot read the agent's stdout: {error}");
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<ResponseLine>(&line) {
            Ok(answer) => {
                let waiter = lock(&pending).remove(&answer.id);
                match waiter {
                    Some(waiter) => {
                        // The waiter can have timed out already.
                        let _ = waiter.send(answer);
                    }
                    None => tracing::warn!("An answer for no request: {line}"),
                }
            }
            Err(error) => tracing::error!("An invalid answer line ({error}): {line}"),
        }
    }
    // Dropping the waiters tells each pending request that the agent exited.
    lock(&pending).clear();
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
