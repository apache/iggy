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

use std::sync::Arc;
use std::time::Duration;

use ext_php_rs::{exception::PhpResult, php_class, php_impl, zend::ce};
use futures::StreamExt;
use iggy::prelude::IggyConsumer as RustIggyConsumer;
use tokio::sync::Mutex;
use tokio::time::timeout;

use crate::consumer::lock_consumer;
use crate::error::to_php_exception;
use crate::receive_message::ReceiveMessage;
use crate::runtime::runtime;

#[php_class]
#[php(name = "Iggy\\MessageIterator")]
#[php(implements(ce = ce::iterator, stub = "\\Iterator"))]
pub struct MessageIterator {
    pub(crate) inner: Arc<Mutex<RustIggyConsumer>>,
    idle_timeout: Option<Duration>,
    current: Option<ReceiveMessage>,
    key: u64,
    started: bool,
}

impl MessageIterator {
    pub(crate) fn new(inner: Arc<Mutex<RustIggyConsumer>>, idle_timeout: Option<Duration>) -> Self {
        Self {
            inner,
            idle_timeout,
            current: None,
            key: 0,
            started: false,
        }
    }

    fn poll_next(&mut self) -> PhpResult {
        // A read that throws leaves no message, so the message before it is not yielded again.
        self.current = None;
        let mut consumer = lock_consumer(&self.inner, "iterMessages()")?;
        let idle_timeout = self.idle_timeout;

        self.current = runtime().block_on(async {
            // An idle iteration ends without a message. The read is cancel safe, so a message
            // that arrives later stays in the consumer for the next one.
            let next = match idle_timeout {
                Some(idle_timeout) => match timeout(idle_timeout, consumer.next()).await {
                    Ok(next) => next,
                    Err(_) => return Ok(None),
                },
                None => consumer.next().await,
            };
            match next {
                Some(Ok(message)) => Ok(Some(ReceiveMessage::from(message))),
                Some(Err(err)) => Err(to_php_exception(err)),
                None => Ok(None),
            }
        })?;

        Ok(())
    }
}

#[php_impl]
impl MessageIterator {
    pub fn current(&self) -> Option<ReceiveMessage> {
        self.current.clone()
    }

    pub fn key(&self) -> u64 {
        self.key
    }

    // PHP's \Iterator contract fixes this method name, and PhpResult cannot
    // satisfy Iterator::next, so the standard trait is not implementable here.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> PhpResult {
        if self.current.is_some() {
            self.key += 1;
        }

        self.poll_next()
    }

    /// Reads the first message, and on a started iterator the message after the last one it
    /// yielded, as a stream has no earlier messages to go back to.
    pub fn rewind(&mut self) -> PhpResult {
        if !self.started {
            self.started = true;
            return self.poll_next();
        }

        self.next()
    }

    pub fn valid(&self) -> bool {
        self.current.is_some()
    }
}
