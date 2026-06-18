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

use ext_php_rs::{exception::PhpResult, php_class, php_impl, zend::ce};
use futures::StreamExt;
use iggy::prelude::IggyConsumer as RustIggyConsumer;
use tokio::sync::Mutex;

use crate::error::to_php_exception;
use crate::receive_message::ReceiveMessage;
use crate::runtime::runtime;

#[php_class]
#[php(name = "Iggy\\MessageIterator")]
#[php(implements(ce = ce::iterator, stub = "\\Iterator"))]
pub struct MessageIterator {
    pub(crate) inner: Arc<Mutex<RustIggyConsumer>>,
    current: Option<ReceiveMessage>,
    key: u64,
    started: bool,
}

impl MessageIterator {
    pub(crate) fn new(inner: Arc<Mutex<RustIggyConsumer>>) -> Self {
        Self {
            inner,
            current: None,
            key: 0,
            started: false,
        }
    }

    fn poll_next(&mut self) -> PhpResult {
        let inner = self.inner.clone();

        self.current = runtime().block_on(async move {
            let mut inner = inner.lock().await;

            match inner.next().await {
                Some(Ok(message)) => Ok(Some(ReceiveMessage {
                    inner: message.message,
                    partition_id: message.partition_id,
                })),
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

    pub fn next(&mut self) -> PhpResult {
        if self.current.is_some() {
            self.key += 1;
        }

        self.poll_next()
    }

    pub fn rewind(&mut self) -> PhpResult {
        if !self.started {
            self.started = true;
            self.poll_next()?;
        }

        Ok(())
    }

    pub fn valid(&self) -> bool {
        self.current.is_some()
    }
}
