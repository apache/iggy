/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::{sync::Arc, sync::OnceLock};

use ext_php_rs::{
    exception::{PhpException, PhpResult},
    php_class, php_impl,
};
use futures::StreamExt;
use iggy::prelude::IggyConsumer as RustIggyConsumer;
use tokio::{runtime::Runtime, sync::Mutex};

use crate::receive_message::ReceiveMessage;

#[php_class]
pub struct ReceiveMessageIterator {
    pub(crate) inner: Arc<Mutex<RustIggyConsumer>>,
}

#[php_impl]
impl ReceiveMessageIterator {
    pub fn next(&self) -> PhpResult<Option<ReceiveMessage>> {
        let inner = self.inner.clone();

        runtime().block_on(async move {
            let mut inner = inner.lock().await;

            match inner.next().await {
                Some(Ok(message)) => Ok(Some(ReceiveMessage {
                    inner: message.message,
                    partition_id: message.partition_id,
                })),
                Some(Err(err)) => Err(PhpException::default(err.to_string())),
                None => Ok(None),
            }
        })
    }
}

pub fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();

    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to initialize Tokio runtime")
    })
}
