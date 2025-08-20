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
use iggy_common::{Identifier, IggyError, IggyMessage, Partitioning};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use tracing::error;

#[derive(Debug)]
pub struct ErrorCtx {
    pub cause: Box<IggyError>,
    pub stream: Arc<Identifier>,
    pub stream_name: String,
    pub topic: Arc<Identifier>,
    pub topic_name: String,
    pub partitioning: Option<Arc<Partitioning>>,
    pub messages: Arc<Vec<IggyMessage>>,
}

/// A trait for handling background sending errors.
///
/// This is used when a message batch fails to send in an asynchronous background task.
/// Implementors can define custom logic such as logging, retrying, alerting, etc.
pub trait ErrorCallback: Send + Sync + Debug + 'static {
    fn call(&self, ctx: ErrorCtx) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
}

/// Default implementation of [`ErrorCallback`] that logs the error using `tracing::error!`.
///
/// Logs include stream, topic, optional partitioning, number of messages, and the cause.
#[derive(Debug, Default)]
pub struct LogErrorCallback;

impl ErrorCallback for LogErrorCallback {
    fn call(&self, ctx: ErrorCtx) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(async move {
            let partitioning = ctx
                .partitioning
                .as_ref()
                .map(|p| format!("{p:?}"))
                .unwrap_or_else(|| "None".to_string());

            error!(
                cause = %ctx.cause,
                stream = %ctx.stream,
                stream_name = ctx.stream_name,
                topic = %ctx.topic,
                topic_name = ctx.topic_name,
                partitioning = %partitioning,
                num_messages = ctx.messages.len(),
                "Failed to send messages in background task",
            );
        })
    }
}
