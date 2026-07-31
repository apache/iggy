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

use bytes::Bytes;
use ext_php_rs::{binary::Binary, exception::PhpResult, php_class, php_impl};
use iggy::prelude::{
    IggyMessage as RustIggyMessage, IggyMessageHeader,
    SendMessagesConfirmationResponse as RustSendMessagesConfirmationResponse,
    SendMessagesResponse as RustSendMessagesResponse,
};

use crate::error::to_php_exception;

/// A PHP class representing a message to be sent.
#[php_class]
#[php(name = "Iggy\\SendMessage")]
pub struct SendMessage {
    pub(crate) inner: RustIggyMessage,
}

impl Clone for SendMessage {
    fn clone(&self) -> Self {
        Self {
            inner: RustIggyMessage {
                header: IggyMessageHeader {
                    checksum: self.inner.header.checksum,
                    id: self.inner.header.id,
                    offset: self.inner.header.offset,
                    timestamp: self.inner.header.timestamp,
                    origin_timestamp: self.inner.header.origin_timestamp,
                    user_headers_length: self.inner.header.user_headers_length,
                    payload_length: self.inner.header.payload_length,
                    reserved: self.inner.header.reserved,
                },
                payload: self.inner.payload.clone(),
                user_headers: self.inner.user_headers.clone(),
            },
        }
    }
}

#[php_impl]
impl SendMessage {
    /// Constructs a new `SendMessage` instance from a PHP string.
    ///
    /// PHP strings are byte strings, so this accepts both text and binary payloads.
    #[php(constructor)]
    pub fn __construct(data: Binary<u8>) -> PhpResult<Self> {
        // `Binary` already owns the PHP string bytes; `Bytes::from(Vec<_>)` reuses that buffer.
        let inner = RustIggyMessage::builder()
            .payload(Bytes::from(Vec::<u8>::from(data)))
            .build()
            .map_err(to_php_exception)?;

        Ok(Self { inner })
    }

    #[php(getter)]
    pub fn id(&self) -> String {
        self.inner.header.id.to_string()
    }

    #[php(getter)]
    /// The bytes are copied into a PHP string on each getter call; cache the result in PHP if
    /// the payload will be read repeatedly.
    pub fn payload(&self) -> Binary<u8> {
        Binary::new(self.inner.payload.to_vec())
    }
}

/// A PHP class representing the commit confirmations of a send.
#[php_class]
#[php(name = "Iggy\\SendMessagesResponse")]
pub struct SendMessagesResponse {
    pub(crate) inner: RustSendMessagesResponse,
}

impl From<RustSendMessagesResponse> for SendMessagesResponse {
    fn from(inner: RustSendMessagesResponse) -> Self {
        Self { inner }
    }
}

#[php_impl]
impl SendMessagesResponse {
    /// One confirmation per partition the batch landed in.
    ///
    /// The list is empty when the server reports no offsets. The legacy server never
    /// reports any, and a server that does can still commit a batch it has no offsets
    /// to describe, so check for an empty array instead of indexing.
    ///
    /// The confirmations are rebuilt on each getter call; cache the result in PHP if
    /// they will be read repeatedly.
    #[php(getter)]
    pub fn confirmations(&self) -> Vec<SendMessagesConfirmation> {
        self.inner
            .confirmations
            .iter()
            .cloned()
            .map(SendMessagesConfirmation::from)
            .collect()
    }
}

/// A PHP class representing where one partition's batch was committed.
#[php_class]
#[php(name = "Iggy\\SendMessagesConfirmation")]
pub struct SendMessagesConfirmation {
    pub(crate) inner: RustSendMessagesConfirmationResponse,
}

impl From<RustSendMessagesConfirmationResponse> for SendMessagesConfirmation {
    fn from(inner: RustSendMessagesConfirmationResponse) -> Self {
        Self { inner }
    }
}

#[php_impl]
impl SendMessagesConfirmation {
    #[php(getter)]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter)]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[php(getter)]
    pub fn partition_id(&self) -> u32 {
        self.inner.partition_id
    }

    /// The offset assigned to the first message of the batch in this partition.
    ///
    /// Delivery is at-least-once, so an earlier retry may already have committed the
    /// same batch at a lower offset. The value never implies uniqueness.
    ///
    /// A batch is confirmed once it is committed in memory, not once it is fsynced. A
    /// crash-restart can stamp a later batch with an offset a client has already
    /// recorded.
    ///
    /// The legacy server returns an empty confirmation list, so it reports no offset
    /// at all.
    #[php(getter)]
    pub fn base_offset(&self) -> u64 {
        self.inner.base_offset
    }
}
