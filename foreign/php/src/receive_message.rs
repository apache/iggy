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

use ext_php_rs::{binary::Binary, php_class, php_impl};
use iggy::prelude::{
    IggyMessage as RustReceiveMessage, IggyMessageHeader, PollingStrategy as RustPollingStrategy,
};

/// A Python class representing a received message.
///
/// This class wraps a Rust message, allowing for access to its payload and offset from Python.
#[php_class]
pub struct ReceiveMessage {
    pub(crate) inner: RustReceiveMessage,
    pub(crate) partition_id: u32,
}

impl Clone for ReceiveMessage {
    fn clone(&self) -> Self {
        Self {
            inner: RustReceiveMessage {
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
            partition_id: self.partition_id,
        }
    }
}

#[php_impl]
impl ReceiveMessage {
    /// Retrieves the payload of the received message.
    ///
    /// The payload is returned as a PHP string, which can represent both text and binary data.
    pub fn payload(&self) -> Binary<u8> {
        Binary::new(self.inner.payload.to_vec())
    }

    /// Retrieves the offset of the received message.
    ///
    /// The offset represents the position of the message within its topic.
    pub fn offset(&self) -> u64 {
        self.inner.header.offset
    }

    /// Retrieves the timestamp of the received message.
    ///
    /// The timestamp represents the time of the message within its topic.
    pub fn timestamp(&self) -> u64 {
        self.inner.header.timestamp
    }

    /// Retrieves the id of the received message.
    ///
    /// The id represents unique identifier of the message within its topic.
    pub fn id(&self) -> String {
        self.inner.header.id.to_string()
    }

    /// Retrieves the checksum of the received message.
    ///
    /// The checksum represents the integrity of the message within its topic.
    pub fn checksum(&self) -> String {
        self.inner.header.checksum.to_string()
    }

    /// Retrieves the length of the received message.
    ///
    /// The length represents the length of the payload.
    pub fn length(&self) -> u32 {
        self.inner.header.payload_length
    }

    /// Retrieves the partition this message belongs to.
    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }
}

#[php_class]
#[derive(Clone)]
pub struct PollingStrategy {
    pub(crate) inner: RustPollingStrategy,
}

impl From<&PollingStrategy> for RustPollingStrategy {
    fn from(value: &PollingStrategy) -> Self {
        value.inner
    }
}

#[php_impl]
impl PollingStrategy {
    pub fn offset(value: u64) -> Self {
        Self {
            inner: RustPollingStrategy::offset(value),
        }
    }

    pub fn timestamp(value: u64) -> Self {
        Self {
            inner: RustPollingStrategy::timestamp(value.into()),
        }
    }

    pub fn first() -> Self {
        Self {
            inner: RustPollingStrategy::first(),
        }
    }

    pub fn last() -> Self {
        Self {
            inner: RustPollingStrategy::last(),
        }
    }

    pub fn next() -> Self {
        Self {
            inner: RustPollingStrategy::next(),
        }
    }
}
