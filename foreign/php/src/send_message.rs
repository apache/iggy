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

use bytes::Bytes;
use ext_php_rs::{
    binary::Binary,
    exception::{PhpException, PhpResult},
    php_class, php_impl,
};
use iggy::prelude::{IggyMessage as RustIggyMessage, IggyMessageHeader};

/// A PHP class representing a message to be sent.
#[php_class]
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
        Self::from_payload(Vec::from(data))
    }

    #[php(getter)]
    pub fn id(&self) -> String {
        self.inner.header.id.to_string()
    }

    #[php(getter)]
    pub fn payload(&self) -> Binary<u8> {
        Binary::new(self.inner.payload.to_vec())
    }
}

impl SendMessage {
    fn from_payload(payload: Vec<u8>) -> PhpResult<Self> {
        let inner = RustIggyMessage::builder()
            .payload(Bytes::from(payload))
            .build()
            .map_err(|err| PhpException::default(err.to_string()))?;

        Ok(Self { inner })
    }
}
