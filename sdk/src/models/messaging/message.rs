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

use super::message_header::{IggyMessageHeader, IGGY_MESSAGE_HEADER_SIZE};
use super::user_headers::get_headers_size_bytes;
use crate::bytes_serializable::BytesSerializable;
use crate::error::IggyError;
use crate::models::messaging::user_headers::{HeaderKey, HeaderValue};
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use crate::utils::timestamp::IggyTimestamp;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;
use tracing::warn;

/// A message stored in the Iggy messaging system.
///
/// `IggyMessage` represents a single message that can be sent to or received from
/// a stream. Each message consists of:
/// * A header with message metadata
/// * A payload (the actual content)
/// * Optional user-defined headers for additional context
///
/// # Examples
///
/// ```
/// use iggy::prelude::*;
/// use std::str::FromStr;
/// use std::collections::HashMap;
/// use bytes::Bytes;
///
/// // Create a simple text message
/// let message = IggyMessage::create("Hello world!");
///
/// // Create a message with custom ID
/// let message = IggyMessage::with_id(42, Bytes::from("Custom message"));
///
/// // Create a message with headers
/// let mut headers = HashMap::new();
/// let key = HeaderKey::from_str("content-type").unwrap();
/// let value = HeaderValue::from_str("text/plain").unwrap();
/// headers.insert(key, value);
/// let message = IggyMessage::with_headers(Bytes::from("Message with metadata"), headers);
/// ```
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IggyMessage {
    /// Message metadata
    pub header: IggyMessageHeader,

    /// Message content
    #[serde_as(as = "Base64")]
    pub payload: Bytes,

    /// Optional user-defined headers
    pub user_headers: Option<Bytes>,
}

impl IggyMessage {
    /// Creates a new message with the given payload.
    ///
    /// This is the simplest way to create a message when you don't need
    /// custom IDs or headers.
    ///
    /// # Arguments
    ///
    /// * `payload` - The message content
    ///
    /// # Returns
    ///
    /// A new `IggyMessage` with the provided payload
    ///
    /// # Examples
    ///
    /// ```
    /// use iggy::prelude::*;
    /// let message = IggyMessage::create("Hello world!");
    /// ```
    pub fn create<T: Into<Bytes>>(payload: T) -> Self {
        Self::builder()
            .payload(payload)
            .build()
            .expect("Failed to create message with valid payload")
    }

    /// Creates a new message with a specific ID and payload.
    ///
    /// Use this when you need to control the message ID.
    ///
    /// # Arguments
    ///
    /// * `id` - Custom message ID
    /// * `payload` - The message content
    ///
    /// # Returns
    ///
    /// A new `IggyMessage` with the provided ID and payload
    ///
    /// # Examples
    ///
    /// ```
    /// use iggy::prelude::*;
    /// use bytes::Bytes;
    ///
    /// let message = IggyMessage::with_id(42, Bytes::from("My message"));
    /// ```
    pub fn with_id<T: Into<Bytes>>(id: u128, payload: T) -> Self {
        Self::builder()
            .id(id)
            .payload(payload)
            .build()
            .expect("Failed to create message with valid payload")
    }

    /// Creates a new message with payload and user-defined headers.
    ///
    /// # Arguments
    ///
    /// * `payload` - The message content
    /// * `headers` - Key-value headers to attach to the message
    ///
    /// # Returns
    ///
    /// A new `IggyMessage` with the provided payload and headers
    ///
    /// # Examples
    ///
    /// ```
    /// use iggy::prelude::*;
    /// use bytes::Bytes;
    /// use std::collections::HashMap;
    /// use std::str::FromStr;
    ///
    /// let mut headers = HashMap::new();
    /// let key = HeaderKey::from_str("content-type").unwrap();
    /// let value = HeaderValue::from_str("text/plain").unwrap();
    /// headers.insert(key, value);
    ///
    /// let message = IggyMessage::with_headers(Bytes::from("My message"), headers);
    /// ```
    pub fn with_headers<T: Into<Bytes>>(
        payload: T,
        headers: HashMap<HeaderKey, HeaderValue>,
    ) -> Self {
        Self::builder()
            .payload(payload)
            .headers(headers)
            .build()
            .expect("Failed to create message with valid payload and headers")
    }

    /// Creates a new message with a specific ID, payload, and user-defined headers.
    ///
    /// This is the most flexible way to create a message with full control.
    ///
    /// # Arguments
    ///
    /// * `id` - Custom message ID
    /// * `payload` - The message content
    /// * `headers` - Key-value headers to attach to the message
    ///
    /// # Returns
    ///
    /// A new `IggyMessage` with all provided parameters
    ///
    /// # Examples
    ///
    /// ```
    /// use iggy::prelude::*;
    /// use bytes::Bytes;
    /// use std::collections::HashMap;
    /// use std::str::FromStr;
    ///
    /// let mut headers = HashMap::new();
    /// let key = HeaderKey::from_str("content-type").unwrap();
    /// let value = HeaderValue::from_str("text/plain").unwrap();
    /// headers.insert(key, value);
    /// let message = IggyMessage::with_id_and_headers(42, "My message", headers);
    /// ```
    pub fn with_id_and_headers<T: Into<Bytes>>(
        id: u128,
        payload: T,
        headers: HashMap<HeaderKey, HeaderValue>,
    ) -> Self {
        Self::builder()
            .id(id)
            .payload(payload)
            .headers(headers)
            .build()
            .expect("Failed to create message with valid payload and headers")
    }

    /// Creates a message builder for advanced configuration.
    ///
    /// Use the builder when you need fine-grained control over message creation.
    ///
    /// # Returns
    ///
    /// A new `IggyMessageBuilder` instance
    ///
    /// # Examples
    ///
    /// ```
    /// use iggy::prelude::*;
    /// use bytes::Bytes;
    /// use std::str::FromStr;
    ///
    /// let key = HeaderKey::from_str("content-type").unwrap();
    /// let value = HeaderValue::from_str("text/plain").unwrap();
    ///
    /// let message = IggyMessage::builder()
    ///     .id(123)
    ///     .payload("Hello")
    ///     .header(key, value)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn builder() -> IggyMessageBuilder {
        IggyMessageBuilder::new()
    }

    /// Gets the user headers as a typed HashMap.
    ///
    /// This method parses the binary header data into a typed HashMap for easy access.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(HashMap))` - Successfully parsed headers
    /// * `Ok(None)` - No headers present
    /// * `Err(IggyError)` - Error parsing headers
    pub fn user_headers_map(&self) -> Result<Option<HashMap<HeaderKey, HeaderValue>>, IggyError> {
        match &self.user_headers {
            Some(headers) => {
                let headers_bytes = Bytes::copy_from_slice(headers);
                match HashMap::<HeaderKey, HeaderValue>::from_bytes(headers_bytes) {
                    Ok(h) => Ok(Some(h)),
                    Err(e) => {
                        warn!(
                            "Failed to deserialize user headers: {e}, user_headers_length: {}, skipping field...",
                            self.header.user_headers_length
                        );
                        Ok(None)
                    }
                }
            }
            None => Ok(None),
        }
    }

    /// Retrieves a specific user header value by key.
    ///
    /// This is a convenience method to get a specific header without handling the full map.
    ///
    /// # Arguments
    ///
    /// * `key` - The header key to look up
    ///
    /// # Returns
    ///
    /// * `Ok(Some(HeaderValue))` - Header found with its value
    /// * `Ok(None)` - Header not found or headers couldn't be parsed
    /// * `Err(IggyError)` - Error accessing headers
    pub fn get_header(&self, key: &HeaderKey) -> Result<Option<HeaderValue>, IggyError> {
        Ok(self
            .user_headers_map()?
            .and_then(|map| map.get(key).cloned()))
    }

    /// Checks if this message contains a specific header key.
    ///
    /// # Arguments
    ///
    /// * `key` - The header key to check for
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Header exists
    /// * `Ok(false)` - Header doesn't exist or headers couldn't be parsed
    /// * `Err(IggyError)` - Error accessing headers
    pub fn has_header(&self, key: &HeaderKey) -> Result<bool, IggyError> {
        Ok(self
            .user_headers_map()?
            .is_some_and(|map| map.contains_key(key)))
    }

    /// Gets the payload as a UTF-8 string, if valid.
    ///
    /// # Returns
    ///
    /// * `Ok(String)` - Successfully converted payload to string
    /// * `Err(IggyError)` - Payload is not valid UTF-8
    pub fn payload_as_string(&self) -> Result<String, IggyError> {
        String::from_utf8(self.payload.to_vec()).map_err(|_| IggyError::InvalidUtf8)
    }
}

impl Default for IggyMessage {
    fn default() -> Self {
        Self::create("hello world")
    }
}

impl FromStr for IggyMessage {
    type Err = IggyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::create(Bytes::from(s.to_owned())))
    }
}

impl std::fmt::Display for IggyMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match String::from_utf8(self.payload.to_vec()) {
            Ok(payload) => {
                let preview = if payload.len() > 50 {
                    format!("{}... ({}B)", &payload[..47], self.payload.len())
                } else {
                    payload
                };
                write!(
                    f,
                    "[{}] ID:{} '{}'",
                    self.header.offset, self.header.id, preview
                )
            }
            Err(_) => {
                write!(
                    f,
                    "[{}] ID:{} <binary {}B>",
                    self.header.offset,
                    self.header.id,
                    self.payload.len()
                )
            }
        }
    }
}

impl Sizeable for IggyMessage {
    fn get_size_bytes(&self) -> IggyByteSize {
        let payload_len = self.payload.len() as u64;
        let headers_len = self.user_headers.as_ref().map_or(0, |h| h.len() as u64);
        let message_header_len = IGGY_MESSAGE_HEADER_SIZE as u64;

        IggyByteSize::from(payload_len + headers_len + message_header_len)
    }
}

impl BytesSerializable for IggyMessage {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(self.get_size_bytes().as_bytes_usize());
        let message_header = self.header.to_bytes();
        bytes.put_slice(&message_header);
        bytes.put_slice(&self.payload);
        if let Some(headers) = &self.user_headers {
            bytes.put_slice(headers);
        }
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        if bytes.len() < IGGY_MESSAGE_HEADER_SIZE {
            return Err(IggyError::InvalidCommand);
        }

        let mut position = 0;
        let header = IggyMessageHeader::from_bytes(bytes.slice(0..IGGY_MESSAGE_HEADER_SIZE))?;

        position += IGGY_MESSAGE_HEADER_SIZE;
        let payload_end = position + header.payload_length as usize;

        if payload_end > bytes.len() {
            return Err(IggyError::InvalidMessagePayloadLength);
        }

        let payload = bytes.slice(position..payload_end);
        position = payload_end;

        let user_headers = if header.user_headers_length > 0 {
            let headers_end = position + header.user_headers_length as usize;
            if headers_end > bytes.len() {
                return Err(IggyError::InvalidHeaderValue);
            }
            Some(bytes.slice(position..headers_end))
        } else {
            None
        };

        Ok(IggyMessage {
            header,
            payload,
            user_headers,
        })
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.header.to_bytes());
        buf.put_slice(&self.payload);
        if let Some(headers) = &self.user_headers {
            buf.put_slice(headers);
        }
    }
}

/// Builder for creating `IggyMessage` instances with flexible configuration.
///
/// The builder pattern allows for clear, step-by-step construction of complex
/// message configurations, with better error handling than chained constructors.
#[derive(Debug, Default)]
pub struct IggyMessageBuilder {
    id: Option<u128>,
    payload: Option<Bytes>,
    headers: Option<HashMap<HeaderKey, HeaderValue>>,
}

impl IggyMessageBuilder {
    /// Creates a new empty message builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the message ID.
    ///
    /// If not specified, a default ID (0) will be used.
    pub fn id(mut self, id: u128) -> Self {
        self.id = Some(id);
        self
    }

    /// Sets the message payload.
    ///
    /// This method accepts any type that can be converted into `Bytes`,
    /// including strings, byte slices, and vectors.
    pub fn payload<T: Into<Bytes>>(mut self, payload: T) -> Self {
        self.payload = Some(payload.into());
        self
    }

    /// Adds a single header key-value pair to the message.
    ///
    /// Multiple calls will add multiple headers.
    pub fn header(mut self, key: HeaderKey, value: HeaderValue) -> Self {
        let headers = self.headers.get_or_insert_with(HashMap::new);
        headers.insert(key, value);
        self
    }

    /// Adds a string header with the given key and value.
    ///
    /// This is a convenience method for adding string headers without
    /// manually creating HeaderValue objects.
    pub fn string_header(mut self, key: &str, value: &str) -> Result<Self, IggyError> {
        let key = HeaderKey::new(key)?;
        let value = HeaderValue::from_str(value)?;
        let headers = self.headers.get_or_insert_with(HashMap::new);
        headers.insert(key, value);
        Ok(self)
    }

    /// Sets all headers at once from a HashMap.
    ///
    /// This replaces any headers previously added.
    pub fn headers(mut self, headers: HashMap<HeaderKey, HeaderValue>) -> Self {
        self.headers = Some(headers);
        self
    }

    /// Builds the final IggyMessage from the configured parameters.
    ///
    /// # Returns
    ///
    /// * `Ok(IggyMessage)` - Successfully built message
    /// * `Err(IggyError)` - Error during message construction
    pub fn build(self) -> Result<IggyMessage, IggyError> {
        let payload = self.payload.unwrap_or_default();
        let id = self.id.unwrap_or(0);
        let headers_length = get_headers_size_bytes(&self.headers).as_bytes_u64() as u32;

        let header = IggyMessageHeader {
            checksum: 0, // Checksum is calculated on server side
            id,
            offset: 0,
            timestamp: 0,
            origin_timestamp: IggyTimestamp::now().as_micros(),
            user_headers_length: headers_length,
            payload_length: payload.len() as u32,
        };

        let user_headers = self.headers.map(|headers| headers.to_bytes());

        Ok(IggyMessage {
            header,
            payload,
            user_headers,
        })
    }
}

// Clean implementations of conversion traits

impl From<IggyMessage> for Bytes {
    fn from(message: IggyMessage) -> Self {
        message.to_bytes()
    }
}

impl From<String> for IggyMessage {
    fn from(s: String) -> Self {
        Self::create(s)
    }
}

impl From<&str> for IggyMessage {
    fn from(s: &str) -> Self {
        Self::create(Bytes::from(s.to_owned()))
    }
}

impl From<Vec<u8>> for IggyMessage {
    fn from(v: Vec<u8>) -> Self {
        Self::create(v)
    }
}

impl From<&[u8]> for IggyMessage {
    fn from(bytes: &[u8]) -> Self {
        Self::create(Bytes::copy_from_slice(bytes))
    }
}

impl TryFrom<Bytes> for IggyMessage {
    type Error = IggyError;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        Self::from_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_simple_message() {
        let message = IggyMessage::create("test message");
        assert_eq!(message.payload, Bytes::from("test message"));
        assert_eq!(message.header.id, 0);
        assert!(message.user_headers.is_none());
    }

    #[test]
    fn test_create_with_id() {
        let message = IggyMessage::with_id(42, "test with id");
        assert_eq!(message.payload, Bytes::from("test with id"));
        assert_eq!(message.header.id, 42);
    }

    #[test]
    fn test_create_with_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("content-type").unwrap(),
            HeaderValue::from_str("text/plain").unwrap(),
        );

        let message = IggyMessage::with_headers("test with headers", headers);
        assert_eq!(message.payload, Bytes::from("test with headers"));
        assert!(message.user_headers.is_some());

        let headers_map = message.user_headers_map().unwrap().unwrap();
        assert_eq!(headers_map.len(), 1);
        assert!(headers_map.contains_key(&HeaderKey::new("content-type").unwrap()));
    }

    #[test]
    fn test_empty_payload() {
        let message = IggyMessage::create(Bytes::new());
        assert_eq!(message.payload.len(), 0);
        assert_eq!(message.header.payload_length, 0);
    }

    #[test]
    fn test_from_string() {
        let message: IggyMessage = "simple message".into();
        assert_eq!(message.payload, Bytes::from("simple message"));
    }

    #[test]
    fn test_payload_as_string() {
        let message = IggyMessage::create("test message");
        assert_eq!(message.payload_as_string().unwrap(), "test message");
    }

    #[test]
    fn test_serialization_roundtrip() {
        let original = IggyMessage::with_id(123, "serialization test");
        let bytes = original.to_bytes();
        let decoded = IggyMessage::from_bytes(bytes).unwrap();

        assert_eq!(original.header.id, decoded.header.id);
        assert_eq!(original.payload, decoded.payload);
    }
}
