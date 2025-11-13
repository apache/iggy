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

use super::message_header::{IGGY_MESSAGE_HEADER_SIZE, IggyMessageHeader};
use super::user_headers::get_user_headers_size;
use crate::BytesSerializable;
use crate::Sizeable;
use crate::error::IggyError;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::timestamp::IggyTimestamp;
use crate::{HeaderKey, HeaderValue};
use bon::bon;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;
use tracing::warn;

/// Maximum allowed size in bytes for a message payload.
///
/// This constant defines the upper limit for the size of an `IggyMessage` payload.
/// Attempting to create a message with a payload larger than this value will result
/// in an `IggyError::InvalidMessagePayloadLength` error.
///
/// # Constraints
///
/// * Minimum payload size: 1 byte (empty payloads are not allowed)
/// * Maximum payload size: 64 MB
///
pub const MAX_PAYLOAD_SIZE: u32 = 64 * 1000 * 1000;

/// Maximum allowed size in bytes for user-defined headers.
///
/// This constant defines the upper limit for the combined size of all user headers
/// in an `IggyMessage`. Attempting to create a message with user headers larger
/// than this value will result in an `IggyError::TooBigUserHeaders` error.
///
/// # Constraints
///
/// * Maximum headers size: 100 KB
/// * Each individual header key is limited to 255 bytes
/// * Each individual header value is limited to 255 bytes
///
pub const MAX_USER_HEADERS_SIZE: u32 = 100 * 1000;

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
/// use iggy_common::*;
/// use std::str::FromStr;
/// use std::collections::HashMap;
/// use bytes::Bytes;
///
/// // Create a simple text message
/// let message = IggyMessage::builder()
///     .payload("Hello world!".into())
///     .build()
///     .unwrap();
///
/// // Create a simple message with raw binary payload
/// let message = IggyMessage::builder()
///     .payload(Bytes::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
///     .build()
///     .unwrap();
///
/// // Create a message with custom ID
/// let message = IggyMessage::builder()
///     .id(42)
///     .payload("Custom message".into())
///     .build()
///     .unwrap();
///
/// // Create a message with headers
/// let key = HeaderKey::from_str("content-type").unwrap();
/// let value = HeaderValue::from_str("text/plain").unwrap();
/// let user_headers = HashMap::from([(key, value)]);
///
/// let message = IggyMessage::builder()
///     .payload("Message with metadata".into())
///     .user_headers(user_headers)
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, PartialEq, Default)]
pub struct IggyMessage {
    /// Message metadata
    pub header: IggyMessageHeader,

    /// Message content
    pub payload: Bytes,

    /// Optional user-defined headers
    pub user_headers: Option<Bytes>,
}

#[bon]
impl IggyMessage {
    /// Creates a new message with customizable parameters.
    ///
    /// # Arguments
    ///
    /// * `id` - Optional message ID (defaults to 0 if None)
    /// * `payload` - The message content
    /// * `user_headers` - Optional user-defined headers
    ///
    /// # Returns
    ///
    /// A Result containing the new message or an error
    ///
    /// # Examples
    ///
    /// ```
    /// use iggy_common::*;
    /// use bytes::Bytes;
    /// use std::collections::HashMap;
    /// use std::str::FromStr;
    ///
    /// // Simple message with just payload
    /// let msg = IggyMessage::builder()
    ///     .payload("Hello world".into())
    ///     .build()
    ///     .unwrap();
    ///
    /// // Message with custom ID
    /// let msg = IggyMessage::builder()
    ///     .id(42)
    ///     .payload("Hello".into())
    ///     .build()
    ///     .unwrap();
    ///
    /// // Message with headers
    /// let key = HeaderKey::from_str("content-type").unwrap();
    /// let value = HeaderValue::from_str("text/plain").unwrap();
    /// let user_headers = HashMap::from([(key, value)]);
    /// let msg = IggyMessage::builder()
    ///     .payload("Hello".into())
    ///     .user_headers(user_headers)
    ///     .build()
    ///     .unwrap();
    /// ```
    #[builder]
    pub fn new(
        id: Option<u128>,
        payload: Bytes,
        user_headers: Option<HashMap<HeaderKey, HeaderValue>>,
    ) -> Result<Self, IggyError> {
        if payload.is_empty() {
            return Err(IggyError::InvalidMessagePayloadLength);
        }

        if payload.len() > MAX_PAYLOAD_SIZE as usize {
            return Err(IggyError::TooBigMessagePayload);
        }

        let user_headers_length = get_user_headers_size(&user_headers).unwrap_or(0);

        if user_headers_length > MAX_USER_HEADERS_SIZE {
            return Err(IggyError::TooBigUserHeaders);
        }

        let header = IggyMessageHeader {
            checksum: 0,
            id: id.unwrap_or(0),
            offset: 0,
            timestamp: 0,
            origin_timestamp: IggyTimestamp::now().as_micros(),
            user_headers_length,
            payload_length: payload.len() as u32,
        };

        let user_headers = user_headers.map(|h| h.to_bytes());

        Ok(Self {
            header,
            payload,
            user_headers,
        })
    }
}

impl IggyMessage {
    /// Gets the user headers as a typed HashMap.
    ///
    /// This method parses the binary header data into a typed HashMap for easy access.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(HashMap))` - Successfully parsed headers
    /// * `Ok(None)` - No headers present
    /// * `Err(IggyError)` - Error parsing headers
    ///
    /// # Examples
    ///
    /// ```
    /// use iggy_common::*;
    /// use std::str::FromStr;
    /// use std::collections::HashMap;
    ///
    /// let key = HeaderKey::from_str("content-type").unwrap();
    /// let value = HeaderValue::from_str("text/plain").unwrap();
    /// let user_headers_map = HashMap::from([(key.clone(), value)]);
    ///
    /// let message = IggyMessage::builder()
    ///     .payload("Hello".into())
    ///     .user_headers(user_headers_map)
    ///     .build()
    ///     .unwrap();
    ///
    /// let headers = message.user_headers_map().unwrap().unwrap();
    /// assert!(headers.contains_key(&key));
    /// ```
    pub fn user_headers_map(&self) -> Result<Option<HashMap<HeaderKey, HeaderValue>>, IggyError> {
        if let Some(user_headers) = &self.user_headers {
            match HashMap::<HeaderKey, HeaderValue>::from_bytes(user_headers.clone()) {
                Ok(h) => Ok(Some(h)),
                Err(e) => {
                    warn!(
                        "Failed to deserialize user headers: {e} for message at offset {}, sent at: {} ({}), user_headers_length: {}, skipping field...",
                        self.header.offset,
                        IggyTimestamp::from(self.header.origin_timestamp).to_rfc3339_string(),
                        self.header.origin_timestamp,
                        self.header.user_headers_length
                    );
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    /// Retrieves a specific user header value by key.
    ///
    /// This is a convenience method to get a specific user header without handling the full map.
    ///
    /// # Arguments
    ///
    /// * `key` - The user header key to look up
    ///
    /// # Returns
    ///
    /// * `Ok(Some(HeaderValue))` - User header found with its value
    /// * `Ok(None)` - User header not found or user headers couldn't be parsed
    /// * `Err(IggyError)` - Error accessing user headers
    ///
    /// # Examples
    ///
    /// ```
    /// use iggy_common::*;
    /// use std::str::FromStr;
    /// use std::collections::HashMap;
    ///
    /// let key = HeaderKey::from_str("content-type").unwrap();
    /// let value = HeaderValue::from_str("text/plain").unwrap();
    /// let user_headers_map = HashMap::from([(key.clone(), value.clone())]);
    ///
    /// let message = IggyMessage::builder()
    ///     .payload("Hello".into())
    ///     .user_headers(user_headers_map)
    ///     .build()
    ///     .unwrap();
    ///
    /// let header_value = message.get_user_header(&key).unwrap().unwrap();
    /// assert_eq!(header_value, value);
    /// ```
    pub fn get_user_header(&self, key: &HeaderKey) -> Result<Option<HeaderValue>, IggyError> {
        Ok(self
            .user_headers_map()?
            .and_then(|map| map.get(key).cloned()))
    }

    /// Checks if this message contains a specific user header key.
    ///
    /// # Arguments
    ///
    /// * `key` - The user header key to check for
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - User header exists
    /// * `Ok(false)` - User header doesn't exist or user headers couldn't be parsed
    /// * `Err(IggyError)` - Error accessing user headers
    ///
    /// # Examples
    ///
    /// ```
    /// use iggy_common::*;
    /// use std::str::FromStr;
    /// use std::collections::HashMap;
    ///
    /// let key = HeaderKey::from_str("content-type").unwrap();
    /// let value = HeaderValue::from_str("text/plain").unwrap();
    /// let user_headers_map = HashMap::from([(key.clone(), value)]);
    ///
    /// let message = IggyMessage::builder()
    ///     .payload("Hello".into())
    ///     .user_headers(user_headers_map)
    ///     .build()
    ///     .unwrap();
    ///
    /// assert!(message.has_user_header(&key).unwrap());
    /// assert!(!message.has_user_header(&HeaderKey::from_str("non-existent").unwrap()).unwrap());
    /// ```
    pub fn has_user_header(&self, key: &HeaderKey) -> Result<bool, IggyError> {
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
    ///
    /// # Examples
    ///
    /// ```
    /// use iggy_common::*;
    ///
    /// let message = IggyMessage::builder()
    ///     .payload("Hello world".into())
    ///     .build()
    ///     .unwrap();
    ///
    /// assert_eq!(message.payload_as_string().unwrap(), "Hello world");
    /// ```
    pub fn payload_as_string(&self) -> Result<String, IggyError> {
        String::from_utf8(self.payload.to_vec()).map_err(|_| IggyError::InvalidUtf8)
    }
}

impl FromStr for IggyMessage {
    type Err = IggyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::builder().payload(Bytes::from(s.to_owned())).build()
    }
}

impl std::fmt::Display for IggyMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match String::from_utf8(self.payload.to_vec()) {
            Ok(payload) => {
                write!(
                    f,
                    "[{offset}] ID:{id} '{preview}'",
                    offset = self.header.offset,
                    id = self.header.id,
                    preview = if payload.len() > 50 {
                        format!("{}... ({}B)", &payload[..47], self.payload.len())
                    } else {
                        payload
                    }
                )
            }
            Err(_) => {
                write!(
                    f,
                    "[{offset}] ID:{id} <binary {payload_len}B>",
                    offset = self.header.offset,
                    id = self.header.id,
                    payload_len = self.payload.len()
                )
            }
        }
    }
}

impl Sizeable for IggyMessage {
    fn get_size_bytes(&self) -> IggyByteSize {
        let message_header_len = IGGY_MESSAGE_HEADER_SIZE;
        let payload_len = self.payload.len();
        let user_headers_len = self.user_headers.as_ref().map(|h| h.len()).unwrap_or(0);

        #[cfg(debug_assertions)]
        {
            assert_eq!(
                user_headers_len, self.header.user_headers_length as usize,
                "user_headers.len() != header.user_headers_length"
            );

            assert_eq!(
                payload_len, self.header.payload_length as usize,
                "payload.len() != header.payload_length"
            )
        }

        IggyByteSize::from((message_header_len + payload_len + user_headers_len) as u64)
    }
}

impl BytesSerializable for IggyMessage {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(self.get_size_bytes().as_bytes_usize());
        let message_header = self.header.to_bytes();
        bytes.put_slice(&message_header);
        bytes.put_slice(&self.payload);
        if let Some(user_headers) = &self.user_headers {
            bytes.put_slice(user_headers);
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

        #[cfg(debug_assertions)]
        {
            assert_eq!(
                user_headers.as_ref().map(|h| h.len()).unwrap_or(0),
                header.user_headers_length as usize,
                "user_headers.len() != header.user_headers_length"
            );

            assert_eq!(
                payload.len(),
                header.payload_length as usize,
                "payload.len() != header.payload_length"
            )
        }

        Ok(IggyMessage {
            header,
            payload,
            user_headers,
        })
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.header.to_bytes());
        buf.put_slice(&self.payload);
        if let Some(user_headers) = &self.user_headers {
            buf.put_slice(user_headers);
        }
    }
}

impl From<IggyMessage> for Bytes {
    fn from(message: IggyMessage) -> Self {
        message.to_bytes()
    }
}

impl From<String> for IggyMessage {
    fn from(s: String) -> Self {
        Self::builder()
            .payload(Bytes::from(s))
            .build()
            .expect("String conversion should not fail")
    }
}

impl From<&str> for IggyMessage {
    fn from(s: &str) -> Self {
        Self::builder()
            .payload(Bytes::from(s.to_owned()))
            .build()
            .expect("String conversion should not fail")
    }
}

impl From<Vec<u8>> for IggyMessage {
    fn from(v: Vec<u8>) -> Self {
        Self::builder()
            .payload(Bytes::from(v))
            .build()
            .expect("Vec<u8> conversion should not fail")
    }
}

impl From<&[u8]> for IggyMessage {
    fn from(bytes: &[u8]) -> Self {
        Self::builder()
            .payload(Bytes::copy_from_slice(bytes))
            .build()
            .expect("&[u8] conversion should not fail")
    }
}

impl TryFrom<Bytes> for IggyMessage {
    type Error = IggyError;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        Self::from_bytes(bytes)
    }
}

impl Serialize for IggyMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        use serde::ser::SerializeStruct;

        let field_count = 2 + self.user_headers.is_some() as usize;

        let mut state = serializer.serialize_struct("IggyMessage", field_count)?;
        state.serialize_field("header", &self.header)?;

        let base64_payload = STANDARD.encode(&self.payload);
        state.serialize_field("payload", &base64_payload)?;

        if self.user_headers.is_some() {
            let headers_map = self.user_headers_map().map_err(serde::ser::Error::custom)?;

            state.serialize_field("user_headers", &headers_map)?;
        }

        state.end()
    }
}

impl<'de> Deserialize<'de> for IggyMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct IggyMessageVisitor;

        impl<'de> Visitor<'de> for IggyMessageVisitor {
            type Value = IggyMessage;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct IggyMessage")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut header: Option<IggyMessageHeader> = None;
                let mut payload: Option<Bytes> = None;
                let mut user_headers: Option<HashMap<HeaderKey, HeaderValue>> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "header" => {
                            header = Some(map.next_value()?);
                        }
                        "payload" => {
                            let payload_str: String = map.next_value()?;
                            use base64::{Engine as _, engine::general_purpose::STANDARD};
                            let decoded = STANDARD.decode(payload_str.as_bytes()).map_err(|e| {
                                de::Error::custom(format!("Failed to decode base64: {e}"))
                            })?;
                            payload = Some(Bytes::from(decoded));
                        }
                        "user_headers" => {
                            user_headers = Some(map.next_value()?);
                        }
                        _ => {
                            let _ = map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }

                let header = header.ok_or_else(|| de::Error::missing_field("header"))?;
                let payload = payload.ok_or_else(|| de::Error::missing_field("payload"))?;

                let user_headers_bytes = user_headers.map(|headers| headers.to_bytes());

                let user_headers_length = user_headers_bytes
                    .as_ref()
                    .map(|h| h.len() as u32)
                    .unwrap_or(0);

                let mut header = header;
                header.user_headers_length = user_headers_length;

                Ok(IggyMessage {
                    header,
                    payload,
                    user_headers: user_headers_bytes,
                })
            }
        }

        deserializer.deserialize_struct(
            "IggyMessage",
            &["header", "payload", "user_headers"],
            IggyMessageVisitor,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_simple_message() {
        let message = IggyMessage::builder()
            .payload(Bytes::from("test message"))
            .build()
            .expect("String conversion should not fail");
        assert_eq!(message.payload, Bytes::from("test message"));
        assert_eq!(message.header.id, 0);
        assert!(message.user_headers.is_none());
    }

    #[test]
    fn test_create_with_id() {
        let message = IggyMessage::builder()
            .id(42)
            .payload(Bytes::from("test with id"))
            .build()
            .expect("String conversion should not fail");
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

        let message = IggyMessage::builder()
            .payload(Bytes::from("test with headers"))
            .user_headers(headers)
            .build()
            .expect("String conversion should not fail");
        assert_eq!(message.payload, Bytes::from("test with headers"));
        assert!(message.user_headers.is_some());

        let headers_map = message.user_headers_map().unwrap().unwrap();
        assert_eq!(headers_map.len(), 1);
        assert!(headers_map.contains_key(&HeaderKey::new("content-type").unwrap()));
    }

    #[test]
    fn test_empty_payload() {
        let message = IggyMessage::builder().payload(Bytes::new()).build();
        assert_eq!(message, Err(IggyError::InvalidMessagePayloadLength));
    }

    #[test]
    fn test_from_string() {
        let message: IggyMessage = "simple message".into();
        assert_eq!(message.payload, Bytes::from("simple message"));
    }

    #[test]
    fn test_payload_as_string() {
        let message = IggyMessage::builder()
            .payload(Bytes::from("test message"))
            .build()
            .expect("String conversion should not fail");
        assert_eq!(message.payload_as_string().unwrap(), "test message");
    }

    #[test]
    fn test_serialization_roundtrip() {
        let original = IggyMessage::builder()
            .id(123)
            .payload(Bytes::from("serialization test"))
            .build()
            .expect("String conversion should not fail");
        let bytes = original.to_bytes();
        let decoded = IggyMessage::from_bytes(bytes).unwrap();

        assert_eq!(original.header.id, decoded.header.id);
        assert_eq!(original.payload, decoded.payload);
    }

    #[test]
    fn test_json_serialization_with_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("content-type").unwrap(),
            HeaderValue::from_str("text/plain").unwrap(),
        );
        headers.insert(
            HeaderKey::new("correlation-id").unwrap(),
            HeaderValue::from_str("123456").unwrap(),
        );

        let original = IggyMessage::builder()
            .id(42)
            .payload(Bytes::from("JSON serialization test"))
            .user_headers(headers)
            .build()
            .expect("Message creation should not fail");

        let json = serde_json::to_string(&original).expect("JSON serialization should not fail");

        let deserialized: IggyMessage =
            serde_json::from_str(&json).expect("JSON deserialization should not fail");

        assert_eq!(original.header.id, deserialized.header.id);
        assert_eq!(original.payload, deserialized.payload);
        assert!(deserialized.user_headers.is_some());

        let original_headers_result = original.user_headers_map();
        let deserialized_headers_result = deserialized.user_headers_map();

        let original_map = match original_headers_result {
            Ok(Some(map)) => map,
            Ok(None) => {
                panic!("Original user_headers_map() returned None");
            }
            Err(e) => {
                panic!("Error getting original user_headers_map: {e:?}");
            }
        };

        let deserialized_map = match deserialized_headers_result {
            Ok(Some(map)) => map,
            Ok(None) => {
                panic!("Deserialized user_headers_map() returned None");
            }
            Err(e) => {
                panic!("Error getting deserialized user_headers_map: {e:?}");
            }
        };

        assert_eq!(original_map.len(), deserialized_map.len());
        assert_eq!(
            original_map.get(&HeaderKey::new("content-type").unwrap()),
            deserialized_map.get(&HeaderKey::new("content-type").unwrap())
        );
        assert_eq!(
            original_map.get(&HeaderKey::new("correlation-id").unwrap()),
            deserialized_map.get(&HeaderKey::new("correlation-id").unwrap())
        );
    }
}
