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

use ext_php_rs::{boxed::ZBox, exception::PhpResult, php_class, php_impl, types::ZendHashTable};
use iggy::prelude::{
    HeaderKey, OptionKey, ResourceOptions, Stream as RustStream, StreamDetails as RustStreamDetails,
};

use crate::options::options_array;

#[php_class]
#[php(name = "Iggy\\StreamDetails")]
pub struct StreamDetails {
    pub(crate) inner: RustStreamDetails,
}

impl From<RustStreamDetails> for StreamDetails {
    fn from(stream_details: RustStreamDetails) -> Self {
        Self {
            inner: stream_details,
        }
    }
}

#[php_impl]
impl StreamDetails {
    #[php(getter)]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topics_count(&self) -> u32 {
        self.inner.topics_count
    }

    /// Options the creating client set, as option name to scalar value. Key material is
    /// never returned.
    #[php(getter)]
    pub fn options(&self) -> PhpResult<ZBox<ZendHashTable>> {
        options_array(&self.inner.options, true)
    }

    /// Options the server filled in, including the read-only encryption projections.
    #[php(getter, change_case = "snake_case")]
    pub fn derived_options(&self) -> PhpResult<ZBox<ZendHashTable>> {
        options_array(&self.inner.options, false)
    }

    /// Who chose the stream's at-rest key: `stream`, `server`, or `none` for a plaintext
    /// stream. A source a newer server reports is returned as its text.
    #[php(getter, change_case = "snake_case")]
    pub fn encryption_key_source(&self) -> Option<String> {
        key_source_text(&self.inner.options)
    }

    /// Fingerprint of the stream's key, 16 lowercase hex characters. Compare it with
    /// `Client::encryptionKeyFingerprint()` of a key you hold. Null for a plaintext stream.
    #[php(getter, change_case = "snake_case")]
    pub fn encryption_key_fingerprint(&self) -> Option<String> {
        self.inner.encryption_key_fingerprint()
    }

    /// Fingerprints of the server keys that wrap the stream's key, comma-separated: one
    /// outside a server key rotation. Null for a plaintext stream.
    #[php(getter, change_case = "snake_case")]
    pub fn encryption_kek_fingerprint(&self) -> Option<String> {
        comma_separated(&self.inner.encryption_kek_fingerprints())
    }

    /// The geo origin whose topics this shadow stream serves, such as `dc-a`. Null for any
    /// stream that is not a shadow stream.
    #[php(getter, change_case = "snake_case")]
    pub fn shadow_origin(&self) -> Option<String> {
        self.inner.shadow_origin()
    }
}

/// One stream of a `StreamsPage`.
#[php_class]
#[php(name = "Iggy\\Stream")]
#[derive(Clone)]
pub struct Stream {
    // The Rust type is not Clone, and each read of the page's items copies the stream.
    inner: Arc<RustStream>,
}

impl From<RustStream> for Stream {
    fn from(stream: RustStream) -> Self {
        Self {
            inner: Arc::new(stream),
        }
    }
}

#[php_impl]
impl Stream {
    #[php(getter)]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// Creation time in microseconds.
    #[php(getter, change_case = "snake_case")]
    pub fn created_at(&self) -> u64 {
        self.inner.created_at.as_micros()
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn size_bytes(&self) -> u64 {
        self.inner.size.as_bytes_u64()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topics_count(&self) -> u32 {
        self.inner.topics_count
    }

    /// Options the creating client set, as option name to scalar value. Key material is
    /// never returned.
    #[php(getter)]
    pub fn options(&self) -> PhpResult<ZBox<ZendHashTable>> {
        options_array(&self.inner.options, true)
    }

    /// Options the server filled in, including the read-only encryption projections.
    #[php(getter, change_case = "snake_case")]
    pub fn derived_options(&self) -> PhpResult<ZBox<ZendHashTable>> {
        options_array(&self.inner.options, false)
    }

    /// Who chose the stream's at-rest key: `stream`, `server`, or `none` for a plaintext
    /// stream. A source a newer server reports is returned as its text.
    #[php(getter, change_case = "snake_case")]
    pub fn encryption_key_source(&self) -> Option<String> {
        key_source_text(&self.inner.options)
    }

    /// Fingerprint of the stream's key, 16 lowercase hex characters. Compare it with
    /// `Client::encryptionKeyFingerprint()` of a key you hold. Null for a plaintext stream.
    #[php(getter, change_case = "snake_case")]
    pub fn encryption_key_fingerprint(&self) -> Option<String> {
        self.inner.encryption_key_fingerprint()
    }

    /// Fingerprints of the server keys that wrap the stream's key, comma-separated: one
    /// outside a server key rotation. Null for a plaintext stream.
    #[php(getter, change_case = "snake_case")]
    pub fn encryption_kek_fingerprint(&self) -> Option<String> {
        comma_separated(&self.inner.encryption_kek_fingerprints())
    }

    /// The geo origin whose topics this shadow stream serves, such as `dc-a`. Null for any
    /// stream that is not a shadow stream.
    #[php(getter, change_case = "snake_case")]
    pub fn shadow_origin(&self) -> Option<String> {
        self.inner.shadow_origin()
    }
}

// The SDK's encryption_key_source() parses the text and drops a source that this build does
// not know, so the getters read the text.
fn key_source_text(options: &ResourceOptions) -> Option<String> {
    options
        .get(&HeaderKey::from(OptionKey::EncryptionKeySource))
        .and_then(|option| option.value.as_str().ok().map(str::to_owned))
}

fn comma_separated(fingerprints: &[String]) -> Option<String> {
    (!fingerprints.is_empty()).then(|| fingerprints.join(","))
}
