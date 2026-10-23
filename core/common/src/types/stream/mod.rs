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

use crate::{
    HeaderKey, IggyByteSize, IggyTimestamp, OptionKey, ResourceOptions, StreamEncryptionSource,
    Topic,
};
use serde::{Deserialize, Serialize};

/// `Stream` represents the highest level of logical separation of data.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the stream.
/// - `created_at`: the timestamp when the stream was created.
/// - `name`: the unique name of the stream.
/// - `size_bytes`: the total size of the stream in bytes.
/// - `messages_count`: the total number of messages in the stream.
/// - `topics_count`: the total number of topics in the stream.
#[derive(Debug, Serialize, Deserialize)]
pub struct Stream {
    /// The unique identifier (numeric) of the stream.
    pub id: u32,
    /// The timestamp when the stream was created.
    pub created_at: IggyTimestamp,
    /// The unique name of the stream.
    pub name: String,
    /// The total size of the stream in bytes.
    pub size: IggyByteSize,
    /// The total number of messages in the stream.
    pub messages_count: u64,
    /// The total number of topics in the stream.
    pub topics_count: u32,
    /// Options by provenance, plus the read-only encryption key source and
    /// fingerprints as derived entries.
    #[serde(default, with = "crate::resource_options_json")]
    pub options: ResourceOptions,
}

/// `StreamDetails` represents the detailed information about the stream.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the stream.
/// - `created_at`: the timestamp when the stream was created.
/// - `name`: the unique name of the stream.
/// - `size_bytes`: the total size of the stream in bytes.
/// - `messages_count`: the total number of messages in the stream.
/// - `topics_count`: the total number of topics in the stream.
/// - `topics`: the list of topics in the stream.
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamDetails {
    /// The unique identifier (numeric) of the stream.
    pub id: u32,
    /// The timestamp when the stream was created.
    pub created_at: IggyTimestamp,
    /// The unique name of the stream.
    pub name: String,
    /// The total size of the stream in bytes.
    pub size: IggyByteSize,
    /// The total number of messages in the stream.
    pub messages_count: u64,
    /// The total number of topics in the stream.
    pub topics_count: u32,
    /// The collection of topics in the stream.
    pub topics: Vec<Topic>,
    /// Options by provenance, plus the read-only encryption key source and
    /// fingerprints as derived entries.
    #[serde(default, with = "crate::resource_options_json")]
    pub options: ResourceOptions,
}

impl Stream {
    /// Who chose the stream's at-rest key, `None` when the server reported no
    /// source.
    #[must_use]
    pub fn encryption_key_source(&self) -> Option<StreamEncryptionSource> {
        encryption_key_source(&self.options)
    }

    /// Fingerprint of the stream's key, absent for a plaintext stream.
    #[must_use]
    pub fn encryption_key_fingerprint(&self) -> Option<String> {
        text_option(&self.options, OptionKey::EncryptionKeyFingerprint)
    }

    /// Fingerprints of the server keys that wrap the stream's key: one
    /// outside a key rotation, empty for a plaintext stream.
    #[must_use]
    pub fn encryption_kek_fingerprints(&self) -> Vec<String> {
        kek_fingerprints(&self.options)
    }
}

impl StreamDetails {
    /// Who chose the stream's at-rest key, `None` when the server reported no
    /// source.
    #[must_use]
    pub fn encryption_key_source(&self) -> Option<StreamEncryptionSource> {
        encryption_key_source(&self.options)
    }

    /// Fingerprint of the stream's key, absent for a plaintext stream.
    #[must_use]
    pub fn encryption_key_fingerprint(&self) -> Option<String> {
        text_option(&self.options, OptionKey::EncryptionKeyFingerprint)
    }

    /// Fingerprints of the server keys that wrap the stream's key: one
    /// outside a key rotation, empty for a plaintext stream.
    #[must_use]
    pub fn encryption_kek_fingerprints(&self) -> Vec<String> {
        kek_fingerprints(&self.options)
    }
}

fn encryption_key_source(options: &ResourceOptions) -> Option<StreamEncryptionSource> {
    options
        .get(&HeaderKey::from(OptionKey::EncryptionKeySource))
        .and_then(|option| option.value.as_str().ok()?.parse().ok())
}

fn text_option(options: &ResourceOptions, key: OptionKey) -> Option<String> {
    options
        .get(&HeaderKey::from(key))
        .and_then(|option| option.value.as_str().ok().map(str::to_owned))
}

fn kek_fingerprints(options: &ResourceOptions) -> Vec<String> {
    text_option(options, OptionKey::EncryptionKekFingerprint).map_or_else(Vec::new, |text| {
        text.split(',').map(str::to_owned).collect()
    })
}
