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

use iggy_common::{EncryptorKind, IggyError, ServerKeyContext, StreamEncryption};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// One stream incarnation. A stream id reused after a delete gets a new
/// `created_op`, so its key is never mistaken for the previous stream's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamKeyIdentity {
    pub stream_id: usize,
    pub created_op: u64,
}

/// A shard's stream encryptors by stream incarnation, opened from each
/// stream's committed wrap set with this node's key-encryption keys.
pub struct StreamKeyring {
    context: Arc<ServerKeyContext>,
    encryptors: BTreeMap<StreamKeyIdentity, Arc<EncryptorKind>>,
}

impl StreamKeyring {
    #[must_use]
    pub const fn new(context: Arc<ServerKeyContext>) -> Self {
        Self {
            context,
            encryptors: BTreeMap::new(),
        }
    }

    /// This node's key-encryption keys, for wrapping new stream keys.
    #[must_use]
    pub const fn key_context(&self) -> &Arc<ServerKeyContext> {
        &self.context
    }

    /// The encryptor of one stream incarnation, `None` only for a plaintext
    /// stream.
    ///
    /// # Errors
    ///
    /// The stream's critical state is unreadable, or no envelope of its wrap
    /// set opens on this node. The stream is unavailable then, it never falls
    /// back to plaintext or to another key.
    pub fn resolve(
        &mut self,
        identity: StreamKeyIdentity,
        encryption: &StreamEncryption,
    ) -> Result<Option<Arc<EncryptorKind>>, IggyError> {
        let set = match encryption {
            StreamEncryption::Plaintext => return Ok(None),
            StreamEncryption::Unavailable(error) => return Err(error.clone()),
            StreamEncryption::Wrapped(set) => set,
        };
        if let Some(encryptor) = self.encryptors.get(&identity) {
            return Ok(Some(Arc::clone(encryptor)));
        }
        let encryptor = self.context.open(set)?.encryptor()?;
        self.encryptors.insert(identity, Arc::clone(&encryptor));
        Ok(Some(encryptor))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.encryptors.is_empty()
    }

    /// Drop the encryptors of the stream incarnations `is_live` rejects.
    pub fn retain(&mut self, mut is_live: impl FnMut(StreamKeyIdentity) -> bool) {
        self.encryptors.retain(|identity, _| is_live(*identity));
    }
}

impl Debug for StreamKeyring {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamKeyring")
            .field("context", &self.context)
            .field("streams", &self.encryptors.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{StreamEncryptionKey, StreamKeyOrigin, WrapSet};

    const CONFIG_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
    const OTHER_CONFIG_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";

    #[test]
    fn given_reused_stream_id_when_resolving_should_never_use_previous_key() {
        let context = context(CONFIG_KEY);
        let mut keyring = StreamKeyring::new(Arc::clone(&context));
        let old = StreamKeyIdentity {
            stream_id: 0,
            created_op: 5,
        };
        let recreated = StreamKeyIdentity {
            stream_id: 0,
            created_op: 9,
        };
        let old_encryptor = keyring
            .resolve(old, &wrapped(&context, 1))
            .expect("opens")
            .expect("encrypted");
        let new_encryptor = keyring
            .resolve(recreated, &wrapped(&context, 2))
            .expect("opens")
            .expect("encrypted");

        let ciphertext = old_encryptor.encrypt(b"payload").expect("encrypts");
        assert!(new_encryptor.decrypt(&ciphertext).is_err());
        keyring.retain(|identity| identity == recreated);
        assert_eq!(keyring.encryptors.len(), 1);
        let again = keyring
            .resolve(recreated, &wrapped(&context, 2))
            .expect("opens")
            .expect("encrypted");
        assert!(Arc::ptr_eq(&again, &new_encryptor));
    }

    #[test]
    fn given_plaintext_and_wrapped_streams_when_resolving_should_never_fall_back() {
        let context = context(CONFIG_KEY);
        let mut keyring = StreamKeyring::new(context);
        let identity = StreamKeyIdentity {
            stream_id: 3,
            created_op: 1,
        };
        assert!(
            keyring
                .resolve(identity, &StreamEncryption::Plaintext)
                .expect("plaintext")
                .is_none()
        );
        let foreign = wrapped(&self::context(OTHER_CONFIG_KEY), 4);
        assert!(keyring.resolve(identity, &foreign).is_err());
        assert!(
            keyring
                .resolve(
                    identity,
                    &StreamEncryption::Unavailable(IggyError::UnsupportedCriticalOption(
                        "future".to_owned()
                    ))
                )
                .is_err()
        );
        assert!(keyring.encryptors.is_empty());
    }

    fn context(config_key: &str) -> Arc<ServerKeyContext> {
        Arc::new(ServerKeyContext::from_config(config_key, &[], true).expect("valid config"))
    }

    fn wrapped(context: &ServerKeyContext, seed: u8) -> StreamEncryption {
        let key = StreamEncryptionKey::try_from([seed; 32].as_slice()).expect("32 bytes");
        let envelope = context.wrap(&key, StreamKeyOrigin::Server).expect("wraps");
        StreamEncryption::Wrapped(WrapSet::from(envelope))
    }
}
