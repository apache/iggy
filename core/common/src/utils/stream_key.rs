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

use crate::IggyError;
use crate::utils::crypto::{Aes256GcmSivEncryptor, EncryptorKind};
use crate::utils::text;
use aes_gcm::aead::{Aead, Generate, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ring::rand::{SecureRandom, SystemRandom};
use ring::{digest, hkdf};
use secrecy::zeroize::Zeroizing;
use secrecy::{ExposeSecret, ExposeSecretMut, SecretBox};
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

pub use iggy_binary_protocol::requests::streams::{
    MAX_WRAP_SET_ENVELOPES, WRAPPED_STREAM_KEY_LENGTH,
};

pub const STREAM_KEY_LENGTH: usize = 32;
/// Most retired config keys a node keeps for unwrapping during a rotation.
pub const MAX_PREVIOUS_KEYS: usize = MAX_WRAP_SET_ENVELOPES - 1;

const WRAPPED_STREAM_KEY_VERSION: u8 = 1;
const FINGERPRINT_LENGTH: usize = 8;
const NONCE_LENGTH: usize = 12;
const WRAPPED_KEY_CIPHERTEXT_LENGTH: usize = STREAM_KEY_LENGTH + 16;
const ORIGIN_OFFSET: usize = 1;
const KEK_FINGERPRINT_OFFSET: usize = ORIGIN_OFFSET + 1;
const NONCE_OFFSET: usize = KEK_FINGERPRINT_OFFSET + FINGERPRINT_LENGTH;
const CIPHERTEXT_OFFSET: usize = NONCE_OFFSET + NONCE_LENGTH;
const KEY_FINGERPRINT_OFFSET: usize = CIPHERTEXT_OFFSET + WRAPPED_KEY_CIPHERTEXT_LENGTH;
const STREAM_KEY_WRAP_INFO: &[u8] = b"iggy/stream-key-wrap/v1";
const STREAM_KEY_AAD_LABEL: &[u8] = b"iggy/stream-key/v1";
const STREAM_KEY_FINGERPRINT_LABEL: &[u8] = b"iggy/stream-key-fingerprint/v1";
const KEK_FINGERPRINT_LABEL: &[u8] = b"iggy/stream-kek-fingerprint/v1";

/// A stream's own data encryption key: 32 uniformly random bytes.
///
/// Never printed and never serialized. The bytes leave this type only to be
/// wrapped, encoded into a create request, or turned into an encryptor. A key
/// derived from a password turns its public fingerprint into an offline
/// guessing oracle, so callers must draw it from a secure random generator.
pub struct StreamEncryptionKey(SecretBox<[u8; STREAM_KEY_LENGTH]>);

/// Who chose a stream's key, recorded and authenticated in its envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamKeyOrigin {
    /// The client sent the key with the create.
    Client = 1,
    /// The server generated the key because the client sent none.
    Server = 2,
}

/// A stream key wrapped under one key-encryption key.
///
/// Layout: `[version:1][origin:1][kek_fingerprint:8][nonce:12][ciphertext:32][tag:16][key_fingerprint:8]`.
/// The version, the origin and both fingerprints are authenticated as
/// associated data.
#[derive(Clone, PartialEq, Eq)]
pub struct WrappedStreamKey([u8; WRAPPED_STREAM_KEY_LENGTH]);

/// One stream key wrapped under one or more key-encryption keys: one envelope
/// outside a rotation, one per configured key during it. All envelopes hold
/// the same key from the same origin, sorted by KEK fingerprint, none twice.
#[derive(Clone, PartialEq, Eq)]
pub struct WrapSet {
    envelopes: Vec<WrappedStreamKey>,
    bytes: Vec<u8>,
}

/// The key-encryption keys derived from the server config: the current one,
/// the previous ones a rotation still unwraps with, and the fingerprints of
/// retired ones whose wraps a rewrap drops.
#[derive(Clone)]
pub struct ServerKeyContext {
    current: Option<KeyEncryptionKey>,
    previous: Vec<KeyEncryptionKey>,
    retired: Vec<KekFingerprint>,
    generates: bool,
}

/// Why a node cannot plan the rewrap a stored wrap set calls for.
#[derive(Debug, Error)]
pub enum RewrapPlanError {
    /// The set holds the most wraps, none under the current key and none
    /// under a retired one, so a wrap under the current key does not fit.
    #[error(
        "wrap set already holds {MAX_WRAP_SET_ENVELOPES} wraps and none is retired, retire a key-encryption key fingerprint"
    )]
    WrapSetFull,
    #[error(transparent)]
    Key(#[from] IggyError),
}

#[derive(Clone)]
struct KeyEncryptionKey {
    cipher: Aes256Gcm,
    fingerprint: [u8; FINGERPRINT_LENGTH],
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct KekFingerprint([u8; FINGERPRINT_LENGTH]);

struct DerivedKeyLength;

impl StreamEncryptionKey {
    /// A new random key from the operating system's secure generator.
    ///
    /// # Errors
    ///
    /// `CannotEncryptData` when the generator fails.
    pub fn generate() -> Result<Self, IggyError> {
        let mut key = SecretBox::new(Box::new([0u8; STREAM_KEY_LENGTH]));
        SystemRandom::new()
            .fill(key.expose_secret_mut())
            .map_err(|_| IggyError::CannotEncryptData)?;
        Ok(Self(key))
    }

    /// 16 lowercase hex characters identifying this key without revealing it,
    /// as long as the key is uniformly random.
    #[must_use]
    pub fn fingerprint(&self) -> String {
        hex(&self.fingerprint_bytes())
    }

    /// The raw key, for the request encoder and the stream encryptor.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; STREAM_KEY_LENGTH] {
        self.0.expose_secret()
    }

    /// The encryptor for this stream's message payloads and user headers.
    ///
    /// # Errors
    ///
    /// `InvalidEncryptionKey` when the cipher refuses the key.
    pub fn encryptor(&self) -> Result<Arc<EncryptorKind>, IggyError> {
        Ok(Arc::new(EncryptorKind::Aes256GcmSiv(
            Aes256GcmSivEncryptor::new(self.as_bytes())?,
        )))
    }

    fn fingerprint_bytes(&self) -> [u8; FINGERPRINT_LENGTH] {
        labelled_fingerprint(STREAM_KEY_FINGERPRINT_LABEL, self.as_bytes())
    }
}

impl WrappedStreamKey {
    /// Who chose the key inside this envelope.
    #[must_use]
    pub fn origin(&self) -> StreamKeyOrigin {
        // Construction refuses any other origin byte.
        if self.0[ORIGIN_OFFSET] == StreamKeyOrigin::Server as u8 {
            StreamKeyOrigin::Server
        } else {
            StreamKeyOrigin::Client
        }
    }

    /// Fingerprint of the stream key inside this envelope.
    #[must_use]
    pub fn key_fingerprint(&self) -> String {
        hex(self.key_fingerprint_bytes())
    }

    /// Fingerprint of the key-encryption key that wrapped this envelope.
    #[must_use]
    pub fn kek_fingerprint(&self) -> String {
        hex(self.kek_fingerprint_bytes())
    }

    fn key_fingerprint_bytes(&self) -> &[u8] {
        &self.0[KEY_FINGERPRINT_OFFSET..]
    }

    fn kek_fingerprint_bytes(&self) -> &[u8] {
        &self.0[KEK_FINGERPRINT_OFFSET..NONCE_OFFSET]
    }

    fn associated_data(&self) -> Vec<u8> {
        associated_data(
            self.0[ORIGIN_OFFSET],
            self.kek_fingerprint_bytes(),
            self.key_fingerprint_bytes(),
        )
    }
}

impl WrapSet {
    /// A set of `envelopes` in canonical order.
    ///
    /// # Errors
    ///
    /// `UnsupportedCriticalOption` for an empty set, more than three
    /// envelopes, a KEK fingerprint twice, or envelopes of different keys or
    /// origins.
    pub fn new(mut envelopes: Vec<WrappedStreamKey>) -> Result<Self, IggyError> {
        envelopes.sort_by(|left, right| {
            left.kek_fingerprint_bytes()
                .cmp(right.kek_fingerprint_bytes())
        });
        let Some(first) = envelopes.first() else {
            return Err(unsupported_wrapped_key());
        };
        let consistent = envelopes.len() <= MAX_WRAP_SET_ENVELOPES
            && envelopes.iter().all(|envelope| {
                envelope.origin() == first.origin()
                    && envelope.key_fingerprint_bytes() == first.key_fingerprint_bytes()
            })
            && envelopes
                .windows(2)
                .all(|pair| pair[0].kek_fingerprint_bytes() < pair[1].kek_fingerprint_bytes());
        if !consistent {
            return Err(unsupported_wrapped_key());
        }
        let bytes = envelopes.iter().flat_map(|envelope| envelope.0).collect();
        Ok(Self { envelopes, bytes })
    }

    #[must_use]
    pub fn envelopes(&self) -> &[WrappedStreamKey] {
        &self.envelopes
    }

    /// Who chose the key every envelope holds.
    #[must_use]
    pub fn origin(&self) -> StreamKeyOrigin {
        self.envelopes[0].origin()
    }

    /// Fingerprint of the key every envelope holds.
    #[must_use]
    pub fn key_fingerprint(&self) -> String {
        self.envelopes[0].key_fingerprint()
    }

    /// The raw fingerprint of the key every envelope holds.
    #[must_use]
    pub fn key_fingerprint_bytes(&self) -> &[u8] {
        self.envelopes[0].key_fingerprint_bytes()
    }

    /// Fingerprints of the key-encryption keys, in canonical order.
    #[must_use]
    pub fn kek_fingerprints(&self) -> Vec<String> {
        self.envelopes
            .iter()
            .map(WrappedStreamKey::kek_fingerprint)
            .collect()
    }
}

impl ServerKeyContext {
    /// No config key: streams stay plaintext and stream keys are refused.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            current: None,
            previous: Vec::new(),
            retired: Vec::new(),
            generates: false,
        }
    }

    /// Derive the key-encryption keys from the base64 config keys.
    ///
    /// An empty `key` is valid only when encryption is disabled and no
    /// previous key is set, and then nothing is derived. A supplied `key`
    /// always derives the key-encryption key, so a client may bring its own
    /// stream key while keyless streams stay plaintext. `enabled` decides
    /// whether a keyless stream gets a generated key.
    ///
    /// # Errors
    ///
    /// `InvalidEncryptionKey` when a key is not 32 bytes of base64, when
    /// `key` is empty while `enabled` is true or previous keys are set, for
    /// more than two previous keys, or for a key configured twice.
    pub fn from_config(
        key: &str,
        previous_keys: &[String],
        enabled: bool,
    ) -> Result<Self, IggyError> {
        if key.is_empty() {
            return if enabled || !previous_keys.is_empty() {
                Err(IggyError::InvalidEncryptionKey)
            } else {
                Ok(Self::disabled())
            };
        }
        if previous_keys.len() > MAX_PREVIOUS_KEYS {
            return Err(IggyError::InvalidEncryptionKey);
        }
        let current = KeyEncryptionKey::from_config_key(key)?;
        let previous = previous_keys
            .iter()
            .map(|previous| KeyEncryptionKey::from_config_key(previous))
            .collect::<Result<Vec<_>, _>>()?;
        let mut fingerprints: Vec<_> = previous.iter().map(|kek| kek.fingerprint).collect();
        fingerprints.push(current.fingerprint);
        fingerprints.sort_unstable();
        if fingerprints.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(IggyError::InvalidEncryptionKey);
        }
        Ok(Self {
            current: Some(current),
            previous,
            retired: Vec::new(),
            generates: enabled,
        })
    }

    /// This context with the key-encryption key fingerprints whose wraps a
    /// rewrap drops, as reads print them.
    ///
    /// # Errors
    ///
    /// `InvalidEncryptionKey` without a config key, for an entry that is not
    /// 16 lowercase hex characters, one naming a configured key, or one given
    /// twice.
    pub fn with_retired_kek_fingerprints(
        mut self,
        fingerprints: &[String],
    ) -> Result<Self, IggyError> {
        if fingerprints.is_empty() {
            return Ok(self);
        }
        if self.current.is_none() {
            return Err(IggyError::InvalidEncryptionKey);
        }
        let mut retired = fingerprints
            .iter()
            .map(|fingerprint| fingerprint.parse::<KekFingerprint>())
            .collect::<Result<Vec<_>, _>>()?;
        retired.sort_unstable();
        let repeated = retired.windows(2).any(|pair| pair[0] == pair[1]);
        let configured = retired
            .iter()
            .any(|retired| self.keks().any(|kek| kek.fingerprint == retired.0));
        if repeated || configured {
            return Err(IggyError::InvalidEncryptionKey);
        }
        self.retired = retired;
        Ok(self)
    }

    /// Whether a stream may bring its own key on this node.
    #[must_use]
    pub const fn accepts_stream_keys(&self) -> bool {
        self.current.is_some()
    }

    /// Whether this node gives a keyless stream a generated key.
    #[must_use]
    pub const fn generates_stream_keys(&self) -> bool {
        self.current.is_some() && self.generates
    }

    /// Fingerprint of the key-encryption key that wraps, when a key is set.
    #[must_use]
    pub fn kek_fingerprint(&self) -> Option<String> {
        self.current.as_ref().map(|kek| hex(&kek.fingerprint))
    }

    /// Whether this node's keys can call for a rewrap: a previous key, whose
    /// streams need a wrap under the current key, or a retired key to drop.
    /// Without either no stored set can differ from its target.
    #[must_use]
    pub const fn rotating(&self) -> bool {
        self.current.is_some() && !(self.previous.is_empty() && self.retired.is_empty())
    }

    /// Fingerprints of every configured key-encryption key, current first.
    #[must_use]
    pub fn kek_fingerprints(&self) -> Vec<String> {
        self.keks().map(|kek| hex(&kek.fingerprint)).collect()
    }

    /// Wrap a stream key under the current key-encryption key.
    ///
    /// # Errors
    ///
    /// `StreamEncryptionUnavailable` without a config key, `CannotEncryptData`
    /// when the cipher fails.
    pub fn wrap(
        &self,
        key: &StreamEncryptionKey,
        origin: StreamKeyOrigin,
    ) -> Result<WrappedStreamKey, IggyError> {
        self.current
            .as_ref()
            .ok_or(IggyError::StreamEncryptionUnavailable)?
            .wrap(key, origin)
    }

    /// The wrap set a new stream stores: its key wrapped under every
    /// configured key-encryption key, so it also opens on nodes that still
    /// hold only a previous key while a rotation rolls out.
    ///
    /// # Errors
    ///
    /// As [`Self::wrap`].
    pub fn wrap_set(
        &self,
        key: &StreamEncryptionKey,
        origin: StreamKeyOrigin,
    ) -> Result<WrapSet, IggyError> {
        if self.current.is_none() {
            return Err(IggyError::StreamEncryptionUnavailable);
        }
        let envelopes = self
            .keks()
            .map(|kek| kek.wrap(key, origin))
            .collect::<Result<Vec<_>, _>>()?;
        WrapSet::new(envelopes)
    }

    /// Authenticate and unwrap one envelope with the configured key-encryption
    /// key it names.
    ///
    /// # Errors
    ///
    /// `StreamEncryptionUnavailable` without a config key, `CannotDecryptData`
    /// when no configured key matches, the envelope does not authenticate, or
    /// its key fingerprint does not match the key inside.
    pub fn unwrap(&self, wrapped: &WrappedStreamKey) -> Result<StreamEncryptionKey, IggyError> {
        if self.current.is_none() {
            return Err(IggyError::StreamEncryptionUnavailable);
        }
        let kek = self
            .keks()
            .find(|kek| kek.fingerprint == wrapped.kek_fingerprint_bytes())
            .ok_or(IggyError::CannotDecryptData)?;
        let nonce = Nonce::try_from(&wrapped.0[NONCE_OFFSET..CIPHERTEXT_OFFSET])
            .map_err(|_| IggyError::CannotDecryptData)?;
        let plaintext = Zeroizing::new(
            kek.cipher
                .decrypt(
                    &nonce,
                    Payload {
                        msg: &wrapped.0[CIPHERTEXT_OFFSET..KEY_FINGERPRINT_OFFSET],
                        aad: &wrapped.associated_data(),
                    },
                )
                .map_err(|_| IggyError::CannotDecryptData)?,
        );
        let key = StreamEncryptionKey::try_from(plaintext.as_slice())
            .map_err(|_| IggyError::CannotDecryptData)?;
        if key.fingerprint_bytes() != wrapped.key_fingerprint_bytes() {
            return Err(IggyError::CannotDecryptData);
        }
        Ok(key)
    }

    /// The stream key of a wrap set, from the first envelope that opens.
    ///
    /// # Errors
    ///
    /// As [`Self::unwrap`], from the last envelope tried.
    pub fn open(&self, set: &WrapSet) -> Result<StreamEncryptionKey, IggyError> {
        let mut last = IggyError::CannotDecryptData;
        for envelope in set.envelopes() {
            match self.unwrap(envelope) {
                Ok(key) => return Ok(key),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    /// Whether a stored wrap set already is what a rewrap on this node would
    /// store: one envelope under the current key-encryption key and none
    /// under a retired one. Reads only the fingerprints in the raw stored
    /// value, so a sweep over every stream allocates nothing. Without a
    /// config key nothing can change, so every set is settled.
    #[must_use]
    pub fn is_settled(&self, stored: &[u8]) -> bool {
        let Some(current) = &self.current else {
            return true;
        };
        let (envelopes, rest) = stored.as_chunks::<WRAPPED_STREAM_KEY_LENGTH>();
        rest.is_empty()
            && envelopes.iter().any(|envelope| {
                envelope[KEK_FINGERPRINT_OFFSET..NONCE_OFFSET] == current.fingerprint
            })
            && !envelopes
                .iter()
                .any(|envelope| self.retires(&envelope[KEK_FINGERPRINT_OFFSET..NONCE_OFFSET]))
    }

    /// Whether a stored wrap set holds an envelope under a configured
    /// key-encryption key. Reads only the fingerprints, like
    /// [`Self::is_settled`], so a scan over every stream allocates nothing.
    #[must_use]
    pub fn holds_wrap_for(&self, stored: &[u8]) -> bool {
        let (envelopes, rest) = stored.as_chunks::<WRAPPED_STREAM_KEY_LENGTH>();
        rest.is_empty()
            && envelopes.iter().any(|envelope| {
                self.keks()
                    .any(|kek| envelope[KEK_FINGERPRINT_OFFSET..NONCE_OFFSET] == kek.fingerprint)
            })
    }

    /// The set a rewrap should store instead of `stored`: every envelope not
    /// under a retired key-encryption key, plus one under the current key
    /// when missing. Envelopes under keys this node does not hold stay,
    /// because a node still being rolled out may need them. `None` when
    /// nothing changes.
    ///
    /// # Errors
    ///
    /// When the set must change but this node cannot do it, and the set is
    /// left alone: `WrapSetFull` when the set already holds the most
    /// envelopes and none is retired, `CannotDecryptData` when no envelope
    /// opens here, `CannotEncryptData` when wrapping fails.
    pub fn target_set(&self, stored: &WrapSet) -> Result<Option<WrapSet>, RewrapPlanError> {
        let Some(current) = &self.current else {
            return Ok(None);
        };
        if self.is_settled(stored.as_ref()) {
            return Ok(None);
        }
        let key = self.open(stored)?;
        let mut envelopes: Vec<WrappedStreamKey> = stored
            .envelopes()
            .iter()
            .filter(|envelope| !self.retires(envelope.kek_fingerprint_bytes()))
            .cloned()
            .collect();
        if !envelopes
            .iter()
            .any(|envelope| envelope.kek_fingerprint_bytes() == current.fingerprint)
        {
            if envelopes.len() == MAX_WRAP_SET_ENVELOPES {
                return Err(RewrapPlanError::WrapSetFull);
            }
            envelopes.push(current.wrap(&key, stored.origin())?);
        }
        Ok(Some(WrapSet::new(envelopes)?))
    }

    fn keks(&self) -> impl Iterator<Item = &KeyEncryptionKey> {
        self.current.iter().chain(&self.previous)
    }

    fn retires(&self, kek_fingerprint: &[u8]) -> bool {
        self.retired
            .iter()
            .any(|retired| retired.0 == kek_fingerprint)
    }
}

impl Default for ServerKeyContext {
    fn default() -> Self {
        Self::disabled()
    }
}

impl KeyEncryptionKey {
    fn from_config_key(config_key: &str) -> Result<Self, IggyError> {
        let config_key = Zeroizing::new(
            text::from_base64_as_bytes(config_key).map_err(|_| IggyError::InvalidEncryptionKey)?,
        );
        if config_key.len() != STREAM_KEY_LENGTH {
            return Err(IggyError::InvalidEncryptionKey);
        }
        let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, &[]).extract(config_key.as_slice());
        let wrapping_key = derive_key(&prk, STREAM_KEY_WRAP_INFO)?;
        Ok(Self {
            cipher: Aes256Gcm::new_from_slice(wrapping_key.expose_secret())
                .map_err(|_| IggyError::InvalidEncryptionKey)?,
            fingerprint: labelled_fingerprint(KEK_FINGERPRINT_LABEL, wrapping_key.expose_secret()),
        })
    }

    fn wrap(
        &self,
        key: &StreamEncryptionKey,
        origin: StreamKeyOrigin,
    ) -> Result<WrappedStreamKey, IggyError> {
        let key_fingerprint = key.fingerprint_bytes();
        let aad = associated_data(origin as u8, &self.fingerprint, &key_fingerprint);
        let nonce = Nonce::generate();
        let sealed = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: key.as_bytes(),
                    aad: &aad,
                },
            )
            .map_err(|_| IggyError::CannotEncryptData)?;
        if sealed.len() != WRAPPED_KEY_CIPHERTEXT_LENGTH {
            return Err(IggyError::CannotEncryptData);
        }
        let mut envelope = [0u8; WRAPPED_STREAM_KEY_LENGTH];
        envelope[0] = WRAPPED_STREAM_KEY_VERSION;
        envelope[ORIGIN_OFFSET] = origin as u8;
        envelope[KEK_FINGERPRINT_OFFSET..NONCE_OFFSET].copy_from_slice(&self.fingerprint);
        envelope[NONCE_OFFSET..CIPHERTEXT_OFFSET].copy_from_slice(&nonce);
        envelope[CIPHERTEXT_OFFSET..KEY_FINGERPRINT_OFFSET].copy_from_slice(&sealed);
        envelope[KEY_FINGERPRINT_OFFSET..].copy_from_slice(&key_fingerprint);
        Ok(WrappedStreamKey(envelope))
    }
}

impl FromStr for KekFingerprint {
    type Err = IggyError;

    /// Exactly 16 lowercase hex characters, the form reads print.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let lowercase_hex = text
            .bytes()
            .all(|digit| matches!(digit, b'0'..=b'9' | b'a'..=b'f'));
        if text.len() != 2 * FINGERPRINT_LENGTH || !lowercase_hex {
            return Err(IggyError::InvalidEncryptionKey);
        }
        let mut fingerprint = [0u8; FINGERPRINT_LENGTH];
        for (index, byte) in fingerprint.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[2 * index..2 * index + 2], 16)
                .map_err(|_| IggyError::InvalidEncryptionKey)?;
        }
        Ok(Self(fingerprint))
    }
}

impl TryFrom<&[u8]> for StreamEncryptionKey {
    type Error = IggyError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != STREAM_KEY_LENGTH {
            return Err(IggyError::InvalidEncryptionKey);
        }
        // Filled in place so no copy of the key outlives this call on the stack.
        let mut key = SecretBox::new(Box::new([0u8; STREAM_KEY_LENGTH]));
        key.expose_secret_mut().copy_from_slice(bytes);
        Ok(Self(key))
    }
}

impl FromStr for StreamEncryptionKey {
    type Err = IggyError;

    /// Canonical standard padded base64 of exactly 32 random bytes. Decoding and
    /// re-encoding must reproduce the input, which refuses whitespace, the
    /// URL-safe alphabet, missing padding and noncanonical trailing bits.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let bytes = Zeroizing::new(
            STANDARD
                .decode(text)
                .map_err(|_| IggyError::InvalidEncryptionKey)?,
        );
        if *Zeroizing::new(STANDARD.encode(bytes.as_slice())) != text {
            return Err(IggyError::InvalidEncryptionKey);
        }
        Self::try_from(bytes.as_slice())
    }
}

impl Clone for StreamEncryptionKey {
    fn clone(&self) -> Self {
        let mut key = SecretBox::new(Box::new([0u8; STREAM_KEY_LENGTH]));
        key.expose_secret_mut().copy_from_slice(self.as_bytes());
        Self(key)
    }
}

impl PartialEq for StreamEncryptionKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for StreamEncryptionKey {}

impl Debug for StreamEncryptionKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamEncryptionKey")
            .field("fingerprint", &self.fingerprint())
            .finish()
    }
}

impl TryFrom<u8> for StreamKeyOrigin {
    type Error = IggyError;

    fn try_from(code: u8) -> Result<Self, Self::Error> {
        match code {
            1 => Ok(Self::Client),
            2 => Ok(Self::Server),
            _ => Err(unsupported_wrapped_key()),
        }
    }
}

impl TryFrom<&[u8]> for WrappedStreamKey {
    type Error = IggyError;

    /// # Errors
    ///
    /// `UnsupportedCriticalOption` when the envelope has the wrong length, a
    /// version this build does not understand, or an unknown origin.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let envelope: [u8; WRAPPED_STREAM_KEY_LENGTH] =
            bytes.try_into().map_err(|_| unsupported_wrapped_key())?;
        if envelope[0] != WRAPPED_STREAM_KEY_VERSION {
            return Err(unsupported_wrapped_key());
        }
        StreamKeyOrigin::try_from(envelope[ORIGIN_OFFSET])?;
        Ok(Self(envelope))
    }
}

impl AsRef<[u8]> for WrappedStreamKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<&[u8]> for WrapSet {
    type Error = IggyError;

    /// # Errors
    ///
    /// `UnsupportedCriticalOption` when the bytes are not whole envelopes in
    /// canonical order, see [`WrapSet::new`].
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.is_empty() || !bytes.len().is_multiple_of(WRAPPED_STREAM_KEY_LENGTH) {
            return Err(unsupported_wrapped_key());
        }
        let (chunks, _) = bytes.as_chunks::<WRAPPED_STREAM_KEY_LENGTH>();
        let envelopes = chunks
            .iter()
            .map(|chunk| WrappedStreamKey::try_from(chunk.as_slice()))
            .collect::<Result<Vec<_>, _>>()?;
        let set = Self::new(envelopes)?;
        if set.bytes != bytes {
            return Err(unsupported_wrapped_key());
        }
        Ok(set)
    }
}

impl From<WrappedStreamKey> for WrapSet {
    fn from(envelope: WrappedStreamKey) -> Self {
        Self {
            bytes: envelope.0.to_vec(),
            envelopes: vec![envelope],
        }
    }
}

impl AsRef<[u8]> for WrapSet {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Debug for WrappedStreamKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WrappedStreamKey")
            .field("origin", &self.origin())
            .field("kek_fingerprint", &self.kek_fingerprint())
            .field("key_fingerprint", &self.key_fingerprint())
            .finish()
    }
}

impl Debug for WrapSet {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WrapSet")
            .field("origin", &self.origin())
            .field("key_fingerprint", &self.key_fingerprint())
            .field("kek_fingerprints", &self.kek_fingerprints())
            .finish()
    }
}

impl Debug for ServerKeyContext {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerKeyContext")
            .field("kek_fingerprints", &self.kek_fingerprints())
            .field(
                "retired_kek_fingerprints",
                &self
                    .retired
                    .iter()
                    .map(|retired| hex(&retired.0))
                    .collect::<Vec<_>>(),
            )
            .field("generates_stream_keys", &self.generates_stream_keys())
            .finish()
    }
}

impl hkdf::KeyType for DerivedKeyLength {
    fn len(&self) -> usize {
        STREAM_KEY_LENGTH
    }
}

/// Fingerprint of a stream key given as 32 raw bytes, without the server.
/// It hides only a uniformly random key, never one derived from a password.
///
/// # Errors
///
/// `InvalidEncryptionKey` when `key` is not exactly 32 bytes.
pub fn stream_key_fingerprint(key: &[u8]) -> Result<String, IggyError> {
    StreamEncryptionKey::try_from(key).map(|key| key.fingerprint())
}

fn derive_key(
    prk: &hkdf::Prk,
    info: &[u8],
) -> Result<SecretBox<[u8; STREAM_KEY_LENGTH]>, IggyError> {
    let mut key = SecretBox::new(Box::new([0u8; STREAM_KEY_LENGTH]));
    let info = [info];
    prk.expand(&info, DerivedKeyLength)
        .and_then(|okm| okm.fill(key.expose_secret_mut()))
        .map_err(|_| IggyError::InvalidEncryptionKey)?;
    Ok(key)
}

fn labelled_fingerprint(label: &[u8], key: &[u8]) -> [u8; FINGERPRINT_LENGTH] {
    let mut context = digest::Context::new(&digest::SHA256);
    context.update(label);
    context.update(key);
    let digest = context.finish();
    let mut fingerprint = [0u8; FINGERPRINT_LENGTH];
    fingerprint.copy_from_slice(&digest.as_ref()[..FINGERPRINT_LENGTH]);
    fingerprint
}

fn associated_data(origin: u8, kek_fingerprint: &[u8], key_fingerprint: &[u8]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(STREAM_KEY_AAD_LABEL.len() + 2 + 2 * FINGERPRINT_LENGTH);
    aad.extend_from_slice(STREAM_KEY_AAD_LABEL);
    aad.push(WRAPPED_STREAM_KEY_VERSION);
    aad.push(origin);
    aad.extend_from_slice(kek_fingerprint);
    aad.extend_from_slice(key_fingerprint);
    aad
}

fn unsupported_wrapped_key() -> IggyError {
    IggyError::UnsupportedCriticalOption("encryption_key_wrapped".to_owned())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(2 * bytes.len());
    for byte in bytes {
        text.push(char::from(DIGITS[usize::from(byte >> 4)]));
        text.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::crypto::{Aes256GcmEncryptor, Encryptor};

    const CONFIG_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
    const OTHER_CONFIG_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";
    const THIRD_CONFIG_KEY: &str = "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8=";
    const FOURTH_CONFIG_KEY: &str = "QEFCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaW1xdXl8=";

    #[test]
    fn given_stream_key_when_encrypting_should_use_a_distinct_cipher_from_client_payloads() {
        let key = StreamEncryptionKey::try_from([7u8; 32].as_slice()).expect("valid key");
        let server = key.encryptor().expect("server encryptor");
        let client = Aes256GcmEncryptor::new(key.as_bytes()).expect("client encryptor");
        let encrypted = server
            .encrypt(b"stream payload")
            .expect("encrypted payload");
        assert_eq!(
            server.decrypt(&encrypted).expect("server decrypts"),
            b"stream payload"
        );
        assert!(client.decrypt(&encrypted).is_err());
    }

    #[test]
    fn given_config_key_when_deriving_should_produce_only_the_pinned_kek() {
        let config = text::from_base64_as_bytes(CONFIG_KEY).expect("valid base64");
        let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, &[]).extract(&config);
        let wrapping_key = derive_key(&prk, STREAM_KEY_WRAP_INFO).expect("derive kek");
        assert_ne!(wrapping_key.expose_secret().as_slice(), config.as_slice());
        // Pinned against an independent HKDF-SHA256 computation (RFC 5869,
        // empty salt, 32-byte output) of the same config key and label.
        assert_eq!(hex(wrapping_key.expose_secret()), WRAPPING_KEY_VECTOR);

        let enabled = context(CONFIG_KEY, &[], true);
        assert_eq!(
            enabled.kek_fingerprint().as_deref(),
            Some(KEK_FINGERPRINT_VECTOR)
        );
        assert!(enabled.accepts_stream_keys());
        assert!(enabled.generates_stream_keys());
        let disabled = context(CONFIG_KEY, &[], false);
        assert!(disabled.accepts_stream_keys());
        assert!(!disabled.generates_stream_keys());
        let empty = context("", &[], false);
        assert!(!empty.accepts_stream_keys());
        assert!(!empty.generates_stream_keys());
        assert!(ServerKeyContext::from_config("", &[], true).is_err());
        assert!(ServerKeyContext::from_config("c2hvcnQ=", &[], false).is_err());
        assert_eq!(
            context(OTHER_CONFIG_KEY, &[], true)
                .kek_fingerprint()
                .as_deref(),
            Some(OTHER_KEK_FINGERPRINT_VECTOR)
        );
    }

    #[test]
    fn given_wrapped_key_when_tampering_should_reject_every_envelope_component() {
        let context = context(CONFIG_KEY, &[], false);
        let key = fixed_key();
        let wrapped = context.wrap(&key, StreamKeyOrigin::Client).expect("wrap");
        assert_eq!(wrapped.as_ref().len(), WRAPPED_STREAM_KEY_LENGTH);
        assert_eq!(context.unwrap(&wrapped).expect("unwrap"), key);
        assert_eq!(wrapped.key_fingerprint(), key.fingerprint());
        assert_eq!(wrapped.origin(), StreamKeyOrigin::Client);

        for (component, position) in [
            ("version", 0),
            ("origin", ORIGIN_OFFSET),
            ("kek fingerprint", KEK_FINGERPRINT_OFFSET),
            ("nonce", NONCE_OFFSET),
            ("ciphertext", CIPHERTEXT_OFFSET),
            ("tag", KEY_FINGERPRINT_OFFSET - 1),
            ("key fingerprint", KEY_FINGERPRINT_OFFSET),
        ] {
            let mut tampered = wrapped.0;
            tampered[position] ^= 0x03;
            let outcome = WrappedStreamKey::try_from(tampered.as_slice())
                .and_then(|tampered| context.unwrap(&tampered));
            assert!(outcome.is_err(), "tampered {component} must not unwrap");
        }
        // An origin flipped to the other valid value parses, and still fails
        // to authenticate.
        let mut relabelled = wrapped.0;
        relabelled[ORIGIN_OFFSET] = StreamKeyOrigin::Server as u8;
        let relabelled = WrappedStreamKey::try_from(relabelled.as_slice()).expect("valid origin");
        assert!(context.unwrap(&relabelled).is_err());
    }

    #[test]
    fn given_same_key_when_wrapping_twice_should_use_a_fresh_nonce_each_time() {
        let context = context(CONFIG_KEY, &[], true);
        let key = fixed_key();
        let first = context.wrap(&key, StreamKeyOrigin::Client).expect("wrap");
        let second = context.wrap(&key, StreamKeyOrigin::Client).expect("wrap");
        assert_ne!(
            first.0[NONCE_OFFSET..CIPHERTEXT_OFFSET],
            second.0[NONCE_OFFSET..CIPHERTEXT_OFFSET]
        );
        assert_ne!(
            first.0[CIPHERTEXT_OFFSET..KEY_FINGERPRINT_OFFSET],
            second.0[CIPHERTEXT_OFFSET..KEY_FINGERPRINT_OFFSET]
        );
        assert_eq!(context.unwrap(&first).expect("unwrap"), key);
        assert_eq!(context.unwrap(&second).expect("unwrap"), key);
    }

    #[test]
    fn given_wrong_config_key_when_unwrapping_should_return_error() {
        let context = context(CONFIG_KEY, &[], true);
        let other = self::context(OTHER_CONFIG_KEY, &[], true);
        let wrapped = context
            .wrap(&fixed_key(), StreamKeyOrigin::Client)
            .expect("wrap");
        assert_eq!(
            other.unwrap(&wrapped).expect_err("wrong kek").as_code(),
            IggyError::CannotDecryptData.as_code()
        );
        assert_ne!(other.kek_fingerprint(), Some(wrapped.kek_fingerprint()));
        assert_eq!(context.kek_fingerprint(), Some(wrapped.kek_fingerprint()));
        assert_eq!(
            ServerKeyContext::disabled()
                .wrap(&fixed_key(), StreamKeyOrigin::Server)
                .expect_err("no kek")
                .as_code(),
            IggyError::StreamEncryptionUnavailable.as_code()
        );
    }

    #[test]
    fn given_generated_keys_when_wrapping_should_differ_and_record_server_origin() {
        let context = context(CONFIG_KEY, &[], true);
        let first = StreamEncryptionKey::generate().expect("random key");
        let second = StreamEncryptionKey::generate().expect("random key");
        assert_ne!(first, second);
        let wrapped = context.wrap(&first, StreamKeyOrigin::Server).expect("wrap");
        let set = WrapSet::from(wrapped);
        assert_eq!(set.origin(), StreamKeyOrigin::Server);
        assert_eq!(set.key_fingerprint(), first.fingerprint());
        assert_eq!(context.open(&set).expect("opens"), first);
        assert_eq!(
            WrapSet::try_from(set.as_ref()).expect("canonical bytes"),
            set
        );
    }

    #[test]
    fn given_wrap_set_bytes_when_parsing_should_enforce_canonical_form() {
        let key = fixed_key();
        let wraps: Vec<WrappedStreamKey> = [CONFIG_KEY, OTHER_CONFIG_KEY, THIRD_CONFIG_KEY]
            .iter()
            .map(|config_key| {
                context(config_key, &[], true)
                    .wrap(&key, StreamKeyOrigin::Client)
                    .expect("wrap")
            })
            .collect();
        let set = WrapSet::new(wraps.clone()).expect("three wraps");
        let mut sorted = wraps.clone();
        sorted.sort_by_key(WrappedStreamKey::kek_fingerprint);
        assert_eq!(set.envelopes(), sorted.as_slice());

        let concat = |envelopes: &[WrappedStreamKey]| -> Vec<u8> {
            envelopes.iter().flat_map(|envelope| envelope.0).collect()
        };
        let other_key = context(CONFIG_KEY, &[], true)
            .wrap(
                &StreamEncryptionKey::try_from([9u8; 32].as_slice()).unwrap(),
                StreamKeyOrigin::Client,
            )
            .expect("wrap");
        let server_origin = context(OTHER_CONFIG_KEY, &[], true)
            .wrap(&key, StreamKeyOrigin::Server)
            .expect("wrap");
        let mut unsorted = sorted.clone();
        unsorted.reverse();
        for (case, bytes) in [
            ("empty", Vec::new()),
            ("partial envelope", sorted[0].0[..40].to_vec()),
            ("unsorted", concat(&unsorted)),
            (
                "duplicate kek",
                concat(&[sorted[0].clone(), sorted[0].clone()]),
            ),
            (
                "mixed keys",
                concat(&[sorted[0].clone(), other_key.clone()]),
            ),
            (
                "mixed origins",
                concat(&[wraps[0].clone(), server_origin.clone()]),
            ),
            (
                "four envelopes",
                concat(&[sorted.clone(), vec![other_key]].concat()),
            ),
        ] {
            assert!(WrapSet::try_from(bytes.as_slice()).is_err(), "{case}");
        }
    }

    #[test]
    fn given_previous_keys_when_opening_wrap_set_should_use_any_configured_kek() {
        let key = fixed_key();
        let old = context(CONFIG_KEY, &[], true);
        let set = WrapSet::from(old.wrap(&key, StreamKeyOrigin::Server).expect("wrap"));
        let rotated = context(OTHER_CONFIG_KEY, &[CONFIG_KEY.to_owned()], true);
        assert_eq!(rotated.open(&set).expect("previous key opens"), key);
        let new_only = context(OTHER_CONFIG_KEY, &[], true);
        assert!(new_only.open(&set).is_err());
        let rewrapped = rotated.wrap(&key, StreamKeyOrigin::Server).expect("wrap");
        assert_eq!(Some(rewrapped.kek_fingerprint()), rotated.kek_fingerprint());

        for invalid in [
            vec![CONFIG_KEY.to_owned(); 2],
            vec![OTHER_CONFIG_KEY.to_owned()],
            vec![
                CONFIG_KEY.to_owned(),
                THIRD_CONFIG_KEY.to_owned(),
                "AAAA".to_owned(),
            ],
            vec!["c2hvcnQ=".to_owned()],
        ] {
            assert!(
                ServerKeyContext::from_config(OTHER_CONFIG_KEY, &invalid, true).is_err(),
                "{} previous keys",
                invalid.len()
            );
        }
        assert!(ServerKeyContext::from_config("", &[CONFIG_KEY.to_owned()], false).is_err());
    }

    #[test]
    fn given_rotation_phases_when_planning_should_add_current_and_drop_only_retired_wraps() {
        let key = fixed_key();
        let old_only = WrapSet::from(
            context(CONFIG_KEY, &[], true)
                .wrap(&key, StreamKeyOrigin::Client)
                .expect("wrap"),
        );
        let phase_one = context(OTHER_CONFIG_KEY, &[CONFIG_KEY.to_owned()], true);
        let both = phase_one
            .target_set(&old_only)
            .expect("plans")
            .expect("adds the current wrap");
        assert_eq!(
            both.kek_fingerprints(),
            sorted(&[KEK_FINGERPRINT_VECTOR, OTHER_KEK_FINGERPRINT_VECTOR])
        );
        assert_eq!(both.origin(), StreamKeyOrigin::Client);
        assert_eq!(phase_one.open(&both).expect("opens"), key);
        assert_eq!(phase_one.target_set(&both).expect("plans"), None);

        let new_alone = context(OTHER_CONFIG_KEY, &[], true);
        assert_eq!(new_alone.target_set(&both).expect("plans"), None);

        let phase_two = new_alone
            .clone()
            .with_retired_kek_fingerprints(&[KEK_FINGERPRINT_VECTOR.to_owned()])
            .expect("valid retirement");
        let new_only = phase_two
            .target_set(&both)
            .expect("plans")
            .expect("drops the retired wrap");
        assert_eq!(
            new_only.kek_fingerprints(),
            vec![OTHER_KEK_FINGERPRINT_VECTOR]
        );
        assert_eq!(phase_two.open(&new_only).expect("opens"), key);
        assert_eq!(phase_two.target_set(&new_only).expect("plans"), None);
        assert_eq!(
            phase_one.target_set(&new_only).expect("plans"),
            None,
            "a phase 1 primary must not re-add a wrap phase 2 dropped"
        );

        for (node, stuck) in [
            (context(THIRD_CONFIG_KEY, &[], true), &old_only),
            (phase_two, &old_only),
        ] {
            assert!(matches!(
                node.target_set(stuck).expect_err("no wrap opens here"),
                RewrapPlanError::Key(IggyError::CannotDecryptData)
            ));
        }
    }

    #[test]
    fn given_full_wrap_set_when_planning_should_refuse_until_a_wrap_is_retired() {
        let key = fixed_key();
        let full = WrapSet::new(
            [CONFIG_KEY, OTHER_CONFIG_KEY, THIRD_CONFIG_KEY]
                .iter()
                .map(|config_key| {
                    context(config_key, &[], true)
                        .wrap(&key, StreamKeyOrigin::Server)
                        .expect("wrap")
                })
                .collect(),
        )
        .expect("three wraps");
        let fourth = context(FOURTH_CONFIG_KEY, &[CONFIG_KEY.to_owned()], true);
        assert!(matches!(
            fourth
                .target_set(&full)
                .expect_err("a fourth wrap does not fit"),
            RewrapPlanError::WrapSetFull
        ));

        let retired = fourth
            .with_retired_kek_fingerprints(&[OTHER_KEK_FINGERPRINT_VECTOR.to_owned()])
            .expect("valid retirement");
        let replaced = retired
            .target_set(&full)
            .expect("plans")
            .expect("swaps the retired wrap for the current one");
        assert_eq!(replaced.envelopes().len(), 3);
        assert!(
            !replaced
                .kek_fingerprints()
                .contains(&OTHER_KEK_FINGERPRINT_VECTOR.to_owned())
        );
        assert_eq!(retired.open(&replaced).expect("opens"), key);
    }

    #[test]
    fn given_retired_fingerprints_when_configuring_should_accept_only_unconfigured_lowercase_hex() {
        let rotated = context(OTHER_CONFIG_KEY, &[THIRD_CONFIG_KEY.to_owned()], true);
        let accepted = rotated
            .clone()
            .with_retired_kek_fingerprints(&[KEK_FINGERPRINT_VECTOR.to_owned()])
            .expect("an unconfigured fingerprint");
        assert!(accepted.rotating());
        let third_fingerprint = context(THIRD_CONFIG_KEY, &[], true)
            .kek_fingerprint()
            .expect("a key");
        for invalid in [
            vec![KEK_FINGERPRINT_VECTOR.to_uppercase()],
            vec![KEK_FINGERPRINT_VECTOR[..15].to_owned()],
            vec![format!("{KEK_FINGERPRINT_VECTOR}0")],
            vec!["39e4f0f95c3693cg".to_owned()],
            vec![OTHER_KEK_FINGERPRINT_VECTOR.to_owned()],
            vec![third_fingerprint],
            vec![KEK_FINGERPRINT_VECTOR.to_owned(); 2],
        ] {
            assert!(
                rotated
                    .clone()
                    .with_retired_kek_fingerprints(&invalid)
                    .is_err(),
                "{invalid:?} must be refused"
            );
        }
        assert!(
            ServerKeyContext::disabled()
                .with_retired_kek_fingerprints(&[KEK_FINGERPRINT_VECTOR.to_owned()])
                .is_err()
        );
        assert!(
            !ServerKeyContext::disabled()
                .with_retired_kek_fingerprints(&[])
                .expect("nothing to retire")
                .rotating()
        );

        assert!(!context(CONFIG_KEY, &[], true).rotating());
        assert!(!context(CONFIG_KEY, &[], false).rotating());
        assert!(rotated.rotating());
        assert!(
            context(OTHER_CONFIG_KEY, &[], true)
                .with_retired_kek_fingerprints(&[KEK_FINGERPRINT_VECTOR.to_owned()])
                .expect("valid retirement")
                .rotating()
        );
    }

    #[test]
    fn given_raw_stored_sets_when_checking_settled_should_match_the_planner() {
        let key = fixed_key();
        let old_only = WrapSet::from(
            context(CONFIG_KEY, &[], true)
                .wrap(&key, StreamKeyOrigin::Server)
                .expect("wrap"),
        );
        let phase_one = context(OTHER_CONFIG_KEY, &[CONFIG_KEY.to_owned()], true);
        let both = phase_one
            .wrap_set(&key, StreamKeyOrigin::Server)
            .expect("wraps");
        let phase_two = context(OTHER_CONFIG_KEY, &[], true)
            .with_retired_kek_fingerprints(&[KEK_FINGERPRINT_VECTOR.to_owned()])
            .expect("valid retirement");

        for (node, set, settled) in [
            (&phase_one, &old_only, false),
            (&phase_one, &both, true),
            (&phase_two, &both, false),
            (&phase_two, &old_only, false),
        ] {
            assert_eq!(node.is_settled(set.as_ref()), settled, "{set:?}");
            assert_eq!(
                node.target_set(set).is_ok_and(|target| target.is_none()),
                settled,
                "{set:?}"
            );
        }
        assert!(!phase_one.is_settled(&both.as_ref()[..WRAPPED_STREAM_KEY_LENGTH + 1]));
        assert!(!phase_one.is_settled(&[]));
        assert!(ServerKeyContext::disabled().is_settled(old_only.as_ref()));
    }

    #[test]
    fn given_previous_keys_when_wrapping_new_key_should_wrap_under_every_configured_kek() {
        let key = fixed_key();
        let phase_one = context(
            OTHER_CONFIG_KEY,
            &[CONFIG_KEY.to_owned(), THIRD_CONFIG_KEY.to_owned()],
            true,
        );
        let set = phase_one
            .wrap_set(&key, StreamKeyOrigin::Client)
            .expect("wraps");
        assert_eq!(set.envelopes().len(), 3);
        assert_eq!(set.origin(), StreamKeyOrigin::Client);
        for config_key in [CONFIG_KEY, OTHER_CONFIG_KEY, THIRD_CONFIG_KEY] {
            assert_eq!(
                context(config_key, &[], true)
                    .open(&set)
                    .expect("opens under each key alone"),
                key
            );
        }
        let single = context(CONFIG_KEY, &[], true)
            .wrap_set(&key, StreamKeyOrigin::Client)
            .expect("wraps");
        assert_eq!(single.kek_fingerprints(), vec![KEK_FINGERPRINT_VECTOR]);
        assert_eq!(
            ServerKeyContext::disabled()
                .wrap_set(&key, StreamKeyOrigin::Client)
                .expect_err("no kek")
                .as_code(),
            IggyError::StreamEncryptionUnavailable.as_code()
        );
    }

    #[test]
    fn given_wraps_under_unknown_keks_when_planning_should_keep_them() {
        let key = fixed_key();
        let phase_one = context(OTHER_CONFIG_KEY, &[CONFIG_KEY.to_owned()], true);
        let both = WrapSet::new(vec![
            phase_one.wrap(&key, StreamKeyOrigin::Client).expect("wrap"),
            context(CONFIG_KEY, &[], true)
                .wrap(&key, StreamKeyOrigin::Client)
                .expect("wrap"),
        ])
        .expect("two wraps");

        for (config, previous) in [(CONFIG_KEY, &[][..]), (OTHER_CONFIG_KEY, &[][..])] {
            assert_eq!(
                context(config, previous, true)
                    .target_set(&both)
                    .expect("plans"),
                None,
                "a node holding one of the keys must keep the other wrap"
            );
        }
    }

    #[test]
    fn given_fixed_key_when_fingerprinting_should_match_shared_vector() {
        let key = fixed_key();
        assert_eq!(key.fingerprint(), FINGERPRINT_VECTOR);
        assert_eq!(
            stream_key_fingerprint(key.as_bytes()).expect("32 bytes"),
            FINGERPRINT_VECTOR
        );
        assert!(stream_key_fingerprint(&[0u8; 31]).is_err());
        assert!(stream_key_fingerprint(&[0u8; 33]).is_err());
    }

    #[test]
    fn given_secret_values_when_debugging_should_redact_raw_and_base64_forms() {
        let key = fixed_key();
        let base64 = STANDARD.encode(key.as_bytes());
        let context = context(CONFIG_KEY, &[OTHER_CONFIG_KEY.to_owned()], true);
        let wrapped = context.wrap(&key, StreamKeyOrigin::Client).expect("wrap");
        let set = WrapSet::from(wrapped.clone());
        for rendered in [
            format!("{key:?}"),
            format!("{wrapped:?}"),
            format!("{set:?}"),
            format!("{context:?}"),
        ] {
            assert!(!rendered.contains(&base64), "{rendered}");
            assert!(!rendered.contains(CONFIG_KEY), "{rendered}");
            assert!(!rendered.contains(OTHER_CONFIG_KEY), "{rendered}");
            assert!(
                !rendered.contains(&format!("{:?}", key.as_bytes())),
                "{rendered}"
            );
        }
    }

    #[test]
    fn given_noncanonical_base64_when_parsing_key_should_reject() {
        let canonical = STANDARD.encode(fixed_key().as_bytes());
        assert_eq!(
            canonical.parse::<StreamEncryptionKey>().expect("canonical"),
            fixed_key()
        );
        for invalid in [
            format!(" {canonical}"),
            canonical.trim_end_matches('=').to_owned(),
            STANDARD
                .encode([0xFB; 32])
                .replace('+', "-")
                .replace('/', "_"),
            STANDARD.encode([7u8; 31]),
            STANDARD.encode([7u8; 33]),
            // Same 32 bytes, noncanonical trailing bits in the last symbol.
            format!("{}B=", &canonical[..canonical.len() - 2]),
        ] {
            assert!(
                invalid.parse::<StreamEncryptionKey>().is_err(),
                "{invalid} must be refused"
            );
        }
    }

    fn context(key: &str, previous_keys: &[String], enabled: bool) -> ServerKeyContext {
        ServerKeyContext::from_config(key, previous_keys, enabled).expect("valid config")
    }

    fn sorted(fingerprints: &[&str]) -> Vec<String> {
        let mut sorted: Vec<String> = fingerprints
            .iter()
            .map(|fingerprint| (*fingerprint).to_owned())
            .collect();
        sorted.sort();
        sorted
    }

    fn fixed_key() -> StreamEncryptionKey {
        let bytes: Vec<u8> = (0u8..32).collect();
        StreamEncryptionKey::try_from(bytes.as_slice()).expect("32 bytes")
    }

    const FINGERPRINT_VECTOR: &str = "8441e38538cbfa57";
    const WRAPPING_KEY_VECTOR: &str =
        "8433bbdfaceaf9e7540529aad95a49cd8680ce3d30a1ef1103d5c6d122a1ba3e";
    const KEK_FINGERPRINT_VECTOR: &str = "39e4f0f95c3693c7";
    const OTHER_KEK_FINGERPRINT_VECTOR: &str = "d55d479829bc2936";
}
