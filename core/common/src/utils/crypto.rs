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
use crate::text;
use aes_gcm::aead::{Aead, Generate};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use aes_gcm_siv::aead::AeadInOut;
use aes_gcm_siv::{Aes256GcmSiv, Nonce as SivNonce, Tag as SivTag};
use std::fmt::Debug;

const AES_GCM_NONCE_SIZE: usize = 12;
const AES_GCM_TAG_SIZE: usize = 16;
const AES_GCM_SIV_NONCE_SIZE: usize = 12;
const AES_GCM_SIV_TAG_SIZE: usize = 16;

#[derive(Debug, Clone)]
pub enum EncryptorKind {
    Aes256Gcm(Aes256GcmEncryptor),
    Aes256GcmSiv(Aes256GcmSivEncryptor),
}

impl EncryptorKind {
    pub fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        match self {
            EncryptorKind::Aes256Gcm(e) => e.encrypt(data),
            EncryptorKind::Aes256GcmSiv(e) => e.encrypt(data),
        }
    }
    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        match self {
            EncryptorKind::Aes256Gcm(e) => e.decrypt(data),
            EncryptorKind::Aes256GcmSiv(e) => e.decrypt(data),
        }
    }
}

pub trait Encryptor {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError>;
    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError>;
}

#[derive(Clone)]
pub struct Aes256GcmEncryptor {
    cipher: Aes256Gcm,
}

impl Debug for Aes256GcmEncryptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Encryptor").finish()
    }
}

impl Aes256GcmEncryptor {
    pub fn new(key: &[u8]) -> Result<Self, IggyError> {
        Ok(Self {
            cipher: Aes256Gcm::new_from_slice(key).map_err(|_| IggyError::InvalidEncryptionKey)?,
        })
    }

    pub fn from_base64_key(key: &str) -> Result<Self, IggyError> {
        Self::new(&text::from_base64_as_bytes(key)?)
    }
}

impl Encryptor for Aes256GcmEncryptor {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        let nonce = Nonce::generate();
        let encrypted_data = self.cipher.encrypt(&nonce, data);
        if encrypted_data.is_err() {
            return Err(IggyError::CannotEncryptData);
        }
        let payload = [&nonce, encrypted_data.unwrap().as_slice()].concat();
        Ok(payload)
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        if data.len() < AES_GCM_NONCE_SIZE + AES_GCM_TAG_SIZE {
            return Err(IggyError::CannotDecryptData);
        }
        let (nonce, ciphertext) = data.split_at(AES_GCM_NONCE_SIZE);
        let nonce = nonce.try_into().map_err(|_| IggyError::CannotDecryptData)?;
        self.cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| IggyError::CannotDecryptData)
    }
}

#[derive(Clone)]
pub struct Aes256GcmSivEncryptor {
    cipher: Aes256GcmSiv,
}

impl Debug for Aes256GcmSivEncryptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Aes256GcmSivEncryptor").finish()
    }
}

impl Aes256GcmSivEncryptor {
    pub fn new(key: &[u8]) -> Result<Self, IggyError> {
        Ok(Self {
            cipher: Aes256GcmSiv::new_from_slice(key)
                .map_err(|_| IggyError::InvalidEncryptionKey)?,
        })
    }
}

impl Encryptor for Aes256GcmSivEncryptor {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        let nonce = SivNonce::generate();
        let mut output =
            Vec::with_capacity(AES_GCM_SIV_NONCE_SIZE + data.len() + AES_GCM_SIV_TAG_SIZE);
        output.extend_from_slice(&nonce);
        output.extend_from_slice(data);
        let tag = self
            .cipher
            .encrypt_inout_detached(&nonce, b"", (&mut output[AES_GCM_SIV_NONCE_SIZE..]).into())
            .map_err(|_| IggyError::CannotEncryptData)?;
        output.extend_from_slice(&tag);
        Ok(output)
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        if data.len() < AES_GCM_SIV_NONCE_SIZE + AES_GCM_SIV_TAG_SIZE {
            return Err(IggyError::CannotDecryptData);
        }
        let (nonce, body) = data.split_at(AES_GCM_SIV_NONCE_SIZE);
        let (ciphertext, tag) = body.split_at(body.len() - AES_GCM_SIV_TAG_SIZE);
        let nonce = SivNonce::try_from(nonce).map_err(|_| IggyError::CannotDecryptData)?;
        let tag = SivTag::try_from(tag).map_err(|_| IggyError::CannotDecryptData)?;
        let mut plaintext = ciphertext.to_vec();
        self.cipher
            .decrypt_inout_detached(&nonce, b"", plaintext.as_mut_slice().into(), &tag)
            .map_err(|_| IggyError::CannotDecryptData)?;
        Ok(plaintext)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HeaderKey, HeaderValue, IggyMessage};

    #[test]
    fn given_siv_ciphertext_when_decrypting_should_authenticate_every_byte() {
        let encryptor = Aes256GcmSivEncryptor::new(&[7u8; 32]).expect("valid key");
        let other = Aes256GcmSivEncryptor::new(&[8u8; 32]).expect("valid key");
        let first = encryptor.encrypt(b"stream payload").expect("encrypts");
        let second = encryptor
            .encrypt(b"stream payload")
            .expect("encrypts again");
        assert_ne!(first, second);
        assert_eq!(
            encryptor.decrypt(&first).expect("decrypts"),
            b"stream payload"
        );
        assert_eq!(other.decrypt(&first), Err(IggyError::CannotDecryptData));

        let mut tampered_ciphertext = first.clone();
        tampered_ciphertext[AES_GCM_SIV_NONCE_SIZE] ^= 1;
        assert_eq!(
            encryptor.decrypt(&tampered_ciphertext),
            Err(IggyError::CannotDecryptData)
        );
        let mut tampered_tag = first.clone();
        *tampered_tag.last_mut().expect("tag exists") ^= 1;
        assert_eq!(
            encryptor.decrypt(&tampered_tag),
            Err(IggyError::CannotDecryptData)
        );
    }

    #[test]
    fn given_short_or_empty_siv_payload_when_decrypting_should_handle_lengths() {
        let encryptor = Aes256GcmSivEncryptor::new(&[7u8; 32]).expect("valid key");
        for length in 0..AES_GCM_SIV_NONCE_SIZE + AES_GCM_SIV_TAG_SIZE {
            assert_eq!(
                encryptor.decrypt(&vec![0x5a; length]),
                Err(IggyError::CannotDecryptData),
                "length {length}"
            );
        }
        let encrypted = encryptor.encrypt(&[]).expect("empty plaintext encrypts");
        assert_eq!(
            encrypted.len(),
            AES_GCM_SIV_NONCE_SIZE + AES_GCM_SIV_TAG_SIZE
        );
        assert_eq!(
            encryptor
                .decrypt(&encrypted)
                .expect("empty plaintext decrypts"),
            b""
        );
    }

    #[test]
    fn given_short_ciphertext_when_decrypting_should_return_cannot_decrypt_data() {
        let encryptor = Aes256GcmEncryptor::new(&[3; 32]).expect("valid key");
        for length in 0..28 {
            let ciphertext = vec![0xA5; length];
            assert_eq!(
                encryptor
                    .decrypt(&ciphertext)
                    .expect_err("ciphertext shorter than nonce and tag must fail")
                    .as_code(),
                IggyError::CannotDecryptData.as_code(),
                "length {length}"
            );
        }
        let empty = encryptor.encrypt(&[]).expect("encrypt empty payload");
        assert_eq!(empty.len(), 28);
        assert_eq!(
            encryptor.decrypt(&empty).expect("decrypt empty payload"),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn given_the_same_key_data_should_be_encrypted_and_decrypted_correctly() {
        let key = [1; 32];
        let encryptor = Aes256GcmEncryptor::new(&key).unwrap();
        let data = b"Hello World!";
        let encrypted_data = encryptor.encrypt(data);
        assert!(encrypted_data.is_ok());
        let encrypted_data = encrypted_data.unwrap();
        let decrypted_data = encryptor.decrypt(&encrypted_data);
        assert!(decrypted_data.is_ok());
        let decrypted_data = decrypted_data.unwrap();
        assert_eq!(data, decrypted_data.as_slice());
    }

    #[test]
    fn given_the_invalid_key_data_should_not_be_decrypted_correctly() {
        let first_key = [1; 32];
        let second_key = [2; 32];
        let first_encryptor = Aes256GcmEncryptor::new(&first_key).unwrap();
        let second_encryptor = Aes256GcmEncryptor::new(&second_key).unwrap();
        let data = b"Hello World!";
        let encrypted_data = first_encryptor.encrypt(data);
        assert!(encrypted_data.is_ok());
        let encrypted_data = encrypted_data.unwrap();
        let decrypted_data = second_encryptor.decrypt(&encrypted_data);
        assert!(decrypted_data.is_err());
        let error = decrypted_data.err().unwrap();
        assert_eq!(error.as_code(), IggyError::CannotDecryptData.as_code());
    }

    #[test]
    fn message_payload_and_headers_should_encrypt_and_decrypt_correctly() {
        use bytes::Bytes;
        use std::collections::BTreeMap;

        let key = [1; 32];
        let encryptor = Aes256GcmEncryptor::new(&key).unwrap();

        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::try_from("batch").unwrap(),
            HeaderValue::from(1u64),
        );
        headers.insert(
            HeaderKey::try_from("type").unwrap(),
            HeaderValue::try_from("test-message").unwrap(),
        );

        let mut message = IggyMessage::builder()
            .payload(Bytes::from("test payload data"))
            .user_headers(headers)
            .build()
            .unwrap();

        let original_payload = message.payload.clone();
        let original_headers = message.user_headers.clone().unwrap();

        message.payload = Bytes::from(encryptor.encrypt(&message.payload).unwrap());
        message.header.payload_length = message.payload.len() as u32;

        let encrypted_headers = encryptor.encrypt(&original_headers).unwrap();
        message.header.user_headers_length = encrypted_headers.len() as u32;
        message.user_headers = Some(Bytes::from(encrypted_headers));

        assert_ne!(message.payload, original_payload);
        assert_ne!(message.user_headers.as_ref().unwrap(), &original_headers);

        let decrypted_payload = encryptor.decrypt(&message.payload).unwrap();
        message.payload = Bytes::from(decrypted_payload);
        message.header.payload_length = message.payload.len() as u32;

        let decrypted_headers = encryptor
            .decrypt(message.user_headers.as_ref().unwrap())
            .unwrap();
        message.header.user_headers_length = decrypted_headers.len() as u32;
        message.user_headers = Some(Bytes::from(decrypted_headers));

        assert_eq!(message.payload, original_payload);
        assert_eq!(message.user_headers.as_ref().unwrap(), &original_headers);

        let parsed = message.user_headers_map().unwrap().unwrap();
        assert_eq!(
            parsed
                .get(&HeaderKey::try_from("batch").unwrap())
                .unwrap()
                .as_uint64()
                .unwrap(),
            1
        );
        assert_eq!(
            parsed
                .get(&HeaderKey::try_from("type").unwrap())
                .unwrap()
                .as_str()
                .unwrap(),
            "test-message"
        );
    }
}
