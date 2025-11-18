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

use crate::IggyError;
use crate::text;
use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, KeyInit};
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub enum EncryptorKind {
    Aes256Gcm(Aes256GcmEncryptor),
}

impl EncryptorKind {
    pub fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        match self {
            EncryptorKind::Aes256Gcm(e) => e.encrypt(data),
        }
    }
    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        match self {
            EncryptorKind::Aes256Gcm(e) => e.decrypt(data),
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
        if key.len() != 32 {
            return Err(IggyError::InvalidEncryptionKey);
        }
        Ok(Self {
            cipher: Aes256Gcm::new(key.into()),
        })
    }

    pub fn from_base64_key(key: &str) -> Result<Self, IggyError> {
        Self::new(&text::from_base64_as_bytes(key)?)
    }
}

impl Encryptor for Aes256GcmEncryptor {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let encrypted_data = self.cipher.encrypt(&nonce, data);
        if encrypted_data.is_err() {
            return Err(IggyError::CannotEncryptData);
        }
        let payload = [&nonce, encrypted_data.unwrap().as_slice()].concat();
        Ok(payload)
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        let nonce = (&data[0..12]).into();
        let payload = self.cipher.decrypt(nonce, &data[12..]);
        if payload.is_err() {
            return Err(IggyError::CannotDecryptData);
        }
        Ok(payload.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
