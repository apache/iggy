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

//! The store identity object, which claims a root for one cluster.
//!
//! Matching configuration cannot prove that every node of a cluster reaches
//! the same store: per-node `fs` directories at one path, or a `MinIO` on
//! each node's `localhost`, look the same to each node. So the cluster writes this
//! object once under its root, by a conditional create, and each node reads
//! it back through its own store.

use std::fmt;
use std::str::FromStr;

use crate::error::ObjectStoreError;
use crate::layout::KeyLayout;
use crate::store::{ObjectStore, PutBody, PutMode, PutOptions, UserMetadata};

/// The first line of the object, which names its format.
const HEADER: &str = "iggy-store-identity 1";

/// The most bytes that a read of the object takes. [`StoreIdentity`] writes
/// 67, so a larger object is not one that it wrote.
const MAX_IDENTITY_LEN: u64 = 1024;

/// What the store identity object holds: the id of the cluster lifetime that
/// owns the root.
///
/// The object is two lines of text, so an operator can read it with any client
/// of the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreIdentity {
    pub cluster_uid: u128,
}

/// Bytes under the identity key that [`StoreIdentity`] did not write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the store identity object holds bytes that this build did not write")]
pub struct InvalidStoreIdentity;

/// What a read of the identity object under a root found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityRead {
    /// No object is under the key, so no cluster claimed the root.
    Missing,
    Found(StoreIdentity),
    /// An object that [`StoreIdentity`] did not write. No cluster that this
    /// build knows owns the root, so nothing under it is safe to use.
    Invalid,
}

impl StoreIdentity {
    /// Write this identity under the identity key of `layout`, unless an
    /// object is there already. Returns `true` when this call wrote the
    /// object, and `false` when it found one, whatever that object holds.
    ///
    /// # Errors
    /// [`ObjectStoreError::Unsupported`] from a store that cannot create an
    /// object only when its key is free, and any other error of the put.
    pub async fn claim<S: ObjectStore>(
        self,
        store: &S,
        layout: &KeyLayout,
    ) -> Result<bool, ObjectStoreError> {
        let body = self.to_string();
        let options = PutOptions {
            mode: PutMode::IfAbsent,
            metadata: UserMetadata::default(),
            crc32c: Some(crc32c::crc32c(body.as_bytes())),
        };
        match store
            .put(&layout.identity_key(), PutBody::from_bytes(body), &options)
            .await
        {
            Ok(()) => Ok(true),
            Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Read the identity object under the identity key of `layout`.
    ///
    /// # Errors
    /// Any error of the read but [`ObjectStoreError::NotFound`], which is
    /// [`IdentityRead::Missing`].
    pub async fn read<S: ObjectStore>(
        store: &S,
        layout: &KeyLayout,
    ) -> Result<IdentityRead, ObjectStoreError> {
        match store
            .get_range(&layout.identity_key(), 0..MAX_IDENTITY_LEN)
            .await
        {
            Ok(range) if range.size > MAX_IDENTITY_LEN => Ok(IdentityRead::Invalid),
            Ok(range) => Ok(std::str::from_utf8(&range.bytes)
                .ok()
                .and_then(|text| text.parse().ok())
                .map_or(IdentityRead::Invalid, IdentityRead::Found)),
            Err(ObjectStoreError::NotFound { .. }) => Ok(IdentityRead::Missing),
            // An empty object has no readable range.
            Err(ObjectStoreError::InvalidRange { .. }) => Ok(IdentityRead::Invalid),
            Err(error) => Err(error),
        }
    }
}

impl fmt::Display for StoreIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{HEADER}")?;
        writeln!(f, "cluster_uid {:032x}", self.cluster_uid)
    }
}

impl FromStr for StoreIdentity {
    type Err = InvalidStoreIdentity;

    /// Parses only the text that [`fmt::Display`] writes, byte for byte.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let digits = text
            .strip_prefix(HEADER)
            .and_then(|rest| rest.strip_prefix("\ncluster_uid "))
            .and_then(|rest| rest.strip_suffix('\n'))
            .ok_or(InvalidStoreIdentity)?;
        let identity = Self {
            cluster_uid: u128::from_str_radix(digits, 16).map_err(|_| InvalidStoreIdentity)?,
        };
        // `from_str_radix` also takes a sign, upper case and fewer digits,
        // which `Display` never writes.
        if identity.to_string() == text {
            Ok(identity)
        } else {
            Err(InvalidStoreIdentity)
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::executor::block_on;

    use super::*;
    use crate::memory::MemoryStore;

    const OURS: StoreIdentity = StoreIdentity {
        cluster_uid: 0x0123_4567_89ab_cdef_0011_2233_4455_6677,
    };
    const THEIRS: StoreIdentity = StoreIdentity { cluster_uid: 7 };

    fn layout() -> KeyLayout {
        KeyLayout::new("tenant/iggy").unwrap()
    }

    fn put_raw(store: &MemoryStore, bytes: &'static [u8]) {
        block_on(store.put(
            &layout().identity_key(),
            PutBody::from_bytes(Bytes::from_static(bytes)),
            &PutOptions::default(),
        ))
        .unwrap();
    }

    #[test]
    fn given_identity_when_printing_should_write_two_lines_that_parse_back() {
        let text = OURS.to_string();

        assert_eq!(
            text,
            "iggy-store-identity 1\ncluster_uid 0123456789abcdef0011223344556677\n"
        );
        assert_eq!(text.len(), 67);
        assert_eq!(text.parse::<StoreIdentity>(), Ok(OURS));
        assert_eq!(
            THEIRS.to_string().parse::<StoreIdentity>(),
            Ok(THEIRS),
            "leading zeros are written and read"
        );
    }

    #[test]
    fn given_text_that_display_did_not_write_when_parsing_should_refuse() {
        for text in [
            "",
            "iggy-store-identity 1\n",
            "iggy-store-identity 2\ncluster_uid 0123456789abcdef0011223344556677\n",
            "iggy-store-identity 1\ncluster_uid 0123456789abcdef0011223344556677",
            "iggy-store-identity 1\ncluster_uid 0123456789ABCDEF0011223344556677\n",
            "iggy-store-identity 1\ncluster_uid 123456789abcdef0011223344556677\n",
            "iggy-store-identity 1\ncluster_uid +123456789abcdef0011223344556677\n",
            "iggy-store-identity 1\ncluster_uid 0123456789abcdef0011223344556677\n\n",
            "iggy-store-identity 1\r\ncluster_uid 0123456789abcdef0011223344556677\r\n",
        ] {
            assert_eq!(
                text.parse::<StoreIdentity>(),
                Err(InvalidStoreIdentity),
                "{text:?}"
            );
        }
    }

    #[test]
    fn given_free_key_when_claiming_should_write_identity_that_reads_back() {
        let store = MemoryStore::new();

        assert!(block_on(OURS.claim(&store, &layout())).unwrap());
        assert_eq!(store.keys(), vec![layout().identity_key()]);
        assert_eq!(
            block_on(StoreIdentity::read(&store, &layout())).unwrap(),
            IdentityRead::Found(OURS)
        );
    }

    #[test]
    fn given_claimed_root_when_claiming_again_should_keep_first_identity() {
        let store = MemoryStore::new();
        block_on(THEIRS.claim(&store, &layout())).unwrap();

        assert!(!block_on(OURS.claim(&store, &layout())).unwrap());
        assert_eq!(
            block_on(StoreIdentity::read(&store, &layout())).unwrap(),
            IdentityRead::Found(THEIRS)
        );
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn given_no_object_when_reading_should_return_missing() {
        let store = MemoryStore::new();

        assert_eq!(
            block_on(StoreIdentity::read(&store, &layout())).unwrap(),
            IdentityRead::Missing
        );
    }

    #[test]
    fn given_object_identity_did_not_write_when_reading_should_return_invalid() {
        for bytes in [
            &b"not an identity"[..],
            &[0xff, 0xfe][..],
            &[b'x'; 2048][..],
        ] {
            let store = MemoryStore::new();
            put_raw(&store, bytes);

            assert_eq!(
                block_on(StoreIdentity::read(&store, &layout())).unwrap(),
                IdentityRead::Invalid,
                "{} bytes",
                bytes.len()
            );
        }
    }

    #[test]
    fn given_empty_object_when_reading_should_return_invalid() {
        let store = MemoryStore::new();
        put_raw(&store, b"");

        assert_eq!(
            block_on(StoreIdentity::read(&store, &layout())).unwrap(),
            IdentityRead::Invalid
        );
    }
}
