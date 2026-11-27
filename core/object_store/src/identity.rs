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
use crate::store::{ObjectRange, ObjectStore, PutBody, PutMode, PutOptions, UserMetadata};

/// The first line of the object, which names its format.
const HEADER: &str = "iggy-store-identity 1";

/// The most bytes that a read of the object takes. [`StoreIdentity`] writes
/// 67, so a larger object is not one that it wrote.
pub const MAX_IDENTITY_LEN: u64 = 1024;

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
            ..PutOptions::default()
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

    /// Check that `store` honors [`PutMode::IfAbsent`], by putting this
    /// identity under the identity key of `layout` once more as a
    /// conditional create. Call it only once the object there holds this
    /// identity: a store that decides conditional creates refuses the put,
    /// and one that ignores the condition writes the same bytes again.
    ///
    /// Every object that two writers can race on is created this way, so a
    /// store that ignores the condition lets both of them win.
    ///
    /// # Errors
    /// [`ObjectStoreError::Unsupported`] when the store took the put, and any
    /// other error of the put but [`ObjectStoreError::PreconditionFailed`].
    pub async fn check_conditional_create<S: ObjectStore>(
        self,
        store: &S,
        layout: &KeyLayout,
    ) -> Result<(), ObjectStoreError> {
        if self.claim(store, layout).await? {
            return Err(ObjectStoreError::Unsupported(format!(
                "conditional creates: a create-only put over the existing object {:?} \
                 succeeded, so the store ignores the condition",
                layout.identity_key()
            )));
        }
        Ok(())
    }

    /// Write this identity under the identity key of `layout`, over the
    /// object of the cluster that owned the root before.
    ///
    /// Only the recovery of a cluster that lost every disk calls this, once
    /// it stored the [`crate::RecoveryRecord`] that retires the cluster the
    /// object names. A store has no conditional replace, so nothing stops a
    /// cluster that still writes the root: the operator stops the retired
    /// cluster for good before the recovery runs, and its nodes stop their
    /// uploads once their next check finds this identity.
    ///
    /// # Errors
    /// Any error of the put.
    pub async fn replace<S: ObjectStore>(
        self,
        store: &S,
        layout: &KeyLayout,
    ) -> Result<(), ObjectStoreError> {
        let body = self.to_string();
        let options = PutOptions {
            mode: PutMode::Overwrite,
            metadata: UserMetadata::default(),
            crc32c: Some(crc32c::crc32c(body.as_bytes())),
            ..PutOptions::default()
        };
        store
            .put(&layout.identity_key(), PutBody::from_bytes(body), &options)
            .await
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
        IdentityRead::of(
            store
                .get_range(&layout.identity_key(), 0..MAX_IDENTITY_LEN)
                .await,
        )
    }
}

impl IdentityRead {
    /// What the answer to a read of the first [`MAX_IDENTITY_LEN`] bytes of
    /// the identity object found, for a reader that cannot take an
    /// [`ObjectStore`].
    ///
    /// # Errors
    /// Any error of the read but [`ObjectStoreError::NotFound`], which is
    /// [`IdentityRead::Missing`].
    pub fn of(answer: Result<ObjectRange, ObjectStoreError>) -> Result<Self, ObjectStoreError> {
        match answer {
            Ok(range) if range.size > MAX_IDENTITY_LEN => Ok(Self::Invalid),
            Ok(range) => Ok(std::str::from_utf8(&range.bytes)
                .ok()
                .and_then(|text| text.parse().ok())
                .map_or(Self::Invalid, Self::Found)),
            Err(ObjectStoreError::NotFound { .. }) => Ok(Self::Missing),
            // An empty object has no readable range.
            Err(ObjectStoreError::InvalidRange { .. }) => Ok(Self::Invalid),
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
    use std::ops::Range;

    use bytes::Bytes;
    use futures::executor::block_on;

    use super::*;
    use crate::fs::FsStore;
    use crate::memory::MemoryStore;
    use crate::mock_s3::{MockS3Latency, MockS3Store};
    use crate::store::{DeleteOutcome, ListPage, ListRequest, ObjectMeta, ObjectRange};

    /// A store that takes every put as an overwrite, as a store does that
    /// ignores `If-None-Match`.
    struct IgnoresCondition(MemoryStore);

    impl ObjectStore for IgnoresCondition {
        async fn put(
            &self,
            key: &str,
            body: PutBody,
            options: &PutOptions,
        ) -> Result<(), ObjectStoreError> {
            let overwrite = PutOptions {
                mode: PutMode::Overwrite,
                ..options.clone()
            };
            self.0.put(key, body, &overwrite).await
        }

        async fn get_range(
            &self,
            key: &str,
            range: Range<u64>,
        ) -> Result<ObjectRange, ObjectStoreError> {
            self.0.get_range(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
            self.0.head(key).await
        }

        async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
            self.0.delete(keys).await
        }

        async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
            self.0.list(request).await
        }
    }

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
    fn given_root_of_a_retired_cluster_when_replacing_should_name_the_successor() {
        let store = MemoryStore::new();
        block_on(THEIRS.claim(&store, &layout())).unwrap();

        block_on(OURS.replace(&store, &layout())).unwrap();

        assert_eq!(
            block_on(StoreIdentity::read(&store, &layout())).unwrap(),
            IdentityRead::Found(OURS)
        );
        assert!(!block_on(THEIRS.claim(&store, &layout())).unwrap());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn given_stores_that_decide_conditional_creates_when_checking_should_pass_and_keep_the_object()
    {
        let directory = tempfile::tempdir().unwrap();
        let memory = MemoryStore::new();
        let mock_s3 = MockS3Store::new(MockS3Latency::default());
        let fs = FsStore::new(directory.path());

        crate::contract::shard_runtime().block_on(async {
            assert!(OURS.claim(&memory, &layout()).await.unwrap());
            assert!(OURS.claim(&mock_s3, &layout()).await.unwrap());
            assert!(OURS.claim(&fs, &layout()).await.unwrap());

            OURS.check_conditional_create(&memory, &layout())
                .await
                .unwrap();
            OURS.check_conditional_create(&mock_s3, &layout())
                .await
                .unwrap();
            OURS.check_conditional_create(&fs, &layout()).await.unwrap();

            assert_eq!(
                StoreIdentity::read(&fs, &layout()).await.unwrap(),
                IdentityRead::Found(OURS)
            );
        });
        assert_eq!(memory.len(), 1);
        assert_eq!(mock_s3.keys(), vec![layout().identity_key()]);
    }

    #[test]
    fn given_store_that_ignores_the_condition_when_checking_should_refuse_as_unsupported() {
        let store = IgnoresCondition(MemoryStore::new());
        assert!(block_on(OURS.claim(&store, &layout())).unwrap());

        let result = block_on(OURS.check_conditional_create(&store, &layout()));

        assert!(
            matches!(&result, Err(ObjectStoreError::Unsupported(reason)) if reason.contains("ignores the condition")),
            "{result:?}"
        );
        assert_eq!(
            block_on(StoreIdentity::read(&store, &layout())).unwrap(),
            IdentityRead::Found(OURS),
            "the put wrote the same bytes again"
        );
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
