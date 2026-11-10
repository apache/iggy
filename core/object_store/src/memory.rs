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

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::ops::{Bound, Range};
use std::time::SystemTime;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;

use crate::error::ObjectStoreError;
use crate::store::{
    DeleteFailure, DeleteOutcome, ListPage, ListRequest, ListedObject, ObjectMeta, ObjectStore,
    PutBody, PutMode, PutOptions, UserMetadata, validate_delete, validate_key, validate_metadata,
    validate_prefix, validate_range,
};

/// An [`ObjectStore`] in memory, with no faults.
///
/// Every call answers at once, except that a put waits for its body. It lets
/// tests run against the trait before any provider exists. It keeps the
/// contract, which [`crate::contract`] checks.
#[derive(Debug, Default)]
pub struct MemoryStore {
    objects: RefCell<BTreeMap<String, StoredObject>>,
    puts: Cell<u64>,
}

#[derive(Debug)]
struct StoredObject {
    bytes: Bytes,
    metadata: UserMetadata,
    etag: String,
    last_modified: SystemTime,
    crc32c: u32,
}

impl MemoryStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every key, in byte order.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.objects.borrow().keys().cloned().collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.objects.borrow().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.objects.borrow().is_empty()
    }
}

impl ObjectStore for MemoryStore {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        validate_key(key)?;
        validate_metadata(&options.metadata)?;
        let mut pieces = body.into_pieces();
        let mut bytes = BytesMut::new();
        while let Some(piece) = pieces.next().await {
            bytes.extend_from_slice(&piece.map_err(ObjectStoreError::Body)?);
        }
        let bytes = bytes.freeze();
        let crc32c = crc32c::crc32c(&bytes);
        if options.crc32c.is_some_and(|expected| expected != crc32c) {
            return Err(ObjectStoreError::ChecksumMismatch {
                key: key.to_owned(),
            });
        }
        let mut objects = self.objects.borrow_mut();
        if options.mode == PutMode::IfAbsent && objects.contains_key(key) {
            return Err(ObjectStoreError::PreconditionFailed {
                key: key.to_owned(),
            });
        }
        let version = self.puts.get() + 1;
        self.puts.set(version);
        objects.insert(
            key.to_owned(),
            StoredObject {
                bytes,
                metadata: options.metadata.clone(),
                etag: format!("{version:016x}"),
                last_modified: SystemTime::now(),
                crc32c,
            },
        );
        Ok(())
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, ObjectStoreError> {
        validate_key(key)?;
        validate_range(&range)?;
        let objects = self.objects.borrow();
        let bytes = &objects.get(key).ok_or_else(|| not_found(key))?.bytes;
        let start = usize::try_from(range.start)
            .ok()
            .filter(|&start| start < bytes.len())
            .ok_or_else(|| ObjectStoreError::InvalidRange {
                key: key.to_owned(),
                start: range.start,
                end: range.end,
            })?;
        let end = usize::try_from(range.end).map_or(bytes.len(), |end| end.min(bytes.len()));
        Ok(bytes.slice(start..end))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        validate_key(key)?;
        let objects = self.objects.borrow();
        let object = objects.get(key).ok_or_else(|| not_found(key))?;
        Ok(ObjectMeta {
            size: object.bytes.len() as u64,
            etag: object.etag.clone(),
            last_modified: object.last_modified,
            metadata: object.metadata.clone(),
            crc32c: Some(object.crc32c),
        })
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        validate_delete(keys)?;
        let mut objects = self.objects.borrow_mut();
        let mut outcome = DeleteOutcome::default();
        for key in keys {
            match validate_key(key) {
                Ok(()) => {
                    objects.remove(key);
                }
                Err(error) => outcome.failed.push(DeleteFailure {
                    key: key.clone(),
                    error,
                }),
            }
        }
        Ok(outcome)
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        validate_prefix(request.prefix)?;
        // A continuation from another listing can sort before the prefix,
        // and the scan must still start inside it.
        let start = match request.continuation {
            Some(after) if after >= request.prefix => Bound::Excluded(after),
            _ => Bound::Included(request.prefix),
        };
        let objects = self.objects.borrow();
        let mut matching = objects
            .range::<str, _>((start, Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(request.prefix));
        let listed: Vec<ListedObject> = matching
            .by_ref()
            .take(request.page_len())
            .map(|(key, object)| ListedObject {
                key: key.clone(),
                size: object.bytes.len() as u64,
                etag: object.etag.clone(),
                last_modified: object.last_modified,
            })
            .collect();
        let next = matching
            .next()
            .and_then(|_| listed.last())
            .map(|last| last.key.clone());
        Ok(ListPage {
            objects: listed,
            next,
        })
    }
}

fn not_found(key: &str) -> ObjectStoreError {
    ObjectStoreError::NotFound {
        key: key.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::executor::block_on;

    use super::*;

    #[test]
    fn given_put_when_heading_should_stamp_current_time_and_new_etag() {
        let store = MemoryStore::new();
        let before = SystemTime::now();
        let put = |bytes: &'static [u8]| {
            block_on(store.put(
                "a/b",
                PutBody::from_bytes(Bytes::from_static(bytes)),
                &PutOptions::default(),
            ))
            .unwrap();
            block_on(store.head("a/b")).unwrap()
        };

        let first = put(b"first");
        let second = put(b"second");

        assert!(first.last_modified >= before);
        assert!(first.last_modified <= SystemTime::now() + Duration::from_secs(1));
        assert_ne!(first.etag, second.etag);
        assert_eq!(second.crc32c, Some(crc32c::crc32c(b"second")));
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
    }

    #[test]
    fn given_foreign_continuation_when_listing_should_stay_inside_prefix() {
        let store = MemoryStore::new();
        for key in ["a/1", "b/1", "b/2"] {
            block_on(store.put(
                key,
                PutBody::from_bytes(Bytes::from_static(b"x")),
                &PutOptions::default(),
            ))
            .unwrap();
        }

        let page = block_on(store.list(&ListRequest::new("b/").after("a/1"))).unwrap();

        let keys: Vec<&str> = page
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect();
        assert_eq!(keys, ["b/1", "b/2"]);
        assert_eq!(page.next, None);
    }
}
