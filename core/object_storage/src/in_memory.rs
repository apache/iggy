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

//! In-memory [`ObjectStore`] test fixture. Models the two load-bearing invariants the tiered
//! design needs:
//! * atomic publish (an object appears whole or not at all)
//! * out-of-range reads that *error* rather than silently clamp
//! * [`ConditionalWriteSupport::BestEffort`] mode exercises the non-atomic fencing branch

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::Range;
use std::rc::Rc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

use crate::{
    ConditionalWriteSupport, MultipartUpload, ObjectMeta, ObjectStorageError, ObjectStore,
    PutOutcome,
};

type Store = Rc<RefCell<BTreeMap<String, Bytes>>>;

/// HashMap/BTreeMap-backed [`ObjectStore`]
#[derive(Debug, Clone)]
pub struct InMemoryStorage {
    objects: Store,
    conditional: ConditionalWriteSupport,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            objects: Rc::new(RefCell::new(BTreeMap::new())),
            conditional: ConditionalWriteSupport::Atomic,
        }
    }
    pub fn best_effort() -> Self {
        Self {
            objects: Rc::new(RefCell::new(BTreeMap::new())),
            conditional: ConditionalWriteSupport::BestEffort,
        }
    }
}

#[async_trait(?Send)]
impl ObjectStore for InMemoryStorage {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<PutOutcome, ObjectStorageError> {
        self.objects.borrow_mut().insert(key.to_owned(), bytes);
        Ok(PutOutcome::default())
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<PutOutcome, ObjectStorageError> {
        let mut objects = self.objects.borrow_mut();
        match self.conditional {
            ConditionalWriteSupport::Atomic => {
                if objects.contains_key(key) {
                    return Err(ObjectStorageError::AlreadyExists(key.to_owned()));
                }
                objects.insert(key.to_owned(), bytes);
            }
            // in a non-conditional backend the second create wins
            ConditionalWriteSupport::BestEffort => {
                objects.insert(key.to_owned(), bytes);
            }
        }
        Ok(PutOutcome::default())
    }

    async fn put_multipart(
        &self,
        key: &str,
        part_size: u64,
    ) -> Result<Box<dyn MultipartUpload>, ObjectStorageError> {
        if part_size < crate::S3_MIN_PART_SIZE {
            return Err(ObjectStorageError::PartSizeTooSmall {
                got: part_size,
                min: crate::S3_MIN_PART_SIZE,
            });
        }
        Ok(Box::new(InMemoryMultipart {
            objects: Rc::clone(&self.objects),
            key: key.to_owned(),
            parts: BytesMut::new(),
        }))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, ObjectStorageError> {
        let objects = self.objects.borrow();
        let object = objects
            .get(key)
            .ok_or_else(|| ObjectStorageError::NotFound(key.to_owned()))?;
        let size = object.len() as u64;
        if range.start > range.end || range.end > size {
            return Err(ObjectStorageError::RangeOutOfBounds {
                start: range.start,
                end: range.end,
                size,
            });
        }
        Ok(object.slice(range.start as usize..range.end as usize))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStorageError> {
        let objects = self.objects.borrow();
        let object = objects
            .get(key)
            .ok_or_else(|| ObjectStorageError::NotFound(key.to_owned()))?;
        Ok(ObjectMeta {
            key: key.to_owned(),
            size: object.len() as u64,
            etag: None,
        })
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ObjectStorageError> {
        let objects = self.objects.borrow();
        // Keys are sorted, so `take_while` stops at the first that no longer shares the prefix
        Ok(objects
            .range(prefix.to_owned()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, bytes)| ObjectMeta {
                key: key.clone(),
                size: bytes.len() as u64,
                etag: None,
            })
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStorageError> {
        self.objects.borrow_mut().remove(key);
        Ok(())
    }

    fn conditional_write_support(&self) -> ConditionalWriteSupport {
        self.conditional
    }
}

struct InMemoryMultipart {
    objects: Store,
    key: String,
    parts: BytesMut,
}

#[async_trait(?Send)]
impl MultipartUpload for InMemoryMultipart {
    async fn upload_part(&mut self, bytes: Bytes) -> Result<(), ObjectStorageError> {
        self.parts.extend_from_slice(&bytes);
        Ok(())
    }

    async fn complete(self: Box<Self>) -> Result<PutOutcome, ObjectStorageError> {
        self.objects
            .borrow_mut()
            .insert(self.key, self.parts.freeze());
        Ok(PutOutcome::default())
    }

    async fn abort(self: Box<Self>) -> Result<(), ObjectStorageError> {
        Ok(())
    }
}
