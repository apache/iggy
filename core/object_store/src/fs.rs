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

//! A directory used as an object store, for Docker-free tests and for a store mounted from
//! another filesystem. Each object is one file: the payload, then a trailer holding the user
//! metadata and the payload's CRC32C, so one rename or link publishes both together. Keys map to
//! paths, so unlike S3 one key cannot be a directory of another (`a/b` and `a/b/c`).

use crate::body::read_piece;
use crate::offload::offload;
use crate::{
    ListPage, ListedObject, MAX_KEYS_PER_REQUEST, ObjectMeta, ObjectStore, ObjectStoreError,
    PutBody, PutOptions, validate_key, validate_metadata, validate_prefix,
};
use bytes::Bytes;
use compio::BufResult;
use compio::fs::{File, OpenOptions};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use std::io;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const TRAILER_MAGIC: [u8; 8] = *b"IGGYOBJ1";
/// Trailer length (u32) then the magic, at the very end of the file.
const FOOTER_LEN: u64 = 4 + TRAILER_MAGIC.len() as u64;

pub struct FsStore {
    root: PathBuf,
}

impl FsStore {
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self, ObjectStoreError> {
        let root = root.into();
        compio::fs::create_dir_all(&root)
            .await
            .map_err(ObjectStoreError::Io)?;
        Ok(Self { root })
    }

    fn path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    async fn write_temp(
        &self,
        temp: &Path,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(temp)
            .await
            .map_err(ObjectStoreError::Io)?;
        let mut checksum = 0;
        let mut written = 0;
        match body {
            PutBody::Bytes(bytes) => {
                checksum = crc32c::crc32c(&bytes);
                written = bytes.len() as u64;
                write_all_at(&mut file, bytes, 0).await?;
            }
            PutBody::File {
                file: source,
                offset,
                len,
                pacer,
            } => {
                while written < len {
                    let piece = read_piece(&source, offset, len, written, pacer.as_deref())
                        .await
                        .map_err(ObjectStoreError::Source)?;
                    checksum = crc32c::crc32c_append(checksum, &piece);
                    let piece_len = piece.len() as u64;
                    write_all_at(&mut file, piece, written).await?;
                    written += piece_len;
                }
            }
        }
        if options.crc32c.is_some_and(|expected| expected != checksum) {
            return Err(ObjectStoreError::ChecksumMismatch);
        }
        let trailer = encode_trailer(checksum, &options.metadata);
        write_all_at(&mut file, Bytes::from(trailer), written).await?;
        file.sync_all().await.map_err(ObjectStoreError::Io)
    }
}

impl ObjectStore for FsStore {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        validate_key(key)?;
        validate_metadata(&options.metadata)?;
        let path = self.path(key);
        let directory = parent(&path)?;
        compio::fs::create_dir_all(directory)
            .await
            .map_err(ObjectStoreError::Io)?;
        let temp = directory.join(format!(
            ".{}.{:016x}.tmp",
            file_name(&path)?,
            rand::random::<u64>()
        ));
        // Temp names are never listed, so a removal that fails below leaks space, not an object.
        if let Err(error) = self.write_temp(&temp, body, options).await {
            let _ = compio::fs::remove_file(&temp).await;
            return Err(error);
        }
        let published = if options.if_absent {
            // A link refuses an existing name where a rename would replace it.
            let linked = compio::fs::hard_link(&temp, &path).await;
            let _ = compio::fs::remove_file(&temp).await;
            linked.map_err(|error| match error.kind() {
                io::ErrorKind::AlreadyExists => ObjectStoreError::PreconditionFailed,
                _ => ObjectStoreError::Io(error),
            })
        } else {
            let renamed = compio::fs::rename(&temp, &path).await;
            if renamed.is_err() {
                let _ = compio::fs::remove_file(&temp).await;
            }
            renamed.map_err(ObjectStoreError::Io)
        };
        published?;
        sync_directory(directory).await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, ObjectStoreError> {
        validate_key(key)?;
        if range.is_empty() {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "empty range {range:?}"
            )));
        }
        let file = open_object(&self.path(key)).await?;
        let (size, _) = read_footer(&file).await?;
        if range.start >= size {
            return Err(ObjectStoreError::InvalidRange);
        }
        let len = (range.end.min(size) - range.start) as usize;
        let BufResult(result, buffer) = file
            .read_exact_at(Vec::with_capacity(len), range.start)
            .await;
        result.map_err(ObjectStoreError::Io)?;
        Ok(Bytes::from(buffer))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        validate_key(key)?;
        let file = open_object(&self.path(key)).await?;
        let (size, trailer_len) = read_footer(&file).await?;
        let BufResult(result, trailer) = file
            .read_exact_at(Vec::with_capacity(trailer_len as usize), size)
            .await;
        result.map_err(ObjectStoreError::Io)?;
        let (checksum, mut metadata) = decode_trailer(&trailer)?;
        metadata.sort();
        Ok(ObjectMeta {
            size,
            etag: format!("{checksum:08x}"),
            metadata,
        })
    }

    async fn delete(&self, keys: &[String]) -> Result<(), ObjectStoreError> {
        if keys.len() > MAX_KEYS_PER_REQUEST {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "{} keys in one delete, at most {MAX_KEYS_PER_REQUEST}",
                keys.len()
            )));
        }
        for key in keys {
            validate_key(key)?;
        }
        for key in keys {
            match compio::fs::remove_file(self.path(key)).await {
                Err(error) if error.kind() != io::ErrorKind::NotFound => {
                    return Err(ObjectStoreError::Io(error));
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        continuation: Option<&str>,
    ) -> Result<ListPage, ObjectStoreError> {
        validate_prefix(prefix)?;
        let root = self.root.clone();
        let prefix = prefix.to_owned();
        let continuation = continuation.map(str::to_owned);
        offload("iggy-object-store-list", move || {
            list_blocking(&root, &prefix, continuation.as_deref())
        })
        .await
        .map_err(ObjectStoreError::Io)?
    }
}

async fn write_all_at(
    file: &mut File,
    bytes: Bytes,
    position: u64,
) -> Result<(), ObjectStoreError> {
    let BufResult(result, _) = file.write_all_at(bytes, position).await;
    result.map_err(ObjectStoreError::Io)
}

async fn sync_directory(directory: &Path) -> Result<(), ObjectStoreError> {
    let directory = File::open(directory).await.map_err(ObjectStoreError::Io)?;
    directory.sync_all().await.map_err(ObjectStoreError::Io)
}

async fn open_object(path: &Path) -> Result<File, ObjectStoreError> {
    File::open(path).await.map_err(|error| match error.kind() {
        io::ErrorKind::NotFound => ObjectStoreError::NotFound,
        _ => ObjectStoreError::Io(error),
    })
}

/// Returns the payload size and the trailer length.
async fn read_footer(file: &File) -> Result<(u64, u64), ObjectStoreError> {
    let file_len = file.metadata().await.map_err(ObjectStoreError::Io)?.len();
    if file_len < FOOTER_LEN {
        return Err(ObjectStoreError::Malformed(
            "object file has no footer".to_owned(),
        ));
    }
    let BufResult(result, footer) = file
        .read_exact_at(
            Vec::with_capacity(FOOTER_LEN as usize),
            file_len - FOOTER_LEN,
        )
        .await;
    result.map_err(ObjectStoreError::Io)?;
    parse_footer(file_len, &footer)
}

fn parse_footer(file_len: u64, footer: &[u8]) -> Result<(u64, u64), ObjectStoreError> {
    let (len, magic) = footer.split_at(4);
    if magic != TRAILER_MAGIC {
        return Err(ObjectStoreError::Malformed(
            "object file has no trailer magic".to_owned(),
        ));
    }
    let trailer_len = u64::from(u32::from_le_bytes(len.try_into().map_err(|_| {
        ObjectStoreError::Malformed("object footer is truncated".to_owned())
    })?));
    file_len
        .checked_sub(FOOTER_LEN + trailer_len)
        .map(|size| (size, trailer_len))
        .ok_or_else(|| {
            ObjectStoreError::Malformed("object trailer is longer than the file".to_owned())
        })
}

/// CRC32C (u32), entry count (u16), then per entry a u16 length and the bytes of the key and of
/// the value, followed by the footer. Metadata validation bounds every length well below u16.
fn encode_trailer(checksum: u32, metadata: &[(String, String)]) -> Vec<u8> {
    let mut trailer = Vec::with_capacity(64);
    trailer.extend_from_slice(&checksum.to_le_bytes());
    trailer.extend_from_slice(&(metadata.len() as u16).to_le_bytes());
    for (key, value) in metadata {
        for field in [key, value] {
            trailer.extend_from_slice(&(field.len() as u16).to_le_bytes());
            trailer.extend_from_slice(field.as_bytes());
        }
    }
    let trailer_len = trailer.len() as u32;
    trailer.extend_from_slice(&trailer_len.to_le_bytes());
    trailer.extend_from_slice(&TRAILER_MAGIC);
    trailer
}

fn decode_trailer(trailer: &[u8]) -> Result<(u32, Vec<(String, String)>), ObjectStoreError> {
    let mut reader = TrailerReader(trailer);
    let checksum = u32::from_le_bytes(reader.take_array()?);
    let count = u16::from_le_bytes(reader.take_array()?);
    let mut metadata = Vec::with_capacity(count as usize);
    for _ in 0..count {
        metadata.push((reader.take_string()?, reader.take_string()?));
    }
    if !reader.0.is_empty() {
        return Err(ObjectStoreError::Malformed(
            "object trailer has trailing bytes".to_owned(),
        ));
    }
    Ok((checksum, metadata))
}

struct TrailerReader<'a>(&'a [u8]);

impl TrailerReader<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8], ObjectStoreError> {
        if self.0.len() < len {
            return Err(ObjectStoreError::Malformed(
                "object trailer is truncated".to_owned(),
            ));
        }
        let (taken, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(taken)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], ObjectStoreError> {
        let mut array = [0; N];
        array.copy_from_slice(self.take(N)?);
        Ok(array)
    }

    fn take_string(&mut self) -> Result<String, ObjectStoreError> {
        let len = u16::from_le_bytes(self.take_array()?) as usize;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| ObjectStoreError::Malformed("object metadata is not UTF-8".to_owned()))
    }
}

fn list_blocking(
    root: &Path,
    prefix: &str,
    continuation: Option<&str>,
) -> Result<ListPage, ObjectStoreError> {
    let base = match prefix.rfind('/') {
        Some(end) => root.join(&prefix[..end]),
        None => root.to_path_buf(),
    };
    let mut candidates = Vec::new();
    let mut directories = vec![base];
    while let Some(directory) = directories.pop() {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(ObjectStoreError::Io(error)),
        };
        for entry in entries {
            let entry = entry.map_err(ObjectStoreError::Io)?;
            let path = entry.path();
            // Dot-names are temp files, never objects: keys cannot contain them.
            let Some(key) = key_of(root, &path).filter(|key| !is_hidden(key)) else {
                continue;
            };
            if entry.file_type().map_err(ObjectStoreError::Io)?.is_dir() {
                let directory_prefix = format!("{key}/");
                if directory_prefix.starts_with(prefix) || prefix.starts_with(&directory_prefix) {
                    directories.push(path);
                }
            } else if key.starts_with(prefix)
                && continuation.is_none_or(|after| key.as_str() > after)
            {
                candidates.push((key, path));
            }
        }
    }
    candidates.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let truncated = candidates.len() > MAX_KEYS_PER_REQUEST;
    candidates.truncate(MAX_KEYS_PER_REQUEST);
    let mut objects = Vec::with_capacity(candidates.len());
    for (key, path) in candidates {
        let size = payload_size_blocking(&path)?;
        objects.push(ListedObject { key, size });
    }
    let next = truncated
        .then(|| objects.last().map(|object| object.key.clone()))
        .flatten();
    Ok(ListPage { objects, next })
}

fn payload_size_blocking(path: &Path) -> Result<u64, ObjectStoreError> {
    let file = std::fs::File::open(path).map_err(ObjectStoreError::Io)?;
    let file_len = file.metadata().map_err(ObjectStoreError::Io)?.len();
    if file_len < FOOTER_LEN {
        return Err(ObjectStoreError::Malformed(
            "object file has no footer".to_owned(),
        ));
    }
    let mut footer = [0; FOOTER_LEN as usize];
    file.read_exact_at(&mut footer, file_len - FOOTER_LEN)
        .map_err(ObjectStoreError::Io)?;
    parse_footer(file_len, &footer).map(|(size, _)| size)
}

fn key_of(root: &Path, path: &Path) -> Option<String> {
    let relative = path.strip_prefix(root).ok()?;
    let segments = relative
        .components()
        .map(|component| component.as_os_str().to_str())
        .collect::<Option<Vec<_>>>()?;
    Some(segments.join("/"))
}

fn is_hidden(key: &str) -> bool {
    key.rsplit('/')
        .next()
        .is_some_and(|name| name.starts_with('.'))
}

fn parent(path: &Path) -> Result<&Path, ObjectStoreError> {
    path.parent()
        .ok_or_else(|| ObjectStoreError::InvalidKey(path.display().to_string()))
}

fn file_name(path: &Path) -> Result<&str, ObjectStoreError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| ObjectStoreError::InvalidKey(path.display().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_metadata_when_encoding_trailer_should_round_trip() {
        let metadata = vec![
            ("end-offset".to_owned(), "99".to_owned()),
            ("start-offset".to_owned(), "0".to_owned()),
        ];
        let encoded = encode_trailer(0xdead_beef, &metadata);
        let file_len = 10 + encoded.len() as u64;
        let footer = &encoded[encoded.len() - FOOTER_LEN as usize..];
        let (size, trailer_len) = parse_footer(file_len, footer).unwrap();
        assert_eq!(size, 10);
        let trailer = &encoded[..trailer_len as usize];
        assert_eq!(decode_trailer(trailer).unwrap(), (0xdead_beef, metadata));
    }

    #[test]
    fn given_truncated_trailer_when_decoding_should_fail_as_malformed() {
        let encoded = encode_trailer(1, &[("a".to_owned(), "b".to_owned())]);
        let trailer = &encoded[..encoded.len() - FOOTER_LEN as usize - 1];
        assert!(matches!(
            decode_trailer(trailer),
            Err(ObjectStoreError::Malformed(_))
        ));
    }
}
