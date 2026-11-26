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

//! The JSON bodies that the GCS provider sends and parses.

use std::fmt;
use std::time::SystemTime;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::DateTime;
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};

use crate::error::ObjectStoreError;
use crate::store::{ListedObject, ObjectMeta, UserMetadata};

/// The most bytes of an error body that an error message quotes when the
/// body is not the JSON error of the API.
const QUOTED_BODY_LEN: usize = 200;

/// The metadata part of an upload.
#[derive(Serialize)]
pub struct UploadMetadata<'a> {
    pub name: &'a str,
    #[serde(skip_serializing_if = "is_empty")]
    pub metadata: &'a UserMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crc32c: Option<String>,
}

/// The fields of an object resource that a head or a listing reads.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectResource {
    pub name: String,
    #[serde(deserialize_with = "number")]
    pub size: u64,
    #[serde(deserialize_with = "number")]
    pub generation: u64,
    /// When this generation of the object was written. `updated` moves on
    /// a change of metadata or of storage class, so an old object could read
    /// as a new one.
    pub time_created: String,
    #[serde(default)]
    pub crc32c: Option<String>,
    #[serde(default)]
    pub metadata: Option<UserMetadata>,
}

/// One page of a listing, as the API answers it.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectList {
    #[serde(default)]
    pub items: Vec<ObjectResource>,
    #[serde(default)]
    pub next_page_token: Option<String>,
}

/// What an error answer says: the reason of its first error and its
/// message. Both are empty for an answer without a body.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ErrorBody {
    pub reason: String,
    pub message: String,
}

#[derive(Deserialize)]
struct ErrorDocument {
    error: ErrorDetail,
}

#[derive(Deserialize)]
struct ErrorDetail {
    #[serde(default)]
    message: String,
    #[serde(default)]
    errors: Vec<ErrorItem>,
}

#[derive(Deserialize)]
struct ErrorItem {
    #[serde(default)]
    reason: String,
}

impl ObjectResource {
    /// What [`crate::ObjectStore::head`] reports. The etag is the
    /// generation, which a listing and a ranged read report too.
    pub fn meta(self) -> Result<ObjectMeta, ObjectStoreError> {
        Ok(ObjectMeta {
            size: self.size,
            etag: self.generation.to_string(),
            last_modified: parse_time(&self.time_created)?,
            crc32c: self.crc32c.as_deref().map(parse_crc32c).transpose()?,
            metadata: self.metadata.unwrap_or_default(),
        })
    }

    pub fn listed(self) -> Result<ListedObject, ObjectStoreError> {
        Ok(ListedObject {
            last_modified: parse_time(&self.time_created)?,
            etag: self.generation.to_string(),
            size: self.size,
            key: self.name,
        })
    }
}

pub fn parse_object(body: &[u8]) -> Result<ObjectResource, ObjectStoreError> {
    serde_json::from_slice(body)
        .map_err(|error| ObjectStoreError::Malformed(format!("an object resource: {error}")))
}

pub fn parse_list(body: &[u8]) -> Result<ObjectList, ObjectStoreError> {
    serde_json::from_slice(body)
        .map_err(|error| ObjectStoreError::Malformed(format!("an object listing: {error}")))
}

/// The error in an error answer: the JSON error of the API, or the start of
/// whatever else the body holds, such as the text of a failed media read.
pub fn parse_error(body: &[u8]) -> ErrorBody {
    if let Ok(document) = serde_json::from_slice::<ErrorDocument>(body) {
        return ErrorBody {
            reason: document
                .error
                .errors
                .into_iter()
                .next()
                .map(|item| item.reason)
                .unwrap_or_default(),
            message: document.error.message,
        };
    }
    let text = String::from_utf8_lossy(body);
    let text = text.trim();
    let end = text
        .char_indices()
        .nth(QUOTED_BODY_LEN)
        .map_or(text.len(), |(index, _)| index);
    ErrorBody {
        reason: String::new(),
        message: text[..end].to_owned(),
    }
}

/// The CRC32C of the API: the big-endian value in base64.
pub fn encode_crc32c(crc32c: u32) -> String {
    BASE64.encode(crc32c.to_be_bytes())
}

pub fn parse_crc32c(text: &str) -> Result<u32, ObjectStoreError> {
    let malformed = || ObjectStoreError::Malformed(format!("crc32c {text:?}"));
    let bytes = BASE64.decode(text).map_err(|_| malformed())?;
    let bytes: [u8; 4] = bytes.try_into().map_err(|_| malformed())?;
    Ok(u32::from_be_bytes(bytes))
}

fn parse_time(text: &str) -> Result<SystemTime, ObjectStoreError> {
    DateTime::parse_from_rfc3339(text)
        .map(SystemTime::from)
        .map_err(|_| ObjectStoreError::Malformed(format!("time {text:?}")))
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_empty(metadata: &&UserMetadata) -> bool {
    metadata.is_empty()
}

/// A 64-bit number that the API sends as a string, as JSON cannot hold
/// every one of them as a number. A plain number is taken too.
fn number<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    struct Number;

    impl Visitor<'_> for Number {
        type Value = u64;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an unsigned 64-bit number or a string that holds one")
        }

        fn visit_u64<E: de::Error>(self, value: u64) -> Result<u64, E> {
            Ok(value)
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<u64, E> {
            value.parse().map_err(E::custom)
        }
    }

    deserializer.deserialize_any(Number)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn given_object_resource_when_reading_should_report_generation_crc32c_and_metadata() {
        let resource = parse_object(
            br#"{"kind":"storage#object","name":"a/b","size":"11","generation":"1795094486939773",
               "timeCreated":"2026-10-28T00:00:00.500Z",
               "updated":"2026-11-02T00:00:00Z","crc32c":"yZRlqg==","etag":"CP2Q",
               "metadata":{"start-offset":"7"}}"#,
        )
        .unwrap();

        let meta = resource.meta().unwrap();

        assert_eq!(meta.size, 11);
        assert_eq!(meta.etag, "1795094486939773");
        assert_eq!(
            meta.last_modified,
            SystemTime::UNIX_EPOCH + Duration::from_millis(1_793_145_600_500)
        );
        assert_eq!(meta.crc32c, Some(crc32c::crc32c(b"hello world")));
        assert_eq!(
            meta.metadata,
            UserMetadata::from([("start-offset".to_owned(), "7".to_owned())])
        );
    }

    #[test]
    fn given_an_object_whose_storage_class_changed_when_reading_should_report_when_it_was_created()
    {
        let resource = br#"{"name":"a","size":"1","generation":"2","storageClass":"COLDLINE",
            "timeCreated":"2026-10-28T00:00:00Z","updated":"2026-11-28T00:00:00Z",
            "timeStorageClassUpdated":"2026-11-28T00:00:00Z"}"#;
        let created = SystemTime::UNIX_EPOCH + Duration::from_secs(1_793_145_600);

        let meta = parse_object(resource).unwrap().meta().unwrap();
        let listed = parse_list(&[br#"{"items":["#.as_slice(), resource, b"]}"].concat())
            .unwrap()
            .items
            .remove(0)
            .listed()
            .unwrap();

        assert_eq!(meta.last_modified, created);
        assert_eq!(listed.last_modified, created);
    }

    #[test]
    fn given_listing_when_reading_should_take_numbers_as_strings_or_numbers() {
        let list = parse_list(
            br#"{"items":[{"name":"a","size":1,"generation":2,"timeCreated":"2026-10-28T00:00:00Z"},
                          {"name":"b","size":"3","generation":"4","timeCreated":"2026-10-28T00:00:01Z"}],
                 "nextPageToken":"token"}"#,
        )
        .unwrap();
        let empty = parse_list(br#"{"kind":"storage#objects"}"#).unwrap();

        assert_eq!(list.next_page_token.as_deref(), Some("token"));
        let listed: Vec<_> = list
            .items
            .into_iter()
            .map(|item| item.listed().unwrap())
            .map(|object| (object.key, object.size, object.etag))
            .collect();
        assert_eq!(
            listed,
            [
                ("a".to_owned(), 1, "2".to_owned()),
                ("b".to_owned(), 3, "4".to_owned())
            ]
        );
        assert!(empty.items.is_empty() && empty.next_page_token.is_none());
        assert!(matches!(
            parse_list(br#"{"items":[{"name":"a","size":"-1"}]}"#),
            Err(ObjectStoreError::Malformed(_))
        ));
    }

    #[test]
    fn given_error_answers_when_parsing_should_take_the_reason_and_message() {
        let json = parse_error(
            br#"{"error":{"code":412,"message":"At least one of the pre-conditions you specified did not hold.","errors":[{"domain":"global","reason":"conditionNotMet"}]}}"#,
        );
        let text = parse_error(b"No such object: bucket/a\n");
        let long = parse_error("x".repeat(500).as_bytes());

        assert_eq!(json.reason, "conditionNotMet");
        assert!(json.message.starts_with("At least one"));
        assert_eq!(
            text,
            ErrorBody {
                reason: String::new(),
                message: "No such object: bucket/a".to_owned()
            }
        );
        assert_eq!(long.message.len(), QUOTED_BODY_LEN);
        assert_eq!(parse_error(b""), ErrorBody::default());
    }

    #[test]
    fn given_crc32c_when_encoding_should_round_trip_as_base64_big_endian() {
        assert_eq!(encode_crc32c(0xc994_65aa), "yZRlqg==");
        assert_eq!(parse_crc32c("yZRlqg==").unwrap(), 0xc994_65aa);
        assert!(parse_crc32c("yZRl").is_err());
        assert!(parse_crc32c("not base64").is_err());
    }

    #[test]
    fn given_upload_metadata_when_serializing_should_leave_out_what_is_unset() {
        let metadata = UserMetadata::from([("end-offset".to_owned(), "9".to_owned())]);
        let empty = UserMetadata::new();

        let full = serde_json::to_string(&UploadMetadata {
            name: "a/b",
            metadata: &metadata,
            crc32c: Some(encode_crc32c(1)),
        })
        .unwrap();
        let bare = serde_json::to_string(&UploadMetadata {
            name: "a/b",
            metadata: &empty,
            crc32c: None,
        })
        .unwrap();

        assert_eq!(
            full,
            r#"{"name":"a/b","metadata":{"end-offset":"9"},"crc32c":"AAAAAQ=="}"#
        );
        assert_eq!(bare, r#"{"name":"a/b"}"#);
    }
}
