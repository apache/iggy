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

//! The XML bodies that the S3 provider sends and parses.

use std::time::SystemTime;

use chrono::DateTime;
use percent_encoding::percent_decode_str;
use quick_xml::escape::escape;
use serde::Deserialize;

use crate::error::ObjectStoreError;
use crate::store::{ListPage, ListedObject};

/// The code and the message of an S3 error body.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ErrorBody {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
}

/// A key that a batch delete did not delete, with the store's reason.
#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteError {
    pub key: String,
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    #[serde(default)]
    contents: Vec<Contents>,
    #[serde(default)]
    is_truncated: bool,
    next_continuation_token: Option<String>,
    encoding_type: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Contents {
    key: String,
    size: u64,
    #[serde(rename = "ETag", default)]
    etag: String,
    last_modified: String,
}

#[derive(Deserialize)]
struct DeleteResult {
    #[serde(default, rename = "Error")]
    errors: Vec<DeleteError>,
}

/// The error in `body`. A HEAD answer has no body, and a proxy can send one
/// that is not XML, so this never fails: what it cannot read stays empty.
pub fn parse_error(body: &[u8]) -> ErrorBody {
    std::str::from_utf8(body)
        .ok()
        .and_then(|text| quick_xml::de::from_str(text).ok())
        .unwrap_or_default()
}

/// One page of a `ListObjectsV2` answer.
///
/// # Errors
/// [`ObjectStoreError::Malformed`] for a body that is not such an answer.
pub fn parse_list(body: &[u8]) -> Result<ListPage, ObjectStoreError> {
    let result: ListBucketResult = parse(body)?;
    let next = match (result.is_truncated, result.next_continuation_token) {
        (false, _) => None,
        (true, Some(token)) => Some(token),
        (true, None) => {
            return Err(ObjectStoreError::Malformed(
                "a truncated listing without a continuation token".to_owned(),
            ));
        }
    };
    let url_encoded = result.encoding_type.as_deref() == Some("url");
    let objects = result
        .contents
        .into_iter()
        .map(|contents| {
            Ok(ListedObject {
                key: if url_encoded {
                    decode_url_key(&contents.key)?
                } else {
                    contents.key
                },
                size: contents.size,
                etag: unquote_etag(&contents.etag).to_owned(),
                last_modified: parse_timestamp(&contents.last_modified)?,
            })
        })
        .collect::<Result<_, ObjectStoreError>>()?;
    Ok(ListPage { objects, next })
}

/// The keys of a `DeleteObjects` answer that were not deleted.
///
/// # Errors
/// [`ObjectStoreError::Malformed`] for a body that is not such an answer.
pub fn parse_delete(body: &[u8]) -> Result<Vec<DeleteError>, ObjectStoreError> {
    let result: DeleteResult = parse(body)?;
    Ok(result.errors)
}

/// The body of a quiet `DeleteObjects` request: the answer lists only the
/// keys that were not deleted.
pub fn delete_request<'a>(keys: impl IntoIterator<Item = &'a str>) -> String {
    let mut body = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Quiet>true</Quiet>"#,
    );
    for key in keys {
        body.push_str("<Object><Key>");
        body.push_str(&escape(key));
        body.push_str("</Key></Object>");
    }
    body.push_str("</Delete>");
    body
}

/// An etag as the store quotes it in headers and listings, without the
/// quotes, so that every call reports the same value.
pub fn unquote_etag(etag: &str) -> &str {
    etag.trim_matches('"')
}

fn parse<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, ObjectStoreError> {
    let text = std::str::from_utf8(body)
        .map_err(|error| ObjectStoreError::Malformed(error.to_string()))?;
    quick_xml::de::from_str(text).map_err(|error| ObjectStoreError::Malformed(error.to_string()))
}

/// A key of a listing with `encoding-type=url`. S3 encodes it as a form
/// value, so a space comes as `+` and a `+` as `%2B`.
fn decode_url_key(encoded: &str) -> Result<String, ObjectStoreError> {
    percent_decode_str(&encoded.replace('+', " "))
        .decode_utf8()
        .map(Into::into)
        .map_err(|_| ObjectStoreError::Malformed(format!("listed key {encoded:?} is not UTF-8")))
}

fn parse_timestamp(text: &str) -> Result<SystemTime, ObjectStoreError> {
    DateTime::parse_from_rfc3339(text)
        .map(SystemTime::from)
        .map_err(|error| ObjectStoreError::Malformed(format!("timestamp {text:?}: {error}")))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

    fn at(seconds: u64, millis: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(seconds) + Duration::from_millis(millis)
    }

    #[test]
    fn given_truncated_listing_when_parsing_should_return_objects_and_token() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name><Prefix>a/</Prefix><KeyCount>2</KeyCount><MaxKeys>2</MaxKeys>
  <IsTruncated>true</IsTruncated>
  <Contents><Key>a/1&amp;2.log</Key><LastModified>2026-10-28T00:00:00.000Z</LastModified>
    <ETag>&quot;x&quot;</ETag><Size>10</Size><StorageClass>STANDARD</StorageClass></Contents>
  <Contents><Key>a/3.log</Key><LastModified>2026-10-28T00:00:01.500Z</LastModified>
    <ETag>"y"</ETag><Size>0</Size></Contents>
  <NextContinuationToken>token/+=</NextContinuationToken>
</ListBucketResult>"#;

        let page = parse_list(body).unwrap();

        assert_eq!(
            page.objects,
            vec![
                ListedObject {
                    key: "a/1&2.log".to_owned(),
                    size: 10,
                    etag: "x".to_owned(),
                    last_modified: at(1_793_145_600, 0),
                },
                ListedObject {
                    key: "a/3.log".to_owned(),
                    size: 0,
                    etag: "y".to_owned(),
                    last_modified: at(1_793_145_601, 500),
                },
            ]
        );
        assert_eq!(page.next.as_deref(), Some("token/+="));
    }

    #[test]
    fn given_url_encoded_listing_when_parsing_should_decode_each_key() {
        let body = br#"<ListBucketResult><IsTruncated>false</IsTruncated>
  <Contents><Key>a/space+in+name%2Bplus/caf%C3%A9</Key>
    <LastModified>2026-10-28T00:00:00Z</LastModified><ETag>"x"</ETag><Size>1</Size></Contents>
  <EncodingType>url</EncodingType>
</ListBucketResult>"#;

        let page = parse_list(body).unwrap();

        assert_eq!(page.objects[0].key, "a/space in name+plus/caf\u{e9}");
        assert_eq!(page.next, None);
    }

    #[test]
    fn given_empty_listing_when_parsing_should_return_no_objects() {
        let body = br"<ListBucketResult><Name>bucket</Name><KeyCount>0</KeyCount>
<IsTruncated>false</IsTruncated></ListBucketResult>";

        assert_eq!(parse_list(body).unwrap(), ListPage::default());
    }

    #[test]
    fn given_broken_listing_when_parsing_should_fail_as_malformed() {
        for body in [
            &b"<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>"[..],
            b"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>a</Key>\
              <LastModified>yesterday</LastModified><Size>1</Size></Contents></ListBucketResult>",
            b"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>%FF</Key>\
              <LastModified>2026-10-28T00:00:00Z</LastModified><Size>1</Size></Contents>\
              <EncodingType>url</EncodingType></ListBucketResult>",
            b"not xml",
        ] {
            assert!(
                matches!(parse_list(body), Err(ObjectStoreError::Malformed(_))),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn given_delete_answer_when_parsing_should_return_only_the_failed_keys() {
        let body = br"<DeleteResult>
  <Deleted><Key>gone</Key></Deleted>
  <Error><Key>a&lt;b</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error>
  <Deleted><Key>also gone</Key></Deleted>
  <Error><Key>c</Key><Code>InternalError</Code></Error>
</DeleteResult>";

        let failed = parse_delete(body).unwrap();

        assert_eq!(
            failed,
            vec![
                DeleteError {
                    key: "a<b".to_owned(),
                    code: "AccessDenied".to_owned(),
                    message: "Access Denied".to_owned(),
                },
                DeleteError {
                    key: "c".to_owned(),
                    code: "InternalError".to_owned(),
                    message: String::new(),
                },
            ]
        );
        assert_eq!(parse_delete(b"<DeleteResult></DeleteResult>").unwrap(), []);
    }

    #[test]
    fn given_key_with_markup_when_building_delete_should_escape_it() {
        let body = delete_request(["a<b>&c", "d\"e'f"]);

        assert!(body.contains("<Quiet>true</Quiet>"));
        assert!(body.contains("<Key>a&lt;b&gt;&amp;c</Key>"));
        assert!(body.contains("<Key>d&quot;e&apos;f</Key>"));
    }

    #[test]
    fn given_error_body_when_parsing_should_read_code_and_tolerate_anything_else() {
        let error = parse_error(b"<Error><Code>NoSuchKey</Code><Message>gone</Message></Error>");

        assert_eq!(
            (error.code.as_str(), error.message.as_str()),
            ("NoSuchKey", "gone")
        );
        assert_eq!(parse_error(b"").code, "");
        assert_eq!(parse_error(b"<html>bad gateway</html>").code, "");
    }
}
