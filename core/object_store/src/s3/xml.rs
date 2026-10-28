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

//! The S3 XML bodies the provider sends and parses.

use crate::{ListPage, ListedObject, ObjectStoreError};
use quick_xml::escape::escape;
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct ErrorBody {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    #[serde(default)]
    contents: Vec<Contents>,
    #[serde(default)]
    is_truncated: bool,
    next_continuation_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Contents {
    key: String,
    size: u64,
}

#[derive(Debug, Deserialize)]
struct DeleteResult {
    #[serde(default, rename = "Error")]
    errors: Vec<DeleteError>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteError {
    key: String,
    #[serde(default)]
    code: String,
}

/// Parses an error body, which HEAD responses and some proxies leave empty.
pub(crate) fn parse_error(body: &[u8]) -> ErrorBody {
    std::str::from_utf8(body)
        .ok()
        .and_then(|text| quick_xml::de::from_str(text).ok())
        .unwrap_or_default()
}

pub(crate) fn parse_list(body: &[u8]) -> Result<ListPage, ObjectStoreError> {
    let result: ListBucketResult = parse(body)?;
    let next = match (result.is_truncated, result.next_continuation_token) {
        (false, _) => None,
        (true, Some(token)) => Some(token),
        (true, None) => {
            return Err(ObjectStoreError::Malformed(
                "truncated listing without a continuation token".to_owned(),
            ));
        }
    };
    let objects = result
        .contents
        .into_iter()
        .map(|contents| ListedObject {
            key: contents.key,
            size: contents.size,
        })
        .collect();
    Ok(ListPage { objects, next })
}

pub(crate) fn parse_delete(body: &[u8]) -> Result<(), ObjectStoreError> {
    let result: DeleteResult = parse(body)?;
    match result.errors.first() {
        None => Ok(()),
        Some(first) => Err(ObjectStoreError::DeleteFailed {
            failed: result.errors.len(),
            key: first.key.clone(),
            code: first.code.clone(),
        }),
    }
}

/// A quiet multi-object delete: the response lists only the keys that failed.
pub(crate) fn delete_request(keys: &[String]) -> String {
    let mut body = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Quiet>true</Quiet>"#,
    );
    for key in keys {
        body.push_str("<Object><Key>");
        body.push_str(&escape(key.as_str()));
        body.push_str("</Key></Object>");
    }
    body.push_str("</Delete>");
    body
}

fn parse<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, ObjectStoreError> {
    let text = std::str::from_utf8(body)
        .map_err(|error| ObjectStoreError::Malformed(error.to_string()))?;
    quick_xml::de::from_str(text).map_err(|error| ObjectStoreError::Malformed(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_truncated_listing_when_parsing_should_return_keys_and_token() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name><Prefix>a/</Prefix><KeyCount>2</KeyCount><MaxKeys>2</MaxKeys>
  <IsTruncated>true</IsTruncated>
  <Contents><Key>a/1&amp;2.log</Key><LastModified>2026-10-28T00:00:00.000Z</LastModified>
    <ETag>"x"</ETag><Size>10</Size><StorageClass>STANDARD</StorageClass></Contents>
  <Contents><Key>a/3.log</Key><Size>0</Size></Contents>
  <NextContinuationToken>token/+=</NextContinuationToken>
</ListBucketResult>"#;
        let page = parse_list(body).unwrap();
        assert_eq!(
            page.objects,
            vec![
                ListedObject {
                    key: "a/1&2.log".to_owned(),
                    size: 10
                },
                ListedObject {
                    key: "a/3.log".to_owned(),
                    size: 0
                },
            ]
        );
        assert_eq!(page.next.as_deref(), Some("token/+="));
    }

    #[test]
    fn given_empty_listing_when_parsing_should_return_no_keys() {
        let body = br#"<ListBucketResult><Name>bucket</Name><KeyCount>0</KeyCount>
<IsTruncated>false</IsTruncated></ListBucketResult>"#;
        assert_eq!(parse_list(body).unwrap(), ListPage::default());
    }

    #[test]
    fn given_truncated_listing_without_token_when_parsing_should_fail() {
        let body = b"<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>";
        assert!(matches!(
            parse_list(body),
            Err(ObjectStoreError::Malformed(_))
        ));
    }

    #[test]
    fn given_delete_errors_when_parsing_should_report_first_failure() {
        let body = br#"<DeleteResult>
  <Error><Key>a</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error>
  <Error><Key>b</Key><Code>InternalError</Code></Error>
</DeleteResult>"#;
        match parse_delete(body) {
            Err(ObjectStoreError::DeleteFailed { failed, key, code }) => {
                assert_eq!(
                    (failed, key.as_str(), code.as_str()),
                    (2, "a", "AccessDenied")
                );
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(parse_delete(b"<DeleteResult></DeleteResult>").is_ok());
    }

    #[test]
    fn given_key_with_markup_when_building_delete_should_escape_it() {
        let body = delete_request(&["a<b>&c".to_owned()]);
        assert!(body.contains("<Key>a&lt;b&gt;&amp;c</Key>"));
    }

    #[test]
    fn given_error_body_when_parsing_should_read_code_and_tolerate_empty() {
        let error = parse_error(b"<Error><Code>NoSuchKey</Code><Message>gone</Message></Error>");
        assert_eq!(
            (error.code.as_str(), error.message.as_str()),
            ("NoSuchKey", "gone")
        );
        assert_eq!(parse_error(b"").code, "");
    }
}
