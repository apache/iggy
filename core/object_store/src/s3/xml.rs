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
use quick_xml::Reader;
use quick_xml::escape::escape;
use quick_xml::events::Event;
use serde::Deserialize;

use crate::error::ObjectStoreError;
use crate::store::{ListPage, ListedObject, StoredPart};

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

#[derive(Deserialize)]
struct InitiateMultipartUploadResult {
    #[serde(rename = "UploadId")]
    id: String,
}

#[derive(Deserialize)]
struct CompleteMultipartUploadResult {
    #[serde(rename = "ETag", default)]
    etag: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListMultipartUploadsResult {
    #[serde(default, rename = "Upload")]
    uploads: Vec<ListedUpload>,
    #[serde(default)]
    is_truncated: bool,
    next_key_marker: Option<String>,
    next_upload_id_marker: Option<String>,
    encoding_type: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListedUpload {
    key: String,
    upload_id: String,
    initiated: String,
}

/// One multipart upload in progress, as a listing of them reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingUpload {
    pub key: String,
    pub upload_id: String,
    pub initiated: SystemTime,
}

/// One page of a `ListMultipartUploads` answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadsPage {
    pub uploads: Vec<PendingUpload>,
    /// The key marker and the upload id marker of the next page, `None` on
    /// the last one.
    pub next: Option<(String, String)>,
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

/// The upload id of a `CreateMultipartUpload` answer.
///
/// # Errors
/// [`ObjectStoreError::Malformed`] for a body that is not such an answer.
pub fn parse_initiate(body: &[u8]) -> Result<String, ObjectStoreError> {
    let result: InitiateMultipartUploadResult = parse(body)?;
    if result.id.is_empty() {
        return Err(ObjectStoreError::Malformed(
            "a multipart upload without an upload id".to_owned(),
        ));
    }
    Ok(result.id)
}

/// The body of a `CompleteMultipartUpload` request: each part with its
/// number, its etag and its CRC32C, in order.
pub fn complete_request(parts: &[StoredPart], encode_crc32c: impl Fn(u32) -> String) -> String {
    let mut body = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
    );
    for part in parts {
        body.push_str("<Part><PartNumber>");
        body.push_str(&part.number.to_string());
        body.push_str("</PartNumber><ETag>\"");
        body.push_str(&escape(unquote_etag(&part.tag)));
        body.push_str("\"</ETag><ChecksumCRC32C>");
        body.push_str(&encode_crc32c(part.crc32c));
        body.push_str("</ChecksumCRC32C></Part>");
    }
    body.push_str("</CompleteMultipartUpload>");
    body
}

/// What a `CompleteMultipartUpload` answer of 200 holds. The store sends its
/// status before it finishes the work, so a failure comes in the body of a
/// 200.
#[derive(Debug)]
pub enum Completion {
    /// The store created the object.
    Created,
    /// The store failed after it sent the status.
    Failed(ErrorBody),
}

/// What a `CompleteMultipartUpload` answer of 200 holds, by its root element:
/// a result with the object's etag, or an error.
///
/// # Errors
/// [`ObjectStoreError::Malformed`] for a body that is neither, an empty one
/// included.
pub fn parse_complete(body: &[u8]) -> Result<Completion, ObjectStoreError> {
    match root_name(body)?.as_str() {
        "CompleteMultipartUploadResult" => {
            let result: CompleteMultipartUploadResult = parse(body)?;
            if result.etag.is_empty() {
                return Err(ObjectStoreError::Malformed(
                    "a completed multipart upload without an etag".to_owned(),
                ));
            }
            Ok(Completion::Created)
        }
        "Error" => Ok(Completion::Failed(parse_error(body))),
        root => Err(ObjectStoreError::Malformed(format!(
            "a completion answered with a {root:?} root"
        ))),
    }
}

/// One page of a `ListMultipartUploads` answer.
///
/// # Errors
/// [`ObjectStoreError::Malformed`] for a body that is not such an answer.
pub fn parse_uploads(body: &[u8]) -> Result<UploadsPage, ObjectStoreError> {
    let result: ListMultipartUploadsResult = parse(body)?;
    let url_encoded = result.encoding_type.as_deref() == Some("url");
    let decode = |key: String| {
        if url_encoded {
            decode_url_key(&key)
        } else {
            Ok(key)
        }
    };
    let next = match (
        result.is_truncated,
        result.next_key_marker,
        result.next_upload_id_marker,
    ) {
        (false, _, _) => None,
        (true, Some(key), Some(upload_id)) => Some((decode(key)?, upload_id)),
        (true, _, _) => {
            return Err(ObjectStoreError::Malformed(
                "a truncated listing of uploads without its markers".to_owned(),
            ));
        }
    };
    let uploads = result
        .uploads
        .into_iter()
        .map(|upload| {
            Ok(PendingUpload {
                key: decode(upload.key)?,
                upload_id: upload.upload_id,
                initiated: parse_timestamp(&upload.initiated)?,
            })
        })
        .collect::<Result<_, ObjectStoreError>>()?;
    Ok(UploadsPage { uploads, next })
}

/// An etag as the store quotes it in headers and listings, without the
/// quotes, so that every call reports the same value.
pub fn unquote_etag(etag: &str) -> &str {
    etag.trim_matches('"')
}

/// The local name of the root element of `body`.
fn root_name(body: &[u8]) -> Result<String, ObjectStoreError> {
    let text = std::str::from_utf8(body)
        .map_err(|error| ObjectStoreError::Malformed(error.to_string()))?;
    let mut reader = Reader::from_str(text);
    reader.config_mut().trim_text(true);
    loop {
        match reader.read_event() {
            Ok(Event::Start(root) | Event::Empty(root)) => {
                return Ok(root.local_name().as_ref().to_owned());
            }
            Ok(Event::Decl(_) | Event::Comment(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                return Err(ObjectStoreError::Malformed(
                    "an answer without a root element".to_owned(),
                ));
            }
            Ok(_) => {
                return Err(ObjectStoreError::Malformed(
                    "an answer with text before its root element".to_owned(),
                ));
            }
            Err(error) => return Err(ObjectStoreError::Malformed(error.to_string())),
        }
    }
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

    #[test]
    fn given_a_completion_result_with_an_etag_when_parsing_should_say_created() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?>

<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>https://segments.s3.amazonaws.com/a/b</Location><Bucket>segments</Bucket>
  <Key>a/b</Key><ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
</CompleteMultipartUploadResult>"#;

        assert!(matches!(parse_complete(body), Ok(Completion::Created)));
    }

    #[test]
    fn given_an_error_root_with_attributes_when_parsing_a_completion_should_return_its_code() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?>
<Error xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Code>InternalError</Code>
<Message>We encountered an internal error.</Message></Error>"#;

        let completion = parse_complete(body).unwrap();

        assert!(
            matches!(&completion, Completion::Failed(error) if error.code == "InternalError"),
            "{completion:?}"
        );
    }

    #[test]
    fn given_a_completion_answer_without_a_result_or_an_error_when_parsing_should_fail_as_malformed()
     {
        for body in [
            &b""[..],
            b"   \n",
            b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            b"<CompleteMultipartUploadResult/>",
            b"<CompleteMultipartUploadResult><Key>a/b</Key><ETag></ETag>\
              </CompleteMultipartUploadResult>",
            b"<html>bad gateway</html>",
            b"bad gateway",
            b"<CompleteMultipartUploadResult><ETag>x</ETag>",
            b"\xff\xfe",
        ] {
            assert!(
                matches!(parse_complete(body), Err(ObjectStoreError::Malformed(_))),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }
}
