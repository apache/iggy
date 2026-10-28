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

//! AWS Signature Version 4 in the `Authorization` header, for S3. Pure functions of the request
//! and the clock, so the tests pin them to AWS's published examples.

use chrono::{DateTime, Utc};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use ring::{digest, hmac};
use std::fmt;

/// SigV4 leaves letters, digits and `-._~` unencoded.
const UNRESERVED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');
/// S3 paths are encoded once and keep `/` between key segments.
const PATH: &AsciiSet = &UNRESERVED.remove(b'/');

pub(crate) const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub(crate) const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

pub(crate) struct SigningRequest<'a> {
    pub method: &'a str,
    /// Already encoded with [`encode_path`], exactly as sent.
    pub path: &'a str,
    /// Already canonical, from [`canonical_query`], exactly as sent.
    pub query: &'a str,
    /// Every header to sign with a lowercase name, including `host`, `x-amz-date` and
    /// `x-amz-content-sha256`.
    pub headers: &'a [(String, String)],
    pub payload_sha256: &'a str,
}

pub(crate) fn encode_path(path: &str) -> String {
    utf8_percent_encode(path, PATH).to_string()
}

/// Sorted and encoded `name=value` pairs, used both in the URL and in the signature.
pub(crate) fn canonical_query(query: &[(&str, &str)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(name, value)| {
            (
                utf8_percent_encode(name, UNRESERVED).to_string(),
                utf8_percent_encode(value, UNRESERVED).to_string(),
            )
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

pub(crate) fn amz_date(time: DateTime<Utc>) -> String {
    time.format("%Y%m%dT%H%M%SZ").to_string()
}

pub(crate) fn sha256_hex(data: &[u8]) -> String {
    hex::encode(digest::digest(&digest::SHA256, data))
}

/// The `Authorization` header value. `time` must be the one sent in `x-amz-date`.
pub(crate) fn authorization(
    request: &SigningRequest<'_>,
    credentials: &Credentials,
    region: &str,
    time: DateTime<Utc>,
) -> String {
    let amz_date = amz_date(time);
    let date = &amz_date[..8];
    let mut headers: Vec<(&str, String)> = request
        .headers
        .iter()
        .map(|(name, value)| (name.as_str(), canonical_header_value(value)))
        .collect();
    headers.sort_by(|left, right| left.0.cmp(right.0));
    let signed_headers = headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();
    let canonical_request = format!(
        "{}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{}",
        request.method, request.path, request.query, request.payload_sha256
    );
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key = [region, "s3", "aws4_request"].iter().fold(
        hmac_sha256(
            format!("AWS4{}", credentials.secret_access_key).as_bytes(),
            date.as_bytes(),
        ),
        |key, part| hmac_sha256(&key, part.as_bytes()),
    );
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        credentials.access_key_id
    )
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
        .as_ref()
        .to_vec()
}

fn canonical_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    // The examples in AWS's "Signature Calculations for the Authorization Header: Transferring
    // Payload in a Single Chunk" page.
    fn example_credentials() -> Credentials {
        Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_owned(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_owned(),
            session_token: None,
        }
    }

    fn example_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap()
    }

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn signature(authorization: &str) -> &str {
        authorization.rsplit("Signature=").next().unwrap()
    }

    #[test]
    fn given_aws_get_object_example_when_signing_should_match_published_signature() {
        let headers = headers(&[
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", EMPTY_PAYLOAD_SHA256),
            ("x-amz-date", "20130524T000000Z"),
        ]);
        let request = SigningRequest {
            method: "GET",
            path: &encode_path("/test.txt"),
            query: "",
            headers: &headers,
            payload_sha256: EMPTY_PAYLOAD_SHA256,
        };
        let authorization = authorization(
            &request,
            &example_credentials(),
            "us-east-1",
            example_time(),
        );
        assert!(authorization.contains("SignedHeaders=host;range;x-amz-content-sha256;x-amz-date"));
        assert_eq!(
            signature(&authorization),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn given_aws_put_object_example_when_signing_should_match_published_signature() {
        let payload_sha256 = sha256_hex(b"Welcome to Amazon S3.");
        let headers = headers(&[
            ("date", "Fri, 24 May 2013 00:00:00 GMT"),
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", &payload_sha256),
            ("x-amz-date", "20130524T000000Z"),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
        ]);
        let path = encode_path("/test$file.text");
        assert_eq!(path, "/test%24file.text");
        let request = SigningRequest {
            method: "PUT",
            path: &path,
            query: "",
            headers: &headers,
            payload_sha256: &payload_sha256,
        };
        let authorization = authorization(
            &request,
            &example_credentials(),
            "us-east-1",
            example_time(),
        );
        assert_eq!(
            signature(&authorization),
            "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
    }

    #[test]
    fn given_aws_list_objects_example_when_signing_should_match_published_signature() {
        let headers = headers(&[
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", EMPTY_PAYLOAD_SHA256),
            ("x-amz-date", "20130524T000000Z"),
        ]);
        let query = canonical_query(&[("prefix", "J"), ("max-keys", "2")]);
        assert_eq!(query, "max-keys=2&prefix=J");
        let request = SigningRequest {
            method: "GET",
            path: "/",
            query: &query,
            headers: &headers,
            payload_sha256: EMPTY_PAYLOAD_SHA256,
        };
        let authorization = authorization(
            &request,
            &example_credentials(),
            "us-east-1",
            example_time(),
        );
        assert_eq!(
            signature(&authorization),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    #[test]
    fn given_reserved_characters_when_encoding_should_keep_only_unreserved_and_path_slashes() {
        assert_eq!(encode_path("/a b/c+d/e~f"), "/a%20b/c%2Bd/e~f");
        assert_eq!(
            canonical_query(&[("prefix", "a/b c"), ("continuation-token", "x=y")]),
            "continuation-token=x%3Dy&prefix=a%2Fb%20c"
        );
    }
}
