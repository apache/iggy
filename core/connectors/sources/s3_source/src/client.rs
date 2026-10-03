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

use std::{borrow::Cow, str::FromStr, time::Duration};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{
    Client,
    config::timeout::TimeoutConfig,
    error::{ProvideErrorMetadata, SdkError},
    operation::get_object::GetObjectOutput,
    types::EncodingType,
};
use iggy_connector_sdk::Error;
use percent_encoding::percent_decode_str;

use crate::{config::ResolvedConfig, state::ActiveObjectState};

pub(crate) async fn create_client(config: &ResolvedConfig) -> Client {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(region) = &config.region {
        loader = loader.region(Region::new(region.clone()));
    }
    let shared_config = loader.load().await;
    let mut builder = aws_sdk_s3::config::Builder::from(&shared_config)
        .force_path_style(config.path_style)
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(10))
                .operation_timeout(Duration::from_secs(60))
                .build(),
        );
    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint_url(endpoint);
    }
    Client::from_conf(builder.build())
}

pub(crate) async fn list_next_object(
    client: &Client,
    config: &ResolvedConfig,
    start_after: Option<&str>,
) -> Result<Option<ActiveObjectState>, Error> {
    let response = client
        .list_objects_v2()
        .bucket(&config.bucket)
        .set_prefix(config.prefix.clone())
        .set_start_after(start_after.map(str::to_owned))
        .encoding_type(EncodingType::Url)
        .max_keys(1)
        .send()
        .await
        .map_err(|error| request_error("list", &error))?;

    let Some(object) = response.contents().first() else {
        // Without a grouping delimiter an empty, truncated page cannot move
        // this key-based cursor forward. Do not mistake it for exhaustion.
        if response.is_truncated() == Some(true) {
            return Err(Error::HttpRequestFailed(
                "S3 returned an empty truncated listing".into(),
            ));
        }
        return Ok(None);
    };
    let key = object
        .key()
        .ok_or_else(|| invalid_response("missing key"))?;
    let key = if response.encoding_type() == Some(&EncodingType::Url) {
        percent_decode_str(key)
            .decode_utf8()
            .map_err(|_| invalid_response("key is not valid UTF-8"))?
    } else {
        Cow::Borrowed(key)
    };
    if start_after.is_some_and(|cursor| key.as_ref() <= cursor)
        || config
            .prefix
            .as_ref()
            .is_some_and(|prefix| !key.starts_with(prefix))
    {
        return Err(invalid_response("listing does not honor prefix/StartAfter"));
    }
    Ok(Some(ActiveObjectState {
        key: key.into_owned(),
        etag: object
            .e_tag()
            .ok_or_else(|| invalid_response("missing ETag"))?
            .to_owned(),
        size: object_size(object.size())?,
        next_byte_offset: 0,
    }))
}

pub(crate) async fn open_object(
    client: &Client,
    config: &ResolvedConfig,
    object: &mut ActiveObjectState,
) -> Result<GetObjectOutput, Error> {
    for attempt in 0..2 {
        let mut request = client
            .get_object()
            .bucket(&config.bucket)
            .key(&object.key)
            .if_match(&object.etag);
        if object.next_byte_offset > 0 && object.next_byte_offset < object.size {
            request = request.range(format!("bytes={}-", object.next_byte_offset));
        }
        match request.send().await {
            Ok(response) => {
                validate_response(&response, object)?;
                return Ok(response);
            }
            Err(error)
                if attempt == 0
                    && error
                        .raw_response()
                        .is_some_and(|response| response.status().as_u16() == 412) =>
            {
                // Refresh only this key. Listing after the cursor could skip a
                // deleted/replaced active object and lose acknowledged context.
                let metadata = client
                    .head_object()
                    .bucket(&config.bucket)
                    .key(&object.key)
                    .send()
                    .await
                    .map_err(|error| request_error("head", &error))?;
                object.etag = metadata
                    .e_tag()
                    .ok_or_else(|| invalid_response("missing ETag"))?
                    .to_owned();
                object.size = object_size(metadata.content_length())?;
                object.next_byte_offset = 0;
            }
            Err(error) => return Err(request_error("get", &error)),
        }
    }
    Err(Error::HttpRequestFailed(
        "S3 object changed repeatedly during selection".into(),
    ))
}

pub(crate) async fn validate_access(client: &Client, config: &ResolvedConfig) -> Result<(), Error> {
    if let Some(mut object) = list_next_object(client, config, None).await? {
        // GET, rather than HEAD alone, also checks permission to decrypt/read.
        // Drop the body: startup never consumes or checkpoints records.
        open_object(client, config, &mut object).await?;
    }
    Ok(())
}

fn validate_response(response: &GetObjectOutput, object: &ActiveObjectState) -> Result<(), Error> {
    if response.e_tag() != Some(object.etag.as_str()) {
        return Err(invalid_response("GET ETag differs from selected object"));
    }
    let start = if object.next_byte_offset < object.size {
        object.next_byte_offset
    } else {
        0
    };
    let length = object_size(response.content_length())?;
    if length != object.size - start {
        return Err(invalid_response(
            "GET Content-Length differs from expected length",
        ));
    }
    if start > 0 {
        let range: ContentRange = response
            .content_range()
            .ok_or_else(|| invalid_response("resumed GET is missing Content-Range"))?
            .parse()?;
        if range.start != start || range.end != object.size - 1 || range.total != object.size {
            return Err(invalid_response(
                "resumed GET has an incorrect Content-Range",
            ));
        }
    } else if response.content_range().is_some() {
        return Err(invalid_response("unexpected range on a full GET"));
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct ContentRange {
    start: u64,
    end: u64,
    total: u64,
}

impl FromStr for ContentRange {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || invalid_response("invalid Content-Range");
        let (range, total) = value
            .strip_prefix("bytes ")
            .ok_or_else(invalid)?
            .split_once('/')
            .ok_or_else(invalid)?;
        let (start, end) = range.split_once('-').ok_or_else(invalid)?;
        let range = Self {
            start: start.parse().map_err(|_| invalid())?,
            end: end.parse().map_err(|_| invalid())?,
            total: total.parse().map_err(|_| invalid())?,
        };
        if range.start > range.end || range.end >= range.total {
            return Err(invalid());
        }
        Ok(range)
    }
}

fn object_size(size: Option<i64>) -> Result<u64, Error> {
    u64::try_from(size.ok_or_else(|| invalid_response("missing size"))?)
        .map_err(|_| invalid_response("negative size"))
}

fn invalid_response(message: &str) -> Error {
    Error::PermanentHttpError(format!("Invalid S3 response: {message}"))
}

fn request_error<E: ProvideErrorMetadata>(operation: &str, error: &SdkError<E>) -> Error {
    let status = error
        .raw_response()
        .map(|response| response.status().as_u16());
    // Do not include SDK error details: requests can carry endpoint credentials.
    let category = match error {
        SdkError::ConstructionFailure(_) => "request construction",
        SdkError::TimeoutError(_) => "timeout",
        SdkError::DispatchFailure(_) => "connection",
        SdkError::ResponseError(_) => "invalid response",
        SdkError::ServiceError(_) => "service",
        _ => "unknown",
    };
    let mut message = match status {
        Some(status) => format!("S3 {operation} failed ({category}; HTTP status: {status})"),
        None => format!("S3 {operation} failed ({category}; no HTTP response)"),
    };
    if let Some(code) = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
    {
        // Compatible endpoints can return arbitrary text instead of a code.
        let safe_code = if !code.is_empty()
            && code.len() <= 64
            && code
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            code
        } else {
            "redacted"
        };
        message.push_str("; service code: ");
        message.push_str(safe_code);
    }
    if status
        .is_some_and(|status| (400..500).contains(&status) && ![408, 412, 429].contains(&status))
    {
        Error::PermanentHttpError(message)
    } else {
        Error::HttpRequestFailed(message)
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::{
        config::http::HttpResponse,
        error::{ConnectorError, ErrorMetadata},
        operation::get_object::GetObjectError,
    };

    use super::*;

    #[test]
    fn given_sdk_failures_when_reported_should_identify_category_without_exposing_details() {
        let sensitive = "https://user:secret@example.test/?token=private";
        let failures: [(SdkError<GetObjectError>, &str); 4] = [
            (
                SdkError::construction_failure(sensitive),
                "request construction",
            ),
            (SdkError::timeout_error(sensitive), "timeout"),
            (
                SdkError::dispatch_failure(ConnectorError::io(sensitive.into())),
                "connection",
            ),
            (
                SdkError::response_error(
                    sensitive,
                    HttpResponse::new(200.try_into().expect("status"), sensitive.into()),
                ),
                "invalid response",
            ),
        ];
        for (failure, category) in failures {
            let Error::HttpRequestFailed(message) = request_error("list", &failure) else {
                panic!("failure must keep its transient classification");
            };
            assert!(message.contains(category), "{message}");
            if failure.raw_response().is_none() {
                assert!(message.contains("no HTTP response"), "{message}");
            }
            assert!(!message.contains("secret"), "{message}");
            assert!(!message.contains("private"), "{message}");
            assert!(!message.contains("example.test"), "{message}");
        }
    }

    #[test]
    fn given_service_errors_when_reported_should_preserve_status_and_safe_code_only() {
        for (status, code, expected_code) in [
            (403, "AccessDenied", "AccessDenied"),
            (404, "NoSuchKey", "NoSuchKey"),
            (503, "SlowDown", "SlowDown"),
            (403, "https://user:secret@example.test", "redacted"),
            (403, "AccessDenied\nsecret", "redacted"),
        ] {
            let failure = SdkError::service_error(
                GetObjectError::generic(
                    ErrorMetadata::builder()
                        .code(code)
                        .message("secret")
                        .build(),
                ),
                HttpResponse::new(status.try_into().expect("status"), "secret".into()),
            );
            let result = request_error("get", &failure);
            let message = match result {
                Error::PermanentHttpError(message) if status < 500 => message,
                Error::HttpRequestFailed(message) if status >= 500 => message,
                other => panic!("unexpected error classification: {other:?}"),
            };
            assert!(message.contains("service"), "{message}");
            assert!(
                message.contains(&format!("HTTP status: {status}")),
                "{message}"
            );
            assert!(message.contains(expected_code), "{message}");
            assert!(!message.contains("secret"), "{message}");
            assert!(!message.contains('\n'), "{message}");
        }
    }

    fn object(offset: u64) -> ActiveObjectState {
        ActiveObjectState {
            key: "logs/a".into(),
            etag: "etag".into(),
            size: 10,
            next_byte_offset: offset,
        }
    }

    #[test]
    fn given_resumed_response_when_validated_should_require_exact_range_and_identity() {
        let valid = GetObjectOutput::builder()
            .e_tag("etag")
            .content_length(6)
            .content_range("bytes 4-9/10")
            .build();
        assert_eq!(validate_response(&valid, &object(4)), Ok(()));
        for range in [
            None,
            Some("bytes 0-5/10"),
            Some("bytes 4-8/10"),
            Some("bytes 4-9/11"),
            Some("bytes */10"),
        ] {
            let response = GetObjectOutput::builder()
                .e_tag("etag")
                .content_length(6)
                .set_content_range(range.map(str::to_owned))
                .build();
            assert!(validate_response(&response, &object(4)).is_err());
        }
        for (etag, length) in [("new", 6), ("etag", 5)] {
            let response = GetObjectOutput::builder()
                .e_tag(etag)
                .content_length(length)
                .content_range("bytes 4-9/10")
                .build();
            assert!(validate_response(&response, &object(4)).is_err());
        }
    }

    #[test]
    fn given_full_response_when_validated_should_check_length_and_etag() {
        let response = GetObjectOutput::builder()
            .e_tag("etag")
            .content_length(10)
            .build();
        assert_eq!(validate_response(&response, &object(0)), Ok(()));
        assert_eq!(validate_response(&response, &object(10)), Ok(()));
        let empty = ActiveObjectState {
            size: 0,
            ..object(0)
        };
        assert_eq!(
            validate_response(
                &GetObjectOutput::builder()
                    .e_tag("etag")
                    .content_length(0)
                    .build(),
                &empty
            ),
            Ok(())
        );
        assert!(
            validate_response(
                &GetObjectOutput::builder().content_length(10).build(),
                &object(0)
            )
            .is_err()
        );
    }
}
