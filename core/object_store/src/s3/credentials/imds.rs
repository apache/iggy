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

//! The instance metadata service of EC2, version 2: a session token, then
//! the role of the instance, then the credentials of that role.

use bytes::Bytes;
use http::Method;
use serde::Deserialize;

use super::{
    CredentialError, CredentialRequest, CredentialResponse, CredentialTransport, Credentials,
    FetchedCredentials, parse_expiration,
};
use crate::s3::sigv4::encode_path;

/// How long a session token of the service lasts, in seconds: the longest
/// that it allows.
const TOKEN_TTL_SECONDS: &str = "21600";
const TOKEN_TTL_HEADER: &str = "x-aws-ec2-metadata-token-ttl-seconds";
const TOKEN_HEADER: &str = "x-aws-ec2-metadata-token";
const CREDENTIALS_PATH: &str = "/latest/meta-data/iam/security-credentials/";

/// The instance metadata service at `endpoint`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceMetadata {
    /// The base URL of the service, `http://169.254.169.254` on EC2.
    pub endpoint: String,
}

/// The credentials document of a role.
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RoleCredentials {
    code: String,
    access_key_id: String,
    secret_access_key: String,
    token: String,
    expiration: String,
}

impl InstanceMetadata {
    pub(super) async fn fetch(
        &self,
        transport: &impl CredentialTransport,
    ) -> Result<FetchedCredentials, CredentialError> {
        let token = text(&checked(
            transport.send(self.token_request()).await?,
            "a session token",
        )?)?;
        let roles = text(&checked(
            transport.send(self.role_request(&token)).await?,
            "a role for the instance",
        )?)?;
        let role = roles
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .ok_or_else(|| CredentialError::Refused("the instance has no role".to_owned()))?;
        let answer = transport
            .send(self.credentials_request(&token, role))
            .await?;
        parse_credentials(&checked(answer, "credentials for the role")?)
    }

    fn token_request(&self) -> CredentialRequest {
        CredentialRequest {
            method: Method::PUT,
            url: format!("{}/latest/api/token", self.base()),
            headers: vec![(TOKEN_TTL_HEADER, TOKEN_TTL_SECONDS.to_owned())],
            body: String::new(),
        }
    }

    fn role_request(&self, token: &str) -> CredentialRequest {
        CredentialRequest {
            method: Method::GET,
            url: format!("{}{CREDENTIALS_PATH}", self.base()),
            headers: vec![(TOKEN_HEADER, token.to_owned())],
            body: String::new(),
        }
    }

    fn credentials_request(&self, token: &str, role: &str) -> CredentialRequest {
        CredentialRequest {
            method: Method::GET,
            url: format!("{}{CREDENTIALS_PATH}{}", self.base(), encode_path(role)),
            headers: vec![(TOKEN_HEADER, token.to_owned())],
            body: String::new(),
        }
    }

    fn base(&self) -> &str {
        self.endpoint.trim_end_matches('/')
    }
}

/// The body of a success, or the error that the status means. `what` names
/// what the request asked for.
fn checked(response: CredentialResponse, what: &str) -> Result<Bytes, CredentialError> {
    match response.status {
        200..=299 => Ok(response.body),
        status @ 400..=499 => Err(CredentialError::Refused(format!(
            "the instance metadata service answered {status} for {what}"
        ))),
        status => Err(CredentialError::Unavailable(format!(
            "the instance metadata service answered {status} for {what}"
        ))),
    }
}

fn text(body: &[u8]) -> Result<String, CredentialError> {
    String::from_utf8(body.to_vec()).map_err(|_| {
        CredentialError::Malformed(
            "an answer of the instance metadata service is not UTF-8".to_owned(),
        )
    })
}

fn parse_credentials(body: &[u8]) -> Result<FetchedCredentials, CredentialError> {
    // The error of a parser can quote the document, which holds keys.
    let document: RoleCredentials = serde_json::from_slice(body).map_err(|_| {
        CredentialError::Malformed("the credentials document of the role".to_owned())
    })?;
    if document.code != "Success" {
        return Err(CredentialError::Unavailable(format!(
            "the instance metadata service answered {} for the credentials of the role",
            document.code
        )));
    }
    Ok(FetchedCredentials {
        expires_at: Some(parse_expiration(&document.expiration)?),
        credentials: Credentials {
            access_key_id: document.access_key_id,
            secret_access_key: document.secret_access_key,
            session_token: Some(document.token),
        },
    })
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::super::tests::RecordedTransport;
    use super::*;

    /// A credentials document as EC2 hands it out, with example values.
    const DOCUMENT: &str = r#"{
  "Code" : "Success",
  "LastUpdated" : "2026-11-13T10:00:00Z",
  "Type" : "AWS-HMAC",
  "AccessKeyId" : "ASIAIOSFODNN7EXAMPLE",
  "SecretAccessKey" : "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "Token" : "IQoJb3JpZ2luX2VjEXAMPLETOKEN",
  "Expiration" : "2026-11-13T16:00:00Z"
}"#;

    fn service() -> InstanceMetadata {
        InstanceMetadata {
            endpoint: "http://169.254.169.254/".to_owned(),
        }
    }

    fn fetch(transport: &RecordedTransport) -> Result<FetchedCredentials, CredentialError> {
        crate::contract::shard_runtime().block_on(service().fetch(transport))
    }

    #[test]
    fn given_an_instance_with_a_role_when_fetching_should_hand_out_its_credentials() {
        let transport = RecordedTransport::answering([
            (200, "session-token"),
            (200, "iggy-node-role\n"),
            (200, DOCUMENT),
        ]);

        let fetched = fetch(&transport).unwrap();

        assert_eq!(fetched.credentials.access_key_id, "ASIAIOSFODNN7EXAMPLE");
        assert_eq!(
            fetched.credentials.session_token.as_deref(),
            Some("IQoJb3JpZ2luX2VjEXAMPLETOKEN")
        );
        assert_eq!(
            fetched.expires_at,
            Some(SystemTime::UNIX_EPOCH + Duration::from_hours(498_496))
        );
        let requests = transport.requests.borrow();
        let sent: Vec<(&Method, &str)> = requests
            .iter()
            .map(|request| (&request.method, request.url.as_str()))
            .collect();
        assert_eq!(
            sent,
            [
                (&Method::PUT, "http://169.254.169.254/latest/api/token"),
                (
                    &Method::GET,
                    "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
                ),
                (
                    &Method::GET,
                    "http://169.254.169.254/latest/meta-data/iam/security-credentials/iggy-node-role"
                ),
            ]
        );
        assert_eq!(
            requests[0].headers,
            [(TOKEN_TTL_HEADER, TOKEN_TTL_SECONDS.to_owned())]
        );
        for request in &requests[1..] {
            assert_eq!(
                request.headers,
                [(TOKEN_HEADER, "session-token".to_owned())]
            );
        }
    }

    #[test]
    fn given_answers_that_hand_out_nothing_when_fetching_should_tell_a_refusal_from_an_outage() {
        let no_role = fetch(&RecordedTransport::answering([(200, "token"), (404, "")]));
        let empty_role = fetch(&RecordedTransport::answering([(200, "token"), (200, "\n")]));
        let forbidden = fetch(&RecordedTransport::answering([(403, "")]));
        let failing = fetch(&RecordedTransport::answering([(503, "")]));
        let pending = fetch(&RecordedTransport::answering([
            (200, "token"),
            (200, "role"),
            (
                200,
                r#"{"Code":"Pending","AccessKeyId":"","SecretAccessKey":"","Token":"","Expiration":""}"#,
            ),
        ]));
        let no_answer = fetch(&RecordedTransport::default());

        for refused in [no_role, empty_role, forbidden] {
            assert!(
                matches!(refused, Err(CredentialError::Refused(_))),
                "{refused:?}"
            );
        }
        for transient in [failing, pending, no_answer] {
            assert!(
                transient.as_ref().is_err_and(CredentialError::is_transient),
                "{transient:?}"
            );
        }
    }

    #[test]
    fn given_a_broken_document_when_fetching_should_fail_without_quoting_it() {
        let result = fetch(&RecordedTransport::answering([
            (200, "token"),
            (200, "role"),
            (
                200,
                r#"{"Code":"Success","SecretAccessKey":"wJalrXUtnFEMI"}"#,
            ),
        ]));

        let error = result.unwrap_err();
        assert!(matches!(error, CredentialError::Malformed(_)), "{error:?}");
        assert!(!error.to_string().contains("wJalrXUtnFEMI"), "{error}");
    }
}
