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

//! Web identity: a token file that a platform such as EKS keeps fresh,
//! exchanged at STS with `AssumeRoleWithWebIdentity` for temporary
//! credentials. The request carries the token, so it needs no signature.

use std::io;
use std::path::PathBuf;

use http::Method;
use serde::Deserialize;

use super::{
    CredentialError, CredentialRequest, CredentialResponse, CredentialTransport, Credentials,
    FetchedCredentials, parse_expiration,
};

/// The STS error codes that a retry can get past.
const TRANSIENT_CODES: [&str; 4] = [
    "IDPCommunicationError",
    "InternalFailure",
    "ServiceUnavailable",
    "Throttling",
];

/// A role that STS hands out credentials of for the token in `token_file`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebIdentity {
    pub token_file: PathBuf,
    pub role_arn: String,
    /// The name of the session, which the role's logs show.
    pub session_name: String,
    /// The base URL of STS.
    pub endpoint: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AssumeRoleWithWebIdentityResponse {
    assume_role_with_web_identity_result: AssumeRoleWithWebIdentityResult,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AssumeRoleWithWebIdentityResult {
    credentials: StsCredentials,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct StsCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    expiration: String,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ErrorResponse {
    #[serde(default)]
    error: StsError,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct StsError {
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
}

impl WebIdentity {
    pub(super) async fn fetch(
        &self,
        transport: &impl CredentialTransport,
    ) -> Result<FetchedCredentials, CredentialError> {
        // The platform replaces the token before it expires, so each fetch
        // reads the file again.
        let token = compio::fs::read(&self.token_file)
            .await
            .and_then(|bytes| {
                String::from_utf8(bytes)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "not UTF-8"))
            })
            .map_err(|source| CredentialError::TokenFile {
                path: self.token_file.clone(),
                source,
            })?;
        let answer = transport.send(self.request(token.trim())).await?;
        parse_answer(&answer)
    }

    fn request(&self, token: &str) -> CredentialRequest {
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("Action", "AssumeRoleWithWebIdentity")
            .append_pair("Version", "2011-06-15")
            .append_pair("RoleArn", &self.role_arn)
            .append_pair("RoleSessionName", &self.session_name)
            .append_pair("WebIdentityToken", token)
            .finish();
        CredentialRequest {
            method: Method::POST,
            url: format!("{}/", self.endpoint.trim_end_matches('/')),
            headers: vec![(
                "content-type",
                "application/x-www-form-urlencoded".to_owned(),
            )],
            body,
        }
    }
}

fn parse_answer(answer: &CredentialResponse) -> Result<FetchedCredentials, CredentialError> {
    let text = std::str::from_utf8(&answer.body)
        .map_err(|_| CredentialError::Malformed("an answer of STS is not UTF-8".to_owned()))?;
    if !(200..=299).contains(&answer.status) {
        let error = quick_xml::de::from_str::<ErrorResponse>(text)
            .unwrap_or_default()
            .error;
        let detail = format!(
            "STS answered {} {}: {}",
            answer.status, error.code, error.message
        );
        return Err(
            if answer.status >= 500 || TRANSIENT_CODES.contains(&error.code.as_str()) {
                CredentialError::Unavailable(detail)
            } else {
                CredentialError::Refused(detail)
            },
        );
    }
    // The error of a parser can quote the document, which holds keys.
    let response: AssumeRoleWithWebIdentityResponse =
        quick_xml::de::from_str(text).map_err(|_| {
            CredentialError::Malformed("the credentials that STS handed out".to_owned())
        })?;
    let credentials = response.assume_role_with_web_identity_result.credentials;
    Ok(FetchedCredentials {
        expires_at: Some(parse_expiration(&credentials.expiration)?),
        credentials: Credentials {
            access_key_id: credentials.access_key_id,
            secret_access_key: credentials.secret_access_key,
            session_token: Some(credentials.session_token),
        },
    })
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::super::tests::RecordedTransport;
    use super::*;

    /// An answer of STS as AWS documents it, with example values.
    const ANSWER: &str = r#"<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <SubjectFromWebIdentityToken>system:serviceaccount:iggy:iggy</SubjectFromWebIdentityToken>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::123456789012:assumed-role/iggy/iggy-tiered-storage</Arn>
      <AssumedRoleId>AROACLKWSDQRAOEXAMPLE:iggy-tiered-storage</AssumedRoleId>
    </AssumedRoleUser>
    <Credentials>
      <SessionToken>AQoDYXdzEE0a8ANXXXXXXXXNO1ewxE5TijQyp+IEXAMPLE</SessionToken>
      <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY</SecretAccessKey>
      <Expiration>2026-11-13T12:00:00Z</Expiration>
      <AccessKeyId>ASgeIAIOSFODNN7EXAMPLE</AccessKeyId>
    </Credentials>
    <Provider>www.amazon.com</Provider>
  </AssumeRoleWithWebIdentityResult>
  <ResponseMetadata><RequestId>ad4156e9-bce1-11e2-82e6-6b6efEXAMPLE</RequestId></ResponseMetadata>
</AssumeRoleWithWebIdentityResponse>"#;

    fn source(token_file: PathBuf) -> WebIdentity {
        WebIdentity {
            token_file,
            role_arn: "arn:aws:iam::123456789012:role/iggy".to_owned(),
            session_name: "iggy-tiered-storage".to_owned(),
            endpoint: "https://sts.eu-central-1.amazonaws.com".to_owned(),
        }
    }

    fn fetch(
        transport: &RecordedTransport,
        token: Option<&str>,
    ) -> Result<FetchedCredentials, CredentialError> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        if let Some(token) = token {
            std::fs::write(&path, token).unwrap();
        }
        crate::contract::shard_runtime().block_on(source(path).fetch(transport))
    }

    #[test]
    fn given_a_token_when_fetching_should_exchange_it_for_credentials() {
        let transport = RecordedTransport::answering([(200, ANSWER)]);

        let fetched = fetch(&transport, Some("eyJhbGciOiJSUzI1NiJ9.token+/=\n")).unwrap();

        assert_eq!(fetched.credentials.access_key_id, "ASgeIAIOSFODNN7EXAMPLE");
        assert_eq!(
            fetched.credentials.session_token.as_deref(),
            Some("AQoDYXdzEE0a8ANXXXXXXXXNO1ewxE5TijQyp+IEXAMPLE")
        );
        assert_eq!(
            fetched.expires_at,
            Some(SystemTime::UNIX_EPOCH + Duration::from_hours(498_492))
        );
        let requests = transport.requests.borrow();
        let [request] = requests.as_slice() else {
            panic!("one request: {requests:?}");
        };
        assert_eq!(request.method, Method::POST);
        assert_eq!(request.url, "https://sts.eu-central-1.amazonaws.com/");
        assert_eq!(
            request.body,
            "Action=AssumeRoleWithWebIdentity&Version=2011-06-15\
             &RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Figgy\
             &RoleSessionName=iggy-tiered-storage\
             &WebIdentityToken=eyJhbGciOiJSUzI1NiJ9.token%2B%2F%3D"
        );
    }

    #[test]
    fn given_sts_errors_when_fetching_should_tell_a_refusal_from_an_outage() {
        let error = |status, code: &str| {
            let body: &'static str = Box::leak(
                format!(
                    "<ErrorResponse><Error><Type>Sender</Type><Code>{code}</Code>\
                     <Message>m</Message></Error></ErrorResponse>"
                )
                .into_boxed_str(),
            );
            fetch(
                &RecordedTransport::answering([(status, body)]),
                Some("token"),
            )
        };

        for refused in [
            error(403, "AccessDenied"),
            error(400, "InvalidIdentityToken"),
            error(400, "ExpiredTokenException"),
        ] {
            assert!(
                matches!(refused, Err(CredentialError::Refused(_))),
                "{refused:?}"
            );
        }
        for transient in [
            error(400, "IDPCommunicationError"),
            error(400, "Throttling"),
            error(503, ""),
        ] {
            assert!(
                transient.as_ref().is_err_and(CredentialError::is_transient),
                "{transient:?}"
            );
        }
    }

    #[test]
    fn given_no_token_file_when_fetching_should_fail_without_a_request() {
        let transport = RecordedTransport::answering([(200, ANSWER)]);

        let result = fetch(&transport, None);

        assert!(
            matches!(result, Err(CredentialError::TokenFile { .. })),
            "{result:?}"
        );
        assert!(transport.requests.borrow().is_empty());
    }
}
