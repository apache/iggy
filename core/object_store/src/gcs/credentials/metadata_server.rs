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

//! The metadata server of a Compute Engine instance or a GKE node: the
//! access token of the instance's default service account. On GKE with
//! Workload Identity, the node's metadata server hands out the token of the
//! pod's service account instead.

use std::time::SystemTime;

use http::Method;

use super::{FetchedToken, parse_token, refusal};
use crate::credentials::{CredentialError, CredentialRequest, CredentialTransport};

const TOKEN_PATH: &str = "/computeMetadata/v1/instance/service-accounts/default/token";
/// The server refuses a request without this header, so that a request that
/// a web page or a proxy forwards never reaches it.
const FLAVOR_HEADER: &str = "metadata-flavor";
const FLAVOR: &str = "Google";

/// The metadata server at `endpoint`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataServer {
    /// The base URL of the server, `http://metadata.google.internal` on
    /// Google Cloud.
    pub endpoint: String,
}

impl MetadataServer {
    pub(super) async fn fetch(
        &self,
        transport: &impl CredentialTransport,
    ) -> Result<FetchedToken, CredentialError> {
        let answer = transport.send(self.token_request()).await?;
        if !(200..300).contains(&answer.status) {
            return Err(refusal(
                answer.status,
                "the metadata server, asked for the token of the default service account,",
            ));
        }
        parse_token(&answer.body, SystemTime::now(), "the metadata server")
    }

    fn token_request(&self) -> CredentialRequest {
        CredentialRequest {
            method: Method::GET,
            url: format!("{}{TOKEN_PATH}", self.endpoint.trim_end_matches('/')),
            headers: vec![(FLAVOR_HEADER, FLAVOR.to_owned())],
            body: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::credentials::testing::RecordedTransport;

    fn server() -> MetadataServer {
        MetadataServer {
            endpoint: "http://metadata.google.internal/".to_owned(),
        }
    }

    fn fetch(transport: &RecordedTransport) -> Result<FetchedToken, CredentialError> {
        crate::contract::shard_runtime().block_on(server().fetch(transport))
    }

    #[test]
    fn given_metadata_server_when_fetching_should_ask_for_the_default_account_with_the_flavor() {
        let transport = RecordedTransport::answering([(
            200,
            r#"{"access_token":"ya29.instance","expires_in":3599,"token_type":"Bearer"}"#,
        )]);
        let before = SystemTime::now();

        let fetched = fetch(&transport).unwrap();

        let requests = transport.requests.borrow();
        let [request] = requests.as_slice() else {
            panic!("one request: {requests:?}");
        };
        assert_eq!(request.method, Method::GET);
        assert_eq!(
            request.url,
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
        );
        assert_eq!(request.headers, [("metadata-flavor", "Google".to_owned())]);
        assert_eq!(fetched.token.secret(), "ya29.instance");
        assert!(fetched.expires_at >= before + Duration::from_secs(3599));
    }

    #[test]
    fn given_metadata_server_errors_when_fetching_should_tell_a_refusal_from_an_outage() {
        let refused = fetch(&RecordedTransport::answering([(404, "no account")]));
        let failing = fetch(&RecordedTransport::answering([(503, "")]));
        let unreachable = fetch(&RecordedTransport::default());

        assert!(
            matches!(refused, Err(CredentialError::Refused(_))),
            "{refused:?}"
        );
        assert!(
            matches!(failing, Err(CredentialError::Unavailable(_))),
            "{failing:?}"
        );
        assert!(
            matches!(unreachable, Err(CredentialError::Unreachable(_))),
            "{unreachable:?}"
        );
    }
}
