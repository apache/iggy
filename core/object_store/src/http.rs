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

//! The HTTP client that every remote provider and credential source uses.

use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use send_wrapper::SendWrapper;

use crate::error::ObjectStoreError;
use crate::resolver::ShardResolver;

/// A client for a shard: host lookups off the shard thread, no redirects,
/// and the system's certificate authorities only for `https`.
///
/// A redirect would resend a body that is already sent, under a signature
/// or a token meant for another host.
pub fn client(scheme: &str) -> Result<cyper::Client, ObjectStoreError> {
    let builder = cyper::Client::builder()
        .custom_resolver(ShardResolver::default())
        .redirect(cyper::redirect::Policy::none());
    let builder = if scheme == "https" {
        builder.use_rustls_default()
    } else {
        // Without a config, the client loads the system's certificate
        // authorities when it is built, and fails on a host without them,
        // although it never dials https.
        builder.use_rustls(Arc::new(plain_http_tls_config()?))
    };
    builder.build().map_err(transport)
}

/// `pieces` as a request body. A failure of a piece is left in `failure`,
/// so the put returns the body's own error, not the one that the client
/// makes of it.
pub fn streamed(
    pieces: impl Stream<Item = io::Result<Bytes>> + 'static,
    failure: Rc<RefCell<Option<io::Error>>>,
) -> cyper::Body {
    let pieces = pieces.map(move |piece| {
        piece.map_err(|error| {
            let kind = error.kind();
            *failure.borrow_mut() = Some(error);
            cyper::Error::System(io::Error::from(kind))
        })
    });
    // The client polls the body on the shard thread that sends it.
    cyper::Body::stream(SendWrapper::new(pieces))
}

#[allow(clippy::needless_pass_by_value)]
pub fn transport(error: cyper::Error) -> ObjectStoreError {
    ObjectStoreError::Transport(error.to_string())
}

/// A TLS config that trusts no authority, for a client that dials only plain
/// http.
fn plain_http_tls_config() -> Result<rustls::ClientConfig, ObjectStoreError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Ok(rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| ObjectStoreError::Transport(error.to_string()))?
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth())
}
