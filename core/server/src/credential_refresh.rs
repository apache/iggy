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

//! The thread that keeps the credentials of an object store fresh.
//!
//! A source of credentials that expire gets a thread of its own, which
//! fetches the first credentials and then fetches new ones before the last
//! ones expire. What it fetches goes into a cell that every shard's store
//! reads, so one refresh reaches every shard. The `s3` backend and the
//! `gcs` backend and peers use the same loop.
//!
//! Log lines name the source and the expiry, never a key or a token.

use std::future::Future;
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender};
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use iggy_object_store::credentials::{CredentialError, CredentialTransport, HttpTransport};
use tracing::{error, info, warn};

use crate::server_error::ServerError;

/// How long the boot waits for the first credentials. A source that has not
/// answered by then is an outage, and the node boots without credentials.
const FIRST_FETCH_TIMEOUT: Duration = Duration::from_secs(5);
/// The longest wait for one request to a source.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// A source of credentials that expire, with the cell that the shards read
/// them from.
pub trait Refresh: Send + 'static {
    /// What log lines and errors call the store, such as `the s3 backend`.
    fn subject(&self) -> &str;

    /// What log lines call the source.
    fn source_name(&self) -> &'static str;

    /// The URL of the service that hands out the credentials.
    fn endpoint(&self) -> &str;

    /// Fetches once, puts what came into the cell, and returns when it
    /// expires, `None` for credentials that never do.
    fn fetch(
        &self,
        transport: &impl CredentialTransport,
    ) -> impl Future<Output = Result<Option<SystemTime>, CredentialError>>;
}

/// When a refresher fetches again after a failed fetch.
#[derive(Debug, Clone, Copy)]
pub struct RetryDelays {
    pub first: Duration,
    pub max: Duration,
}

impl RetryDelays {
    pub const DEFAULT: Self = Self {
        first: Duration::from_secs(1),
        max: Duration::from_mins(1),
    };
}

/// What the first fetch of a refresher found: the expiry of the credentials
/// that it handed out, or why it handed out none.
pub type FirstFetch = Result<Option<SystemTime>, CredentialError>;

/// Starts the thread that keeps the cell of `refresh` fresh, and waits for
/// its first fetch at most [`FIRST_FETCH_TIMEOUT`].
///
/// # Errors
/// [`ServerError::TieredStorageCredentials`] when the source refuses the
/// first fetch for a reason that a retry cannot fix, and
/// [`ServerError::TieredStorageCredentialsThread`] when the thread does not
/// start.
pub fn start_and_wait(refresh: impl Refresh) -> Result<(), ServerError> {
    let name = refresh.source_name();
    let subject = refresh.subject().to_owned();
    let (first_sender, first) = mpsc::sync_channel(1);
    spawn(refresh, Some(first_sender))?;
    match first.recv_timeout(FIRST_FETCH_TIMEOUT) {
        Ok(Ok(expires_at)) => {
            info!(
                source = name,
                expires_at = expires_at.map(rfc3339),
                "{subject} has its first credentials"
            );
            Ok(())
        }
        Ok(Err(error)) if error.is_transient() => {
            warn!(
                source = name,
                %error,
                "{subject} has no credentials yet, so the node boots without them and keeps \
                 fetching"
            );
            Ok(())
        }
        Ok(Err(error)) => Err(ServerError::TieredStorageCredentials {
            subject,
            source_name: name,
            source: error,
        }),
        Err(RecvTimeoutError::Timeout) => {
            warn!(
                source = name,
                "the credential service of {subject} did not answer in time, so the node boots \
                 without credentials and keeps fetching"
            );
            Ok(())
        }
        Err(RecvTimeoutError::Disconnected) => Err(ServerError::TieredStorageCredentialsThread {
            subject,
            source: std::io::Error::other("the refresher stopped before its first fetch"),
        }),
    }
}

/// Starts the thread that keeps the cell of `refresh` fresh, without
/// waiting for it. A fetch that fails, even for good, is logged and tried
/// again, so a store that only some features read never stops the boot.
///
/// # Errors
/// [`ServerError::TieredStorageCredentialsThread`] when the thread does not
/// start.
pub fn start(refresh: impl Refresh) -> Result<(), ServerError> {
    spawn(refresh, None)
}

fn spawn(refresh: impl Refresh, first: Option<SyncSender<FirstFetch>>) -> Result<(), ServerError> {
    let subject = refresh.subject().to_owned();
    std::thread::Builder::new()
        .name("iggy-credentials".to_owned())
        .spawn(move || {
            let runtime = match compio::runtime::Runtime::new() {
                Ok(runtime) => runtime,
                Err(error) => {
                    if let Some(first) = first {
                        let _ = first.send(Err(CredentialError::Unreachable(format!(
                            "no runtime for the refresher: {error}"
                        ))));
                    }
                    return;
                }
            };
            runtime.block_on(run(refresh, first));
        })
        .map(drop)
        .map_err(|source| ServerError::TieredStorageCredentialsThread { subject, source })
}

/// The body of the refresher thread.
async fn run(refresh: impl Refresh, first: Option<SyncSender<FirstFetch>>) {
    match HttpTransport::new(refresh.endpoint(), REQUEST_TIMEOUT) {
        Ok(transport) => keep_fresh(&refresh, &transport, first, RetryDelays::DEFAULT).await,
        Err(error) => {
            let failure = CredentialError::Refused(format!(
                "the credential service at {:?}: {error}",
                refresh.endpoint()
            ));
            if let Some(first) = first {
                let _ = first.send(Err(failure));
            } else {
                error!(
                    source = refresh.source_name(),
                    error = %failure,
                    "{} gets no credentials",
                    refresh.subject()
                );
            }
        }
    }
}

/// Fetches credentials for `refresh` for as long as the node runs: the
/// first ones at once, each next ones before the last expire, and after a
/// failed fetch again with a delay that doubles up to `delays.max`.
/// `first` gets the outcome of the first fetch. A first fetch that the
/// source refuses for good ends the refresher, because the node does not
/// boot. Without `first`, every failure is logged and fetched again.
pub async fn keep_fresh<R: Refresh>(
    refresh: &R,
    transport: &impl CredentialTransport,
    mut first: Option<SyncSender<FirstFetch>>,
    delays: RetryDelays,
) {
    let source = refresh.source_name();
    let subject = refresh.subject();
    let mut retry = delays.first;
    loop {
        let wait = match refresh.fetch(transport).await {
            Ok(expires_at) => {
                retry = delays.first;
                if let Some(first) = first.take() {
                    let _ = first.send(Ok(expires_at));
                } else {
                    info!(
                        source,
                        expires_at = expires_at.map(rfc3339),
                        "refreshed the credentials of {subject}"
                    );
                }
                // Credentials without an expiry need no refresh.
                let Some(expires_at) = expires_at else {
                    return;
                };
                iggy_object_store::credentials::next_refresh(expires_at, SystemTime::now())
            }
            Err(failure) => {
                let transient = failure.is_transient();
                if let Some(first) = first.take() {
                    let _ = first.send(Err(failure));
                    if !transient {
                        return;
                    }
                } else if transient {
                    warn!(source, error = %failure, retry_in = ?retry, "could not refresh the credentials of {subject}");
                } else {
                    error!(source, error = %failure, retry_in = ?retry, "the credential service refused to refresh the credentials of {subject}");
                }
                let wait = retry;
                retry = (retry * 2).min(delays.max);
                wait
            }
        };
        compio::time::sleep(wait).await;
    }
}

pub fn rfc3339(time: SystemTime) -> String {
    DateTime::<Utc>::from(time).to_rfc3339()
}
