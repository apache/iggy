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

//! The star count of the Apache Iggy repository for the badge in the top bar. The server asks
//! GitHub and the pages ask the server, so a page makes no request outside the dashboard. The
//! server asks again every hour, because it can run for months.

use reqwest::Client;
use reqwest::header::ACCEPT;
use serde::Deserialize;
use std::fmt::Display;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tracing::{debug, warn};

const REPOSITORY_API_URL: &str = "https://api.github.com/repos/apache/iggy";
const GITHUB_JSON: &str = "application/vnd.github+json";
/// GitHub refuses an API request without a user agent.
const USER_AGENT: &str = concat!("iggy-bench-dashboard/", env!("CARGO_PKG_VERSION"));
/// 24 requests a day. GitHub allows 60 an hour to an address without a token.
const REFRESH_INTERVAL: Duration = Duration::from_secs(60 * 60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The last count from GitHub. A failed request keeps it, so a short GitHub outage never takes
/// the count off the page.
#[derive(Debug, Default)]
pub struct StarCount {
    /// 0 until the first answer. A repository without stars shows no count, as an unknown count
    /// does.
    stars: AtomicU32,
}

impl StarCount {
    pub fn get(&self) -> Option<u32> {
        NonZeroU32::new(self.stars.load(Ordering::Relaxed)).map(NonZeroU32::get)
    }

    /// Asks GitHub now and then every hour, for as long as the server runs.
    pub fn refresh_hourly(self: Arc<Self>) {
        actix_web::rt::spawn(async move {
            let client = match Client::builder()
                .user_agent(USER_AGENT)
                .timeout(REQUEST_TIMEOUT)
                .build()
            {
                Ok(client) => client,
                Err(e) => {
                    warn!("GitHub stars: can't build the HTTP client: {e}");
                    return;
                }
            };
            let mut interval = actix_web::rt::time::interval(REFRESH_INTERVAL);
            loop {
                interval.tick().await;
                self.record(fetch_stars(&client).await);
            }
        });
    }

    /// Keeps the count of an answer, and the previous count after a failed request.
    pub fn record<E: Display>(&self, fetched: Result<u32, E>) {
        match fetched {
            Ok(stars) => {
                debug!("GitHub stars: {stars}");
                self.stars.store(stars, Ordering::Relaxed);
            }
            Err(e) => match self.get() {
                Some(previous) => warn!("GitHub stars: request failed, keeping {previous}: {e}"),
                None => warn!("GitHub stars: request failed, so the top bar shows no count: {e}"),
            },
        }
    }
}

/// The one field of GitHub's repository answer that the badge needs.
#[derive(Debug, Deserialize)]
struct Repository {
    stargazers_count: u32,
}

async fn fetch_stars(client: &Client) -> Result<u32, reqwest::Error> {
    let repository: Repository = client
        .get(REPOSITORY_API_URL)
        .header(ACCEPT, GITHUB_JSON)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(repository.stargazers_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    const OFFLINE: Result<u32, &str> = Err("connection refused");

    #[test]
    fn given_no_answer_yet_when_read_should_have_no_count() {
        let count = StarCount::default();

        count.record(OFFLINE);

        assert_eq!(count.get(), None);
    }

    #[test]
    fn given_count_when_request_fails_should_keep_it() {
        let count = StarCount::default();
        count.record(Ok::<_, &str>(5123));

        count.record(OFFLINE);

        assert_eq!(count.get(), Some(5123));
    }

    #[test]
    fn given_new_answer_when_recorded_should_replace_count() {
        let count = StarCount::default();
        count.record(Ok::<_, &str>(5123));

        count.record(Ok::<_, &str>(5124));

        assert_eq!(count.get(), Some(5124));
    }

    #[test]
    fn given_github_repository_answer_when_parsed_should_read_star_count() {
        let answer = r#"{"id": 1, "full_name": "apache/iggy", "stargazers_count": 5123,
                         "watchers_count": 5123, "owner": {"login": "apache"}}"#;

        let repository: Repository = serde_json::from_str(answer).expect("the answer parses");

        assert_eq!(repository.stargazers_count, 5123);
    }
}
