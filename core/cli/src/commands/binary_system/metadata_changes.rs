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

use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::{Context, bail};
use async_trait::async_trait;
use iggy_common::{
    Client, IggyDuration, MetadataChangeStart, MetadataChangeStatus, MetadataChangesQuery,
};
use tracing::{Level, event};

/// Reads metadata change pages and prints each as one JSON line.
pub struct GetMetadataChangesCmd {
    query: MetadataChangesQuery,
    follow: bool,
    interval: IggyDuration,
}

impl GetMetadataChangesCmd {
    pub fn new(query: MetadataChangesQuery, follow: bool, interval: IggyDuration) -> Self {
        Self {
            query,
            follow,
            interval,
        }
    }
}

#[async_trait]
impl CliCommand for GetMetadataChangesCmd {
    fn explain(&self) -> String {
        "metadata changes command".to_owned()
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut query = self.query.clone();
        loop {
            let page = client
                .get_metadata_changes(&query)
                .await
                .with_context(|| "Problem reading metadata changes".to_owned())?;
            let quiet_follow_page = self.follow
                && page.status == MetadataChangeStatus::Ok
                && page.records.is_empty()
                && page.items.is_empty();
            if !quiet_follow_page {
                event!(target: PRINT_TARGET, Level::INFO, "{}", serde_json::to_string(&page)?);
            }
            match page.status {
                MetadataChangeStatus::Ok => {}
                MetadataChangeStatus::Behind if self.follow => {
                    tokio::time::sleep(self.interval.get_duration()).await;
                    continue;
                }
                MetadataChangeStatus::Behind => return Ok(()),
                MetadataChangeStatus::Expired | MetadataChangeStatus::Reset if self.follow => {
                    query.start = MetadataChangeStart::Current(None);
                    query.since = None;
                    query.until = None;
                    continue;
                }
                MetadataChangeStatus::Expired | MetadataChangeStatus::Reset => {
                    bail!(
                        "The history after the cursor is not available ({}), read it again with --from current",
                        serde_json::to_string(&page.status)?
                    );
                }
            }
            if let MetadataChangeStart::Current(_) = query.start {
                match (page.current_page_token, page.next) {
                    (Some(token), _) => {
                        query.start = MetadataChangeStart::Current(Some(token));
                        continue;
                    }
                    (None, Some(anchor)) if self.follow => {
                        query.start = MetadataChangeStart::After(anchor);
                    }
                    _ => return Ok(()),
                }
                continue;
            }
            if let Some(next) = page.next {
                query.start = MetadataChangeStart::After(next);
            }
            if page.has_more {
                continue;
            }
            if !self.follow {
                return Ok(());
            }
            tokio::time::sleep(self.interval.get_duration()).await;
        }
    }
}
