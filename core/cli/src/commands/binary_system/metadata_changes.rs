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

use crate::commands::cli_command::CliCommand;
use anyhow::{Context, bail};
use async_trait::async_trait;
use iggy_common::{
    Client, IggyDuration, IggyError, MetadataChangeGroup, MetadataChangeStart,
    MetadataChangeStatus, MetadataChanges, MetadataChangesQuery,
};
use std::future::Future;
use std::io::{Write, stdout};

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

    /// Reads pages from `read` and writes each to `output` until the query ends.
    async fn read_pages<R, F>(
        &self,
        mut read: R,
        output: &mut (impl Write + Send),
    ) -> anyhow::Result<()>
    where
        R: FnMut(MetadataChangesQuery) -> F + Send,
        F: Future<Output = Result<MetadataChanges, IggyError>> + Send,
    {
        let mut query = self.first_query();
        loop {
            let page = match read(query.clone()).await {
                Ok(page) => page,
                Err(IggyError::TransientNotAccepted) if self.follow => {
                    tokio::time::sleep(self.interval.get_duration()).await;
                    continue;
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| "Problem reading metadata changes".to_owned());
                }
            };
            let quiet_follow_page = self.follow
                && page.status == MetadataChangeStatus::Ok
                && page.records.is_empty()
                && page.items.is_empty();
            if !quiet_follow_page {
                writeln!(output, "{}", serde_json::to_string(&page)?)
                    .and_then(|()| output.flush())
                    .with_context(|| "Problem writing metadata changes page".to_owned())?;
            }
            query = match self.step(query, page)? {
                Step::Read(next) => next,
                Step::Wait(next) => {
                    tokio::time::sleep(self.interval.get_duration()).await;
                    next
                }
                Step::Stop => return Ok(()),
            };
        }
    }

    /// The query that starts the read, with the parent groups a current scan needs.
    fn first_query(&self) -> MetadataChangesQuery {
        match self.query.start {
            MetadataChangeStart::Current(_) => MetadataChangesQuery {
                groups: self.scan_groups(),
                ..self.query.clone()
            },
            _ => self.query.clone(),
        }
    }

    /// What to read after `page` answered `query`.
    fn step(&self, mut query: MetadataChangesQuery, page: MetadataChanges) -> anyhow::Result<Step> {
        match page.status {
            MetadataChangeStatus::Ok => {}
            MetadataChangeStatus::Behind if self.follow => return Ok(Step::Wait(query)),
            MetadataChangeStatus::Behind => return Ok(Step::Stop),
            MetadataChangeStatus::Expired | MetadataChangeStatus::Reset if self.follow => {
                return Ok(Step::Read(self.current_state_query()));
            }
            MetadataChangeStatus::Expired | MetadataChangeStatus::Reset => {
                bail!(
                    "The history after the cursor is not available ({}), read it again with --from current",
                    serde_json::to_string(&page.status)?
                );
            }
        }
        if let MetadataChangeStart::Current(_) = query.start {
            return Ok(match (page.current_page_token, page.next) {
                (Some(token), _) => {
                    query.start = MetadataChangeStart::Current(Some(token));
                    Step::Read(query)
                }
                (None, Some(anchor)) if self.follow => Step::Read(MetadataChangesQuery {
                    start: MetadataChangeStart::After(anchor),
                    ..self.query.clone()
                }),
                _ => Step::Stop,
            });
        }
        if let Some(next) = page.next {
            query.start = MetadataChangeStart::After(next);
        }
        Ok(if page.has_more {
            Step::Read(query)
        } else if self.follow {
            Step::Wait(query)
        } else {
            Step::Stop
        })
    }

    /// The rebuild scan, with the parent groups and no time bounds the server requires.
    fn current_state_query(&self) -> MetadataChangesQuery {
        MetadataChangesQuery {
            start: MetadataChangeStart::Current(None),
            groups: self.scan_groups(),
            since: None,
            until: None,
            limit: self.query.limit,
        }
    }

    /// The requested groups and their parents.
    fn scan_groups(&self) -> Vec<MetadataChangeGroup> {
        let mut groups = self.query.groups.clone();
        groups.extend(self.query.groups.iter().flat_map(|group| group.parents()));
        groups
    }
}

#[async_trait]
impl CliCommand for GetMetadataChangesCmd {
    fn explain(&self) -> String {
        "metadata changes command".to_owned()
    }

    fn stdout_is_data(&self) -> bool {
        true
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        self.read_pages(
            |query| async move { client.get_metadata_changes(&query).await },
            &mut stdout(),
        )
        .await
    }
}

#[derive(Debug, PartialEq)]
enum Step {
    Read(MetadataChangesQuery),
    Wait(MetadataChangesQuery),
    Stop,
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{
        IggyTimestamp, MetadataChangeCursor, MetadataChangeGroup, MetadataChangeGroups,
        MetadataChangePhase,
    };
    use std::collections::VecDeque;
    use std::future::ready;

    #[test]
    fn given_current_start_for_topics_when_building_first_query_should_add_their_parent_groups() {
        let command = GetMetadataChangesCmd::new(
            MetadataChangesQuery {
                start: MetadataChangeStart::Current(None),
                groups: vec![MetadataChangeGroup::Topics],
                ..MetadataChangesQuery::default()
            },
            false,
            IggyDuration::ONE_SECOND,
        );

        assert_eq!(
            command.first_query().validate(),
            Ok(MetadataChangeGroups::requested(&[
                MetadataChangeGroup::Streams,
                MetadataChangeGroup::Topics
            ]))
        );
    }

    #[tokio::test]
    async fn given_transient_refusal_when_following_should_retry_the_same_query() {
        let command = GetMetadataChangesCmd::new(
            MetadataChangesQuery::default(),
            true,
            IggyDuration::from(1),
        );
        let mut answers = VecDeque::from([
            Err(IggyError::TransientNotAccepted),
            Ok(page(MetadataChangeStatus::Behind, None)),
            Err(IggyError::Unauthenticated),
        ]);
        let mut queries = Vec::new();
        let mut output = Vec::new();

        let result = command
            .read_pages(
                |query| {
                    queries.push(query);
                    ready(answers.pop_front().expect("scripted answer"))
                },
                &mut output,
            )
            .await;

        assert!(result.is_err(), "the scripted refusal ends the read");
        assert_eq!(queries.len(), 3);
        assert!(
            queries
                .iter()
                .all(|query| *query == MetadataChangesQuery::default())
        );
        assert_eq!(String::from_utf8(output).expect("utf-8").lines().count(), 1);
    }

    #[test]
    fn given_expired_cursor_when_following_topics_should_rebuild_with_their_parent_groups() {
        let command = following(vec![MetadataChangeGroup::Topics]);
        let step = command
            .step(
                command.query.clone(),
                page(MetadataChangeStatus::Expired, None),
            )
            .expect("recovery step");
        let Step::Read(recovery) = step else {
            panic!("expected an immediate read, got {step:?}");
        };
        assert_eq!(recovery.start, MetadataChangeStart::Current(None));
        assert_eq!(
            recovery.validate(),
            Ok(MetadataChangeGroups::requested(&[
                MetadataChangeGroup::Streams,
                MetadataChangeGroup::Topics
            ]))
        );
    }

    #[test]
    fn given_finished_recovery_scan_when_following_should_restore_requested_groups_and_bounds() {
        let command = following(vec![MetadataChangeGroup::Topics]);
        let Step::Read(recovery) = command
            .step(
                command.query.clone(),
                page(MetadataChangeStatus::Reset, None),
            )
            .expect("recovery step")
        else {
            panic!("expected an immediate read");
        };
        let anchor = MetadataChangeCursor {
            log_id: 7,
            op: 42,
            checksum: 9,
            ordinal: 0,
        };
        let step = command
            .step(
                recovery,
                MetadataChanges {
                    phase: MetadataChangePhase::Current,
                    ..page(MetadataChangeStatus::Ok, Some(anchor))
                },
            )
            .expect("hand over step");
        assert_eq!(
            step,
            Step::Read(MetadataChangesQuery {
                start: MetadataChangeStart::After(anchor),
                ..command.query.clone()
            })
        );
    }

    fn following(groups: Vec<MetadataChangeGroup>) -> GetMetadataChangesCmd {
        GetMetadataChangesCmd::new(
            MetadataChangesQuery {
                start: MetadataChangeStart::Earliest,
                groups,
                since: Some(IggyTimestamp::from(1_000)),
                until: Some(IggyTimestamp::from(9_000)),
                limit: 50,
            },
            true,
            IggyDuration::ONE_SECOND,
        )
    }

    fn page(status: MetadataChangeStatus, next: Option<MetadataChangeCursor>) -> MetadataChanges {
        MetadataChanges {
            status,
            phase: MetadataChangePhase::Changes,
            log_id: Some(7),
            oldest_op: 1,
            retained_since: 0,
            head_op: 42,
            next,
            has_more: false,
            records: vec![],
            items: vec![],
            current_page_token: None,
        }
    }
}
