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

//! Which messages of a partition a table already holds. At least once, the runtime hands a
//! message over again when it cannot prove it delivered, for example after a crash between the
//! commit and the offset store, or after a batch committed to one table and failed on another.
//! Every append records the last message it holds in its snapshot summary, and a delivery of
//! the same messages again appends only the ones after the newest append.

use crate::router::BatchOrigin;
use crate::router::commit::table_load_error;
use dashmap::DashMap;
use iceberg::spec::Snapshot;
use iceberg::table::Table;
use iceberg::util::snapshot::ancestors_of;
use iceberg::{Catalog, TableIdent};
use iggy_connector_sdk::{ConsumedMessage, Error};
use std::collections::{BTreeMap, HashMap};
use tracing::{info, warn};
use uuid::Uuid;

const STREAM_PROPERTY: &str = "iggy.stream";
const TOPIC_PROPERTY: &str = "iggy.topic";
const PARTITION_ID_PROPERTY: &str = "iggy.partition-id";
const LAST_OFFSET_PROPERTY: &str = "iggy.last-offset";
const LAST_MESSAGE_ID_PROPERTY: &str = "iggy.last-message-id";
const LAST_TIMESTAMP_PROPERTY: &str = "iggy.last-timestamp";

/// The last message of a partition that an append holds. The message ID and the time the
/// server stored the message tell it apart from another message at the same offset in a later
/// history of the partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LastMessage {
    offset: u64,
    id: u128,
    timestamp: u64,
}

impl From<&ConsumedMessage> for LastMessage {
    fn from(message: &ConsumedMessage) -> Self {
        Self {
            offset: message.offset,
            id: message.id,
            timestamp: message.timestamp,
        }
    }
}

impl LastMessage {
    fn from_properties(properties: &HashMap<String, String>) -> Option<Self> {
        Some(Self {
            offset: properties.get(LAST_OFFSET_PROPERTY)?.parse().ok()?,
            id: properties.get(LAST_MESSAGE_ID_PROPERTY)?.parse().ok()?,
            timestamp: properties.get(LAST_TIMESTAMP_PROPERTY)?.parse().ok()?,
        })
    }
}

/// The snapshot summary properties that record `last` as the last message of `origin` the
/// append holds.
pub(crate) fn snapshot_properties(
    origin: &BatchOrigin<'_>,
    last: LastMessage,
) -> HashMap<String, String> {
    HashMap::from([
        (STREAM_PROPERTY.to_owned(), origin.stream.to_owned()),
        (TOPIC_PROPERTY.to_owned(), origin.topic.to_owned()),
        (
            PARTITION_ID_PROPERTY.to_owned(),
            origin.partition_id.to_string(),
        ),
        (LAST_OFFSET_PROPERTY.to_owned(), last.offset.to_string()),
        (LAST_MESSAGE_ID_PROPERTY.to_owned(), last.id.to_string()),
        (
            LAST_TIMESTAMP_PROPERTY.to_owned(),
            last.timestamp.to_string(),
        ),
    ])
}

/// A partition delivered to a table. The table UUID tells a table recreated under the same
/// name apart.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TablePartition {
    table: Uuid,
    stream: String,
    topic: String,
    partition_id: u32,
}

impl TablePartition {
    fn new(origin: &BatchOrigin<'_>, table: &Table) -> Self {
        Self {
            table: table.metadata().uuid(),
            stream: origin.stream.to_owned(),
            topic: origin.topic.to_owned(),
            partition_id: origin.partition_id,
        }
    }
}

/// The appends of one partition that a table holds.
#[derive(Debug, Default)]
struct Appends {
    /// The last message of each append, by offset. Where two histories of the partition
    /// appended at the same offset, the newer one is kept.
    last_messages: BTreeMap<u64, LastMessage>,
    newest: Option<LastMessage>,
}

impl Appends {
    fn record(&mut self, last: LastMessage) {
        self.last_messages.insert(last.offset, last);
        self.newest = Some(last);
    }
}

/// What the sink knows each table holds of each partition it delivers. A partition is unknown
/// until the sink reads the table's snapshot history, and again after a commit that failed,
/// since the catalog may have applied it.
#[derive(Debug, Default)]
pub(crate) struct HeldMessages {
    known: DashMap<TablePartition, Appends>,
}

impl HeldMessages {
    /// The tail of `messages` that `table` does not hold yet. `current` is a copy of `table`
    /// just loaded from the catalog, which saves loading it again.
    pub(crate) async fn unheld<'m>(
        &self,
        origin: &BatchOrigin<'_>,
        table: &Table,
        current: Option<&Table>,
        catalog: &dyn Catalog,
        messages: &'m [ConsumedMessage],
    ) -> Result<&'m [ConsumedMessage], Error> {
        let key = TablePartition::new(origin, table);
        if !self.known.contains_key(&key) {
            let appends = match current {
                Some(current) => appends_in(current, origin),
                None => {
                    let identifier = table.identifier();
                    let current = catalog
                        .load_table(identifier)
                        .await
                        .map_err(|error| table_load_error(&identifier.to_string(), &error))?;
                    appends_in(&current, origin)
                }
            };
            self.known.entry(key.clone()).or_insert(appends);
        }
        let held = match self.known.get_mut(&key) {
            Some(mut appends) => {
                match held_prefix(origin, table.identifier(), messages, &appends) {
                    Prefix::Held(held) => held,
                    Prefix::NewHistory => {
                        *appends = Appends::default();
                        0
                    }
                    Prefix::Unproven => 0,
                }
            }
            None => 0,
        };
        Ok(&messages[held..])
    }

    /// The last message of a commit to record in its snapshot. A batch that the table could not
    /// prove it held, but that ends below the newest append, records that append again, so that
    /// the record never moves back within one history of the partition.
    pub(crate) fn last_to_record(
        &self,
        origin: &BatchOrigin<'_>,
        table: &Table,
        last: LastMessage,
    ) -> LastMessage {
        self.known
            .get(&TablePartition::new(origin, table))
            .and_then(|appends| appends.newest)
            .filter(|newest| newest.offset > last.offset)
            .unwrap_or(last)
    }

    pub(crate) fn committed(&self, origin: &BatchOrigin<'_>, table: &Table, last: LastMessage) {
        self.known
            .entry(TablePartition::new(origin, table))
            .or_default()
            .record(last);
    }

    pub(crate) fn forget(&self, origin: &BatchOrigin<'_>, table: &Table) {
        self.known.remove(&TablePartition::new(origin, table));
    }
}

/// The appends of `origin` on the current snapshot's ancestry. Snapshots that a rollback or a
/// replace left off that ancestry do not count, since the table no longer holds their rows.
fn appends_in(table: &Table, origin: &BatchOrigin<'_>) -> Appends {
    let mut appends = Appends::default();
    let Some(current) = table.metadata().current_snapshot_id() else {
        return appends;
    };
    let partition_id = origin.partition_id.to_string();
    for snapshot in ancestors_of(&table.metadata_ref(), current) {
        if !records(&snapshot, origin, &partition_id) {
            continue;
        }
        let properties = &snapshot.summary().additional_properties;
        let Some(last) = LastMessage::from_properties(properties) else {
            warn!(
                "Iceberg table {} has snapshot {} of {origin} without a readable last message. Skipping it",
                table.identifier(),
                snapshot.snapshot_id()
            );
            continue;
        };
        // The walk runs from the newest snapshot back, so the first one seen at an offset wins.
        appends.last_messages.entry(last.offset).or_insert(last);
        appends.newest.get_or_insert(last);
    }
    appends
}

fn records(snapshot: &Snapshot, origin: &BatchOrigin<'_>, partition_id: &str) -> bool {
    let properties = &snapshot.summary().additional_properties;
    properties.get(STREAM_PROPERTY).map(String::as_str) == Some(origin.stream)
        && properties.get(TOPIC_PROPERTY).map(String::as_str) == Some(origin.topic)
        && properties.get(PARTITION_ID_PROPERTY).map(String::as_str) == Some(partition_id)
}

/// What a table holds of the start of a batch.
#[derive(Debug, PartialEq, Eq)]
enum Prefix {
    /// This many messages, proven to be the history the table holds.
    Held(usize),
    /// None: the table holds other messages at the batch's offsets.
    NewHistory,
    /// None that can be proven, although the batch reaches offsets the table holds.
    Unproven,
}

/// What the table holds of the start of `messages`. Messages count as held only when the batch
/// carries the last message of one of the table's appends, which proves that it is the history
/// the table holds. Then every message up to the newest append is held.
fn held_prefix(
    origin: &BatchOrigin<'_>,
    table: &TableIdent,
    messages: &[ConsumedMessage],
    appends: &Appends,
) -> Prefix {
    let Some(newest) = appends.newest else {
        return Prefix::Held(0);
    };
    // A run of one partition carries ascending offsets.
    let held = messages.partition_point(|message| message.offset <= newest.offset);
    if held == 0 {
        return Prefix::Held(0);
    }
    let mut other_history = false;
    for message in &messages[..held] {
        match appends.last_messages.get(&message.offset) {
            Some(last) if *last == LastMessage::from(message) => {
                info!(
                    "Skipping {held} messages from offset: {} to offset: {} ({origin}) that Iceberg table {table} already holds",
                    messages[0].offset,
                    messages[held - 1].offset,
                );
                return Prefix::Held(held);
            }
            Some(_) => other_history = true,
            None => {}
        }
    }
    if other_history {
        warn!(
            "Iceberg table {table} holds other messages at the offsets of the batch ({origin}), so the partition has a new history. Appending every message of the batch"
        );
        return Prefix::NewHistory;
    }
    warn!(
        "Iceberg table {table} holds messages up to offset: {} ({origin}), but the batch carries no message that proves which ones. Appending every message of the batch, which can append some twice",
        newest.offset
    );
    Prefix::Unproven
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::Payload;

    const ORIGIN: BatchOrigin<'static> = BatchOrigin {
        connector_id: 1,
        stream: "stream",
        topic: "topic",
        partition_id: 2,
    };

    fn table() -> TableIdent {
        TableIdent::from_strs(["ns", "table"]).expect("a valid table name")
    }

    fn message(offset: u64) -> ConsumedMessage {
        ConsumedMessage {
            id: u128::from(offset) + 100,
            offset,
            checksum: 0,
            timestamp: offset + 1_000,
            origin_timestamp: 0,
            headers: None,
            payload: Payload::Raw(Vec::new()),
        }
    }

    fn messages(offsets: std::ops::RangeInclusive<u64>) -> Vec<ConsumedMessage> {
        offsets.map(message).collect()
    }

    fn appends(last_offsets: &[u64]) -> Appends {
        let mut appends = Appends::default();
        for &offset in last_offsets {
            appends.record(LastMessage::from(&message(offset)));
        }
        appends
    }

    #[test]
    fn given_nothing_held_should_append_every_message() {
        let prefix = held_prefix(&ORIGIN, &table(), &messages(0..=1), &Appends::default());

        assert_eq!(prefix, Prefix::Held(0));
    }

    #[test]
    fn given_a_batch_carrying_the_newest_append_should_hold_up_to_it() {
        let prefix = held_prefix(&ORIGIN, &table(), &messages(3..=5), &appends(&[4]));

        assert_eq!(prefix, Prefix::Held(2));
    }

    #[test]
    fn given_a_backlog_batch_carrying_an_older_append_should_hold_up_to_the_newest() {
        let prefix = held_prefix(&ORIGIN, &table(), &messages(0..=1), &appends(&[1, 3, 5]));

        assert_eq!(prefix, Prefix::Held(2));
    }

    #[test]
    fn given_another_message_at_an_appended_offset_should_report_a_new_history() {
        let mut batch = messages(3..=4);
        batch[1].timestamp += 1;

        let prefix = held_prefix(&ORIGIN, &table(), &batch, &appends(&[4]));

        assert_eq!(prefix, Prefix::NewHistory);
    }

    #[test]
    fn given_a_batch_after_the_newest_append_should_append_every_message() {
        let prefix = held_prefix(&ORIGIN, &table(), &messages(5..=6), &appends(&[4]));

        assert_eq!(prefix, Prefix::Held(0));
    }

    #[test]
    fn given_a_batch_without_an_appended_message_should_append_every_message() {
        let prefix = held_prefix(&ORIGIN, &table(), &messages(2..=3), &appends(&[1, 4]));

        assert_eq!(prefix, Prefix::Unproven);
    }
}
