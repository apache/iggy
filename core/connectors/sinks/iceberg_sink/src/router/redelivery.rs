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
//! Every append records in its snapshot summary the first offset and the last message it holds,
//! and a delivery of the same messages again skips the ones that proven appends cover.

use crate::router::BatchOrigin;
use crate::router::commit::table_load_error;
use dashmap::DashMap;
use iceberg::spec::Snapshot;
use iceberg::table::Table;
use iceberg::util::snapshot::ancestors_of;
use iceberg::{Catalog, TableIdent};
use iggy_connector_sdk::{ConsumedMessage, Error, FlushPartition, TopicMetadata};
use std::collections::{BTreeMap, HashMap};
use tracing::{info, warn};
use uuid::Uuid;

const STREAM_PROPERTY: &str = "iggy.stream";
const TOPIC_PROPERTY: &str = "iggy.topic";
const PARTITION_ID_PROPERTY: &str = "iggy.partition-id";
const FIRST_OFFSET_PROPERTY: &str = "iggy.first-offset";
const LAST_OFFSET_PROPERTY: &str = "iggy.last-offset";
const LAST_MESSAGE_ID_PROPERTY: &str = "iggy.last-message-id";
const LAST_TIMESTAMP_PROPERTY: &str = "iggy.last-timestamp";

/// The messages of a partition that one append holds: every message from offset `first` to
/// `last`, except the ones the sink skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Append {
    first: u64,
    last: LastMessage,
}

impl Append {
    /// The append of the messages from `first` to `last`.
    pub(crate) fn new(first: &ConsumedMessage, last: &ConsumedMessage) -> Self {
        Self {
            first: first.offset,
            last: LastMessage::from(last),
        }
    }

    fn from_properties(properties: &HashMap<String, String>) -> Option<Self> {
        Some(Self {
            first: properties.get(FIRST_OFFSET_PROPERTY)?.parse().ok()?,
            last: LastMessage::from_properties(properties)?,
        })
    }

    fn offsets(&self) -> (u64, u64) {
        (self.first, self.last.offset)
    }
}

/// The last message of a partition that an append holds. The message ID and the time the
/// server stored the message tell it apart from another message at the same offset in a later
/// history of the partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LastMessage {
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

/// The snapshot summary properties that record `append` as the messages of `origin` it holds.
pub(crate) fn snapshot_properties(
    origin: &BatchOrigin<'_>,
    append: Append,
) -> HashMap<String, String> {
    HashMap::from([
        (STREAM_PROPERTY.to_owned(), origin.stream.to_owned()),
        (TOPIC_PROPERTY.to_owned(), origin.topic.to_owned()),
        (
            PARTITION_ID_PROPERTY.to_owned(),
            origin.partition_id.to_string(),
        ),
        (FIRST_OFFSET_PROPERTY.to_owned(), append.first.to_string()),
        (
            LAST_OFFSET_PROPERTY.to_owned(),
            append.last.offset.to_string(),
        ),
        (
            LAST_MESSAGE_ID_PROPERTY.to_owned(),
            append.last.id.to_string(),
        ),
        (
            LAST_TIMESTAMP_PROPERTY.to_owned(),
            append.last.timestamp.to_string(),
        ),
    ])
}

/// A partition delivered to a table. The table UUID tells a table recreated under the same
/// name apart, once a router loads the table again. The static router keeps the table it loaded
/// first, so a recreated table needs a restart of the sink.
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
    /// Each append by the offset of its last message. Where two appends end at the same offset,
    /// as two histories of the partition can, the newer one is kept.
    by_last: BTreeMap<u64, Append>,
    /// The offset of the last message of the append committed last.
    newest: Option<u64>,
}

impl Appends {
    fn record(&mut self, append: Append) {
        self.by_last.insert(append.last.offset, append);
        self.newest = Some(append.last.offset);
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
    /// The tail of `messages` that `table` does not hold yet. `fresh` says that `table` was just
    /// loaded from the catalog, which saves loading it again.
    pub(crate) async fn unheld<'m>(
        &self,
        origin: &BatchOrigin<'_>,
        table: &Table,
        fresh: bool,
        catalog: &dyn Catalog,
        messages: &'m [ConsumedMessage],
    ) -> Result<&'m [ConsumedMessage], Error> {
        let key = TablePartition::new(origin, table);
        let loaded = if self.known.contains_key(&key) {
            None
        } else if fresh {
            Some(appends_in(table, origin))
        } else {
            let identifier = table.identifier();
            let current = catalog
                .load_table(identifier)
                .await
                .map_err(|error| table_load_error(&identifier.to_string(), &error))?;
            Some(appends_in(&current, origin))
        };
        let mut appends = self
            .known
            .entry(key)
            .or_insert_with(|| loaded.unwrap_or_default());
        let held = match held_prefix(origin, table.identifier(), messages, &appends) {
            Prefix::Held(held) => held,
            Prefix::NewHistory => {
                *appends = Appends::default();
                0
            }
            Prefix::Unproven => 0,
        };
        Ok(&messages[held..])
    }

    pub(crate) fn committed(&self, origin: &BatchOrigin<'_>, table: &Table, append: Append) {
        self.known
            .entry(TablePartition::new(origin, table))
            .or_default()
            .record(append);
    }

    pub(crate) fn forget(&self, origin: &BatchOrigin<'_>, table: &Table) {
        self.known.remove(&TablePartition::new(origin, table));
    }

    /// Forgets the appends that end at or before the offset each of `partitions` flushed. The
    /// runtime never hands those messages over again, and a restart reads the table anew.
    pub(crate) fn flushed(&self, topic_metadata: &TopicMetadata, partitions: &[FlushPartition]) {
        for mut known in self.known.iter_mut() {
            let (key, appends) = known.pair_mut();
            if key.stream != topic_metadata.stream || key.topic != topic_metadata.topic {
                continue;
            }
            if let Some(flushed) = partitions
                .iter()
                .find(|partition| partition.partition_id == key.partition_id)
            {
                appends.by_last.retain(|&last, _| last > flushed.offset);
            }
        }
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
        let Some(append) = Append::from_properties(properties) else {
            warn!(
                "Iceberg table {} has snapshot {} of {origin} without readable offsets. Skipping it",
                table.identifier(),
                snapshot.snapshot_id()
            );
            continue;
        };
        // The walk runs from the newest snapshot back, so the first one seen at an offset wins.
        appends.by_last.entry(append.last.offset).or_insert(append);
        appends.newest.get_or_insert(append.last.offset);
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
    /// None that can be proven, although appends cover some of the batch's offsets.
    Unproven,
}

/// What the table holds of the start of `messages`. An append counts only when the batch carries
/// its last message, which proves that the batch is the history the append holds. Once one does,
/// the appends after the end of the batch count too, up to the newest one, since a batch cut at
/// other offsets than before carries none of their last messages. The messages held are the
/// start of the batch that the counted appends cover without a gap, so a message that no append
/// holds, for example one an at-most-once run lost, is appended again.
fn held_prefix(
    origin: &BatchOrigin<'_>,
    table: &TableIdent,
    messages: &[ConsumedMessage],
    appends: &Appends,
) -> Prefix {
    let (Some(first), Some(last)) = (messages.first(), messages.last()) else {
        return Prefix::Held(0);
    };
    let mut proven = Vec::new();
    let mut other_history = false;
    for message in messages {
        match appends.by_last.get(&message.offset) {
            Some(append) if append.last == LastMessage::from(message) => {
                proven.push(append.offsets());
            }
            Some(_) => other_history = true,
            None => {}
        }
    }
    if proven.is_empty() {
        return prefix_without_proof(
            origin,
            table,
            first.offset,
            last.offset,
            appends,
            other_history,
        );
    }
    if let Some(newest) = appends.newest
        && newest > last.offset
    {
        proven.extend(
            appends
                .by_last
                .range(last.offset + 1..=newest)
                .map(|(_, append)| append.offsets()),
        );
    }
    proven.sort_unstable();
    let held = covered_prefix(messages, &proven);
    if held > 0 {
        info!(
            "Skipping {held} messages from offset: {} to offset: {} ({origin}) that Iceberg table {table} already holds",
            first.offset,
            messages[held - 1].offset,
        );
    }
    Prefix::Held(held)
}

/// What the table holds of a batch from offset `first` to `last` that carries the last message
/// of no append.
fn prefix_without_proof(
    origin: &BatchOrigin<'_>,
    table: &TableIdent,
    first: u64,
    last: u64,
    appends: &Appends,
    other_history: bool,
) -> Prefix {
    if other_history {
        warn!(
            "Iceberg table {table} holds other messages at the offsets of the batch ({origin}), so the partition has a new history. Appending every message of the batch"
        );
        return Prefix::NewHistory;
    }
    let covered = appends
        .by_last
        .range(first..)
        .any(|(_, append)| append.first <= last);
    if !covered {
        return Prefix::Held(0);
    }
    warn!(
        "Iceberg table {table} holds messages between offset: {first} and offset: {last} ({origin}), but the batch carries no message that proves which ones. Appending every message of the batch, which can append some twice"
    );
    Prefix::Unproven
}

/// How many messages from the start of `messages` the offset ranges cover without a gap. The
/// ranges are sorted by their first offset.
fn covered_prefix(messages: &[ConsumedMessage], ranges: &[(u64, u64)]) -> usize {
    let mut next = 0;
    let mut covered_to = None;
    messages
        .iter()
        .take_while(|message| {
            while let Some(&(first, last)) = ranges.get(next)
                && first <= message.offset
            {
                covered_to = covered_to.max(Some(last));
                next += 1;
            }
            covered_to.is_some_and(|to| message.offset <= to)
        })
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::commit::tests::{message, messages};

    const ORIGIN: BatchOrigin<'static> = BatchOrigin {
        connector_id: 1,
        stream: "stream",
        topic: "topic",
        partition_id: 2,
    };

    fn table() -> TableIdent {
        TableIdent::from_strs(["ns", "table"]).expect("a valid table name")
    }

    /// Appends of the messages from each first to each last offset, committed in this order.
    fn appends(offsets: &[(u64, u64)]) -> Appends {
        let mut appends = Appends::default();
        for &(first, last) in offsets {
            appends.record(Append::new(&message(first), &message(last)));
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
        let prefix = held_prefix(&ORIGIN, &table(), &messages(3..=5), &appends(&[(0, 4)]));

        assert_eq!(prefix, Prefix::Held(2));
    }

    #[test]
    fn given_a_backlog_batch_carrying_an_older_append_should_hold_the_appends_after_it() {
        let prefix = held_prefix(
            &ORIGIN,
            &table(),
            &messages(0..=2),
            &appends(&[(0, 1), (2, 3), (4, 5)]),
        );

        assert_eq!(prefix, Prefix::Held(3));
    }

    #[test]
    fn given_a_gap_between_appends_should_hold_only_the_messages_before_it() {
        let prefix = held_prefix(
            &ORIGIN,
            &table(),
            &messages(0..=7),
            &appends(&[(0, 1), (6, 7)]),
        );

        assert_eq!(prefix, Prefix::Held(2));
    }

    #[test]
    fn given_a_batch_ending_after_the_newest_append_should_not_count_older_appends_after_it() {
        let prefix = held_prefix(
            &ORIGIN,
            &table(),
            &messages(5..=12),
            &appends(&[(10, 19), (0, 9)]),
        );

        assert_eq!(prefix, Prefix::Held(5));
    }

    #[test]
    fn given_a_batch_ending_before_the_newest_append_should_not_count_older_appends_after_it() {
        let prefix = held_prefix(
            &ORIGIN,
            &table(),
            &messages(3..=7),
            &appends(&[(5, 19), (0, 4), (9, 10)]),
        );

        assert_eq!(prefix, Prefix::Held(2));
    }

    #[test]
    fn given_another_message_at_an_appended_offset_should_report_a_new_history() {
        let mut batch = messages(3..=4);
        batch[1].timestamp += 1;

        let prefix = held_prefix(&ORIGIN, &table(), &batch, &appends(&[(0, 4)]));

        assert_eq!(prefix, Prefix::NewHistory);
    }

    #[test]
    fn given_a_batch_after_the_newest_append_should_append_every_message() {
        let prefix = held_prefix(&ORIGIN, &table(), &messages(5..=6), &appends(&[(0, 4)]));

        assert_eq!(prefix, Prefix::Held(0));
    }

    #[test]
    fn given_a_batch_without_an_appended_message_should_append_every_message() {
        let prefix = held_prefix(
            &ORIGIN,
            &table(),
            &messages(2..=3),
            &appends(&[(0, 1), (2, 4)]),
        );

        assert_eq!(prefix, Prefix::Unproven);
    }
}
