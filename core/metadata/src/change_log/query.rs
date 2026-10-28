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

use crate::change_log::store::{ChangeEntry, ChangeLog, Ring};
use iggy_binary_protocol::WireEncode;
use iggy_binary_protocol::primitives::metadata_changes::{
    WireMetadataChangeCursor, WireMetadataChangePhase, WireMetadataChangeStatus,
};
use iggy_binary_protocol::responses::system::{
    GetMetadataChangesResponse, WireMetadataChangeRecord,
};
use iggy_common::{
    IggyError, MetadataChangeClass, MetadataChangeGroup, MetadataChangeGroups, MetadataChangeStart,
    MetadataChangesQuery,
};

/// Entries one change page examines at most, returned or skipped.
pub const MAX_SCANNED_ENTRIES: usize = 65_536;

/// Encoded record or item bytes one page carries at most, unless its first alone is larger.
pub const MAX_PAGE_BYTES: usize = 1024 * 1024;

const CLASSES: [MetadataChangeClass; 2] =
    [MetadataChangeClass::Resource, MetadataChangeClass::Activity];

/// One change page for a validated query, from a log image that holds every
/// record of the ops up to `head_op` and none above.
///
/// # Errors
/// `InvalidCommand` for a current state start, which [`current_page`](super::current_page) serves.
pub fn change_page(
    log: &ChangeLog,
    head_op: u64,
    query: &MetadataChangesQuery,
    groups: MetadataChangeGroups,
) -> Result<GetMetadataChangesResponse, IggyError> {
    let view = LogView::new(log, groups, head_op);
    let supplied = match &query.start {
        MetadataChangeStart::After(cursor) => Some(WireMetadataChangeCursor::from(*cursor)),
        MetadataChangeStart::Earliest | MetadataChangeStart::Latest => None,
        MetadataChangeStart::Current(_) => return Err(IggyError::InvalidCommand),
    };
    let Some(log_id) = log.log_id() else {
        return Ok(view.answer(WireMetadataChangeStatus::Behind, supplied));
    };
    let latest = view.latest_cursor(log_id);
    let start = match &query.start {
        MetadataChangeStart::Latest => {
            return Ok(view.answer(WireMetadataChangeStatus::Ok, Some(latest)));
        }
        // A partly retained floor operation is skipped whole.
        MetadataChangeStart::Earliest => (view.floor().0, u32::MAX),
        MetadataChangeStart::After(cursor) => {
            match view.check(WireMetadataChangeCursor::from(*cursor)) {
                CursorCheck::Valid(position) => position,
                CursorCheck::Behind => {
                    return Ok(view.answer(WireMetadataChangeStatus::Behind, supplied));
                }
                CursorCheck::Expired => {
                    return Ok(view.answer(WireMetadataChangeStatus::Expired, None));
                }
                CursorCheck::Reset => {
                    return Ok(view.answer(WireMetadataChangeStatus::Reset, None));
                }
            }
        }
        MetadataChangeStart::Current(_) => return Err(IggyError::InvalidCommand),
    };
    let page = view.scan(
        start,
        query.since.map(|since| since.as_micros()),
        query.until.map(|until| until.as_micros()),
        query.limit as usize,
    );
    let next = page
        .last_examined
        .map_or_else(|| supplied.unwrap_or(latest), |entry| entry.cursor(log_id));
    Ok(GetMetadataChangesResponse {
        next: Some(next),
        has_more: page.has_more,
        records: page.records,
        ..view.answer(WireMetadataChangeStatus::Ok, None)
    })
}

/// A log image read for one selection of groups.
pub struct LogView<'a> {
    log: &'a ChangeLog,
    groups: MetadataChangeGroups,
    head_op: u64,
}

/// What the cursor rules decide for a supplied position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorCheck {
    /// Serve the entries after this position.
    Valid((u64, u32)),
    Behind,
    Expired,
    Reset,
}

struct ScannedPage<'a> {
    records: Vec<WireMetadataChangeRecord>,
    last_examined: Option<&'a ChangeEntry>,
    has_more: bool,
}

impl<'a> LogView<'a> {
    pub const fn new(log: &'a ChangeLog, groups: MetadataChangeGroups, head_op: u64) -> Self {
        Self {
            log,
            groups,
            head_op,
        }
    }

    pub const fn head_op(&self) -> u64 {
        self.head_op
    }

    pub const fn log(&self) -> &'a ChangeLog {
        self.log
    }

    /// The rings of the requested classes.
    fn rings(&self) -> impl Iterator<Item = &'a Ring> + '_ {
        CLASSES
            .into_iter()
            .filter(|class| self.groups.contains_class(*class))
            .map(|class| self.log.ring(class))
    }

    /// The newest evicted position of the requested classes.
    pub fn floor(&self) -> (u64, u32) {
        self.rings()
            .map(Ring::evicted_through)
            .max()
            .unwrap_or((0, 0))
    }

    fn retained_since(&self) -> u64 {
        self.rings()
            .map(Ring::evicted_through_timestamp)
            .max()
            .unwrap_or(0)
    }

    /// Cursor at the newest record of the requested classes, or the zero
    /// cursor while they hold none.
    pub fn latest_cursor(&self, log_id: u128) -> WireMetadataChangeCursor {
        self.rings()
            .filter_map(|ring| ring.entries().back())
            .max_by_key(|entry| entry.position())
            .map_or_else(
                || WireMetadataChangeCursor::zero(log_id),
                |entry| entry.cursor(log_id),
            )
    }

    /// Cursor rules 2 to 6. Rule 1, an unknown log id, is the caller's.
    pub fn check(&self, cursor: WireMetadataChangeCursor) -> CursorCheck {
        if self.log.log_id() != Some(cursor.log_id) {
            return CursorCheck::Reset;
        }
        let floor = self.floor();
        if cursor.op == 0 {
            return if floor == (0, 0) {
                CursorCheck::Valid((0, 0))
            } else {
                CursorCheck::Expired
            };
        }
        if cursor.op > self.head_op {
            return CursorCheck::Behind;
        }
        let position = (cursor.op, cursor.ordinal);
        let found = CLASSES.into_iter().find_map(|class| {
            let entries = self.log.ring(class).entries();
            let index = entries.partition_point(|entry| entry.position() < position);
            entries
                .get(index)
                .filter(|entry| entry.position() == position)
        });
        match found {
            Some(entry) if entry.checksum != cursor.checksum => CursorCheck::Reset,
            Some(_) if position <= floor => CursorCheck::Expired,
            Some(_) => CursorCheck::Valid(position),
            None => {
                let highest_floor = CLASSES
                    .into_iter()
                    .map(|class| self.log.ring(class).evicted_through())
                    .max()
                    .unwrap_or((0, 0));
                if position <= highest_floor {
                    CursorCheck::Expired
                } else {
                    CursorCheck::Reset
                }
            }
        }
    }

    /// An answer without records or items, `next` as given.
    pub fn answer(
        &self,
        status: WireMetadataChangeStatus,
        next: Option<WireMetadataChangeCursor>,
    ) -> GetMetadataChangesResponse {
        GetMetadataChangesResponse {
            status,
            phase: WireMetadataChangePhase::Changes,
            log_id: self.log.log_id(),
            oldest_op: self.floor().0.saturating_add(1),
            retained_since: self.retained_since(),
            head_op: self.head_op,
            next,
            has_more: false,
            records: Vec::new(),
            items: Vec::new(),
            current_page_token: None,
        }
    }

    /// Merges the requested rings after `start` by position. The time bounds
    /// work as op bounds, since timestamps never decrease with the op.
    fn scan(
        &self,
        start: (u64, u32),
        since: Option<u64>,
        until: Option<u64>,
        limit: usize,
    ) -> ScannedPage<'a> {
        let mut heads: Vec<(&'a Ring, usize)> = self
            .rings()
            .map(|ring| {
                let entries = ring.entries();
                let after = entries.partition_point(|entry| entry.position() <= start);
                let since = since.map_or(0, |since| {
                    entries.partition_point(|entry| entry.timestamp < since)
                });
                (ring, after.max(since))
            })
            .collect();
        let in_range = |entry: &ChangeEntry| until.is_none_or(|until| entry.timestamp <= until);
        let next_entry = |heads: &[(&'a Ring, usize)]| {
            heads
                .iter()
                .enumerate()
                .filter_map(|(head, (ring, index))| {
                    ring.entries().get(*index).map(|entry| (head, &**entry))
                })
                .min_by_key(|(_, entry)| entry.position())
                .filter(|(_, entry)| in_range(entry))
        };
        let mut page = ScannedPage {
            records: Vec::new(),
            last_examined: None,
            has_more: false,
        };
        let mut examined = 0;
        let mut bytes = 0;
        while let Some((head, entry)) = next_entry(&heads) {
            let record = self
                .groups
                .contains(MetadataChangeGroup::from(entry.group()))
                .then(|| WireMetadataChangeRecord::from(entry));
            let size = record.as_ref().map_or(0, WireEncode::encoded_size);
            if examined == MAX_SCANNED_ENTRIES
                || page.records.len() == limit
                || (!page.records.is_empty() && bytes + size > MAX_PAGE_BYTES)
            {
                page.has_more = true;
                break;
            }
            examined += 1;
            heads[head].1 += 1;
            page.last_examined = Some(entry);
            if let Some(record) = record {
                bytes += size;
                page.records.push(record);
            }
        }
        page
    }
}

impl From<&ChangeEntry> for WireMetadataChangeRecord {
    fn from(entry: &ChangeEntry) -> Self {
        Self {
            op: entry.op,
            ordinal: entry.ordinal,
            checksum: entry.checksum,
            timestamp: entry.timestamp,
            actor_user_id: entry.actor_user_id,
            group: entry.group().as_u8(),
            kind: entry.kind.as_u8(),
            body: entry.body.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_log::AppliedChange;
    use crate::change_log::store::{ChangeLogCapacities, MetadataChangeLog};
    use bytes::Bytes;
    use iggy_binary_protocol::PrepareHeader;
    use iggy_binary_protocol::primitives::metadata_changes::WireMetadataChangeKind as Kind;
    use iggy_common::{IggyTimestamp, MetadataChangeCursor};

    const LOG_ID: u128 = 0x0d4b;
    const NO_RECORDS: [WireMetadataChangeRecord; 0] = [];

    #[test]
    fn given_unknown_log_id_when_reading_any_change_start_should_answer_behind() {
        let log = MetadataChangeLog::new(ChangeLogCapacities::default());
        let cursor = MetadataChangeCursor {
            log_id: LOG_ID,
            op: 3,
            checksum: 3,
            ordinal: 0,
        };

        for (start, next) in [
            (MetadataChangeStart::Earliest, None),
            (MetadataChangeStart::Latest, None),
            (
                MetadataChangeStart::After(cursor),
                Some(WireMetadataChangeCursor::from(cursor)),
            ),
        ] {
            let page = page(&log, 0, start, &[], 10);
            assert_eq!(page.status, WireMetadataChangeStatus::Behind);
            assert_eq!(page.log_id, None);
            assert_eq!(page.next, next);
        }
    }

    #[test]
    fn given_activity_only_reader_when_resource_ring_evicts_should_keep_zero_cursor_valid_until_activity_evicts()
     {
        let log = MetadataChangeLog::new(ChangeLogCapacities {
            resource: 1,
            activity: 1,
        });
        append(&log, 1, &[Kind::UserCreated]);
        append(&log, 2, &[Kind::StreamCreated]);
        let sessions = [MetadataChangeGroup::Sessions];

        let latest = page(&log, 2, MetadataChangeStart::Latest, &sessions, 10);
        assert_eq!(latest.status, WireMetadataChangeStatus::Ok);
        assert_eq!(latest.next, Some(WireMetadataChangeCursor::zero(LOG_ID)));
        let zero = MetadataChangeStart::After(MetadataChangeCursor::zero(LOG_ID));
        assert_eq!(
            page(&log, 2, zero.clone(), &sessions, 10).status,
            WireMetadataChangeStatus::Ok,
            "the resource eviction is outside the requested classes"
        );

        append(&log, 3, &[Kind::SessionRegistered]);
        let first = page(&log, 3, zero.clone(), &sessions, 10);
        assert_eq!(positions(&first), [(3, 0)]);
        assert_eq!(first.next.map(|next| (next.op, next.ordinal)), Some((3, 0)));

        append(&log, 4, &[Kind::SessionClosed]);
        assert_eq!(
            page(&log, 4, zero, &sessions, 10).status,
            WireMetadataChangeStatus::Expired
        );
    }

    #[test]
    fn given_each_cursor_rule_when_checking_should_answer_its_status() {
        let log = MetadataChangeLog::new(ChangeLogCapacities {
            resource: 2,
            activity: 4,
        });
        for op in 1..=4 {
            append(&log, op, &[Kind::StreamCreated]);
        }
        let cursor = |log_id, op: u64, checksum: u128, ordinal| {
            MetadataChangeStart::After(MetadataChangeCursor {
                log_id,
                op,
                checksum,
                ordinal,
            })
        };

        for (start, status) in [
            (cursor(LOG_ID + 1, 4, 4, 0), WireMetadataChangeStatus::Reset),
            (cursor(LOG_ID, 0, 0, 0), WireMetadataChangeStatus::Expired),
            (cursor(LOG_ID, 9, 9, 0), WireMetadataChangeStatus::Behind),
            (cursor(LOG_ID, 4, 5, 0), WireMetadataChangeStatus::Reset),
            (cursor(LOG_ID, 2, 2, 0), WireMetadataChangeStatus::Expired),
            (cursor(LOG_ID, 1, 1, 0), WireMetadataChangeStatus::Expired),
            (cursor(LOG_ID, 3, 3, 7), WireMetadataChangeStatus::Reset),
            (cursor(LOG_ID, 3, 3, 0), WireMetadataChangeStatus::Ok),
        ] {
            let answer = page(&log, 4, start.clone(), &[], 10);
            assert_eq!(answer.status, status, "{start:?}");
            match (status, &start) {
                (WireMetadataChangeStatus::Ok, _) => {}
                (WireMetadataChangeStatus::Behind, MetadataChangeStart::After(supplied)) => {
                    assert_eq!(answer.next, Some(WireMetadataChangeCursor::from(*supplied)));
                    assert_eq!(answer.records, NO_RECORDS);
                }
                _ => {
                    assert_eq!(answer.next, None, "{start:?}");
                    assert_eq!(answer.records, NO_RECORDS);
                }
            }
        }
    }

    #[test]
    fn given_limit_inside_one_operation_when_paging_should_continue_at_the_next_ordinal() {
        let log = MetadataChangeLog::new(ChangeLogCapacities::default());
        append(
            &log,
            1,
            &[
                Kind::StreamDeleted,
                Kind::UserPermissionsUpdated,
                Kind::UserPermissionsUpdated,
            ],
        );

        let first = page(&log, 1, MetadataChangeStart::Earliest, &[], 2);
        assert_eq!(positions(&first), [(1, 0), (1, 1)]);
        assert!(first.has_more);
        let next = first.next.expect("ok pages carry a cursor");
        let second = page(
            &log,
            1,
            MetadataChangeStart::After(MetadataChangeCursor::from(next)),
            &[],
            2,
        );

        assert_eq!(positions(&second), [(1, 2)]);
        assert!(!second.has_more);
    }

    #[test]
    fn given_unrequested_groups_when_paging_should_skip_them_and_still_advance() {
        let log = MetadataChangeLog::new(ChangeLogCapacities::default());
        append(&log, 1, &[Kind::StreamCreated, Kind::UserCreated]);
        append(&log, 2, &[Kind::StreamCreated]);

        let users = page(
            &log,
            2,
            MetadataChangeStart::Earliest,
            &[MetadataChangeGroup::Users],
            10,
        );

        assert_eq!(positions(&users), [(1, 1)]);
        assert_eq!(
            users.next.map(|next| (next.op, next.ordinal)),
            Some((2, 0)),
            "next names the last examined entry, filtered or not"
        );
        assert!(!users.has_more);
    }

    #[test]
    fn given_more_entries_than_the_scan_budget_when_paging_should_stop_with_more_to_examine() {
        let log = MetadataChangeLog::new(ChangeLogCapacities {
            resource: MAX_SCANNED_ENTRIES + 10,
            activity: 1,
        });
        for op in 1..=(MAX_SCANNED_ENTRIES as u64 + 2) {
            append(&log, op, &[Kind::StreamCreated]);
        }

        let users = page(
            &log,
            MAX_SCANNED_ENTRIES as u64 + 2,
            MetadataChangeStart::Earliest,
            &[MetadataChangeGroup::Users],
            1000,
        );

        assert_eq!(users.records, NO_RECORDS);
        assert!(users.has_more);
        assert_eq!(
            users.next.map(|next| next.op),
            Some(MAX_SCANNED_ENTRIES as u64)
        );
    }

    #[test]
    fn given_records_past_the_byte_budget_when_paging_should_stop_before_the_overflow() {
        let log = MetadataChangeLog::new(ChangeLogCapacities::default());
        append_body(&log, 1, Bytes::from(vec![0; MAX_PAGE_BYTES]));
        append_body(&log, 2, Bytes::from(vec![0; MAX_PAGE_BYTES / 2]));
        append_body(&log, 3, Bytes::from(vec![0; MAX_PAGE_BYTES / 2]));

        let mut pages = Vec::new();
        let mut start = MetadataChangeStart::Earliest;
        loop {
            let answer = page(&log, 3, start, &[], 10);
            let next = answer.next.expect("ok pages carry a cursor");
            start = MetadataChangeStart::After(MetadataChangeCursor::from(next));
            pages.push(positions(&answer));
            if !answer.has_more {
                break;
            }
        }

        assert_eq!(
            pages,
            [vec![(1, 0)], vec![(2, 0)], vec![(3, 0)]],
            "a record larger than the budget still makes progress"
        );
    }

    #[test]
    fn given_time_bounds_when_paging_should_keep_the_inclusive_range_with_saturated_timestamps() {
        let log = MetadataChangeLog::new(ChangeLogCapacities::default());
        // A runaway clock saturates: ops 3 to 5 share one timestamp.
        for (op, timestamp) in [(1, 10), (2, 20), (3, 30), (4, 30), (5, 30), (6, 60)] {
            append_at(&log, op, timestamp, &[Kind::StreamCreated]);
        }
        let bounded = |start, since: Option<u64>, until: Option<u64>| {
            let query = MetadataChangesQuery {
                start,
                since: since.map(IggyTimestamp::from),
                until: until.map(IggyTimestamp::from),
                limit: 10,
                ..MetadataChangesQuery::default()
            };
            log.read(|log| change_page(log, 6, &query, MetadataChangeGroups::all()))
                .expect("readable")
                .expect("change start")
        };
        let after_two = MetadataChangeStart::After(MetadataChangeCursor {
            log_id: LOG_ID,
            op: 2,
            checksum: 2,
            ordinal: 0,
        });

        let ops = |answer: &GetMetadataChangesResponse| {
            answer
                .records
                .iter()
                .map(|record| record.op)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ops(&bounded(MetadataChangeStart::Earliest, Some(30), None)),
            [3, 4, 5, 6]
        );
        assert_eq!(
            ops(&bounded(MetadataChangeStart::Earliest, None, Some(30))),
            [1, 2, 3, 4, 5]
        );
        assert_eq!(
            ops(&bounded(MetadataChangeStart::Earliest, Some(20), Some(30))),
            [2, 3, 4, 5]
        );
        let after = bounded(after_two.clone(), Some(10), Some(30));
        assert_eq!(ops(&after), [3, 4, 5]);
        assert!(!after.has_more, "op 6 is outside the range");
        assert_eq!(ops(&bounded(after_two, Some(60), None)), [6]);
    }

    #[test]
    fn given_evictions_when_answering_should_report_oldest_op_and_retained_since() {
        let log = MetadataChangeLog::new(ChangeLogCapacities {
            resource: 2,
            activity: 1,
        });
        append_at(&log, 1, 100, &[Kind::StreamCreated, Kind::StreamCreated]);
        append_at(
            &log,
            2,
            200,
            &[Kind::StreamCreated, Kind::SessionRegistered],
        );

        let resource = page(
            &log,
            2,
            MetadataChangeStart::Earliest,
            &[MetadataChangeGroup::Streams],
            10,
        );
        let all = page(&log, 2, MetadataChangeStart::Earliest, &[], 10);

        assert_eq!((resource.oldest_op, resource.retained_since), (2, 100));
        assert_eq!(
            positions(&resource),
            [(2, 0)],
            "earliest skips the partly retained floor operation"
        );
        assert_eq!((all.oldest_op, all.retained_since), (2, 100));
        assert_eq!(positions(&all), [(2, 0), (2, 1)]);
    }

    fn page(
        log: &MetadataChangeLog,
        head_op: u64,
        start: MetadataChangeStart,
        groups: &[MetadataChangeGroup],
        limit: u32,
    ) -> GetMetadataChangesResponse {
        let query = MetadataChangesQuery {
            start,
            groups: groups.to_vec(),
            limit,
            ..MetadataChangesQuery::default()
        };
        let groups = query.validate().expect("valid query");
        log.read(|log| change_page(log, head_op, &query, groups).expect("change start"))
            .expect("readable")
    }

    fn positions(answer: &GetMetadataChangesResponse) -> Vec<(u64, u32)> {
        answer
            .records
            .iter()
            .map(|record| (record.op, record.ordinal))
            .collect()
    }

    fn append(log: &MetadataChangeLog, op: u64, kinds: &[Kind]) {
        append_at(log, op, op, kinds);
    }

    fn append_body(log: &MetadataChangeLog, op: u64, body: Bytes) {
        let header = PrepareHeader {
            op,
            checksum: if op == 1 { LOG_ID } else { u128::from(op) },
            timestamp: op,
            ..PrepareHeader::default()
        };
        let change = AppliedChange {
            kind: Kind::StreamCreated,
            actor_user_id: None,
            body,
        };
        log.append(&header, vec![change]).expect("append");
    }

    fn append_at(log: &MetadataChangeLog, op: u64, timestamp: u64, kinds: &[Kind]) {
        let header = PrepareHeader {
            op,
            checksum: if op == 1 { LOG_ID } else { u128::from(op) },
            timestamp,
            ..PrepareHeader::default()
        };
        let changes = kinds
            .iter()
            .map(|&kind| AppliedChange {
                kind,
                actor_user_id: None,
                body: Bytes::new(),
            })
            .collect();
        log.append(&header, changes).expect("append");
    }
}
