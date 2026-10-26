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

use crate::client::{CaptureInstance, ChangeKey};
use crate::error::{ConversionError, MssqlError};
use crate::lsn::Position;
use crate::table::TableName;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{
    DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, SecondsFormat, SubsecRound,
    Timelike,
};
use serde::Serialize;
use serde_json::{Number, Value};
use std::collections::{BTreeMap, BTreeSet};
use tiberius::{ColumnData, FromSql};

pub(crate) const OPERATION_DELETE: u8 = 1;
pub(crate) const OPERATION_INSERT: u8 = 2;
pub(crate) const OPERATION_UPDATE_BEFORE: u8 = 3;
pub(crate) const OPERATION_UPDATE_AFTER: u8 = 4;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ChangeRow {
    pub(crate) position: Position,
    pub(crate) operation: u8,
    pub(crate) update_mask: Vec<u8>,
    pub(crate) commit_time: Option<DateTime<FixedOffset>>,
    pub(crate) values: Vec<Value>,
    pub(crate) instance: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct SourceInfo<'a> {
    database: &'a str,
    schema: &'a str,
    table: &'a str,
}

#[derive(Debug, Serialize)]
pub(crate) struct ChangeEvent<'a> {
    source: SourceInfo<'a>,
    op: &'static str,
    lsn: String,
    seqval: String,
    commit_time: Option<String>,
    before: Option<BTreeMap<&'a str, Value>>,
    after: Option<BTreeMap<&'a str, Value>>,
    unavailable_columns: Vec<&'a str>,
}

/// One event ready to send, with its deterministic message id.
#[derive(Debug)]
pub(crate) struct EncodedEvent<'a> {
    pub(crate) id: u128,
    pub(crate) table: &'a TableName,
    pub(crate) position: Position,
    pub(crate) commit_time: Option<DateTime<FixedOffset>>,
    pub(crate) event: ChangeEvent<'a>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Cut {
    /// Instances with keys inside the batch.
    pub(crate) instances: BTreeSet<usize>,
    /// Last position inside the batch. Every row query is bounded by it.
    pub(crate) last: Option<Position>,
    /// Keys were left behind, so the source is not caught up.
    pub(crate) left_behind: bool,
}

/// Sorts keys by (position, operation), takes `limit`, then extends to the end of the last group.
pub(crate) fn cut(mut keys: Vec<ChangeKey>, limit: usize) -> Cut {
    keys.sort_unstable_by_key(|key| (key.position, key.operation));
    let mut end = limit.min(keys.len());
    if end > 0 {
        let last = keys[end - 1].position;
        while end < keys.len() && keys[end].position == last {
            end += 1;
        }
    }
    Cut {
        instances: keys[..end].iter().map(|key| key.instance).collect(),
        last: end.checked_sub(1).map(|index| keys[index].position),
        left_behind: end < keys.len(),
    }
}

/// Merges rows in commit order and pairs update rows. Deletes and inserts pass through.
pub(crate) fn to_events<'a>(
    mut rows: Vec<ChangeRow>,
    instances: &'a [CaptureInstance],
    database: &'a str,
) -> Result<Vec<EncodedEvent<'a>>, MssqlError> {
    rows.sort_by_key(|row| (row.position, row.operation));
    let mut events = Vec::with_capacity(rows.len());
    let mut rows = rows.into_iter().peekable();
    while let Some(row) = rows.next() {
        let instance = &instances[row.instance];
        let event = match row.operation {
            OPERATION_INSERT => {
                build_event(database, instance, Identity::from(&row), None, Some(row))
            }
            OPERATION_DELETE => {
                build_event(database, instance, Identity::from(&row), Some(row), None)
            }
            OPERATION_UPDATE_BEFORE => {
                let after = rows.next_if(|next| {
                    next.operation == OPERATION_UPDATE_AFTER
                        && next.position == row.position
                        && next.instance == row.instance
                });
                let Some(after) = after else {
                    return Err(unpaired(instance, &row));
                };
                build_event(
                    database,
                    instance,
                    Identity::from(&after),
                    Some(row),
                    Some(after),
                )
            }
            _ => return Err(unpaired(instance, &row)),
        };
        events.push(event);
    }
    Ok(events)
}

/// Ordinal 1 is the lowest bit of the last byte.
pub(crate) fn is_column_changed(mask: &[u8], ordinal: u16) -> bool {
    let Some(bit) = ordinal.checked_sub(1) else {
        return false;
    };
    let byte_from_end = usize::from(bit / 8);
    if byte_from_end >= mask.len() {
        return false;
    }
    mask[mask.len() - 1 - byte_from_end] & (1 << (bit % 8)) != 0
}

pub(crate) fn column_value(data: &ColumnData<'static>) -> Result<Value, ConversionError> {
    let value = match data {
        ColumnData::U8(value) => value.map(Value::from),
        ColumnData::I16(value) => value.map(Value::from),
        ColumnData::I32(value) => value.map(Value::from),
        ColumnData::I64(value) => value.map(Value::from),
        ColumnData::F32(value) => value.map(real).transpose()?,
        ColumnData::F64(value) => value.map(float).transpose()?,
        ColumnData::Bit(value) => value.map(Value::Bool),
        ColumnData::String(value) => value.as_ref().map(|value| Value::String(value.to_string())),
        ColumnData::Guid(value) => value.map(|value| Value::String(value.to_string())),
        ColumnData::Binary(value) => value
            .as_ref()
            .map(|value| Value::String(BASE64.encode(value))),
        ColumnData::Numeric(value) => {
            value.map(|value| Value::String(format_numeric(value.value(), value.scale())))
        }
        ColumnData::Xml(value) => value.as_ref().map(|value| Value::String(value.to_string())),
        // datetime ticks are 1/300 s. SQL Server shows them rounded to milliseconds.
        ColumnData::DateTime(_) => from_sql::<NaiveDateTime>(data, "datetime")?
            .map(|value| Value::String(format_datetime(value.round_subsecs(3)))),
        ColumnData::SmallDateTime(_) | ColumnData::DateTime2(_) => {
            from_sql::<NaiveDateTime>(data, "datetime2")?
                .map(|value| Value::String(format_datetime(value)))
        }
        ColumnData::Date(_) => from_sql::<NaiveDate>(data, "date")?
            .map(|value| Value::String(value.format("%Y-%m-%d").to_string())),
        ColumnData::Time(_) => from_sql::<NaiveTime>(data, "time")?.map(|value| {
            Value::String(format!(
                "{}{}",
                value.format("%H:%M:%S"),
                fraction(value.nanosecond())
            ))
        }),
        ColumnData::DateTimeOffset(_) => from_sql::<DateTime<FixedOffset>>(data, "datetimeoffset")?
            .map(|value| {
                Value::String(format!(
                    "{}{}",
                    format_datetime(value.naive_local()),
                    value.format("%:z")
                ))
            }),
    };
    Ok(value.unwrap_or(Value::Null))
}

/// The first 16 bytes of a BLAKE3 hash, never 0, because 0 tells the server to assign an id.
pub(crate) fn message_id(
    database: &str,
    table: &TableName,
    position: Position,
    operation: u8,
) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(database.as_bytes());
    hasher.update(&[0]);
    hasher.update(table.schema.as_bytes());
    hasher.update(&[0]);
    hasher.update(table.table.as_bytes());
    hasher.update(&[0]);
    hasher.update(position.start_lsn.as_bytes());
    hasher.update(position.seqval.as_bytes());
    hasher.update(&[operation]);
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    u128::from_be_bytes(bytes).max(1)
}

/// The fields of the row that names an event: the after row of an update, else the row itself.
#[derive(Debug, Clone, Copy)]
struct Identity {
    position: Position,
    operation: u8,
    commit_time: Option<DateTime<FixedOffset>>,
}

impl From<&ChangeRow> for Identity {
    fn from(row: &ChangeRow) -> Self {
        Self {
            position: row.position,
            operation: row.operation,
            commit_time: row.commit_time,
        }
    }
}

fn build_event<'a>(
    database: &'a str,
    instance: &'a CaptureInstance,
    identity: Identity,
    before: Option<ChangeRow>,
    after: Option<ChangeRow>,
) -> EncodedEvent<'a> {
    let op = match (before.is_some(), after.is_some()) {
        (true, true) => "update",
        (true, false) => "delete",
        _ => "insert",
    };
    let is_update = after.is_some();
    let mut unavailable_columns = Vec::new();
    let before = before.map(|row| {
        let mut image = BTreeMap::new();
        for (column, value) in instance.columns.iter().zip(row.values) {
            let unchanged_lob =
                is_update && column.is_lob && !is_column_changed(&row.update_mask, column.ordinal);
            if column.is_legacy_lob || unchanged_lob {
                unavailable_columns.push(column.name.as_str());
                continue;
            }
            image.insert(column.name.as_str(), value);
        }
        image
    });
    let after = after.map(|row| image(instance, row));
    EncodedEvent {
        id: message_id(
            database,
            &instance.table,
            identity.position,
            identity.operation,
        ),
        table: &instance.table,
        position: identity.position,
        commit_time: identity.commit_time,
        event: ChangeEvent {
            source: SourceInfo {
                database,
                schema: &instance.table.schema,
                table: &instance.table.table,
            },
            op,
            lsn: identity.position.start_lsn.to_string(),
            seqval: identity.position.seqval.to_string(),
            commit_time: identity
                .commit_time
                .map(|time| time.to_rfc3339_opts(SecondsFormat::Millis, false)),
            before,
            after,
            unavailable_columns,
        },
    }
}

fn image(instance: &CaptureInstance, row: ChangeRow) -> BTreeMap<&str, Value> {
    instance
        .columns
        .iter()
        .zip(row.values)
        .map(|(column, value)| (column.name.as_str(), value))
        .collect()
}

fn unpaired(instance: &CaptureInstance, row: &ChangeRow) -> MssqlError {
    MssqlError::Conversion {
        table: instance.table.to_string(),
        column: "__$operation".to_string(),
        position: row.position,
        reason: ConversionError::UnpairedUpdate {
            operation: row.operation,
        },
    }
}

/// Widening f32 to f64 adds digits (3.4e38 becomes 3.399999952144364e38), so go through the shortest f32 text.
fn real(value: f32) -> Result<Value, ConversionError> {
    let widened = value
        .to_string()
        .parse::<f64>()
        .map_err(|_| ConversionError::InvalidNumeric)?;
    float(widened)
}

/// `YYYY-MM-DDTHH:MM:SS` plus the fraction, without an offset.
fn format_datetime(value: NaiveDateTime) -> String {
    format!(
        "{}{}",
        value.format("%Y-%m-%dT%H:%M:%S"),
        fraction(value.nanosecond())
    )
}

/// SQL Server keeps at most 100 ns, so at most 7 digits, trimmed to no fewer than 3.
fn fraction(nanos: u32) -> String {
    if nanos == 0 {
        return String::new();
    }
    let digits = format!("{:07}", nanos / 100);
    let trimmed = digits.trim_end_matches('0');
    format!(".{:0<3}", trimmed)
}

fn float(value: f64) -> Result<Value, ConversionError> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or(ConversionError::InvalidNumeric)
}

fn from_sql<'a, T: FromSql<'a>>(
    data: &'a ColumnData<'static>,
    expected: &'static str,
) -> Result<Option<T>, ConversionError> {
    T::from_sql(data).map_err(|_| ConversionError::UnexpectedVariant { expected })
}

/// Writes every digit of the scale. tiberius `Display` writes `5.0` for a `decimal(18,0)` of 5.
fn format_numeric(value: i128, scale: u8) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let digits = value.unsigned_abs().to_string();
    let scale = usize::from(scale);
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let digits = format!("{digits:0>width$}", width = scale + 1);
    let (integer, fraction) = digits.split_at(digits.len() - scale);
    format!("{sign}{integer}.{fraction}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::CapturedColumn;
    use crate::lsn::test_support::position;
    use serde_json::json;
    use std::borrow::Cow;
    use tiberius::numeric::Numeric;
    use tiberius::xml::XmlData;
    use tiberius::{IntoSql, Uuid};

    fn key(start_lsn: u8, seqval: u8, operation: u8, instance: usize) -> ChangeKey {
        ChangeKey {
            position: position(start_lsn, seqval),
            operation,
            instance,
        }
    }

    fn column(name: &str, ordinal: u16, sql_type: &str, max_length: i16) -> CapturedColumn {
        CapturedColumn::new(
            name.to_string(),
            ordinal,
            sql_type.to_string(),
            Some(max_length),
        )
    }

    fn instance(table: &str, columns: Vec<CapturedColumn>) -> CaptureInstance {
        CaptureInstance {
            name: format!("dbo_{table}"),
            key: format!("dbo.{table}"),
            table: TableName::new("dbo", table),
            columns,
            select_list: String::new(),
        }
    }

    fn users() -> CaptureInstance {
        instance(
            "users",
            vec![
                column("id", 1, "int", 4),
                column("balance", 2, "decimal", 9),
                column("note", 3, "nvarchar", -1),
            ],
        )
    }

    fn row(
        start_lsn: u8,
        seqval: u8,
        operation: u8,
        mask: u8,
        values: Vec<Value>,
        instance: usize,
    ) -> ChangeRow {
        ChangeRow {
            position: position(start_lsn, seqval),
            operation,
            update_mask: vec![mask],
            commit_time: None,
            values,
            instance,
        }
    }

    #[test]
    fn given_interleaved_transactions_cut_should_order_by_start_lsn() {
        let keys = vec![key(9, 1, 2, 0), key(5, 7, 2, 0)];

        let result = cut(keys, 1);

        assert_eq!(result.last, Some(position(5, 7)));
        assert!(result.left_behind);
    }

    #[test]
    fn given_cross_table_transaction_cut_should_include_both_instances() {
        let keys = vec![key(5, 1, 2, 0), key(5, 2, 2, 1), key(6, 1, 2, 0)];

        let result = cut(keys, 2);

        assert_eq!(result.instances, BTreeSet::from([0, 1]));
        assert_eq!(result.last, Some(position(5, 2)));
        assert!(result.left_behind);
    }

    #[test]
    fn given_limit_one_cut_should_keep_update_pair_together() {
        let keys = vec![key(5, 1, 3, 0), key(5, 1, 4, 0), key(6, 1, 2, 0)];

        let result = cut(keys, 1);

        assert_eq!(result.last, Some(position(5, 1)));
        assert!(result.left_behind);
    }

    #[test]
    fn given_limit_one_cut_should_keep_single_row_primary_key_update_together() {
        let keys = vec![key(5, 1, 2, 0), key(5, 1, 1, 0)];

        let result = cut(keys, 1);

        assert_eq!(result.last, Some(position(5, 1)));
        assert!(!result.left_behind);
    }

    #[test]
    fn given_multi_row_primary_key_update_cut_should_split_between_rows() {
        let keys = (1..=6).map(|seqval| key(5, seqval, 1, 0)).collect();

        let result = cut(keys, 5);

        assert_eq!(result.last, Some(position(5, 5)));
        assert!(result.left_behind);
    }

    #[test]
    fn given_no_keys_cut_should_be_empty() {
        let result = cut(Vec::new(), 5);

        assert_eq!(result.last, None);
        assert!(result.instances.is_empty());
        assert!(!result.left_behind);
    }

    #[test]
    fn given_ordinals_across_bytes_should_read_update_mask() {
        let mask = [0b1000_0000, 0b1000_0001];

        assert!(is_column_changed(&mask, 1));
        assert!(is_column_changed(&mask, 8));
        assert!(!is_column_changed(&mask, 9));
        assert!(is_column_changed(&mask, 16));
        assert!(!is_column_changed(&mask, 17));
        assert!(!is_column_changed(&mask, 0));
    }

    #[test]
    fn given_single_byte_masks_should_mark_their_ordinals() {
        assert!(is_column_changed(&[0x04], 3));
        assert!(is_column_changed(&[0x08], 4));
        assert!(!is_column_changed(&[0x08], 3));
    }

    #[test]
    fn given_insert_delete_and_update_should_build_events() {
        let instances = [users()];
        let rows = vec![
            row(5, 1, 2, 0x07, vec![json!(1), json!("10.50"), json!("a")], 0),
            row(
                6,
                1,
                3,
                0x02,
                vec![json!(1), json!("10.50"), Value::Null],
                0,
            ),
            row(6, 1, 4, 0x02, vec![json!(1), json!("11.00"), json!("a")], 0),
            row(7, 1, 1, 0x07, vec![json!(1), json!("11.00"), json!("a")], 0),
        ];

        let events = to_events(rows, &instances, "shop").expect("events");

        let ops: Vec<_> = events.iter().map(|event| event.event.op).collect();
        assert_eq!(ops, ["insert", "update", "delete"]);
        let update = serde_json::to_value(&events[1].event).expect("json");
        assert_eq!(update["before"], json!({"id": 1, "balance": "10.50"}));
        assert_eq!(
            update["after"],
            json!({"id": 1, "balance": "11.00", "note": "a"})
        );
        assert_eq!(update["unavailable_columns"], json!(["note"]));
        assert_eq!(
            update["source"],
            json!({"database": "shop", "schema": "dbo", "table": "users"})
        );
        assert_eq!(
            events[1].id,
            message_id("shop", &instances[0].table, position(6, 1), 4)
        );
    }

    #[test]
    fn given_changed_lob_should_keep_it_in_before_image() {
        let instances = [users()];
        let rows = vec![
            row(6, 1, 3, 0x04, vec![json!(1), json!("1"), json!("old")], 0),
            row(6, 1, 4, 0x04, vec![json!(1), json!("1"), json!("new")], 0),
        ];

        let events = to_events(rows, &instances, "shop").expect("events");

        let update = serde_json::to_value(&events[0].event).expect("json");
        assert_eq!(update["before"]["note"], json!("old"));
        assert_eq!(update["unavailable_columns"], json!([]));
    }

    #[test]
    fn given_unchanged_hierarchyid_should_stay_in_before_image() {
        let instances = [instance(
            "tree",
            vec![
                column("id", 1, "int", 4),
                column("node", 2, "hierarchyid", 892),
            ],
        )];
        let rows = vec![
            row(6, 1, 3, 0x01, vec![json!(1), json!("W0A=")], 0),
            row(6, 1, 4, 0x01, vec![json!(2), json!("W0A=")], 0),
        ];

        let events = to_events(rows, &instances, "shop").expect("events");

        let update = serde_json::to_value(&events[0].event).expect("json");
        assert_eq!(update["before"]["node"], json!("W0A="));
    }

    #[test]
    fn given_delete_with_text_column_should_list_it_unavailable() {
        let instances = [instance(
            "legacy",
            vec![column("id", 1, "int", 4), column("body", 2, "text", 16)],
        )];
        let rows = vec![row(7, 1, 1, 0x03, vec![json!(1), Value::Null], 0)];

        let events = to_events(rows, &instances, "shop").expect("events");

        let delete = serde_json::to_value(&events[0].event).expect("json");
        assert_eq!(delete["before"], json!({"id": 1}));
        assert_eq!(delete["after"], Value::Null);
        assert_eq!(delete["unavailable_columns"], json!(["body"]));
    }

    #[test]
    fn given_single_row_primary_key_update_should_give_two_events_with_two_ids() {
        let instances = [users()];
        let rows = vec![
            row(6, 1, 2, 0x07, vec![json!(2), json!("1"), json!("a")], 0),
            row(6, 1, 1, 0x07, vec![json!(1), json!("1"), json!("a")], 0),
        ];

        let events = to_events(rows, &instances, "shop").expect("events");

        let ops: Vec<_> = events.iter().map(|event| event.event.op).collect();
        assert_eq!(ops, ["delete", "insert"]);
        assert_ne!(events[0].id, events[1].id);
    }

    #[test]
    fn given_same_rows_read_again_after_nack_should_give_same_ids() {
        let instances = [users()];
        let rows = vec![
            row(5, 1, 2, 0x07, vec![json!(1), json!("1"), json!("a")], 0),
            row(6, 1, 1, 0x07, vec![json!(1), json!("1"), json!("a")], 0),
        ];

        let first: Vec<_> = to_events(rows.clone(), &instances, "shop")
            .expect("events")
            .iter()
            .map(|event| event.id)
            .collect();
        let second: Vec<_> = to_events(rows, &instances, "shop")
            .expect("events")
            .iter()
            .map(|event| event.id)
            .collect();

        assert_eq!(first, second);
    }

    #[test]
    fn given_unpaired_update_row_should_fail_conversion() {
        let instances = [users()];
        let rows = vec![row(
            6,
            1,
            3,
            0x02,
            vec![json!(1), json!("1"), Value::Null],
            0,
        )];

        let error = to_events(rows, &instances, "shop").expect_err("unpaired");

        assert!(matches!(
            error,
            MssqlError::Conversion {
                reason: ConversionError::UnpairedUpdate { operation: 3 },
                ..
            }
        ));
    }

    #[test]
    fn given_update_after_row_without_before_row_should_fail_conversion() {
        let instances = [users()];
        let rows = vec![row(
            6,
            1,
            4,
            0x02,
            vec![json!(1), json!("1"), json!("a")],
            0,
        )];

        let error = to_events(rows, &instances, "shop").expect_err("unpaired");

        assert!(matches!(
            error,
            MssqlError::Conversion {
                reason: ConversionError::UnpairedUpdate { operation: 4 },
                ..
            }
        ));
    }

    #[test]
    fn given_integer_float_and_bit_variants_should_convert() {
        assert_eq!(column_value(&ColumnData::U8(Some(7))), Ok(json!(7)));
        assert_eq!(column_value(&ColumnData::I16(Some(-7))), Ok(json!(-7)));
        assert_eq!(
            column_value(&ColumnData::I32(Some(70000))),
            Ok(json!(70000))
        );
        assert_eq!(
            column_value(&ColumnData::I64(Some(i64::MAX))),
            Ok(json!(i64::MAX))
        );
        assert_eq!(column_value(&ColumnData::F32(Some(1.5))), Ok(json!(1.5)));
        assert_eq!(
            column_value(&ColumnData::F64(Some(-2.25))),
            Ok(json!(-2.25))
        );
        assert_eq!(column_value(&ColumnData::Bit(Some(true))), Ok(json!(true)));
    }

    #[test]
    fn given_string_guid_binary_and_xml_variants_should_convert() {
        assert_eq!(
            column_value(&ColumnData::String(Some(Cow::from("tag_1   ")))),
            Ok(json!("tag_1   "))
        );
        let guid = Uuid::parse_str("6F9619FF-8B86-D011-B42D-00C04FC964FF").expect("uuid");
        assert_eq!(
            column_value(&ColumnData::Guid(Some(guid))),
            Ok(json!("6f9619ff-8b86-d011-b42d-00c04fc964ff"))
        );
        assert_eq!(
            column_value(&ColumnData::Binary(Some(Cow::from(vec![0x5B, 0x40])))),
            Ok(json!("W0A="))
        );
        assert_eq!(
            column_value(&ColumnData::Xml(Some(Cow::Owned(XmlData::new("<a/>"))))),
            Ok(json!("<a/>"))
        );
    }

    #[test]
    fn given_null_should_convert_to_json_null() {
        assert_eq!(column_value(&ColumnData::I32(None)), Ok(Value::Null));
        assert_eq!(column_value(&ColumnData::String(None)), Ok(Value::Null));
        assert_eq!(column_value(&ColumnData::DateTime2(None)), Ok(Value::Null));
    }

    #[test]
    fn given_numeric_should_keep_full_scale() {
        let numeric =
            |value, scale| ColumnData::Numeric(Some(Numeric::new_with_scale(value, scale)));

        assert_eq!(column_value(&numeric(5, 0)), Ok(json!("5")));
        assert_eq!(column_value(&numeric(1050, 2)), Ok(json!("10.50")));
        assert_eq!(column_value(&numeric(-5, 2)), Ok(json!("-0.05")));
        assert_eq!(column_value(&numeric(-1250, 3)), Ok(json!("-1.250")));
    }

    #[test]
    fn given_date_and_time_variants_should_convert_to_iso_8601() {
        let datetime2 = NaiveDate::from_ymd_opt(2026, 10, 20)
            .and_then(|date| date.and_hms_milli_opt(22, 42, 52, 530))
            .expect("datetime");
        let date = datetime2.date();
        let time = datetime2.time();
        let offset = DateTime::parse_from_rfc3339("2026-10-20T22:42:52.530+02:00").expect("offset");

        assert_eq!(
            column_value(&datetime2.into_sql()),
            Ok(json!("2026-10-20T22:42:52.530"))
        );
        assert_eq!(column_value(&date.into_sql()), Ok(json!("2026-10-20")));
        assert_eq!(column_value(&time.into_sql()), Ok(json!("22:42:52.530")));
        assert_eq!(
            column_value(&offset.into_sql()),
            Ok(json!("2026-10-20T22:42:52.530+02:00"))
        );
    }

    #[test]
    fn given_datetime_ticks_should_round_to_milliseconds() {
        let epoch = NaiveDate::from_ymd_opt(1900, 1, 1).expect("epoch");
        let date = NaiveDate::from_ymd_opt(2026, 10, 20).expect("date");
        let days = i32::try_from((date - epoch).num_days()).expect("days");
        let ten_o_clock_plus_299_ticks = 10 * 3600 * 300 + 299;
        let datetime = ColumnData::DateTime(Some(tiberius::time::DateTime::new(
            days,
            ten_o_clock_plus_299_ticks,
        )));

        assert_eq!(
            column_value(&datetime),
            Ok(json!("2026-10-20T10:00:00.997"))
        );
    }

    #[test]
    fn given_real_should_keep_its_shortest_decimal_form() {
        assert_eq!(
            column_value(&ColumnData::F32(Some(3.4e38))),
            Ok(json!(3.4e38))
        );
        assert_eq!(column_value(&ColumnData::F32(Some(0.1))), Ok(json!(0.1)));
    }

    #[test]
    fn given_time_7_should_keep_seven_fraction_digits() {
        let last_tick = ColumnData::Time(Some(tiberius::time::Time::new(863_999_999_999, 7)));

        assert_eq!(column_value(&last_tick), Ok(json!("23:59:59.9999999")));
    }

    #[test]
    fn given_fraction_should_trim_to_no_fewer_than_three_digits() {
        assert_eq!(fraction(0), "");
        assert_eq!(fraction(530_000_000), ".530");
        assert_eq!(fraction(123_456_700), ".1234567");
        assert_eq!(fraction(100), ".0000001");
    }

    #[test]
    fn given_same_change_should_hash_to_same_message_id() {
        let table = TableName::new("dbo", "users");

        assert_eq!(
            message_id("shop", &table, position(5, 1), 2),
            message_id("shop", &table, position(5, 1), 2)
        );
        assert_ne!(
            message_id("shop", &table, position(5, 1), 2),
            message_id("shop", &table, position(5, 1), 1)
        );
    }

    #[test]
    fn given_two_databases_with_same_lsn_should_hash_to_different_ids() {
        let table = TableName::new("dbo", "users");

        assert_ne!(
            message_id("shop", &table, position(5, 1), 2),
            message_id("billing", &table, position(5, 1), 2)
        );
    }
}
