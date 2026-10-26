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

use crate::encode::{ChangeRow, OPERATION_UPDATE_BEFORE, column_value};
use crate::error::MssqlError;
use crate::lsn::{Lsn, Position};
use crate::table::{TableName, quote_identifier};
use chrono::{DateTime, FixedOffset};
use std::time::Duration;
use tiberius::{Client, Config, Row, ToSql};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use uuid::Uuid;

const ROW_METADATA_COLUMNS: usize = 5;
const UNKNOWN_BUILT_IN_FUNCTION: u32 = 195;
// From the `times` join, with the offset in effect at commit time so changes before a DST switch stay correct.
const COMMIT_TIME_BY_ZONE: &str = "times.tran_end_time AT TIME ZONE CURRENT_TIMEZONE_ID()";
// Pre-2022 servers lack CURRENT_TIMEZONE_ID() and get today's offset. Untested: the tests run SQL Server 2022.
const COMMIT_TIME_BY_CURRENT_OFFSET: &str =
    "TODATETIMEOFFSET(times.tran_end_time, DATEPART(TZOFFSET, SYSDATETIMEOFFSET()))";

pub(crate) struct CdcClient {
    client: Client<Compat<TcpStream>>,
    query_timeout: Duration,
    commit_time: &'static str,
}

/// A capture instance as listed by `cdc.change_tables`, before its column types are checked.
#[derive(Debug)]
pub(crate) struct DiscoveredInstance {
    pub(crate) name: String,
    pub(crate) columns: Vec<CapturedColumn>,
}

/// A capture instance the connector reads from.
#[derive(Debug)]
pub(crate) struct CaptureInstance {
    pub(crate) name: String,
    /// Canonical names from SQL Server, not the configured string.
    pub(crate) table: TableName,
    /// `schema.table`, the key of this table in the saved state.
    pub(crate) key: String,
    pub(crate) columns: Vec<CapturedColumn>,
    /// Precomputed select list with per-type casts.
    pub(crate) select_list: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedColumn {
    pub(crate) name: String,
    pub(crate) ordinal: u16,
    pub(crate) sql_type: String,
    /// Unchanged values are NULL in update before images.
    pub(crate) is_lob: bool,
    /// text, ntext and image: always NULL in delete and before image rows.
    pub(crate) is_legacy_lob: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChangeKey {
    pub(crate) position: Position,
    pub(crate) operation: u8,
    pub(crate) instance: usize,
}

impl CaptureInstance {
    /// Fails with `UnsupportedType` when a captured column has no JSON mapping.
    pub(crate) fn new(
        discovered: DiscoveredInstance,
        table: TableName,
    ) -> Result<Self, MssqlError> {
        let select_list =
            select_list(&discovered.columns).map_err(|columns| MssqlError::UnsupportedType {
                table: table.to_string(),
                columns,
            })?;
        Ok(Self {
            name: discovered.name,
            key: table.to_string(),
            table,
            columns: discovered.columns,
            select_list,
        })
    }
}

impl CapturedColumn {
    pub(crate) fn new(
        name: String,
        ordinal: u16,
        sql_type: String,
        max_length: Option<i16>,
    ) -> Self {
        let is_legacy_lob = matches!(sql_type.as_str(), "text" | "ntext" | "image");
        Self {
            is_lob: is_legacy_lob || max_length == Some(-1),
            is_legacy_lob,
            name,
            ordinal,
            sql_type,
        }
    }

    /// The `changes`-qualified select expression, or `None` for a type outside the type table.
    pub(crate) fn select_expression(&self) -> Option<String> {
        let quoted = quote_identifier(&self.name);
        let column = format!("changes.{quoted}");
        let expression = match self.sql_type.as_str() {
            "money" | "smallmoney" => format!("CAST({column} AS DECIMAL(19, 4)) AS {quoted}"),
            "geography" | "geometry" | "hierarchyid" => {
                format!("CAST({column} AS VARBINARY(MAX)) AS {quoted}")
            }
            "tinyint" | "smallint" | "int" | "bigint" | "bit" | "decimal" | "numeric" | "real"
            | "float" | "char" | "varchar" | "nchar" | "nvarchar" | "text" | "ntext" | "date"
            | "time" | "datetime" | "datetime2" | "smalldatetime" | "datetimeoffset"
            | "uniqueidentifier" | "binary" | "varbinary" | "image" | "timestamp"
            | "rowversion" | "xml" => format!("{column} AS {quoted}"),
            _ => return None,
        };
        Some(expression)
    }
}

/// Builds the select list, or returns every `(column, type)` that is not supported.
fn select_list(columns: &[CapturedColumn]) -> Result<String, Vec<(String, String)>> {
    let mut expressions = Vec::with_capacity(columns.len());
    let mut unsupported = Vec::new();
    for column in columns {
        match column.select_expression() {
            Some(expression) => expressions.push(expression),
            None => unsupported.push((column.name.clone(), column.sql_type.clone())),
        }
    }
    if !unsupported.is_empty() {
        return Err(unsupported);
    }
    Ok(expressions.join(", "))
}

impl CdcClient {
    pub(crate) async fn connect(
        config: &Config,
        query_timeout: Duration,
    ) -> Result<Self, MssqlError> {
        let connect = async {
            let tcp = TcpStream::connect(config.get_addr())
                .await
                .map_err(|error| MssqlError::Connect(error.to_string()))?;
            tcp.set_nodelay(true)
                .map_err(|error| MssqlError::Connect(error.to_string()))?;
            Client::connect(config.clone(), tcp.compat_write())
                .await
                .map_err(MssqlError::from)
        };
        let client = timeout(query_timeout, connect)
            .await
            .map_err(|_| MssqlError::Timeout)??;
        let mut connected = Self {
            client,
            query_timeout,
            commit_time: COMMIT_TIME_BY_ZONE,
        };
        match connected.rows("SELECT CURRENT_TIMEZONE_ID()", &[]).await {
            Ok(_) => {}
            Err(MssqlError::Server { number, .. }) if number == UNKNOWN_BUILT_IN_FUNCTION => {
                connected.commit_time = COMMIT_TIME_BY_CURRENT_OFFSET;
            }
            Err(error) => return Err(error),
        }
        Ok(connected)
    }

    /// False when the server cannot convert commit times with the offset in effect at commit time.
    pub(crate) fn has_time_zone_support(&self) -> bool {
        self.commit_time == COMMIT_TIME_BY_ZONE
    }

    /// The database name and its `recovery_fork_guid`.
    pub(crate) async fn database(&mut self) -> Result<(String, Uuid), MssqlError> {
        let row = self
            .single_row(
                "SELECT DB_NAME(), recovery_fork_guid FROM sys.database_recovery_status WHERE database_id = DB_ID()",
                &[],
            )
            .await?
            .ok_or(MssqlError::MissingMetadata(
                "sys.database_recovery_status for the current database",
            ))?;
        let name = row
            .try_get::<&str, _>(0)?
            .ok_or(MssqlError::MissingMetadata("DB_NAME()"))?
            .to_string();
        let fork = row
            .try_get::<Uuid, _>(1)?
            .ok_or(MssqlError::MissingMetadata("recovery_fork_guid"))?;
        Ok((name, fork))
    }

    pub(crate) async fn check_cdc_enabled(&mut self, database: &str) -> Result<(), MssqlError> {
        let row = self
            .single_row(
                "SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()",
                &[],
            )
            .await?
            .ok_or(MssqlError::MissingMetadata("sys.databases.is_cdc_enabled"))?;
        let enabled = row.try_get::<bool, _>(0)?.unwrap_or(false);
        if !enabled {
            return Err(MssqlError::CdcNotEnabled {
                database: database.to_string(),
            });
        }
        Ok(())
    }

    /// Canonical schema and table names, or `TableNotFound`.
    pub(crate) async fn resolve_table(
        &mut self,
        table: &TableName,
    ) -> Result<TableName, MssqlError> {
        let quoted = table.quoted();
        let row = self
            .single_row(
                "SELECT OBJECT_SCHEMA_NAME(OBJECT_ID(@P1)), OBJECT_NAME(OBJECT_ID(@P1))",
                &[&quoted],
            )
            .await?;
        let not_found = || MssqlError::TableNotFound {
            table: table.to_string(),
        };
        let row = row.ok_or_else(not_found)?;
        let schema = row.try_get::<&str, _>(0)?.ok_or_else(not_found)?;
        let name = row.try_get::<&str, _>(1)?.ok_or_else(not_found)?;
        Ok(TableName::new(schema, name))
    }

    /// Every capture instance of the table, newest first, with its captured columns.
    pub(crate) async fn capture_instances(
        &mut self,
        table: &TableName,
    ) -> Result<Vec<DiscoveredInstance>, MssqlError> {
        let quoted = table.quoted();
        let rows = self
            .rows(
                "SELECT capture_instance, object_id FROM cdc.change_tables \
                 WHERE source_object_id = OBJECT_ID(@P1) ORDER BY create_date DESC, object_id DESC",
                &[&quoted],
            )
            .await?;
        let mut instances = Vec::with_capacity(rows.len());
        for row in rows {
            let name = row
                .try_get::<&str, _>(0)?
                .ok_or(MssqlError::MissingMetadata(
                    "cdc.change_tables.capture_instance",
                ))?
                .to_string();
            let object_id = row
                .try_get::<i32, _>(1)?
                .ok_or(MssqlError::MissingMetadata("cdc.change_tables.object_id"))?;
            instances.push((name, object_id));
        }

        let mut result = Vec::with_capacity(instances.len());
        for (name, object_id) in instances {
            let columns = self.captured_columns(object_id).await?;
            result.push(DiscoveredInstance { name, columns });
        }
        Ok(result)
    }

    /// Zero when the instance is gone or the login cannot read its change data.
    pub(crate) async fn min_lsn(&mut self, capture_instance: &str) -> Result<Lsn, MssqlError> {
        // fn_cdc_get_min_lsn returns NULL for a new instance with no changes yet. Its start_lsn is the minimum.
        let lsn = self
            .single_lsn(
                "SELECT COALESCE(sys.fn_cdc_get_min_lsn(@P1), \
                 (SELECT start_lsn FROM cdc.change_tables WHERE capture_instance = @P1))",
                &[&capture_instance],
            )
            .await?;
        Ok(lsn.unwrap_or(Lsn::ZERO))
    }

    /// `None` on a CDC database that has no changes yet.
    pub(crate) async fn max_lsn(&mut self) -> Result<Option<Lsn>, MssqlError> {
        self.single_lsn("SELECT sys.fn_cdc_get_max_lsn()", &[])
            .await
    }

    /// Seconds since the transaction at `lsn` committed, by the server's clock.
    pub(crate) async fn lsn_age_seconds(&mut self, lsn: Lsn) -> Result<Option<i64>, MssqlError> {
        let row = self
            .single_row(
                "SELECT DATEDIFF_BIG(SECOND, sys.fn_cdc_map_lsn_to_time(@P1), GETDATE())",
                &[&lsn.as_bytes()],
            )
            .await?;
        match row {
            Some(row) => Ok(row.try_get::<i64, _>(0)?),
            None => Ok(None),
        }
    }

    /// Keys after `from` up to `to`, at least `limit` of them when that many exist, ending on a whole group.
    pub(crate) async fn change_keys(
        &mut self,
        instance: &CaptureInstance,
        instance_index: usize,
        from: Position,
        to: Lsn,
        limit: usize,
    ) -> Result<Vec<ChangeKey>, MssqlError> {
        if from.start_lsn > to {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT TOP ({limit}) WITH TIES __$start_lsn, __$seqval, __$operation \
             FROM cdc.{function}(@P1, @P2, N'all update old') \
             WHERE __$start_lsn > @P1 OR (__$start_lsn = @P1 AND __$seqval > @P3) \
             ORDER BY __$start_lsn, __$seqval",
            function = change_function(&instance.name),
        );
        let rows = self
            .rows(
                &sql,
                &[
                    &from.start_lsn.as_bytes(),
                    &to.as_bytes(),
                    &from.seqval.as_bytes(),
                ],
            )
            .await?;
        rows.iter()
            .map(|row| {
                Ok(ChangeKey {
                    position: read_position(row)?,
                    operation: read_operation(row)?,
                    instance: instance_index,
                })
            })
            .collect()
    }

    /// Full rows after `from` up to and including `through`.
    pub(crate) async fn change_rows(
        &mut self,
        instance: &CaptureInstance,
        instance_index: usize,
        from: Position,
        through: Position,
        to: Lsn,
    ) -> Result<Vec<ChangeRow>, MssqlError> {
        if from.start_lsn > to || from >= through {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT changes.__$start_lsn, changes.__$seqval, changes.__$operation, changes.__$update_mask, \
             {commit_time}, \
             {columns} \
             FROM cdc.{function}(@P1, @P2, N'all update old') AS changes \
             LEFT JOIN cdc.lsn_time_mapping AS times ON times.start_lsn = changes.__$start_lsn \
             WHERE (changes.__$start_lsn > @P1 OR (changes.__$start_lsn = @P1 AND changes.__$seqval > @P3)) \
             AND (changes.__$start_lsn < @P4 OR (changes.__$start_lsn = @P4 AND changes.__$seqval <= @P5)) \
             ORDER BY changes.__$start_lsn, changes.__$seqval, changes.__$operation",
            commit_time = self.commit_time,
            columns = instance.select_list,
            function = change_function(&instance.name),
        );
        let rows = self
            .rows(
                &sql,
                &[
                    &from.start_lsn.as_bytes(),
                    &to.as_bytes(),
                    &from.seqval.as_bytes(),
                    &through.start_lsn.as_bytes(),
                    &through.seqval.as_bytes(),
                ],
            )
            .await?;
        rows.iter()
            .map(|row| read_change_row(row, instance, instance_index))
            .collect()
    }

    async fn captured_columns(
        &mut self,
        object_id: i32,
    ) -> Result<Vec<CapturedColumn>, MssqlError> {
        let rows = self
            .rows(
                "SELECT cc.column_name, cc.column_ordinal, cc.column_type, c.max_length \
                 FROM cdc.captured_columns cc \
                 JOIN cdc.change_tables ct ON ct.object_id = cc.object_id \
                 LEFT JOIN sys.columns c ON c.object_id = ct.source_object_id AND c.column_id = cc.column_id \
                 WHERE cc.object_id = @P1 AND cc.is_computed = 0 \
                 ORDER BY cc.column_ordinal",
                &[&object_id],
            )
            .await?;
        rows.iter()
            .map(|row| {
                let name = row
                    .try_get::<&str, _>(0)?
                    .ok_or(MssqlError::MissingMetadata(
                        "cdc.captured_columns.column_name",
                    ))?;
                let ordinal = row
                    .try_get::<i32, _>(1)?
                    .and_then(|ordinal| u16::try_from(ordinal).ok())
                    .ok_or(MssqlError::MissingMetadata(
                        "cdc.captured_columns.column_ordinal",
                    ))?;
                let sql_type = row
                    .try_get::<&str, _>(2)?
                    .ok_or(MssqlError::MissingMetadata(
                        "cdc.captured_columns.column_type",
                    ))?;
                let max_length = row.try_get::<i16, _>(3)?;
                Ok(CapturedColumn::new(
                    name.to_string(),
                    ordinal,
                    sql_type.to_lowercase(),
                    max_length,
                ))
            })
            .collect()
    }

    async fn single_lsn(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<Option<Lsn>, MssqlError> {
        let Some(row) = self.single_row(sql, params).await? else {
            return Ok(None);
        };
        row.try_get::<&[u8], _>(0)?
            .map(|bytes| Lsn::try_from(bytes).map_err(MssqlError::from))
            .transpose()
    }

    async fn single_row(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<Option<Row>, MssqlError> {
        Ok(self.rows(sql, params).await?.into_iter().next())
    }

    async fn rows(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<Row>, MssqlError> {
        let client = &mut self.client;
        let query = async move { client.query(sql, params).await?.into_first_result().await };
        timeout(self.query_timeout, query)
            .await
            .map_err(|_| MssqlError::Timeout)?
            .map_err(MssqlError::from)
    }
}

fn change_function(capture_instance: &str) -> String {
    quote_identifier(&format!("fn_cdc_get_all_changes_{capture_instance}"))
}

fn read_position(row: &Row) -> Result<Position, MssqlError> {
    Ok(Position::new(read_lsn(row, 0)?, read_lsn(row, 1)?))
}

fn read_lsn(row: &Row, index: usize) -> Result<Lsn, MssqlError> {
    let bytes = row
        .try_get::<&[u8], _>(index)?
        .ok_or(MssqlError::MissingMetadata("change row LSN"))?;
    Lsn::try_from(bytes).map_err(MssqlError::from)
}

fn read_operation(row: &Row) -> Result<u8, MssqlError> {
    row.try_get::<i32, _>(2)?
        .and_then(|operation| u8::try_from(operation).ok())
        .ok_or(MssqlError::MissingMetadata("change row __$operation"))
}

fn read_change_row(
    row: &Row,
    instance: &CaptureInstance,
    instance_index: usize,
) -> Result<ChangeRow, MssqlError> {
    let position = read_position(row)?;
    let operation = read_operation(row)?;
    // Only an update's before image reads the mask.
    let update_mask = if operation == OPERATION_UPDATE_BEFORE {
        row.try_get::<&[u8], _>(3)?
            .map(<[u8]>::to_vec)
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let commit_time = row.try_get::<DateTime<FixedOffset>, _>(4)?;
    let mut values = Vec::with_capacity(instance.columns.len());
    for ((_, data), column) in row
        .cells()
        .skip(ROW_METADATA_COLUMNS)
        .zip(&instance.columns)
    {
        let value = column_value(data).map_err(|reason| MssqlError::Conversion {
            table: instance.table.to_string(),
            column: column.name.clone(),
            position,
            reason,
        })?;
        values.push(value);
    }
    Ok(ChangeRow {
        position,
        operation,
        update_mask,
        commit_time,
        values,
        instance: instance_index,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(sql_type: &str, max_length: Option<i16>) -> CapturedColumn {
        CapturedColumn::new("c".to_string(), 1, sql_type.to_string(), max_length)
    }

    #[test]
    fn given_max_length_minus_one_should_be_lob() {
        assert!(column("nvarchar", Some(-1)).is_lob);
        assert!(!column("nvarchar", Some(-1)).is_legacy_lob);
        assert!(!column("nvarchar", Some(200)).is_lob);
    }

    #[test]
    fn given_text_ntext_or_image_should_be_legacy_lob() {
        for sql_type in ["text", "ntext", "image"] {
            let captured = column(sql_type, Some(16));
            assert!(captured.is_lob, "{sql_type}");
            assert!(captured.is_legacy_lob, "{sql_type}");
        }
    }

    #[test]
    fn given_hierarchyid_should_not_be_lob() {
        assert!(!column("hierarchyid", Some(892)).is_lob);
    }

    #[test]
    fn given_money_should_cast_to_decimal() {
        assert_eq!(
            column("money", Some(8)).select_expression().as_deref(),
            Some("CAST(changes.[c] AS DECIMAL(19, 4)) AS [c]")
        );
    }

    #[test]
    fn given_clr_type_should_cast_to_varbinary() {
        assert_eq!(
            column("geography", Some(-1)).select_expression().as_deref(),
            Some("CAST(changes.[c] AS VARBINARY(MAX)) AS [c]")
        );
    }

    // A string cast truncates long values and drops float and datetime precision, so it is rejected.
    #[test]
    fn given_sql_variant_should_be_unsupported() {
        assert_eq!(column("sql_variant", Some(8016)).select_expression(), None);
    }

    #[test]
    fn given_unsupported_type_should_be_named() {
        let columns = vec![
            column("int", Some(4)),
            CapturedColumn::new("v".to_string(), 2, "vector".to_string(), Some(100)),
        ];

        assert_eq!(
            select_list(&columns),
            Err(vec![("v".to_string(), "vector".to_string())])
        );
    }

    #[test]
    fn given_capture_instance_with_bracket_should_quote_change_function() {
        assert_eq!(
            change_function("sales_order]items"),
            "[fn_cdc_get_all_changes_sales_order]]items]"
        );
    }
}
