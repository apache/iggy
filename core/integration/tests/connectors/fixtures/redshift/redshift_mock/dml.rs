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

use std::{collections::HashSet, sync::Arc};

use bytes::Bytes;
use futures::stream;
use pgwire::{api::results::Response, error::PgWireResult};
use sqlparser::ast::{CopyLegacyOption, CopySource, Statement as SqlStatement};
use tokio_postgres::Client;

use crate::connectors::fixtures::redshift::redshift_mock::util::backend_err;

use super::{
    handler::ExecCtx,
    load::{S3Client, fetch_table_columns, infer_parquet_schema, load_one_object},
    util::{columns_to_field_info, decode_param, row_to_data_row, user_err},
};

/// Shared by INSERT and MERGE
pub async fn execute_dml<'a>(client: &Client, ctx: ExecCtx<'a>) -> PgWireResult<Response> {
    let portal = ctx.portal().ok_or(user_err("Missing portal"))?;

    let raw_sql = &portal.statement.statement.raw_sql;

    let prepared = client
        .prepare(raw_sql)
        .await
        .map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;

    let param_types = prepared.params();
    let mut bound_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        Vec::with_capacity(param_types.len());

    for (i, ty) in param_types.iter().enumerate() {
        bound_params.push(decode_param(portal, ty, i)?);
    }

    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        bound_params.iter().map(|b| b.as_ref() as &_).collect();

    let has_returning = portal
        .statement
        .statement
        .raw_sql
        .to_ascii_uppercase()
        .contains("RETURNING");

    if has_returning {
        let rows = client
            .query(&prepared, &param_refs)
            .await
            .map_err(backend_err)?;

        let fields = Arc::new(columns_to_field_info(prepared.columns()));

        let fields_c = fields.clone();

        let data_rows = stream::iter(
            rows.into_iter()
                .map(move |r| row_to_data_row(&r, &fields_c)),
        );

        Ok(Response::Query(pgwire::api::results::QueryResponse::new(
            fields, data_rows,
        )))
    } else {
        let affected = client
            .execute(&prepared, &param_refs)
            .await
            .map_err(backend_err)?;

        Ok(Response::Execution(
            pgwire::api::results::Tag::new("INSERT").with_rows(affected as usize),
        ))
    }
}

pub async fn execute_copy<'a>(
    client: &Client,
    s3_client: &S3Client,
    ctx: ExecCtx<'a>,
) -> PgWireResult<u64> {
    let portal = ctx.portal().ok_or_else(|| user_err("Missing portal"))?;

    let SqlStatement::Copy {
        ref legacy_options,
        ref source,
        ..
    } = portal.statement.statement.ast
    else {
        return Ok(0);
    };

    let is_parquet = legacy_options
        .iter()
        .any(|v| matches!(v, CopyLegacyOption::Parquet));

    if !is_parquet {
        return Err(user_err("Expected parquet"));
    }

    execute_parquet_copy(
        client,
        s3_client,
        source,
        &portal.statement.statement.raw_sql,
    )
    .await
}

async fn execute_parquet_copy(
    client: &Client,
    s3_client: &S3Client,
    source: &CopySource,
    raw_sql: &str,
) -> PgWireResult<u64> {
    let (table_name, cols) = match source {
        CopySource::Table {
            table_name,
            columns,
        } => (table_name, columns),
        CopySource::Query(_) => return Err(user_err("Unsupported")),
    };

    let s3_uri = extract_s3_path(raw_sql)?;

    let (bucket_name, prefix) = split_s3_uri(&s3_uri)?;

    let existing_cols = fetch_table_columns(client, &table_name.to_string())
        .await
        .map_err(|e| user_err(e.to_string()))?
        .ok_or_else(|| user_err(format!("A required table is missing: {}", table_name)))?;

    let existing_names: HashSet<&str> = existing_cols.iter().map(|v| v.name.as_str()).collect();
    if !cols
        .iter()
        .all(|v| existing_names.contains(v.value.as_str()))
    {
        return Err(user_err(format!(
            "Column mismatch for table: {}",
            table_name
        )));
    }

    let bytes = Bytes::from(
        s3_client
            .get_object(&prefix)
            .await
            .map_err(|e| user_err(e.to_string()))?,
    );
    tracing::info!("File '{}' read", prefix);

    let inferred = infer_parquet_schema(bytes.clone()).map_err(user_err)?;

    let requested_cols: HashSet<&str> = cols.iter().map(|v| v.value.as_str()).collect();
    let in_cols: Vec<_> = inferred
        .into_iter()
        .filter(|c| requested_cols.contains(c.name.as_str()))
        .collect();

    let n = load_one_object(client, &format!("{}", table_name), &in_cols, bytes)
        .await
        .map_err(|e| {
            tracing::error!("[copy] error loading s3://{bucket_name}/{prefix}: {e:#}");
            user_err(format!("Failed to load s3://{bucket_name}/{prefix}: {e}"))
        })?;

    tracing::info!("{n} records stored");
    Ok(n as u64)
}

pub fn extract_s3_path(copy_sql: &str) -> PgWireResult<String> {
    let start = copy_sql.find("s3://").ok_or(user_err(format!(
        "Invalid query - Missing s3:// prefix: {copy_sql}"
    )))?;

    let rest = &copy_sql[start..];
    let end = rest.find('\'').ok_or(user_err(format!(
        "Invalid query - Missing s3 link end: {rest}"
    )))?;

    Ok(rest[..end].to_string())
}

pub fn split_s3_uri(uri: &str) -> PgWireResult<(String, String)> {
    let rest = uri.strip_prefix("s3://").ok_or(user_err(format!(
        "Invalid query - Missing s3:// prefix: {uri}"
    )))?;

    match rest.split_once('/') {
        Some((b, p)) => Ok((b.to_string(), p.to_string())),
        None => Err(user_err(format!("Invalid query - Missing s3 key: {uri}"))),
    }
}
