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

use std::sync::Arc;

use async_trait::async_trait;
use pgwire::{
    api::{
        ClientInfo, ClientPortalStore, PgWireServerHandlers, Type,
        portal::Portal,
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        results::{DescribePortalResponse, DescribeStatementResponse, FieldInfo, Response, Tag},
        stmt::{NoopQueryParser, StoredStatement},
        store::PortalStore,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
};
use sqlparser::{ast::Statement, dialect::RedshiftSqlDialect, parser::Parser};
use tokio_postgres::{Client as PgClient, Statement as PgStatement};

use crate::connectors::fixtures::redshift::redshift_mock::{
    copy::try_parse_redshift_copy,
    create::try_parse_redshift_create_table,
    load::{
        S3Client, ToPgError, execute_create_table, execute_s3_copy, execute_select, split_s3_uri,
    },
};

/// Statement failed to parse under the Redshift dialect.
pub const SYNTAX_ERROR: &str = "26000";
/// Parsed, but uses a construct we don't implement (custom COPY/CREATE
/// extensions, unsupported statement kinds, etc.).
pub const FEATURE_NOT_SUPPORTED: &str = "42601";
/// The Postgres connection backing this mock failed outright (as
/// opposed to Postgres returning a well-formed DB error).
pub const CONNECTION_EXCEPTION: &str = "08000";
/// Empty/missing statement.
pub const INVALID_QUERY: &str = "42601";
/// Statement kind we recognize but intentionally don't support.
pub const WARNING_UNSUPPORTED: &str = "01000";

pub struct RedshiftHandlerFactory {
    pub handler: Arc<RedshiftHandler>,
}

impl PgWireServerHandlers for RedshiftHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl pgwire::api::query::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl pgwire::api::query::ExtendedQueryHandler> {
        self.handler.clone()
    }
}

pub struct RedshiftHandler {
    pg: PgClient,
    s3_endpoint: String,
}

impl RedshiftHandler {
    pub fn new(pg: PgClient, s3_endpoint: String) -> Self {
        Self { pg, s3_endpoint }
    }

    /// Prepares `sql` against the backing Postgres connection for describe
    /// purposes. Returns `Ok(None)` for statement kinds (currently just
    /// `COPY`) that describe to zero fields rather than going through an
    /// unsupported `PREPARE`.
    async fn prepare_describable(&self, sql: &str) -> PgWireResult<Option<PgStatement>> {
        let dialect = RedshiftSqlDialect {};
        let statements = parse_redshift_sql(&dialect, sql)?;

        if matches!(statements.first(), Some(Statement::Copy { .. })) {
            return Ok(None);
        }

        self.pg
            .prepare(sql)
            .await
            .map(Some)
            .map_err(|e| map_pg_client_error(e, CONNECTION_EXCEPTION))
    }
}

#[async_trait]
impl ExtendedQueryHandler for RedshiftHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::new())
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;

        if query.trim().is_empty() {
            return Ok(Response::EmptyQuery);
        }

        execute_statement(query, &self.pg, &self.s3_endpoint).await
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let Some(prepared) = self.prepare_describable(&stmt.statement).await? else {
            return Ok(DescribeStatementResponse::new(vec![], vec![]));
        };

        let param_types: Vec<Type> = prepared.params().to_vec();

        let fields: Vec<FieldInfo> = prepared
            .columns()
            .iter()
            .map(|col| {
                FieldInfo::new(
                    col.name().to_owned(),
                    None,
                    None,
                    col.type_().clone(),
                    pgwire::api::results::FieldFormat::Text,
                )
            })
            .collect();

        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let Some(prepared) = self
            .prepare_describable(&portal.statement.statement)
            .await?
        else {
            return Ok(DescribePortalResponse::new(vec![]));
        };

        let fields: Vec<FieldInfo> = prepared
            .columns()
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                FieldInfo::new(
                    col.name().to_owned(),
                    None,
                    None,
                    col.type_().clone(),
                    portal.result_column_format.format_for(idx),
                )
            })
            .collect();

        Ok(DescribePortalResponse::new(fields))
    }
}

#[async_trait]
impl SimpleQueryHandler for RedshiftHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        if query.trim().is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        Ok(vec![
            execute_statement(query, &self.pg, &self.s3_endpoint).await?,
        ])
    }
}

/// Dispatches a single SQL statement to the appropriate executor based on
/// its parsed kind. Kept intentionally thin — each branch delegates to a
/// dedicated function so individual statement kinds can be read (and
/// tested) in isolation.
async fn execute_statement(
    query: &str,
    pg: &PgClient,
    s3_endpoint: &str,
) -> PgWireResult<Response> {
    let dialect = RedshiftSqlDialect {};
    let statements = parse_redshift_sql(&dialect, query)?;

    match statements.first() {
        Some(Statement::Query(_)) => execute_select(query, pg).await,
        Some(Statement::CreateTable(_)) => execute_create(query, pg).await,
        Some(Statement::Copy { .. }) => execute_copy(query, pg, s3_endpoint).await,
        Some(other) => Err(pg_warning(
            WARNING_UNSUPPORTED,
            format!("Unsupported: {other:?}"),
        )),
        None => Err(pg_error(INVALID_QUERY, "Invalid query")),
    }
}

async fn execute_create(query: &str, pg: &PgClient) -> PgWireResult<Response> {
    let dialect = RedshiftSqlDialect {};
    let parser = Parser::new(&dialect)
        .try_with_sql(query)
        .map_err(|e| pg_error(SYNTAX_ERROR, format!("Unsupported: {e:?}")))?;

    let create = try_parse_redshift_create_table(parser)
        .map_err(|e| pg_error(FEATURE_NOT_SUPPORTED, format!("Unsupported: {e:?}")))?;

    execute_create_table(create, pg).await
}

async fn execute_copy(query: &str, pg: &PgClient, s3_endpoint: &str) -> PgWireResult<Response> {
    let dialect = RedshiftSqlDialect {};
    let parser = Parser::new(&dialect)
        .try_with_sql(query)
        .map_err(|e| pg_error(SYNTAX_ERROR, format!("Unsupported: {e:?}")))?;

    let r_copy = try_parse_redshift_copy(parser)
        .map_err(|e| pg_error(FEATURE_NOT_SUPPORTED, format!("Unsupported: {e:?}")))?;

    let (bucket_name, prefix) = split_s3_uri(&r_copy.s3_uri).map_err(|e| e.to_pg_wire_error())?;

    let s3_client = S3Client::new(
        &bucket_name,
        s3_endpoint,
        &r_copy.access_key_id,
        &r_copy.secret_access_key,
        &r_copy.region,
    )
    .await
    .map_err(|e| e.to_pg_wire_error())?;

    let rows = execute_s3_copy(&r_copy, pg, s3_client, &bucket_name, &prefix)
        .await
        .map_err(|e| pg_error(FEATURE_NOT_SUPPORTED, format!("Unsupported: {e:?}")))?;

    Ok(Response::Execution(Tag::new("copy").with_rows(rows)))
}

/// Builds a `PgWireError::UserError` with severity `ERROR`. Replaces the
/// repeated `PgWireError::UserError(Box::new(ErrorInfo::new(...)))` calls.
fn pg_error(code: &str, message: impl std::fmt::Display) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        code.into(),
        message.to_string(),
    )))
}

/// Same as [`pg_error`] but with severity `WARNING`, for statement kinds
/// we recognize but choose not to support.
fn pg_warning(code: &str, message: impl std::fmt::Display) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "WARNING".into(),
        code.into(),
        message.to_string(),
    )))
}

/// Maps a `tokio_postgres::Error` to a `PgWireError`, preserving the
/// upstream SQLSTATE/message when Postgres itself produced the error, and
/// falling back to `fallback_code` for connection-level failures.
fn map_pg_client_error(err: tokio_postgres::Error, fallback_code: &str) -> PgWireError {
    match err.as_db_error() {
        Some(db_err) => pg_error(db_err.code().code(), db_err.message()),
        None => pg_error(fallback_code, format!("connection failed: {err}")),
    }
}

fn parse_redshift_sql(dialect: &RedshiftSqlDialect, sql: &str) -> PgWireResult<Vec<Statement>> {
    Parser::parse_sql(dialect, sql).map_err(|e| pg_error(SYNTAX_ERROR, e))
}
