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
        results::{DescribePortalResponse, DescribeStatementResponse, FieldInfo, Response},
        stmt::{QueryParser, StoredStatement},
        store::PortalStore,
    },
    error::{PgWireError, PgWireResult},
};
use tokio_postgres::Client as PgClient;

use crate::connectors::fixtures::redshift::redshift_mock::{
    load::S3Client,
    util::{backend_err, columns_to_field_info, user_err},
};

use super::{
    ddl, dml, dql,
    parser::{ParsedStatement, QueryClass, RedshiftQueryParser},
};

pub struct RedshiftHandlerFactory {
    pub pg_dsn: String,
    pub s3_client: Arc<S3Client>,
}

impl PgWireServerHandlers for RedshiftHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl pgwire::api::query::SimpleQueryHandler> {
        Arc::new(RedshiftHandler::new(
            self.pg_dsn.clone(),
            self.s3_client.clone(),
        ))
    }

    fn extended_query_handler(&self) -> Arc<impl pgwire::api::query::ExtendedQueryHandler> {
        Arc::new(RedshiftHandler::new(
            self.pg_dsn.clone(),
            self.s3_client.clone(),
        ))
    }
}

struct RedshiftHandler {
    pg_dsn: String,
    pg: tokio::sync::OnceCell<tokio_postgres::Client>,
    s3_client: Arc<S3Client>,
    query_parser: Arc<RedshiftQueryParser>,
}

impl RedshiftHandler {
    pub fn new(pg_dsn: String, s3_client: Arc<S3Client>) -> Self {
        Self {
            pg_dsn,
            pg: tokio::sync::OnceCell::new(),
            s3_client,
            query_parser: Arc::new(RedshiftQueryParser),
        }
    }

    async fn pg_client(&self) -> Result<&tokio_postgres::Client, PgWireError> {
        self.pg
            .get_or_try_init(|| async {
                let (pg_client, pg_conn) =
                    tokio_postgres::connect(&self.pg_dsn, tokio_postgres::NoTls)
                        .await
                        .map_err(backend_err)?;

                tokio::spawn(async move {
                    if let Err(e) = pg_conn.await {
                        tracing::error!("Postgres connection error: {e}");
                    }
                });
                Ok::<_, PgWireError>(pg_client)
            })
            .await
    }
}

#[async_trait]
impl ExtendedQueryHandler for RedshiftHandler {
    type Statement = ParsedStatement;
    type QueryParser = RedshiftQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
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
        let stmt = portal.statement.statement.clone();

        if stmt.raw_sql.trim().is_empty() {
            return Ok(Response::EmptyQuery);
        }

        let pg = self.pg_client().await?;

        execute_statement(stmt, ExecCtx::Bound(portal), pg, &self.s3_client).await
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if matches!(stmt.statement.class, QueryClass::DmlCopy) {
            return Ok(DescribeStatementResponse::new(vec![], vec![]));
        }

        let prepared = self
            .pg_client()
            .await?
            .prepare(&stmt.statement.raw_sql)
            .await
            .map_err(backend_err)?;

        let param_types: Vec<Type> = prepared.params().to_vec();

        let fields: Vec<FieldInfo> = columns_to_field_info(prepared.columns());

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
        if matches!(portal.statement.statement.class, QueryClass::DmlCopy) {
            return Ok(DescribePortalResponse::new(vec![]));
        }

        let prepared = self
            .pg_client()
            .await?
            .prepare(&portal.statement.statement.raw_sql)
            .await
            .map_err(backend_err)?;

        let fields: Vec<FieldInfo> = columns_to_field_info(prepared.columns());

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

        let stmt = self.query_parser.parse_sql(_client, query, &[]).await?;

        let pg = self.pg_client().await?;

        Ok(vec![
            execute_statement(stmt, ExecCtx::Unbound, pg, &self.s3_client).await?,
        ])
    }
}

async fn execute_statement<'a>(
    stmt: ParsedStatement,
    ctx: ExecCtx<'a>,
    pg: &PgClient,
    s3_client: &S3Client,
) -> PgWireResult<Response> {
    match stmt.class {
        QueryClass::DdlCreate => {
            let affected = ddl::execute_create(pg, &stmt.raw_sql).await?;

            Ok(Response::Execution(
                pgwire::api::results::Tag::new("CREATE").with_rows(affected as usize),
            ))
        }
        QueryClass::DdlTruncate => {
            let affected = ddl::execute_truncate(pg, &stmt).await?;

            Ok(Response::Execution(
                pgwire::api::results::Tag::new("TRUNCATE").with_rows(affected as usize),
            ))
        }
        QueryClass::Dql => {
            let response = dql::execute_select(pg, ctx, 0).await?;

            Ok(response)
        }
        QueryClass::DmlCopy => {
            let affected = dml::execute_copy(pg, s3_client, ctx).await?;

            Ok(Response::Execution(
                pgwire::api::results::Tag::new("COPY").with_rows(affected as usize),
            ))
        }
        QueryClass::DmlInsert => {
            let response = dml::execute_dml(pg, ctx).await?;

            Ok(response)
        }
        QueryClass::Other => Err(user_err(format!("Unsupported: {:?}", stmt.raw_sql))),
    }
}

pub enum ExecCtx<'a> {
    Bound(&'a Portal<ParsedStatement>),
    Unbound,
}

impl<'a> ExecCtx<'a> {
    pub fn portal(&self) -> Option<&'a Portal<ParsedStatement>> {
        match self {
            ExecCtx::Bound(p) => Some(p),
            ExecCtx::Unbound => None,
        }
    }
}
