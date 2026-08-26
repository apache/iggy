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

use pgwire::error::PgWireResult;
use sqlparser::ast::Statement as SqlStatement;
use tokio_postgres::Client;

use super::{parser::ParsedStatement, util::backend_err};

pub async fn execute_create(client: &Client, raw_sql: &str) -> PgWireResult<u64> {
    // Serialize concurrent DDL on the same backend session pool using a
    // Postgres advisory lock, keyed by a hash of statement text. This does
    // NOT protect against DDL issued from other proxies/paths outside this
    // service — pair it with `lock_timeout`/`statement_timeout` GUCs set on
    // the pooled connection so a stuck CREATE can't wedge the pool.
    let lock_key = ddl_lock_key(raw_sql);
    client
        .execute("SELECT pg_advisory_lock($1)", &[&lock_key])
        .await
        .map_err(backend_err)?;

    tracing::debug!(sql = raw_sql, "executing DDL: CREATE");
    let result = client.execute(raw_sql, &[]).await.map_err(|e| {
        tracing::error!("{}", e);
        e
    });

    client
        .execute("SELECT pg_advisory_unlock($1)", &[&lock_key])
        .await
        .map_err(backend_err)?;

    result.map_err(backend_err)
}

pub async fn execute_truncate(client: &Client, stmt: &ParsedStatement) -> PgWireResult<u64> {
    let table_names = truncate_targets(&stmt.ast);

    // Even when allowed, log loudly before it happens — this is the one
    // statement class where "log after success" is useless (there's nothing
    // to roll back to reconstruct intent from).
    tracing::info!(tables = ?table_names, sql = stmt.raw_sql, "executing TRUNCATE");

    client
        .execute(&stmt.raw_sql, &[])
        .await
        .map_err(backend_err)
}

fn truncate_targets(ast: &SqlStatement) -> Vec<String> {
    if let SqlStatement::Truncate(trunc) = ast {
        trunc.table_names.iter().map(|t| t.to_string()).collect()
    } else {
        vec![]
    }
}

fn ddl_lock_key(sql: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish() as i64
}
