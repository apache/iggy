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

use futures::stream;
use pgwire::api::results::{QueryResponse, Response};
use pgwire::error::PgWireResult;
use tokio_postgres::Client;

use super::{
    handler::ExecCtx,
    util::{backend_err, columns_to_field_info, decode_param, row_to_data_row, user_err},
};

pub async fn execute_select<'a>(
    client: &Client,
    ctx: ExecCtx<'a>,
    max_rows: usize,
) -> PgWireResult<Response> {
    let portal = ctx.portal().ok_or(user_err("Missing portal"))?;

    let raw_sql = &portal.statement.statement.raw_sql;

    let prepared = client.prepare(raw_sql).await.map_err(backend_err)?;

    let mut bound_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    for (i, ty) in prepared.params().iter().enumerate() {
        bound_params.push(decode_param(portal, ty, i)?);
    }
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        bound_params.iter().map(|b| b.as_ref() as &_).collect();

    let fields = Arc::new(columns_to_field_info(prepared.columns()));

    if max_rows == 0 {
        // 0 means "no limit" per the wire protocol: fetch everything.
        // Stream via query_raw + try_next rather than query() so you're not
        // buffering a huge result set in one Vec before encoding it —
        // pgwire's Response::Query can take a Stream, not just a Vec.
        let rows = client
            .query(&prepared, &param_refs)
            .await
            .map_err(backend_err)?;

        let fields_c = Arc::clone(&fields);

        let data_rows = stream::iter(
            rows.into_iter()
                .map(move |r| row_to_data_row(&r, &fields_c.clone())),
        );

        Ok(Response::Query(QueryResponse::new(
            fields.clone(),
            data_rows,
        )))
    } else {
        let rows = client
            .query(&prepared, &param_refs)
            .await
            .map_err(backend_err)?;

        let fields_c = Arc::clone(&fields);

        let truncated: Vec<_> = rows.into_iter().take(max_rows).collect();

        let data_rows = stream::iter(
            truncated
                .into_iter()
                .map(move |r| row_to_data_row(&r, &fields_c)),
        );

        Ok(Response::Query(QueryResponse::new(
            fields.clone(),
            data_rows,
        )))
    }
}
