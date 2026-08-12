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

use std::ops::ControlFlow;

use async_trait::async_trait;
use pgwire::{
    api::{
        ClientInfo, Type as PgWireType,
        portal::Format,
        results::{FieldFormat, FieldInfo},
        stmt::QueryParser,
    },
    error::PgWireResult,
};
use sqlparser::{
    ast::{
        CreateTable, DataType, Expr, HiveDistributionStyle, Ident, ObjectName, ObjectNamePart,
        Select, SelectFlavor, SelectItem, SetExpr, Statement as SqlStatement, TableFactor, Value,
        VisitMut, VisitorMut,
    },
    dialect::{PostgreSqlDialect, RedshiftSqlDialect},
    parser::Parser as SqlParser,
};

use super::util::user_err;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryClass {
    DdlCreate,
    DdlTruncate,
    DmlInsert,
    DmlCopy,
    Dql,
    /// Anything we don't special-case: passthrough with no rewriting,
    /// still logged
    Other,
}

#[derive(Debug, Clone)]
pub struct ParsedStatement {
    pub raw_sql: String,
    pub ast: SqlStatement,
    // Captured during Parse and reused during Describe.
    pub parameter_types: Vec<PgWireType>,
    // Fields
    pub result_columns: Vec<FieldInfo>,
    pub class: QueryClass,
}

#[derive(Clone)]
pub struct RedshiftQueryParser;

#[async_trait]
impl QueryParser for RedshiftQueryParser {
    type Statement = ParsedStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        param_types: &[Option<PgWireType>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Send + Sync,
    {
        tracing::debug!("Parsing sql");
        let dialect = RedshiftSqlDialect {};

        let mut asts = SqlParser::parse_sql(&dialect, sql)
            .map_err(|e| user_err(format!("sql parse error: {e}")))?;

        if asts.len() != 1 {
            // Reject multi-statement Parse messages outright.
            return Err(user_err(
                "only a single statement is permitted per Parse message",
            ));
        }

        let _ = asts.visit(&mut RedshiftExprRewriter);

        tracing::debug!("Query rewritten");

        let ast = asts.remove(0);
        let class = classify(&ast);
        let result_columns = select_schema(&ast);

        tracing::debug!("Done parsing");

        let mut p_stmt = ParsedStatement {
            raw_sql: ast.to_string(),
            ast,
            class,
            parameter_types: param_types
                .iter()
                .clone()
                .map(|ty| ty.clone().unwrap_or(PgWireType::UNKNOWN))
                .collect(),
            result_columns,
        };

        p_stmt.rewrite_to_postgres().map_err(user_err)?;

        tracing::debug!("Postgres rewrite, {}", p_stmt.raw_sql);

        Ok(p_stmt)
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<PgWireType>> {
        Ok(stmt.parameter_types.clone())
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(stmt.result_columns.clone())
    }
}

fn classify(stmt: &SqlStatement) -> QueryClass {
    match stmt {
        SqlStatement::CreateTable { .. } => QueryClass::DdlCreate,

        SqlStatement::Truncate { .. } => QueryClass::DdlTruncate,

        SqlStatement::Insert { .. } => QueryClass::DmlInsert,

        SqlStatement::Copy { .. } => QueryClass::DmlCopy,

        SqlStatement::Query(_) => QueryClass::Dql,

        _ => QueryClass::Other,
    }
}

fn select_schema(stmt: &SqlStatement) -> Vec<FieldInfo> {
    let SqlStatement::Query(query) = stmt else {
        return vec![];
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return vec![];
    };

    select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::ExprWithAlias { expr, alias } => {
                let field_info = FieldInfo::new(
                    alias.value.clone(),
                    None,
                    None,
                    expression_type(expr),
                    FieldFormat::Text,
                );

                Some(field_info)
            }
            SelectItem::UnnamedExpr(expr) => {
                tracing::info!(?expr, resolved = ?expression_type(expr));
                let name = match expr {
                    Expr::Identifier(ident) => ident.value.clone(),
                    Expr::CompoundIdentifier(parts) => parts
                        .last()
                        .map(|ident| ident.value.clone())
                        .unwrap_or_else(|| expr.to_string()),
                    _ => expr.to_string(),
                };

                let field_info =
                    FieldInfo::new(name, None, None, expression_type(expr), FieldFormat::Text);

                Some(field_info)
            }

            _ => None,
        })
        .collect()
}

fn expression_type(expr: &Expr) -> PgWireType {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Boolean(_) => PgWireType::BOOL,
            Value::Number(number, _) if number.contains(['.', 'e', 'E']) => PgWireType::NUMERIC,
            Value::Number(number, _) if number.parse::<i32>().is_ok() => PgWireType::INT4,
            Value::Number(number, _) if number.parse::<i64>().is_ok() => PgWireType::INT8,
            Value::Number(_, _) => PgWireType::NUMERIC,
            Value::SingleQuotedString(_)
            | Value::DollarQuotedString(_)
            | Value::EscapedStringLiteral(_)
            | Value::UnicodeStringLiteral(_) => PgWireType::TEXT,
            Value::Null | Value::Placeholder(_) => PgWireType::UNKNOWN,
            _ => PgWireType::UNKNOWN,
        },

        Expr::Cast { data_type, .. } => match data_type.to_string().to_uppercase().as_str() {
            "BOOL" | "BOOLEAN" => PgWireType::BOOL,
            "SMALLINT" | "INT2" => PgWireType::INT2,
            "INTEGER" | "INT" | "INT4" => PgWireType::INT4,
            "BIGINT" | "INT8" => PgWireType::INT8,
            "REAL" | "FLOAT4" => PgWireType::FLOAT4,
            "DOUBLE PRECISION" | "FLOAT8" => PgWireType::FLOAT8,
            "NUMERIC" | "DECIMAL" => PgWireType::NUMERIC,
            "TEXT" => PgWireType::TEXT,
            "VARCHAR" | "CHARACTER VARYING" => PgWireType::VARCHAR,
            "DATE" => PgWireType::DATE,
            "TIMESTAMP" => PgWireType::TIMESTAMP,
            "TIMESTAMP WITH TIME ZONE" => PgWireType::TIMESTAMPTZ,
            _ => PgWireType::UNKNOWN,
        },

        Expr::BinaryOp {
            op:
                sqlparser::ast::BinaryOperator::Eq
                | sqlparser::ast::BinaryOperator::NotEq
                | sqlparser::ast::BinaryOperator::Lt
                | sqlparser::ast::BinaryOperator::LtEq
                | sqlparser::ast::BinaryOperator::Gt
                | sqlparser::ast::BinaryOperator::GtEq
                | sqlparser::ast::BinaryOperator::And
                | sqlparser::ast::BinaryOperator::Or,
            ..
        } => PgWireType::BOOL,

        Expr::UnaryOp { op, expr } => match op {
            sqlparser::ast::UnaryOperator::Not => PgWireType::BOOL,
            sqlparser::ast::UnaryOperator::Minus | sqlparser::ast::UnaryOperator::Plus => {
                expression_type(expr)
            }
            _ => PgWireType::UNKNOWN,
        },

        _ => PgWireType::UNKNOWN,
    }
}

impl ParsedStatement {
    pub fn rewrite_to_postgres(&mut self) -> Result<(), String> {
        match &mut self.ast {
            SqlStatement::CreateTable(create_table) => {
                redshift_create_table_to_postgres(create_table)?;
            }
            SqlStatement::Query(query) if matches!(query.body.as_ref(), SetExpr::Select(_)) => {
                let SetExpr::Select(select) = query.body.as_mut() else {
                    return Err("No select body found".into());
                };

                redshift_select_to_postgres(select)?;
            }
            _ => {}
        }

        self.raw_sql = self.ast.to_string();

        Ok(())
    }
}

pub fn redshift_create_table_to_postgres(create: &mut CreateTable) -> Result<(), String> {
    // These cannot be expressed as PostgreSQL CREATE TABLE.
    let unsupported = [
        ("OR REPLACE", create.or_replace),
        ("EXTERNAL", create.external),
        ("TRANSIENT", create.transient),
        ("ICEBERG", create.iceberg),
        ("SNAPSHOT", create.snapshot),
        ("DYNAMIC", create.dynamic),
        ("WITHOUT ROWID", create.without_rowid),
        ("COPY GRANTS", create.copy_grants),
        ("REQUIRE USER", create.require_user),
        ("STRICT", create.strict),
    ];

    if let Some((feature, _)) = unsupported.into_iter().find(|(_, present)| *present) {
        return Err(format!(
            "Redshift CREATE TABLE uses {feature}, which has no PostgreSQL CREATE TABLE equivalent"
        ));
    }

    if create.file_format.is_some()
        || create.location.is_some()
        || create.hive_formats.is_some()
        || create.hive_distribution != HiveDistributionStyle::NONE
    {
        return Err(
            "External/Hive storage options require a PostgreSQL foreign-table migration, \
             not CREATE TABLE transpilation."
                .into(),
        );
    }

    if create.clone.is_some() || create.version.is_some() {
        return Err(
            "CLONE / table-version syntax has no PostgreSQL CREATE TABLE equivalent".into(),
        );
    }

    // Redshift physical-design directives have no PostgreSQL DDL equivalent.
    if create.diststyle.take().is_some() {
        tracing::warn!("Dropped Redshift DISTSTYLE.");
    }
    if create.distkey.take().is_some() {
        tracing::warn!("Dropped Redshift DISTKEY.");
    }
    if create.sortkey.take().is_some() {
        tracing::warn!("Dropped Redshift SORTKEY; create a PostgreSQL index separately if needed.");
    }
    if create.backup.take().is_some() {
        tracing::warn!("Dropped Redshift BACKUP setting; configure PostgreSQL backups externally.");
    }

    // `VOLATILE` is not PostgreSQL CREATE TABLE syntax. Treat it as TEMPORARY.
    if create.volatile {
        create.volatile = false;
        create.temporary = true;
        tracing::warn!("Translated VOLATILE to TEMPORARY.");
    }

    for column in &mut create.columns {
        match &column.data_type {
            // PostgreSQL BYTEA has no length modifier.
            DataType::Varbinary(_) => {
                column.data_type = DataType::Bytea;
            }

            // Fallback as sqlparser version parses VARBYTE
            // as a custom type instead.
            DataType::Custom(name, _) if name.to_string().eq_ignore_ascii_case("VARBYTE") => {
                column.data_type = DataType::Bytea;
            }

            _ => {}
        }
    }

    let sql = create.to_string();

    // Syntax validation only; sqlparser deliberately does not perform full
    // PostgreSQL semantic validation.
    SqlParser::parse_sql(&PostgreSqlDialect {}, &sql)
        .map_err(|error| format!("Generated SQL is not PostgreSQL syntax: {error}"))?;

    Ok(())
}

pub fn redshift_select_to_postgres(select: &mut Box<Select>) -> Result<(), String> {
    rewrite_pg_table_def(select)?;

    if select.top.is_some() {
        return Err("TOP must be rewritten as LIMIT on the enclosing Query; \
             Select alone has no LIMIT field."
            .into());
    }

    if select.select_modifiers.is_some() {
        return Err("MySQL SELECT modifiers have no PostgreSQL equivalent.".into());
    }

    if select.exclude.is_some() {
        return Err(
            "SELECT * EXCLUDE requires schema information to expand the wildcard \
             before producing PostgreSQL SQL."
                .into(),
        );
    }

    if !select.lateral_views.is_empty() {
        return Err("LATERAL VIEW has no direct PostgreSQL SELECT equivalent.".into());
    }

    if select.prewhere.is_some() {
        return Err("PREWHERE has no PostgreSQL equivalent; rewrite it as WHERE.".into());
    }

    if !select.connect_by.is_empty() {
        return Err("CONNECT BY requires a recursive CTE rewrite.".into());
    }

    if !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return Err(
            "CLUSTER BY, DISTRIBUTE BY, and SORT BY require a query-plan/index rewrite.".into(),
        );
    }

    if select.qualify.is_some() {
        return Err(
            "QUALIFY requires an outer SELECT rewrite; transpile the enclosing Query, \
             not Select alone."
                .into(),
        );
    }

    if select.value_table_mode.is_some() {
        return Err("Value-table mode has no PostgreSQL SELECT equivalent.".into());
    }

    if select.flavor != SelectFlavor::Standard {
        return Err("FROM-first SELECT syntax has no PostgreSQL equivalent.".into());
    }

    // PostgreSQL does not use optimizer hints. They should not affect results.
    if !select.optimizer_hints.is_empty() {
        select.optimizer_hints.clear();
        tracing::warn!("Dropped optimizer hints.");
    }

    // Defensive normalization; this flag is meaningful only with TOP.
    select.top_before_distinct = false;

    let sql = select.to_string();

    SqlParser::parse_sql(&PostgreSqlDialect {}, &sql)
        .map_err(|error| format!("Generated SQL is not PostgreSQL syntax: {error}"))?;

    Ok(())
}

fn rewrite_pg_table_def(select: &mut Select) -> Result<(), String> {
    // Only support:
    //
    // SELECT "column", type
    // FROM pg_table_def
    // WHERE tablename = ...
    //
    //
    let from_size = select.from.len();

    let Some(from) = select.from.first_mut() else {
        return Ok(());
    };

    let TableFactor::Table { name, alias, .. } = &mut from.relation else {
        return Ok(());
    };

    // Fast no-op path: every ordinary SELECT returns here unchanged.
    if !is_unqualified_name(name, "pg_table_def") {
        return Ok(());
    }

    if from_size != 1 || !from.joins.is_empty() || alias.is_some() {
        return Err(format!("Unsupported pg_table_def query shape: {}", select));
    }

    if select.projection.len() != 2
        || !is_identifier_projection(&select.projection[0], "column")
        || !is_identifier_projection(&select.projection[1], "type")
    {
        return Err(
            "Only `SELECT \"column\", type FROM pg_table_def ...` is supported \
             by the pg_table_def compatibility rewrite."
                .into(),
        );
    }

    *name = ObjectName(vec![
        ObjectNamePart::Identifier(Ident::new("information_schema")),
        ObjectNamePart::Identifier(Ident::new("columns")),
    ]);

    select.projection = vec![
        SelectItem::ExprWithAlias {
            expr: Expr::Identifier(Ident::new("column_name")),
            alias: Ident::with_quote('"', "column"),
        },
        SelectItem::ExprWithAlias {
            expr: Expr::Identifier(Ident::new("udt_name")),
            alias: Ident::new("type"),
        },
    ];

    rename_tablename_predicate(select.selection.as_mut())?;

    Ok(())
}

fn rename_tablename_predicate(expr: Option<&mut Expr>) -> Result<(), String> {
    let Some(expr) = expr else {
        return Ok(());
    };

    match expr {
        Expr::BinaryOp { left, right, .. } => {
            rename_tablename_expr(left);
            rename_tablename_expr(right);
        }
        Expr::Nested(expr) => rename_tablename_expr(expr),
        _ => {}
    }

    Ok(())
}

fn rename_tablename_expr(expr: &mut Expr) {
    if let Expr::Identifier(ident) = expr
        && ident.value.eq_ignore_ascii_case("tablename")
    {
        *ident = Ident::new("table_name");
    }
}

fn is_identifier_projection(item: &SelectItem, expected: &str) -> bool {
    matches!(
        item,
        SelectItem::UnnamedExpr(Expr::Identifier(ident))
            if ident.value.eq_ignore_ascii_case(expected)
    )
}

fn is_unqualified_name(name: &ObjectName, expected: &str) -> bool {
    name.0.len() == 1 && name.to_string().eq_ignore_ascii_case(expected)
}

pub struct RedshiftExprRewriter;

impl VisitorMut for RedshiftExprRewriter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::Function(func) = expr
            && func.name.to_string().eq_ignore_ascii_case("GETDATE")
            && func.args.to_string() == "()"
        {
            func.name = ObjectName::from(vec![Ident::new("NOW")])
        }

        ControlFlow::Continue(())
    }
}
