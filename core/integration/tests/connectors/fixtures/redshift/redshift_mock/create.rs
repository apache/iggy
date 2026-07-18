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

use sqlparser::{
    ast::{DataType, Expr, Ident, ObjectName},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

use crate::connectors::fixtures::redshift::redshift_mock::expect_word;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct RedshiftCreateTable {
    pub table: ObjectName,
    pub if_not_exists: bool,
    pub columns: Vec<RedshiftColumnDef>,
    pub table_kind: TableKind, // TEMP / LOCAL TEMP / regular
    pub dist_style: Option<DistStyle>,
    pub dist_key: Option<Ident>, // column name, only valid when dist_style == Key
    pub sort_key: Option<SortKey>,
    pub backup: Option<bool>, // BACKUP YES | NO
    pub terminator: bool,
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct RedshiftColumnDef {
    pub name: Ident,
    pub data_type: DataType,
    // ENCODE ZSTD, LZO, RAW, etc.
    pub encoding: Option<ColumnEncoding>,
    pub not_null: bool,
    pub default: Option<Expr>,
    // IDENTITY(seed, step)
    pub identity: Option<IdentitySpec>,
    pub primary_key: bool,
    // simplified FK target
    pub references: Option<ObjectName>,
}

#[derive(Debug, Clone)]
pub enum TableKind {
    Regular,
    Temp,
    LocalTemp,
}

#[derive(Debug, Clone)]
pub enum DistStyle {
    Even,
    Key,
    All,
    Auto,
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub enum SortKey {
    Compound(Vec<Ident>),
    Interleaved(Vec<Ident>),
}

#[allow(unused)]
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnEncoding {
    Raw,
    Bytedict,
    Delta,
    Delta32k,
    Lzo,
    Mostly8,
    Mostly16,
    Mostly32,
    Runlength,
    Text255,
    Text32k,
    Zstd,
    Az64,
}

#[derive(Debug, Clone)]
pub struct IdentitySpec {
    pub seed: i64,
    pub step: i64,
}

// Parse CREATE
pub fn try_parse_redshift_create_table(
    mut parser: Parser,
) -> Result<RedshiftCreateTable, ParserError> {
    if !parser.parse_keyword(Keyword::CREATE) {
        Err(ParserError::ParserError(
            "Not a create statement".to_string(),
        ))?
    }

    let table_kind =
        if parser.parse_keyword(Keyword::TEMPORARY) || parser.parse_keyword(Keyword::TEMP) {
            TableKind::Temp
        } else if parser.parse_keywords(&[Keyword::LOCAL, Keyword::TEMPORARY])
            || parser.parse_keywords(&[Keyword::LOCAL, Keyword::TEMP])
        {
            TableKind::LocalTemp
        } else {
            TableKind::Regular
        };

    parser.expect_keyword(Keyword::TABLE)?;

    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

    let table = parser.parse_object_name(false)?;

    let columns = parse_column_list(&mut parser)?;

    let mut dist_style = None;
    let mut dist_key = None;
    let mut sort_key = None;
    let mut backup = None;
    let mut terminator = false;

    // Table-level clauses after the column list are unordered, same as COPY options
    while parser.peek_token() != Token::EOF {
        let word = expect_word(&mut parser)?;
        match word.to_uppercase().as_str() {
            "DISTSTYLE" => {
                dist_style = Some(match expect_word(&mut parser)?.to_uppercase().as_str() {
                    "EVEN" => DistStyle::Even,
                    "KEY" => DistStyle::Key,
                    "ALL" => DistStyle::All,
                    "AUTO" => DistStyle::Auto,
                    other => Err(ParserError::ParserError(format!(
                        "unknown DISTSTYLE: {other}"
                    )))?,
                });
            }
            "DISTKEY" => {
                parser.expect_token(&Token::LParen)?;
                dist_key = Some(parser.parse_identifier()?);
                parser.expect_token(&Token::RParen)?;
            }
            "SORTKEY" => {
                sort_key = Some(SortKey::Compound(parse_ident_list(&mut parser)?));
            }
            "COMPOUND" => {
                parser.expect_keyword(Keyword::SORTKEY)?;
                sort_key = Some(SortKey::Compound(parse_ident_list(&mut parser)?));
            }
            "INTERLEAVED" => {
                parser.expect_keyword(Keyword::SORTKEY)?;
                sort_key = Some(SortKey::Interleaved(parse_ident_list(&mut parser)?));
            }
            "BACKUP" => {
                backup = Some(match expect_word(&mut parser)?.to_uppercase().as_str() {
                    "YES" => true,
                    "NO" => false,
                    other => Err(ParserError::ParserError(format!(
                        "expected YES|NO after BACKUP, got {other}"
                    )))?,
                });
            }
            "ENCODE" => {
                // table-level ENCODE AUTO|NONE — tolerate, not modeled per-table yet
                let _ = expect_word(&mut parser)?;
            }
            "SEMICOLON" => terminator = true,
            unknown => {
                return Err(ParserError::ParserError(format!(
                    "unsupported CREATE TABLE clause: {unknown}"
                )));
            }
        }
    }

    Ok(RedshiftCreateTable {
        table,
        if_not_exists,
        columns,
        table_kind,
        dist_style,
        dist_key,
        sort_key,
        backup,
        terminator,
    })
}

fn parse_ident_list(parser: &mut Parser) -> Result<Vec<Ident>, ParserError> {
    parser.expect_token(&Token::LParen)?;
    let idents = parser.parse_comma_separated(Parser::parse_identifier)?;
    parser.expect_token(&Token::RParen)?;
    Ok(idents)
}

fn parse_column_list(parser: &mut Parser) -> Result<Vec<RedshiftColumnDef>, ParserError> {
    parser.expect_token(&Token::LParen)?;
    let mut columns = Vec::new();

    loop {
        let name = parser.parse_identifier()?;
        let data_type = parser.parse_data_type()?;

        let encoding = None;
        let mut not_null = false;
        let mut default = None;
        let identity = None;
        let mut primary_key = false;
        let mut references = None;

        // Column constraints are unordered too — loop until comma or close paren
        loop {
            match parser.peek_token().token {
                Token::Comma | Token::RParen => break,
                _ => {}
            }
            let word = expect_word(parser)?;
            match word.to_uppercase().as_str() {
                "NOT" => {
                    parser.expect_keyword(Keyword::NULL)?;
                    not_null = true;
                }
                "NULL" => not_null = false,
                "DEFAULT" => default = Some(parser.parse_expr()?),
                "PRIMARY" => {
                    parser.expect_keyword(Keyword::KEY)?;
                    primary_key = true;
                }
                "REFERENCES" => {
                    references = Some(parser.parse_object_name(false)?);
                }
                // column-level DISTKEY/SORTKEY flags — tolerate, table-level fields win
                "DISTKEY" | "SORTKEY" => {}
                unknown => {
                    Err(ParserError::ParserError(format!(
                        "Unsupported column constraint: {unknown}"
                    )))?;
                }
            }
        }

        columns.push(RedshiftColumnDef {
            name,
            data_type,
            encoding,
            not_null,
            default,
            identity,
            primary_key,
            references,
        });

        if parser.consume_token(&Token::Comma) {
            continue;
        }
        parser.expect_token(&Token::RParen)?;
        break;
    }

    Ok(columns)
}
