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

use std::collections::HashMap;

use sqlparser::{
    ast::ObjectName,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

use crate::connectors::fixtures::redshift::redshift_mock::{
    expect_word, parse_number_literal, parse_string_literal,
};

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct RedshiftCopy {
    pub table: ObjectName,
    pub s3_uri: String,
    #[allow(dead_code)]
    pub access_key_id: String,
    pub secret_access_key: String,
    pub format: CopyFormat,
    pub max_error: u32,
    #[allow(dead_code)]
    pub region: String,
    pub terminator: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyFormat {
    Parquet,
    Other(String),
}

pub fn try_parse_redshift_copy(mut parser: Parser) -> Result<RedshiftCopy, ParserError> {
    if !parser.parse_keyword(Keyword::COPY) {
        Err(ParserError::ParserError("Not a copy statement".to_string()))?
    }

    let table = parser.parse_object_name(false)?;

    parser.expect_keyword(Keyword::FROM)?;

    let s3_uri = parse_string_literal(&mut parser)?;

    if !s3_uri.starts_with("s3://") {
        Err(ParserError::ParserError(format!(
            "expected s3:// URI, got {s3_uri}"
        )))?
    }

    let mut access_key_id = String::new();
    let mut secret_access_key = String::new();
    let mut format = CopyFormat::Parquet;
    let mut max_error = 0u32;
    let mut region = String::new();
    let mut terminator = false;

    // Real Redshift COPY options are unordered after FROM — loop until EOF
    while parser.peek_token() != Token::EOF {
        let word = expect_word(&mut parser)?;

        match word.to_uppercase().as_str() {
            "CREDENTIALS" | "IAM_ROLE" => {
                let credentials = parse_string_literal(&mut parser)?;
                let mut credentials = parse_credentials(&credentials);
                access_key_id = credentials
                    .remove("ACCESS_KEY_ID")
                    .ok_or_else(|| ParserError::ParserError("Missing access_key_id".into()))?;
                secret_access_key = credentials
                    .remove("SECRET_ACCESS_KEY")
                    .ok_or_else(|| ParserError::ParserError("Missing access_key_id".into()))?;
            }
            "FORMAT" => {
                let _ = parser.parse_keyword(Keyword::AS); // "FORMAT AS X" or bare "FORMAT X"
                format = match expect_word(&mut parser)?.to_uppercase().as_str() {
                    "PARQUET" => CopyFormat::Parquet,
                    other => CopyFormat::Other(other.to_string()),
                };
            }
            "MAXERROR" => max_error = parse_number_literal(&mut parser)?,
            "REGION" => region = parse_string_literal(&mut parser)?,
            // clauses you don't emit but want to tolerate rather than error on
            "GZIP" | "COMPUPDATE" | "STATUPDATE" => {
                let _ = parser.parse_one_of_keywords(&[Keyword::ON, Keyword::OFF]);
            }
            "IGNOREHEADER" => {
                parse_number_literal(&mut parser)?;
            }
            "DELIMITER" => {
                parse_string_literal(&mut parser)?;
            }
            "SEMICOLON" => terminator = true,
            unknown => {
                return Err(ParserError::ParserError(format!(
                    "unsupported COPY clause: {unknown}"
                )));
            }
        }
    }

    Ok(RedshiftCopy {
        table,
        s3_uri,
        access_key_id,
        secret_access_key,
        format,
        max_error,
        region,
        terminator,
    })
}

/// Parses a Redshift-style `CREDENTIALS '...'` value into key-value pairs.
/// Input example: "ACCESS_KEY_ID=admin; SECRET_ACCESS_KEY=1234"
fn parse_credentials(raw: &str) -> HashMap<String, String> {
    raw.split(';')
        .filter_map(|pair| {
            let pair = pair.trim();
            if pair.is_empty() {
                return None;
            }
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?.trim().to_string().to_uppercase();
            let value = parts.next()?.trim().to_string();
            Some((key, value))
        })
        .collect()
}
