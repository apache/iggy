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
    parser::{Parser, ParserError},
    tokenizer::{Token, Word},
};

pub mod copy;
pub mod create;
pub mod handler;
pub mod load;

pub fn expect_word(parser: &mut Parser) -> Result<String, ParserError> {
    match parser.next_token().token {
        Token::Word(Word { value, .. }) => Ok(value),
        Token::SemiColon => Ok("SemiColon".into()),
        other => Err(ParserError::ParserError(format!(
            "expected identifier, got {other:?}"
        ))),
    }
}

pub fn parse_string_literal(parser: &mut Parser) -> Result<String, ParserError> {
    match parser.next_token().token {
        Token::SingleQuotedString(s) => Ok(s),
        other => Err(ParserError::ParserError(format!(
            "expected string literal, got {other:?}"
        ))),
    }
}

pub fn parse_number_literal(parser: &mut Parser) -> Result<u32, ParserError> {
    match parser.next_token().token {
        Token::Number(s, _) => s
            .parse()
            .map_err(|_| ParserError::ParserError(format!("bad number: {s}"))),
        other => Err(ParserError::ParserError(format!(
            "expected number, got {other:?}"
        ))),
    }
}
