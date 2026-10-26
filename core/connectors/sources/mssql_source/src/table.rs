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

use std::fmt;
use std::str::FromStr;

/// An unquoted `schema.table` name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TableName {
    pub(crate) schema: String,
    pub(crate) table: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TableNameError {
    MissingSchema,
    EmptyPart,
    Bracketed,
}

impl fmt::Display for TableNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSchema => write!(formatter, "expected exactly one '.' in schema.table"),
            Self::EmptyPart => write!(formatter, "schema and table must not be empty"),
            Self::Bracketed => write!(
                formatter,
                "use unquoted schema.table names, without [brackets]"
            ),
        }
    }
}

impl TableName {
    pub(crate) fn new(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            table: table.into(),
        }
    }

    /// `[schema].[table]`, as `OBJECT_ID` expects it.
    pub(crate) fn quoted(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.schema),
            quote_identifier(&self.table)
        )
    }
}

impl FromStr for TableName {
    type Err = TableNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.split('.');
        let (Some(schema), Some(table), None) = (parts.next(), parts.next(), parts.next()) else {
            return Err(TableNameError::MissingSchema);
        };
        if schema.is_empty() || table.is_empty() {
            return Err(TableNameError::EmptyPart);
        }
        if [schema, table]
            .iter()
            .any(|part| part.starts_with('[') && part.ends_with(']'))
        {
            return Err(TableNameError::Bracketed);
        }
        Ok(Self::new(schema, table))
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}", self.schema, self.table)
    }
}

pub(crate) fn quote_identifier(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_schema_and_table_should_parse() {
        assert_eq!(
            "dbo.users".parse::<TableName>(),
            Ok(TableName::new("dbo", "users"))
        );
    }

    #[test]
    fn given_name_without_schema_should_reject() {
        assert_eq!(
            "users".parse::<TableName>(),
            Err(TableNameError::MissingSchema)
        );
        assert_eq!(
            "db.dbo.users".parse::<TableName>(),
            Err(TableNameError::MissingSchema)
        );
    }

    #[test]
    fn given_empty_part_should_reject() {
        assert_eq!(
            ".users".parse::<TableName>(),
            Err(TableNameError::EmptyPart)
        );
    }

    #[test]
    fn given_bracketed_name_should_reject() {
        assert_eq!(
            "[dbo].[users]".parse::<TableName>(),
            Err(TableNameError::Bracketed)
        );
    }

    #[test]
    fn given_name_with_space_should_quote_each_part() {
        assert_eq!(
            TableName::new("sales", "order items").quoted(),
            "[sales].[order items]"
        );
    }

    #[test]
    fn given_name_with_closing_bracket_should_escape_it() {
        assert_eq!(
            TableName::new("dbo", "odd]name").quoted(),
            "[dbo].[odd]]name]"
        );
    }
}
