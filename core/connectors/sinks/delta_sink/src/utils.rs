/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use deltalake::kernel::{DataType, PrimitiveType, StructField};
use iggy_connector_sdk::Error;
use tracing::error;

pub(crate) fn parse_schema(schema: &[String]) -> Result<Vec<StructField>, Error> {
    schema
        .iter()
        .map(|entry| {
            let parts: Vec<&str> = entry.split_whitespace().collect();
            if parts.len() != 2 {
                return Err(Error::InvalidConfig);
            }
            let name = parts[0];
            let data_type = parse_delta_type(parts[1])?;
            Ok(StructField::new(name, data_type, true))
        })
        .collect()
}

fn parse_delta_type(type_str: &str) -> Result<DataType, Error> {
    let primitive = match type_str {
        "string" => PrimitiveType::String,
        "byte" => PrimitiveType::Byte,
        "short" => PrimitiveType::Short,
        "integer" => PrimitiveType::Integer,
        "long" => PrimitiveType::Long,
        "float" => PrimitiveType::Float,
        "double" => PrimitiveType::Double,
        "boolean" => PrimitiveType::Boolean,
        "binary" => PrimitiveType::Binary,
        "date" => PrimitiveType::Date,
        "timestamp" => PrimitiveType::Timestamp,
        "timestampNtz" => PrimitiveType::TimestampNtz,
        _ => {
            error!("Unsupported Delta type: {type_str}");
            return Err(Error::InvalidConfig);
        }
    };
    Ok(DataType::Primitive(primitive))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_delta_type_all_valid() {
        let cases = vec![
            ("string", PrimitiveType::String),
            ("byte", PrimitiveType::Byte),
            ("short", PrimitiveType::Short),
            ("integer", PrimitiveType::Integer),
            ("long", PrimitiveType::Long),
            ("float", PrimitiveType::Float),
            ("double", PrimitiveType::Double),
            ("boolean", PrimitiveType::Boolean),
            ("binary", PrimitiveType::Binary),
            ("date", PrimitiveType::Date),
            ("timestamp", PrimitiveType::Timestamp),
            ("timestampNtz", PrimitiveType::TimestampNtz),
        ];
        for (input, expected) in cases {
            assert_eq!(
                parse_delta_type(input).unwrap(),
                DataType::Primitive(expected),
                "Failed for type: {input}"
            );
        }
    }

    #[test]
    fn test_parse_delta_type_invalid() {
        assert_eq!(parse_delta_type("varchar"), Err(Error::InvalidConfig));
        assert_eq!(parse_delta_type(""), Err(Error::InvalidConfig));
        assert_eq!(parse_delta_type("INT"), Err(Error::InvalidConfig));
        assert_eq!(parse_delta_type("String"), Err(Error::InvalidConfig));
    }

    #[test]
    fn test_parse_schema_valid() {
        let schema = vec!["name string".to_string(), "age integer".to_string()];
        let result = parse_schema(&schema).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name(), "name");
        assert_eq!(
            result[0].data_type(),
            &DataType::Primitive(PrimitiveType::String)
        );
        assert!(result[0].is_nullable());
        assert_eq!(result[1].name(), "age");
        assert_eq!(
            result[1].data_type(),
            &DataType::Primitive(PrimitiveType::Integer)
        );
    }

    #[test]
    fn test_parse_schema_empty() {
        let schema: Vec<String> = vec![];
        let result = parse_schema(&schema).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_schema_missing_type() {
        let schema = vec!["name".to_string()];
        assert_eq!(parse_schema(&schema), Err(Error::InvalidConfig));
    }

    #[test]
    fn test_parse_schema_too_many_parts() {
        let schema = vec!["name string extra".to_string()];
        assert_eq!(parse_schema(&schema), Err(Error::InvalidConfig));
    }

    #[test]
    fn test_parse_schema_invalid_type() {
        let schema = vec!["name foobar".to_string()];
        assert_eq!(parse_schema(&schema), Err(Error::InvalidConfig));
    }

    #[test]
    fn test_parse_schema_error_stops_at_first_bad_entry() {
        let schema = vec![
            "valid_col string".to_string(),
            "bad_col".to_string(),
            "another_col integer".to_string(),
        ];
        assert_eq!(parse_schema(&schema), Err(Error::InvalidConfig));
    }
}
