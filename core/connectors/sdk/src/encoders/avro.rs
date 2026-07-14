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
use std::path::PathBuf;

use apache_avro::{
    Schema as AvroSchema,
    schema::{Name, Namespace, ResolvedSchema},
    types::Value as AvroValue,
};
use base64::Engine;
use bson::Bson;
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use uuid::Uuid;

use crate::{Error, Payload, Schema, StreamEncoder, convert::owned_value_to_serde_json};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AvroEncoderConfig {
    pub schema_path: Option<PathBuf>,
    pub schema_json: Option<String>,
    pub field_mappings: Option<HashMap<String, String>>,
}

pub struct AvroStreamEncoder {
    config: AvroEncoderConfig,
    schema: Option<AvroSchema>,
    resolved_names: HashMap<Name, AvroSchema>,
}

impl AvroStreamEncoder {
    /// If you need fail-fast behaviour,
    /// use [`try_new`](Self::try_new) instead.
    pub fn new(config: AvroEncoderConfig) -> Self {
        let mut encoder = Self {
            config,
            schema: None,
            resolved_names: HashMap::new(),
        };
        if let Err(e) = encoder.load_schema() {
            error!("Failed to load Avro schema during encoder creation: {}", e);
        }
        encoder
    }

    pub fn try_new(config: AvroEncoderConfig) -> Result<Self, Error> {
        let mut encoder = Self {
            config,
            schema: None,
            resolved_names: HashMap::new(),
        };
        encoder.load_schema()?;
        Ok(encoder)
    }

    pub fn new_default() -> Self {
        Self::new(AvroEncoderConfig::default())
    }

    pub fn update_config(&mut self, config: AvroEncoderConfig) -> Result<(), Error> {
        let old_config = std::mem::replace(&mut self.config, config);
        if let Err(e) = self.load_schema() {
            // Rollback on failure to keep config/schema consistent
            self.config = old_config;
            return Err(e);
        }
        Ok(())
    }

    fn load_schema(&mut self) -> Result<(), Error> {
        if let Some(schema_json) = &self.config.schema_json {
            match AvroSchema::parse_str(schema_json) {
                Ok(schema) => {
                    self.set_schema(schema)?;
                    info!("Loaded Avro schema from inline JSON");
                    return Ok(());
                }
                Err(e) => {
                    error!("Failed to parse inline Avro schema: {}", e);
                    return Err(Error::InvalidConfigValue(format!(
                        "Invalid Avro schema JSON: {e}"
                    )));
                }
            }
        }

        if let Some(schema_path) = self.config.schema_path.clone() {
            match std::fs::read_to_string(&schema_path) {
                Ok(schema_content) => match AvroSchema::parse_str(&schema_content) {
                    Ok(schema) => {
                        self.set_schema(schema)?;
                        info!("Loaded Avro schema from file: {:?}", schema_path);
                        return Ok(());
                    }
                    Err(e) => {
                        error!("Failed to parse Avro schema file: {}", e);
                        return Err(Error::InvalidConfigValue(format!(
                            "Invalid Avro schema file: {e}"
                        )));
                    }
                },
                Err(e) => {
                    error!("Failed to read Avro schema file: {}", e);
                    return Err(Error::InvalidConfigValue(format!(
                        "Cannot read schema file: {e}"
                    )));
                }
            }
        }

        self.schema = None;
        self.resolved_names.clear();
        Ok(())
    }

    fn set_schema(&mut self, schema: AvroSchema) -> Result<(), Error> {
        let resolved_schema = ResolvedSchema::try_from(&schema).map_err(|error| {
            Error::InvalidConfigValue(format!("Failed to resolve Avro schema: {error}"))
        })?;
        let resolved_names = resolved_schema
            .get_names()
            .iter()
            .map(|(name, schema)| (name.clone(), (*schema).clone()))
            .collect();

        self.schema = Some(schema);
        self.resolved_names = resolved_names;
        Ok(())
    }

    fn apply_field_transformations(&self, payload: Payload) -> Result<Payload, Error> {
        if let Some(mappings) = &self.config.field_mappings {
            match payload {
                Payload::Json(json_value) => {
                    if let simd_json::OwnedValue::Object(mut map) = json_value {
                        let mut new_entries = Vec::new();

                        for (key, value) in map.iter() {
                            if let Some(new_key) = mappings.get(key) {
                                new_entries.push((new_key.clone(), value.clone()));
                            } else {
                                new_entries.push((key.clone(), value.clone()));
                            }
                        }

                        map.clear();
                        for (key, value) in new_entries {
                            map.insert(key, value);
                        }

                        Ok(Payload::Json(simd_json::OwnedValue::Object(map)))
                    } else {
                        Ok(Payload::Json(json_value))
                    }
                }
                Payload::Bson(document) => {
                    let document: bson::Document = document
                        .into_iter()
                        .map(|(key, value)| {
                            let key = mappings.get(&key).cloned().unwrap_or(key);
                            (key, value)
                        })
                        .collect();

                    Ok(Payload::Bson(document))
                }
                other => Ok(other),
            }
        } else {
            Ok(payload)
        }
    }

    fn encode_json_to_avro(&self, json_value: simd_json::OwnedValue) -> Result<Vec<u8>, Error> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            error!("Cannot encode JSON to Avro without a schema");
            Error::InvalidConfigValue("Avro schema is required for encoding".to_string())
        })?;

        let serde_value = owned_value_to_serde_json(&json_value);
        let avro_value = Self::serde_json_to_avro_value(serde_value, schema)?;

        apache_avro::to_avro_datum(schema, avro_value).map_err(|e| {
            error!("Failed to encode Avro datum: {}", e);
            Error::Serialization(format!("Avro encoding failed: {e}"))
        })
    }

    fn encode_bson_to_avro(&self, document: bson::Document) -> Result<Vec<u8>, Error> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            error!("Avro schema is required to encode BSON payloads");
            Error::InvalidConfigValue("Avro schema is required to encode BSON payloads".to_string())
        })?;
        let bson_value = Bson::Document(document);
        let avro_value =
            Self::bson_to_avro_value(&bson_value, schema, &self.resolved_names, &None, "$")?;

        apache_avro::to_avro_datum(schema, avro_value).map_err(|error| {
            error!("Failed to encode BSON as Avro datum: {error}");
            Error::Serialization(format!("Avro encoding failed: {error}"))
        })
    }

    fn bson_to_avro_value(
        value: &Bson,
        schema: &AvroSchema,
        names: &HashMap<Name, AvroSchema>,
        enclosing_namespace: &Namespace,
        path: &str,
    ) -> Result<AvroValue, Error> {
        let conversion_error = || {
            Error::Serialization(format!(
                "BSON value `{value:?}` at `{path}` does not match Avro schema `{schema}`"
            ))
        };

        match schema {
            AvroSchema::Union(union_schema) => {
                for (index, variant_schema) in union_schema.variants().iter().enumerate() {
                    if let Ok(value) = Self::bson_to_avro_value(
                        value,
                        variant_schema,
                        names,
                        enclosing_namespace,
                        path,
                    ) {
                        let index = u32::try_from(index).map_err(|_| {
                            Error::Serialization(
                                "Avro union contains too many variants".to_string(),
                            )
                        })?;
                        return Ok(AvroValue::Union(index, Box::new(value)));
                    }
                }

                Err(conversion_error())
            }
            AvroSchema::Record(record_schema) => {
                let Bson::Document(document) = value else {
                    return Err(conversion_error());
                };
                let record_namespace = record_schema
                    .name
                    .fully_qualified_name(enclosing_namespace)
                    .namespace;
                let mut record = Vec::with_capacity(record_schema.fields.len());

                for field in &record_schema.fields {
                    let field_path = format!("{path}.{}", field.name);
                    let field_value = document.get(&field.name).unwrap_or(&Bson::Null);
                    record.push((
                        field.name.clone(),
                        Self::bson_to_avro_value(
                            field_value,
                            &field.schema,
                            names,
                            &record_namespace,
                            &field_path,
                        )?,
                    ));
                }

                Ok(AvroValue::Record(record))
            }
            AvroSchema::Array(array_schema) => {
                let Bson::Array(items) = value else {
                    return Err(conversion_error());
                };
                let values = items
                    .iter()
                    .enumerate()
                    .map(|(index, item)| {
                        let field_path = format!("{path}[{index}]");
                        Self::bson_to_avro_value(
                            item,
                            &array_schema.items,
                            names,
                            enclosing_namespace,
                            &field_path,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(AvroValue::Array(values))
            }
            AvroSchema::Map(map_schema) => {
                let Bson::Document(document) = value else {
                    return Err(conversion_error());
                };
                let values = document
                    .iter()
                    .map(|(key, value)| {
                        let field_path = format!("{path}.{key}");
                        Self::bson_to_avro_value(
                            value,
                            &map_schema.types,
                            names,
                            enclosing_namespace,
                            &field_path,
                        )
                        .map(|value| (key.clone(), value))
                    })
                    .collect::<Result<HashMap<_, _>, _>>()?;

                Ok(AvroValue::Map(values))
            }
            AvroSchema::Enum(enum_schema) => {
                let Bson::String(symbol) = value else {
                    return Err(conversion_error());
                };
                let index = enum_schema
                    .symbols
                    .iter()
                    .position(|candidate| candidate == symbol)
                    .ok_or_else(|| {
                        Error::Serialization(format!(
                            "Unknown Avro enum symbol `{symbol}` at `{path}`"
                        ))
                    })?;
                let index = u32::try_from(index).map_err(|_| {
                    Error::Serialization("Avro enum contains too many symbols".to_string())
                })?;

                Ok(AvroValue::Enum(index, symbol.clone()))
            }
            AvroSchema::Fixed(fixed_schema) => {
                let Bson::Binary(binary) = value else {
                    return Err(conversion_error());
                };
                if binary.bytes.len() != fixed_schema.size {
                    return Err(Error::Serialization(format!(
                        "BSON binary at `{path}` has {} bytes, expected {}",
                        binary.bytes.len(),
                        fixed_schema.size
                    )));
                }

                Ok(AvroValue::Fixed(fixed_schema.size, binary.bytes.clone()))
            }
            AvroSchema::Ref { name } => {
                let full_name = name.fully_qualified_name(enclosing_namespace);
                let resolved_schema = names.get(&full_name).ok_or_else(|| {
                    Error::Serialization(format!(
                        "Unresolved Avro schema reference `{full_name}` at `{path}`"
                    ))
                })?;

                Self::bson_to_avro_value(value, resolved_schema, names, &full_name.namespace, path)
            }
            AvroSchema::Null => match value {
                Bson::Null => Ok(AvroValue::Null),
                _ => Err(conversion_error()),
            },
            AvroSchema::Boolean => match value {
                Bson::Boolean(value) => Ok(AvroValue::Boolean(*value)),
                _ => Err(conversion_error()),
            },
            AvroSchema::Int => match value {
                Bson::Int32(number) => Ok(AvroValue::Int(*number)),
                Bson::Int64(number) => i32::try_from(*number)
                    .map(AvroValue::Int)
                    .map_err(|_| conversion_error()),
                _ => Err(conversion_error()),
            },
            AvroSchema::Long => match value {
                Bson::Int32(value) => Ok(AvroValue::Long(i64::from(*value))),
                Bson::Int64(value) => Ok(AvroValue::Long(*value)),
                _ => Err(conversion_error()),
            },
            AvroSchema::Float => match value {
                Bson::Double(value) => Ok(AvroValue::Float(*value as f32)),
                _ => Err(conversion_error()),
            },
            AvroSchema::Double => match value {
                Bson::Double(value) => Ok(AvroValue::Double(*value)),
                _ => Err(conversion_error()),
            },
            AvroSchema::Bytes => match value {
                Bson::Binary(binary) => Ok(AvroValue::Bytes(binary.bytes.clone())),
                _ => Err(conversion_error()),
            },
            AvroSchema::String => match value {
                Bson::String(value) | Bson::Symbol(value) => Ok(AvroValue::String(value.clone())),
                Bson::ObjectId(value) => Ok(AvroValue::String(value.to_hex())),
                _ => Err(conversion_error()),
            },
            AvroSchema::Uuid => match value {
                Bson::String(text) => Uuid::parse_str(text)
                    .map(AvroValue::Uuid)
                    .map_err(|_| conversion_error()),
                _ => Err(conversion_error()),
            },
            AvroSchema::Date => match value {
                Bson::Int32(number) => Ok(AvroValue::Date(*number)),
                Bson::DateTime(date_time) => {
                    let days = date_time.timestamp_millis().div_euclid(86_400_000);
                    i32::try_from(days)
                        .map(AvroValue::Date)
                        .map_err(|_| conversion_error())
                }
                _ => Err(conversion_error()),
            },
            AvroSchema::TimeMillis => match value {
                Bson::Int32(value) => Ok(AvroValue::TimeMillis(*value)),
                _ => Err(conversion_error()),
            },
            AvroSchema::TimeMicros => match value {
                Bson::Int32(value) => Ok(AvroValue::TimeMicros(i64::from(*value))),
                Bson::Int64(value) => Ok(AvroValue::TimeMicros(*value)),
                _ => Err(conversion_error()),
            },
            AvroSchema::TimestampMillis => match value {
                Bson::DateTime(value) => Ok(AvroValue::TimestampMillis(value.timestamp_millis())),
                Bson::Int64(value) => Ok(AvroValue::TimestampMillis(*value)),
                _ => Err(conversion_error()),
            },
            AvroSchema::TimestampMicros => match value {
                Bson::DateTime(date_time) => date_time
                    .timestamp_millis()
                    .checked_mul(1_000)
                    .map(AvroValue::TimestampMicros)
                    .ok_or_else(&conversion_error),
                Bson::Int64(value) => Ok(AvroValue::TimestampMicros(*value)),
                _ => Err(conversion_error()),
            },
            AvroSchema::TimestampNanos => match value {
                Bson::DateTime(date_time) => date_time
                    .timestamp_millis()
                    .checked_mul(1_000_000)
                    .map(AvroValue::TimestampNanos)
                    .ok_or_else(&conversion_error),
                Bson::Int64(value) => Ok(AvroValue::TimestampNanos(*value)),
                _ => Err(conversion_error()),
            },
            AvroSchema::LocalTimestampMillis => match value {
                Bson::Int64(value) => Ok(AvroValue::LocalTimestampMillis(*value)),
                _ => Err(conversion_error()),
            },
            AvroSchema::LocalTimestampMicros => match value {
                Bson::Int64(value) => Ok(AvroValue::LocalTimestampMicros(*value)),
                _ => Err(conversion_error()),
            },
            AvroSchema::LocalTimestampNanos => match value {
                Bson::Int64(value) => Ok(AvroValue::LocalTimestampNanos(*value)),
                _ => Err(conversion_error()),
            },
            AvroSchema::Decimal(_) | AvroSchema::BigDecimal | AvroSchema::Duration => {
                Err(Error::Serialization(format!(
                    "BSON value at `{path}` cannot be converted safely to Avro schema `{schema}`"
                )))
            }
        }
    }

    fn serde_json_to_avro_value(
        value: serde_json::Value,
        schema: &apache_avro::Schema,
    ) -> Result<apache_avro::types::Value, Error> {
        use apache_avro::Schema as AvroSchema;
        use apache_avro::types::Value as AvroValue;

        match (value, schema) {
            (serde_json::Value::Object(map), AvroSchema::Record(record_schema)) => {
                let mut record = Vec::new();
                for field in &record_schema.fields {
                    let field_value = map
                        .get(&field.name)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    record.push((
                        field.name.clone(),
                        Self::serde_json_to_avro_value(field_value, &field.schema)?,
                    ));
                }
                Ok(AvroValue::Record(record))
            }
            (serde_json::Value::Array(arr), AvroSchema::Array(array_schema)) => {
                Ok(AvroValue::Array(
                    arr.into_iter()
                        .map(|v| Self::serde_json_to_avro_value(v, &array_schema.items))
                        .collect::<Result<Vec<_>, _>>()?,
                ))
            }
            (serde_json::Value::Object(map), AvroSchema::Map(map_schema)) => Ok(AvroValue::Map(
                map.into_iter()
                    .map(|(k, v)| {
                        Self::serde_json_to_avro_value(v, &map_schema.types).map(|av| (k, av))
                    })
                    .collect::<Result<_, _>>()?,
            )),
            (value, AvroSchema::Union(union_schema)) => {
                let schemas = union_schema.variants();
                for (idx, variant_schema) in schemas.iter().enumerate() {
                    match Self::serde_json_to_avro_value(value.clone(), variant_schema) {
                        Ok(avro_val) if avro_val.validate(variant_schema) => {
                            return Ok(AvroValue::Union(idx as u32, Box::new(avro_val)));
                        }
                        _ => continue,
                    }
                }
                Err(Error::InvalidPayloadType)
            }
            (serde_json::Value::Null, AvroSchema::Null) => Ok(AvroValue::Null),
            (serde_json::Value::Bool(b), AvroSchema::Boolean) => Ok(AvroValue::Boolean(b)),
            (serde_json::Value::Number(n), AvroSchema::Int) => {
                let v = n.as_i64().ok_or_else(|| {
                    Error::Serialization(format!("Cannot convert JSON number to Avro Int: {n}"))
                })?;
                let v_i32 = i32::try_from(v).map_err(|_| {
                    Error::Serialization(format!("JSON number {v} out of range for Avro Int (i32)"))
                })?;
                Ok(AvroValue::Int(v_i32))
            }
            (serde_json::Value::Number(n), AvroSchema::Long) => {
                let v = n.as_i64().ok_or_else(|| {
                    Error::Serialization(format!("Cannot convert JSON number to Avro Long: {n}"))
                })?;
                Ok(AvroValue::Long(v))
            }
            (serde_json::Value::Number(n), AvroSchema::Float) => {
                let v = n.as_f64().ok_or_else(|| {
                    Error::Serialization(format!("Cannot convert JSON number to Avro Float: {n}"))
                })?;
                Ok(AvroValue::Float(v as f32))
            }
            (serde_json::Value::Number(n), AvroSchema::Double) => {
                let v = n.as_f64().ok_or_else(|| {
                    Error::Serialization(format!("Cannot convert JSON number to Avro Double: {n}"))
                })?;
                Ok(AvroValue::Double(v))
            }
            (serde_json::Value::String(s), AvroSchema::String) => Ok(AvroValue::String(s)),
            (serde_json::Value::String(s), AvroSchema::Bytes) => {
                Ok(AvroValue::Bytes(s.into_bytes()))
            }
            (serde_json::Value::Array(arr), AvroSchema::Bytes) => {
                let mut bytes = Vec::with_capacity(arr.len());
                for (i, v) in arr.into_iter().enumerate() {
                    let n = v.as_u64().ok_or_else(|| {
                        Error::Serialization(format!(
                            "Bytes array element at index {i} is not a valid u8: {v}"
                        ))
                    })?;
                    let b = u8::try_from(n).map_err(|_| {
                        Error::Serialization(format!(
                            "Bytes array element at index {i} out of u8 range: {n}"
                        ))
                    })?;
                    bytes.push(b);
                }
                Ok(AvroValue::Bytes(bytes))
            }

            (value, schema) => {
                let avro_val: AvroValue = value.into();
                if avro_val.validate(schema) {
                    Ok(avro_val)
                } else {
                    Err(Error::Serialization(format!(
                        "JSON value does not match Avro schema: {schema:?}"
                    )))
                }
            }
        }
    }

    fn encode_text_to_avro(&self, text: String) -> Result<Vec<u8>, Error> {
        let mut text_bytes = text.into_bytes();
        if let Ok(json_value) = simd_json::to_owned_value(&mut text_bytes) {
            return self.encode_json_to_avro(json_value);
        }

        Err(Error::InvalidJsonPayload)
    }

    fn encode_raw_to_avro(&self, data: Vec<u8>) -> Result<Vec<u8>, Error> {
        Ok(data)
    }

    pub fn convert_format(
        &self,
        payload: Payload,
        target_format: Schema,
    ) -> Result<Payload, Error> {
        match (payload, target_format) {
            (Payload::Avro(data), Schema::Json) => {
                let mut data_copy = data.clone();
                let json_value = simd_json::to_owned_value(&mut data_copy)
                    .unwrap_or_else(|_| simd_json::json!({ "avro_data_base64": base64::engine::general_purpose::STANDARD.encode(&data) }));
                Ok(Payload::Json(json_value))
            }
            (Payload::Avro(data), Schema::Text) => {
                let base64_data = base64::engine::general_purpose::STANDARD.encode(&data);
                Ok(Payload::Text(base64_data))
            }
            (Payload::Avro(data), Schema::Raw) => Ok(Payload::Raw(data)),

            (Payload::Json(json), Schema::Text) => {
                let text =
                    simd_json::to_string_pretty(&json).map_err(|_| Error::InvalidJsonPayload)?;
                Ok(Payload::Text(text))
            }
            (Payload::Json(json), Schema::Raw) => {
                let bytes = simd_json::to_vec(&json).map_err(|_| Error::InvalidJsonPayload)?;
                Ok(Payload::Raw(bytes))
            }

            (Payload::Text(text), Schema::Json) => {
                let mut text_bytes = text.into_bytes();
                let json_value = simd_json::to_owned_value(&mut text_bytes)
                    .map_err(|_| Error::InvalidJsonPayload)?;
                Ok(Payload::Json(json_value))
            }
            (Payload::Text(text), Schema::Raw) => Ok(Payload::Raw(text.into_bytes())),

            (Payload::Raw(data), Schema::Text) => {
                let text = String::from_utf8(data).map_err(|_| Error::InvalidTextPayload)?;
                Ok(Payload::Text(text))
            }
            (Payload::Raw(mut data), Schema::Json) => {
                let json_value =
                    simd_json::to_owned_value(&mut data).map_err(|_| Error::InvalidJsonPayload)?;
                Ok(Payload::Json(json_value))
            }

            (payload, _) => Ok(payload),
        }
    }
}

impl StreamEncoder for AvroStreamEncoder {
    fn schema(&self) -> Schema {
        Schema::Avro
    }

    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        let transformed_payload = self.apply_field_transformations(payload)?;

        match transformed_payload {
            Payload::Json(json_value) => self.encode_json_to_avro(json_value),
            Payload::Text(text) => self.encode_text_to_avro(text),
            Payload::Raw(data) => self.encode_raw_to_avro(data),
            Payload::Avro(data) => Ok(data),
            Payload::Proto(text) => self.encode_text_to_avro(text),
            Payload::FlatBuffer(data) => self.encode_raw_to_avro(data),
            Payload::Bson(document) => self.encode_bson_to_avro(document),
        }
    }
}

impl Default for AvroStreamEncoder {
    fn default() -> Self {
        Self::new_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{Binary, oid::ObjectId, spec::BinarySubtype};

    fn create_test_schema_json() -> String {
        r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }"#
        .to_string()
    }

    fn decode_avro_datum(encoder: &AvroStreamEncoder, encoded: &[u8]) -> AvroValue {
        let schema = encoder
            .schema
            .as_ref()
            .expect("encoder should have a schema");
        let mut reader = encoded;
        apache_avro::from_avro_datum(schema, &mut reader, None)
            .expect("encoded BSON should be a valid Avro datum")
    }

    #[test]
    fn encode_should_handle_json_payload_with_schema() {
        let schema_json = create_test_schema_json();
        let config = AvroEncoderConfig {
            schema_json: Some(schema_json),
            ..AvroEncoderConfig::default()
        };
        let encoder = AvroStreamEncoder::new(config);

        let json_value = simd_json::json!({
            "name": "Alice",
            "age": 30
        });

        let result = encoder.encode(Payload::Json(json_value));

        assert!(result.is_ok());
        let encoded_data = result.unwrap();
        assert!(!encoded_data.is_empty());
    }

    #[test]
    fn encode_should_handle_schema_driven_bson_payload() {
        let schema_json = r#"{
            "type": "record",
            "name": "Event",
            "namespace": "iggy",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"},
                {"name": "scores", "type": {"type": "array", "items": "long"}},
                {"name": "metadata", "type": {"type": "map", "values": "string"}},
                {"name": "nickname", "type": ["null", "string"]},
                {"name": "payload", "type": "bytes"},
                {"name": "state", "type": {
                    "type": "enum",
                    "name": "State",
                    "symbols": ["active", "inactive"]
                }},
                {"name": "token", "type": {
                    "type": "fixed",
                    "name": "Token",
                    "size": 4
                }},
                {"name": "created_at", "type": {
                    "type": "long",
                    "logicalType": "timestamp-millis"
                }},
                {"name": "object_id", "type": "string"},
                {"name": "request_id", "type": {
                    "type": "string",
                    "logicalType": "uuid"
                }}
            ]
        }"#;
        let encoder = AvroStreamEncoder::new(AvroEncoderConfig {
            schema_json: Some(schema_json.to_string()),
            ..AvroEncoderConfig::default()
        });
        let object_id = ObjectId::parse_str("507f1f77bcf86cd799439011")
            .expect("object ID fixture should be valid");
        let request_id = Uuid::parse_str("1481531d-ccc9-46d9-a56f-5b67459c0537")
            .expect("UUID fixture should be valid");
        let document = bson::doc! {
            "name": "Alice",
            "age": 30,
            "scores": [10_i64, 20_i64],
            "metadata": {
                "source": "bson",
                "region": "eu"
            },
            "nickname": Bson::Null,
            "payload": Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![1, 2, 3]
            },
            "state": "inactive",
            "token": Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![4, 5, 6, 7]
            },
            "created_at": bson::DateTime::from_millis(1_700_000_000_000),
            "object_id": object_id,
            "request_id": request_id.to_string()
        };

        let encoded = encoder
            .encode(Payload::Bson(document))
            .expect("BSON document should encode as Avro");
        let AvroValue::Record(fields) = decode_avro_datum(&encoder, &encoded) else {
            panic!("decoded Avro value should be a record");
        };
        let fields: HashMap<_, _> = fields.into_iter().collect();

        assert_eq!(fields["name"], AvroValue::String("Alice".to_string()));
        assert_eq!(fields["age"], AvroValue::Int(30));
        assert_eq!(
            fields["scores"],
            AvroValue::Array(vec![AvroValue::Long(10), AvroValue::Long(20)])
        );
        assert_eq!(
            fields["nickname"],
            AvroValue::Union(0, Box::new(AvroValue::Null))
        );
        assert_eq!(fields["payload"], AvroValue::Bytes(vec![1, 2, 3]));
        assert_eq!(fields["state"], AvroValue::Enum(1, "inactive".to_string()));
        assert_eq!(fields["token"], AvroValue::Fixed(4, vec![4, 5, 6, 7]));
        assert_eq!(
            fields["created_at"],
            AvroValue::TimestampMillis(1_700_000_000_000)
        );
        assert_eq!(
            fields["object_id"],
            AvroValue::String("507f1f77bcf86cd799439011".to_string())
        );
        assert_eq!(fields["request_id"], AvroValue::Uuid(request_id));
        assert!(matches!(fields["metadata"], AvroValue::Map(_)));
    }

    #[test]
    fn encode_should_resolve_named_schema_references_for_bson() {
        let schema_json = r#"{
            "type": "record",
            "name": "Customer",
            "namespace": "iggy",
            "fields": [
                {"name": "shipping", "type": {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                        {"name": "city", "type": "string"}
                    ]
                }},
                {"name": "billing", "type": "Address"}
            ]
        }"#;
        let encoder = AvroStreamEncoder::new(AvroEncoderConfig {
            schema_json: Some(schema_json.to_string()),
            ..AvroEncoderConfig::default()
        });
        let address_name = Name::new("iggy.Address").expect("name should be valid");
        let document = bson::doc! {
            "shipping": {"city": "Warsaw"},
            "billing": {"city": "London"}
        };

        assert!(encoder.resolved_names.contains_key(&address_name));

        let encoded = encoder
            .encode(Payload::Bson(document))
            .expect("named Avro schema reference should resolve");
        let AvroValue::Record(fields) = decode_avro_datum(&encoder, &encoded) else {
            panic!("decoded Avro value should be a record");
        };

        assert!(matches!(fields[0].1, AvroValue::Record(_)));
        assert!(matches!(fields[1].1, AvroValue::Record(_)));
    }

    #[test]
    fn encode_should_apply_field_mappings_to_bson_payload() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("full_name".to_string(), "name".to_string());
        let encoder = AvroStreamEncoder::new(AvroEncoderConfig {
            schema_json: Some(create_test_schema_json()),
            field_mappings: Some(field_mappings),
            ..AvroEncoderConfig::default()
        });
        let document = bson::doc! {
            "full_name": "Alice",
            "age": 30
        };

        let encoded = encoder
            .encode(Payload::Bson(document))
            .expect("mapped BSON document should encode as Avro");
        let AvroValue::Record(fields) = decode_avro_datum(&encoder, &encoded) else {
            panic!("decoded Avro value should be a record");
        };

        assert_eq!(fields[0].1, AvroValue::String("Alice".to_string()));
        assert_eq!(fields[1].1, AvroValue::Int(30));
    }

    #[test]
    fn encode_should_report_bson_field_path_on_type_mismatch() {
        let encoder = AvroStreamEncoder::new(AvroEncoderConfig {
            schema_json: Some(create_test_schema_json()),
            ..AvroEncoderConfig::default()
        });
        let document = bson::doc! {
            "name": "Alice",
            "age": "thirty"
        };

        let error = encoder
            .encode(Payload::Bson(document))
            .expect_err("BSON string should not encode as an Avro int");

        assert!(matches!(
            error,
            Error::Serialization(message) if message.contains("$.age")
        ));
    }

    #[test]
    fn encode_should_fail_without_schema_for_bson_payload() {
        let encoder = AvroStreamEncoder::default();

        let error = encoder
            .encode(Payload::Bson(bson::doc! {"name": "Alice"}))
            .expect_err("BSON encoding should require an Avro schema");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn encode_should_reject_bson_decimal_without_implicit_conversion() {
        let schema_json = r#"{
            "type": "record",
            "name": "Payment",
            "fields": [
                {"name": "amount", "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 10,
                    "scale": 2
                }}
            ]
        }"#;
        let encoder = AvroStreamEncoder::new(AvroEncoderConfig {
            schema_json: Some(schema_json.to_string()),
            ..AvroEncoderConfig::default()
        });
        let decimal = "12.34"
            .parse::<bson::Decimal128>()
            .expect("decimal fixture should be valid");

        let error = encoder
            .encode(Payload::Bson(bson::doc! {"amount": decimal}))
            .expect_err("BSON Decimal128 should require an explicit conversion policy");

        assert!(matches!(
            error,
            Error::Serialization(message) if message.contains("$.amount")
        ));
    }

    #[test]
    fn encode_should_pass_through_avro_payload() {
        let encoder = AvroStreamEncoder::default();

        let avro_data = vec![1, 2, 3, 4, 5];
        let avro_payload = Payload::Avro(avro_data.clone());
        let result = encoder.encode(avro_payload);

        assert!(result.is_ok());
        let encoded_data = result.unwrap();
        assert_eq!(encoded_data, avro_data);
    }

    #[test]
    fn encode_should_apply_field_mappings_when_configured() {
        let schema_json = create_test_schema_json();
        let mut field_mappings = HashMap::new();
        field_mappings.insert("full_name".to_string(), "name".to_string());

        let config = AvroEncoderConfig {
            schema_json: Some(schema_json),
            field_mappings: Some(field_mappings),
            ..AvroEncoderConfig::default()
        };
        let encoder = AvroStreamEncoder::new(config);

        let json_value = simd_json::json!({
            "full_name": "Alice",
            "age": 30
        });

        let result = encoder.encode(Payload::Json(json_value));

        assert!(result.is_ok());
        let encoded_data = result.unwrap();
        assert!(!encoded_data.is_empty());
    }

    #[test]
    fn encode_should_fail_without_schema_for_json_payload() {
        let encoder = AvroStreamEncoder::default();

        let json_value = simd_json::json!({
            "name": "Alice",
            "age": 30
        });

        let result = encoder.encode(Payload::Json(json_value));
        assert!(result.is_err());
    }

    #[test]
    fn encode_should_handle_union_null_string_schema() {
        let schema_json = r#"["null", "string"]"#.to_string();
        let config = AvroEncoderConfig {
            schema_json: Some(schema_json),
            ..AvroEncoderConfig::default()
        };
        let encoder = AvroStreamEncoder::new(config);

        let result = encoder.encode(Payload::Json(simd_json::json!("hello")));
        assert!(result.is_ok(), "Expected string union variant to encode");
        assert!(!result.unwrap().is_empty());
    }

    #[test]
    fn encode_should_handle_union_null_int_schema() {
        let schema_json = r#"["null", "int"]"#.to_string();
        let config = AvroEncoderConfig {
            schema_json: Some(schema_json),
            ..AvroEncoderConfig::default()
        };
        let encoder = AvroStreamEncoder::new(config);

        let result = encoder.encode(Payload::Json(simd_json::json!(42)));
        assert!(result.is_ok(), "Expected int union variant to encode");
        assert!(!result.unwrap().is_empty());
    }

    #[test]
    fn encode_should_fail_on_int_overflow() {
        let schema_json = r#"{"type": "int"}"#.to_string();
        let config = AvroEncoderConfig {
            schema_json: Some(schema_json),
            ..AvroEncoderConfig::default()
        };
        let encoder = AvroStreamEncoder::new(config);

        let result = encoder.encode(Payload::Json(simd_json::json!(i64::MAX)));
        assert!(result.is_err(), "Expected overflow to fail");
    }

    #[test]
    fn encode_should_fail_on_invalid_bytes_array_element() {
        let schema_json = r#"{"type": "bytes"}"#.to_string();
        let config = AvroEncoderConfig {
            schema_json: Some(schema_json),
            ..AvroEncoderConfig::default()
        };
        let encoder = AvroStreamEncoder::new(config);

        let result = encoder.encode(Payload::Json(simd_json::json!([1, 2, 300])));
        assert!(result.is_err(), "Expected out-of-range byte to fail");
    }

    #[test]
    fn convert_format_should_transform_avro_to_json() {
        let encoder = AvroStreamEncoder::default();

        let avro_data = vec![1, 2, 3, 4, 5];
        let result = encoder.convert_format(Payload::Avro(avro_data.clone()), Schema::Json);

        assert!(result.is_ok());
        if let Ok(Payload::Json(json_value)) = result {
            if let simd_json::OwnedValue::Object(map) = json_value {
                assert!(map.contains_key("avro_data_base64"));
            } else {
                panic!("Expected JSON object");
            }
        } else {
            panic!("Expected JSON payload");
        }
    }

    #[test]
    fn convert_format_should_transform_avro_to_text() {
        let encoder = AvroStreamEncoder::default();

        let avro_data = vec![1, 2, 3, 4, 5];
        let result = encoder.convert_format(Payload::Avro(avro_data), Schema::Text);

        assert!(result.is_ok());
        if let Ok(Payload::Text(text)) = result {
            assert!(!text.is_empty());
        } else {
            panic!("Expected Text payload");
        }
    }

    #[test]
    fn config_should_have_sensible_defaults() {
        let config = AvroEncoderConfig::default();

        assert!(config.schema_path.is_none());
        assert!(config.schema_json.is_none());
        assert!(config.field_mappings.is_none());
    }

    #[test]
    fn encoder_should_be_creatable_with_custom_config() {
        let config = AvroEncoderConfig {
            schema_path: Some(PathBuf::from("/path/to/schema.avsc")),
            schema_json: Some(r#"{"type": "string"}"#.to_string()),
            ..AvroEncoderConfig::default()
        };

        let encoder = AvroStreamEncoder::new(config.clone());

        assert_eq!(encoder.config.schema_path, config.schema_path);
        assert_eq!(encoder.config.schema_json, config.schema_json);
    }

    #[test]
    fn try_new_should_fail_on_invalid_schema() {
        let config = AvroEncoderConfig {
            schema_json: Some("not a valid schema".to_string()),
            ..AvroEncoderConfig::default()
        };
        let result = AvroStreamEncoder::try_new(config);
        assert!(
            result.is_err(),
            "Expected try_new to fail with invalid schema"
        );
    }

    #[test]
    fn update_config_should_rollback_on_invalid_schema() {
        let valid_schema = r#"{
            "type": "record",
            "name": "CurrentRecord",
            "fields": [{"name": "value", "type": "string"}]
        }"#
        .to_string();
        let mut encoder = AvroStreamEncoder::new(AvroEncoderConfig {
            schema_json: Some(valid_schema.clone()),
            ..AvroEncoderConfig::default()
        });

        assert!(encoder.schema.is_some());

        let old_config = encoder.config.clone();
        let old_resolved_names = encoder.resolved_names.clone();
        let invalid_config = AvroEncoderConfig {
            schema_json: Some("invalid schema".to_string()),
            ..AvroEncoderConfig::default()
        };

        let result = encoder.update_config(invalid_config);
        assert!(result.is_err(), "Expected update_config to fail");

        // After rollback, config and schema should remain unchanged
        assert_eq!(encoder.config.schema_json, old_config.schema_json);
        assert!(encoder.schema.is_some());
        assert_eq!(encoder.resolved_names, old_resolved_names);
    }

    #[test]
    fn update_config_should_refresh_resolved_name_cache() {
        let mut encoder = AvroStreamEncoder::new(AvroEncoderConfig {
            schema_json: Some(
                r#"{
                    "type": "record",
                    "name": "PreviousRecord",
                    "fields": [{"name": "value", "type": "string"}]
                }"#
                .to_string(),
            ),
            ..AvroEncoderConfig::default()
        });
        let previous_name = Name::new("PreviousRecord").expect("name should be valid");
        let current_name = Name::new("CurrentRecord").expect("name should be valid");

        encoder
            .update_config(AvroEncoderConfig {
                schema_json: Some(
                    r#"{
                        "type": "record",
                        "name": "CurrentRecord",
                        "fields": [{"name": "value", "type": "string"}]
                    }"#
                    .to_string(),
                ),
                ..AvroEncoderConfig::default()
            })
            .expect("valid schema should update the encoder");

        assert!(!encoder.resolved_names.contains_key(&previous_name));
        assert!(encoder.resolved_names.contains_key(&current_name));
    }

    #[test]
    fn update_config_without_schema_should_clear_resolved_name_cache() {
        let mut encoder = AvroStreamEncoder::new(AvroEncoderConfig {
            schema_json: Some(
                r#"{
                    "type": "record",
                    "name": "RecordToClear",
                    "fields": [{"name": "value", "type": "string"}]
                }"#
                .to_string(),
            ),
            ..AvroEncoderConfig::default()
        });

        encoder
            .update_config(AvroEncoderConfig::default())
            .expect("removing the schema should update the encoder");

        assert!(encoder.schema.is_none());
        assert!(encoder.resolved_names.is_empty());
    }
}
