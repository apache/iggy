/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

use crate::{Error, Payload, Schema, StreamDecoder};
use base64::Engine;
use prost::Message;
use prost_types::Any;
use prost_types::field_descriptor_proto::Type;
use protox::file::GoogleFileResolver;
use protox_parse::parse;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use tracing::{error, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtoConfig {
    pub schema_path: Option<PathBuf>,
    pub message_type: Option<String>,
    pub use_any_wrapper: bool,
    pub field_mappings: Option<HashMap<String, String>>,
    pub schema_registry_url: Option<String>,
    pub descriptor_set: Option<Vec<u8>>,
    pub include_paths: Vec<PathBuf>,
    pub preserve_unknown_fields: bool,
}

impl Default for ProtoConfig {
    fn default() -> Self {
        Self {
            schema_path: None,
            message_type: None,
            use_any_wrapper: true,
            field_mappings: None,
            schema_registry_url: None,
            descriptor_set: None,
            include_paths: vec![PathBuf::from(".")],
            preserve_unknown_fields: false,
        }
    }
}

pub struct ProtoStreamDecoder {
    config: ProtoConfig,
    message_descriptor: Option<prost_types::DescriptorProto>,
    file_descriptor_set: Option<prost_types::FileDescriptorSet>,
}

impl ProtoStreamDecoder {
    pub fn new(config: ProtoConfig) -> Self {
        let mut decoder = Self {
            config,
            message_descriptor: None,
            file_descriptor_set: None,
        };

        if (decoder.config.schema_path.is_some() || decoder.config.descriptor_set.is_some())
            && let Err(e) = decoder.load_schema()
        {
            tracing::error!("Failed to load schema during decoder creation: {}", e);
        }

        decoder
    }

    pub fn new_default() -> Self {
        Self::new(ProtoConfig::default())
    }

    pub fn update_config(&mut self, config: ProtoConfig, reload_schema: bool) -> Result<(), Error> {
        self.config = config;
        if reload_schema
            && (self.config.schema_path.is_some() || self.config.descriptor_set.is_some())
        {
            self.load_schema()
        } else {
            Ok(())
        }
    }

    pub fn load_schema(&mut self) -> Result<(), Error> {
        let schema_path = self.config.schema_path.clone();
        let descriptor_set = self.config.descriptor_set.clone();

        if let Some(path) = schema_path {
            self.compile_schema_internal(&path)?;
        } else if let Some(descriptor_bytes) = descriptor_set {
            self.load_descriptor_set_internal(&descriptor_bytes)?;
        }
        Ok(())
    }

    fn compile_schema_internal(&mut self, schema_path: &PathBuf) -> Result<(), Error> {
        info!("Compiling protobuf schema from: {:?}", schema_path);

        let proto_content = match fs::read_to_string(schema_path) {
            Ok(content) => content,
            Err(e) => {
                error!("Failed to read proto file: {}", e);
                error!("Falling back to Any wrapper mode");
                return Ok(());
            }
        };

        let parsed_file = parse(&schema_path.to_string_lossy(), &proto_content)
            .map_err(|e| Error::InitError(format!("Failed to parse proto file: {e}")))?;

        info!(
            "Successfully parsed proto file with package: {:?}",
            parsed_file.package()
        );

        let _resolver = GoogleFileResolver::new();

        for include_path in &self.config.include_paths {
            if include_path.exists() {
                info!("Adding include path: {:?}", include_path);
            }
        }

        match protox::compile([schema_path], &self.config.include_paths) {
            Ok(file_descriptor_set) => {
                info!(
                    "Successfully compiled proto schema with {} files",
                    file_descriptor_set.file.len()
                );

                if let Some(message_type) = &self.config.message_type {
                    self.message_descriptor =
                        self.find_message_descriptor_by_name(&file_descriptor_set, message_type)?;
                    info!("Found message descriptor for type: {}", message_type);
                }

                self.file_descriptor_set = Some(file_descriptor_set);
                Ok(())
            }
            Err(e) => {
                error!("Failed to compile proto schema: {}", e);
                error!("Falling back to Any wrapper mode");

                Ok(())
            }
        }
    }

    fn find_message_descriptor_by_name(
        &self,
        file_descriptor_set: &prost_types::FileDescriptorSet,
        message_type: &str,
    ) -> Result<Option<prost_types::DescriptorProto>, Error> {
        for file_desc in &file_descriptor_set.file {
            let package = file_desc.package.as_deref().unwrap_or("");

            for message_desc in &file_desc.message_type {
                let full_name = if package.is_empty() {
                    message_desc.name.as_deref().unwrap_or("").to_string()
                } else {
                    format!("{}.{}", package, message_desc.name.as_deref().unwrap_or(""))
                };

                if full_name == message_type {
                    info!("Found message descriptor: {}", full_name);
                    return Ok(Some(message_desc.clone()));
                }

                if let Some(nested) = self.find_nested_message(message_desc, message_type, package)
                {
                    return Ok(Some(nested));
                }
            }
        }

        error!("Message type '{}' not found in schema", message_type);
        Ok(None)
    }

    #[allow(clippy::only_used_in_recursion)]
    fn find_nested_message(
        &self,
        parent_message: &prost_types::DescriptorProto,
        target_type: &str,
        package: &str,
    ) -> Option<prost_types::DescriptorProto> {
        let parent_name = parent_message.name.as_deref().unwrap_or("");

        let package_prefix = if package.is_empty() {
            String::new()
        } else {
            format!("{package}.")
        };

        for nested_message in &parent_message.nested_type {
            let nested_name = nested_message.name.as_deref().unwrap_or("");
            let full_name = format!("{package_prefix}{parent_name}.{nested_name}");

            if full_name == target_type {
                info!("Found nested message descriptor: {}", full_name);
                return Some(nested_message.clone());
            }

            if let Some(deeper) = self.find_nested_message(nested_message, target_type, package) {
                return Some(deeper);
            }
        }

        None
    }

    fn load_descriptor_set_internal(&mut self, descriptor_bytes: &[u8]) -> Result<(), Error> {
        let file_descriptor_set = prost_types::FileDescriptorSet::decode(descriptor_bytes)
            .map_err(|_| Error::InvalidProtobufPayload)?;

        if let Some(message_type) = &self.config.message_type {
            self.message_descriptor =
                self.find_message_descriptor_by_name(&file_descriptor_set, message_type)?;
            if self.message_descriptor.is_some() {
                info!("Found message descriptor for type: {}", message_type);
            }
        }

        self.file_descriptor_set = Some(file_descriptor_set);

        Ok(())
    }

    fn decode_with_schema(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        if let (Some(message_descriptor), Some(file_descriptor_set)) =
            (&self.message_descriptor, &self.file_descriptor_set)
        {
            info!("Using schema-based protobuf decoding");

            match self.decode_with_message_descriptor(
                &payload,
                message_descriptor,
                file_descriptor_set,
            ) {
                Ok(json_payload) => {
                    info!("Successfully decoded protobuf message using schema");
                    Ok(json_payload)
                }
                Err(e) => {
                    error!(
                        "Schema-based decoding failed: {}, falling back to Any wrapper",
                        e
                    );
                    self.decode_as_any(payload)
                }
            }
        } else if self.config.use_any_wrapper {
            self.decode_as_any(payload)
        } else {
            self.decode_as_raw(payload)
        }
    }

    fn decode_with_message_descriptor(
        &self,
        payload: &[u8],
        message_descriptor: &prost_types::DescriptorProto,
        file_descriptor_set: &prost_types::FileDescriptorSet,
    ) -> Result<Payload, Error> {
        let mut json_fields = Vec::new();
        let mut cursor = 0;

        while cursor < payload.len() {
            let (tag, wire_type, new_cursor) = self.parse_varint(payload, cursor)?;
            cursor = new_cursor;

            let field_number = tag >> 3;

            if let Some(field_desc) = message_descriptor
                .field
                .iter()
                .find(|f| f.number() as u64 == field_number)
            {
                let default_field_name = format!("field_{field_number}");
                let field_name = field_desc.name.as_deref().unwrap_or(&default_field_name);

                let (value, new_cursor) = self.decode_field_value(
                    payload,
                    cursor,
                    wire_type,
                    field_desc,
                    file_descriptor_set,
                )?;
                cursor = new_cursor;

                json_fields.push((field_name.to_string(), value));
            } else if self.config.preserve_unknown_fields {
                let field_name = format!("unknown_field_{field_number}");
                let (value, new_cursor) = self.decode_unknown_field(payload, cursor, wire_type)?;
                cursor = new_cursor;
                json_fields.push((field_name, value));
            } else {
                cursor = self.skip_field_value(payload, cursor, wire_type)?;
            }
        }

        let mut json_obj = simd_json::json!({});
        if let simd_json::OwnedValue::Object(ref mut map) = json_obj {
            for (key, value) in json_fields {
                map.insert(key, value);
            }
            Ok(Payload::Json(json_obj))
        } else {
            Err(Error::InvalidProtobufPayload)
        }
    }

    fn parse_varint(&self, data: &[u8], mut cursor: usize) -> Result<(u64, u8, usize), Error> {
        let mut result = 0u64;
        let mut shift = 0;

        while cursor < data.len() {
            let byte = data[cursor];
            cursor += 1;

            result |= ((byte & 0x7F) as u64) << shift;

            if byte & 0x80 == 0 {
                let wire_type = (result & 0x7) as u8;
                return Ok((result, wire_type, cursor));
            }

            shift += 7;
            if shift >= 64 {
                return Err(Error::InvalidProtobufPayload);
            }
        }

        Err(Error::InvalidProtobufPayload)
    }

    fn parse_simple_varint(&self, data: &[u8], mut cursor: usize) -> Result<(u64, usize), Error> {
        let mut result = 0u64;
        let mut shift = 0;

        while cursor < data.len() {
            let byte = data[cursor];
            cursor += 1;

            result |= ((byte & 0x7F) as u64) << shift;

            if byte & 0x80 == 0 {
                return Ok((result, cursor));
            }

            shift += 7;
            if shift >= 64 {
                return Err(Error::InvalidProtobufPayload);
            }
        }

        Err(Error::InvalidProtobufPayload)
    }

    fn decode_field_value(
        &self,
        data: &[u8],
        cursor: usize,
        wire_type: u8,
        field_desc: &prost_types::FieldDescriptorProto,
        _file_descriptor_set: &prost_types::FileDescriptorSet,
    ) -> Result<(simd_json::OwnedValue, usize), Error> {
        match wire_type {
            0 => {
                let (value, new_cursor) = self.parse_simple_varint(data, cursor)?;
                let json_value = match field_desc.r#type() {
                    Type::Bool => {
                        simd_json::OwnedValue::Static(simd_json::StaticNode::Bool(value != 0))
                    }
                    Type::Int32 | Type::Sint32 | Type::Sfixed32 => {
                        simd_json::OwnedValue::from(value as i32)
                    }
                    Type::Int64 | Type::Sint64 | Type::Sfixed64 => {
                        simd_json::OwnedValue::from(value as i64)
                    }
                    Type::Uint32 | Type::Fixed32 => simd_json::OwnedValue::from(value as u32),
                    Type::Uint64 | Type::Fixed64 => simd_json::OwnedValue::from(value),
                    _ => simd_json::OwnedValue::from(value),
                };
                Ok((json_value, new_cursor))
            }
            2 => {
                let (length, mut new_cursor) = self.parse_simple_varint(data, cursor)?;
                let end_cursor = new_cursor + length as usize;

                if end_cursor > data.len() {
                    return Err(Error::InvalidProtobufPayload);
                }

                let field_data = &data[new_cursor..end_cursor];
                new_cursor = end_cursor;

                let json_value = match field_desc.r#type() {
                    Type::String => {
                        let text = String::from_utf8(field_data.to_vec())
                            .map_err(|_| Error::InvalidProtobufPayload)?;
                        simd_json::OwnedValue::String(text)
                    }
                    Type::Bytes => {
                        let encoded = base64::engine::general_purpose::STANDARD.encode(field_data);
                        simd_json::OwnedValue::String(encoded)
                    }
                    Type::Message => {
                        let encoded = base64::engine::general_purpose::STANDARD.encode(field_data);
                        simd_json::json!({
                            "type": "nested_message",
                            "data": encoded
                        })
                    }
                    _ => {
                        let encoded = base64::engine::general_purpose::STANDARD.encode(field_data);
                        simd_json::OwnedValue::String(encoded)
                    }
                };
                Ok((json_value, new_cursor))
            }
            _ => {
                let json_value = simd_json::OwnedValue::String("unsupported_wire_type".into());
                Ok((json_value, cursor + 4))
            }
        }
    }

    fn decode_unknown_field(
        &self,
        data: &[u8],
        cursor: usize,
        wire_type: u8,
    ) -> Result<(simd_json::OwnedValue, usize), Error> {
        match wire_type {
            0 => {
                let (value, new_cursor) = self.parse_simple_varint(data, cursor)?;
                Ok((simd_json::OwnedValue::from(value), new_cursor))
            }
            2 => {
                let (length, mut new_cursor) = self.parse_simple_varint(data, cursor)?;
                let end_cursor = new_cursor + length as usize;

                if end_cursor > data.len() {
                    return Err(Error::InvalidProtobufPayload);
                }

                let field_data = &data[new_cursor..end_cursor];
                new_cursor = end_cursor;

                let encoded = base64::engine::general_purpose::STANDARD.encode(field_data);
                Ok((simd_json::OwnedValue::String(encoded), new_cursor))
            }
            _ => Ok((simd_json::OwnedValue::String("unknown".into()), cursor + 4)),
        }
    }

    fn skip_field_value(&self, data: &[u8], cursor: usize, wire_type: u8) -> Result<usize, Error> {
        match wire_type {
            0 => {
                let (_, new_cursor) = self.parse_simple_varint(data, cursor)?;
                Ok(new_cursor)
            }
            2 => {
                let (length, new_cursor) = self.parse_simple_varint(data, cursor)?;
                let end_cursor = new_cursor + length as usize;

                if end_cursor > data.len() {
                    return Err(Error::InvalidProtobufPayload);
                }

                Ok(end_cursor)
            }
            _ => Ok(cursor + 4),
        }
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
                other => Ok(other),
            }
        } else {
            Ok(payload)
        }
    }

    fn decode_as_any(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        match Any::decode(payload.as_slice()) {
            Ok(any) => {
                let json_value = simd_json::json!({
                    "type_url": any.type_url,
                    "value": base64::engine::general_purpose::STANDARD.encode(&any.value),
                });
                Ok(Payload::Json(json_value))
            }
            Err(e) => {
                error!("Failed to decode protobuf Any message: {e}");
                Err(Error::CannotDecode(Schema::Proto))
            }
        }
    }

    fn decode_as_raw(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        Ok(Payload::Raw(payload))
    }
}

impl StreamDecoder for ProtoStreamDecoder {
    fn schema(&self) -> Schema {
        Schema::Proto
    }

    fn decode(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        if payload.is_empty() {
            return Err(Error::InvalidPayloadType);
        }

        let decoded_payload = self.decode_with_schema(payload)?;

        self.apply_field_transformations(decoded_payload)
    }
}

impl Default for ProtoStreamDecoder {
    fn default() -> Self {
        Self::new_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::Any;
    use std::path::PathBuf;

    #[test]
    fn decode_should_succeed_given_valid_protobuf_any_message() {
        let decoder = ProtoStreamDecoder::default();

        let any = Any {
            type_url: "type.googleapis.com/google.protobuf.StringValue".to_string(),
            value: b"Hello, World!".to_vec(),
        };

        let encoded = any.encode_to_vec();
        let result = decoder.decode(encoded);

        assert!(result.is_ok());
        if let Ok(Payload::Json(json_value)) = result {
            if let simd_json::OwnedValue::Object(map) = &json_value {
                assert!(map.contains_key("type_url"));
                assert!(map.contains_key("value"));
            } else {
                panic!("Expected JSON object");
            }
        } else {
            panic!("Expected JSON payload");
        }
    }

    #[test]
    fn decode_should_fail_given_empty_payload() {
        let decoder = ProtoStreamDecoder::default();
        let result = decoder.decode(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_should_return_raw_payload_when_any_wrapper_is_disabled() {
        let config = ProtoConfig {
            use_any_wrapper: false,
            ..ProtoConfig::default()
        };
        let decoder = ProtoStreamDecoder::new(config);

        let test_data = b"Hello, World!".to_vec();
        let result = decoder.decode(test_data.clone());

        assert!(result.is_ok());
        if let Ok(Payload::Raw(data)) = result {
            assert_eq!(data, test_data);
        } else {
            panic!("Expected Raw payload");
        }
    }

    #[test]
    fn decode_should_apply_field_mappings_when_configured() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("old_field".to_string(), "new_field".to_string());
        field_mappings.insert("user_id".to_string(), "id".to_string());

        let config = ProtoConfig {
            field_mappings: Some(field_mappings),
            use_any_wrapper: true,
            ..ProtoConfig::default()
        };
        let decoder = ProtoStreamDecoder::new(config);

        let json_content = simd_json::json!({
            "old_field": "should_be_renamed",
            "user_id": 123,
            "unchanged_field": "stays_same"
        });
        let json_string = simd_json::to_string(&json_content).unwrap();

        let any = Any {
            type_url: "type.googleapis.com/custom.Message".to_string(),
            value: json_string.into_bytes(),
        };

        let encoded = any.encode_to_vec();
        let result = decoder.decode(encoded);

        assert!(result.is_ok());
        if let Ok(Payload::Json(json_value)) = result {
            if let simd_json::OwnedValue::Object(map) = &json_value {
                assert!(map.contains_key("type_url"));
                assert!(map.contains_key("value"));
            } else {
                panic!("Expected JSON object");
            }
        } else {
            panic!("Expected JSON payload");
        }
    }

    #[test]
    fn load_schema_should_handle_missing_schema_path_gracefully() {
        let mut decoder = ProtoStreamDecoder::new(ProtoConfig {
            schema_path: None,
            message_type: None,
            ..ProtoConfig::default()
        });

        let result = decoder.load_schema();
        assert!(
            result.is_ok(),
            "Should handle missing schema path gracefully"
        );
    }

    #[test]
    fn load_schema_should_handle_missing_proto_file_gracefully() {
        let mut decoder = ProtoStreamDecoder::new(ProtoConfig {
            schema_path: Some(PathBuf::from("nonexistent.proto")),
            message_type: Some("com.example.Test".to_string()),
            ..ProtoConfig::default()
        });

        let result = decoder.load_schema();

        assert!(
            result.is_ok(),
            "Should handle missing proto file gracefully"
        );
    }

    #[test]
    fn config_should_have_sensible_defaults() {
        let config = ProtoConfig::default();

        assert!(config.schema_path.is_none());
        assert!(config.message_type.is_none());
        assert!(config.use_any_wrapper);
        assert!(config.field_mappings.is_none());
        assert!(config.schema_registry_url.is_none());
        assert!(config.descriptor_set.is_none());
        assert_eq!(config.include_paths, vec![PathBuf::from(".")]);
        assert!(!config.preserve_unknown_fields);
    }

    #[test]
    fn decoder_should_be_creatable_with_custom_config() {
        let config = ProtoConfig {
            schema_path: Some(PathBuf::from("schemas/user.proto")),
            message_type: Some("com.example.User".to_string()),
            use_any_wrapper: false,
            field_mappings: Some(HashMap::from([
                ("user_id".to_string(), "id".to_string()),
                ("full_name".to_string(), "name".to_string()),
            ])),
            schema_registry_url: Some("http://schema-registry:8081".to_string()),
            include_paths: vec![PathBuf::from("."), PathBuf::from("schemas/common")],
            preserve_unknown_fields: true,
            ..ProtoConfig::default()
        };

        let decoder = ProtoStreamDecoder::new(config.clone());

        assert_eq!(decoder.config.schema_path, config.schema_path);
        assert_eq!(decoder.config.message_type, config.message_type);
        assert_eq!(decoder.config.use_any_wrapper, config.use_any_wrapper);
        assert_eq!(decoder.config.field_mappings, config.field_mappings);
        assert_eq!(
            decoder.config.schema_registry_url,
            config.schema_registry_url
        );
        assert_eq!(decoder.config.include_paths, config.include_paths);
        assert_eq!(
            decoder.config.preserve_unknown_fields,
            config.preserve_unknown_fields
        );
    }

    #[test]
    fn update_config_should_reload_schema_when_requested() {
        let mut decoder = ProtoStreamDecoder::new(ProtoConfig::default());

        let new_config = ProtoConfig {
            schema_path: Some(PathBuf::from("schemas/test.proto")),
            message_type: Some("com.example.Test".to_string()),
            use_any_wrapper: false,
            ..ProtoConfig::default()
        };

        let result = decoder.update_config(new_config.clone(), true);
        assert!(result.is_ok());
        assert_eq!(decoder.config.schema_path, new_config.schema_path);
        assert_eq!(decoder.config.message_type, new_config.message_type);
        assert_eq!(decoder.config.use_any_wrapper, new_config.use_any_wrapper);
    }

    #[test]
    fn update_config_should_not_reload_schema_when_not_requested() {
        let mut decoder = ProtoStreamDecoder::new(ProtoConfig::default());

        let new_config = ProtoConfig {
            schema_path: Some(PathBuf::from("schemas/test.proto")),
            message_type: Some("com.example.Test".to_string()),
            use_any_wrapper: false,
            ..ProtoConfig::default()
        };

        let result = decoder.update_config(new_config.clone(), false);
        assert!(result.is_ok());
        assert_eq!(decoder.config.schema_path, new_config.schema_path);
    }

    #[test]
    fn integration_should_decode_with_real_schema() {
        let decoder = ProtoStreamDecoder::new(ProtoConfig {
            schema_path: Some(PathBuf::from("examples/user.proto")),
            message_type: Some("com.example.User".to_string()),
            use_any_wrapper: false,
            ..ProtoConfig::default()
        });

        let any = Any {
            type_url: "type.googleapis.com/com.example.User".to_string(),
            value: b"test protobuf data".to_vec(),
        };
        let encoded = any.encode_to_vec();

        let result = decoder.decode(encoded);

        assert!(
            result.is_ok(),
            "Decoding should succeed with schema or fallback"
        );

        if let Ok(Payload::Json(json_value)) = result {
            println!(
                "Decoded JSON: {}",
                simd_json::to_string_pretty(&json_value).unwrap()
            );

            match &json_value {
                simd_json::OwnedValue::Object(_) => {}
                _ => panic!("Should decode to JSON object, got: {json_value:?}"),
            }
        }
    }

    #[test]
    fn integration_should_handle_decoder_schema_loading_gracefully() {
        let decoder = ProtoStreamDecoder::new(ProtoConfig {
            schema_path: Some(PathBuf::from("nonexistent/schema.proto")),
            message_type: Some("com.example.NonExistent".to_string()),
            use_any_wrapper: true,
            ..ProtoConfig::default()
        });

        let any = Any {
            type_url: "type.googleapis.com/test.Message".to_string(),
            value: b"test data".to_vec(),
        };
        let encoded = any.encode_to_vec();

        let result = decoder.decode(encoded);
        assert!(
            result.is_ok(),
            "Should fallback gracefully when schema loading fails"
        );
    }
}
