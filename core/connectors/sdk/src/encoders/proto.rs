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

use crate::{Error, Payload, Schema, StreamEncoder};
use base64::{Engine as Base64Engine, engine::general_purpose};
use prost::Message;
use prost_types::Any;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{error, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtoEncoderConfig {
    pub schema_path: Option<PathBuf>,
    pub message_type: Option<String>,
    pub use_any_wrapper: bool,
    pub field_mappings: Option<HashMap<String, String>>,
    pub schema_registry_url: Option<String>,
    pub descriptor_set: Option<Vec<u8>>,
    pub include_paths: Vec<PathBuf>,
    pub preserve_unknown_fields: bool,
    pub format_options: ProtoFormatOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtoFormatOptions {
    pub compact_encoding: bool,
    pub validate_message: bool,
    pub type_url_prefix: String,
    pub deterministic_encoding: bool,
}

impl Default for ProtoEncoderConfig {
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
            format_options: ProtoFormatOptions::default(),
        }
    }
}

impl Default for ProtoFormatOptions {
    fn default() -> Self {
        Self {
            compact_encoding: true,
            validate_message: true,
            type_url_prefix: "type.googleapis.com".to_string(),
            deterministic_encoding: false,
        }
    }
}

pub struct ProtoStreamEncoder {
    config: ProtoEncoderConfig,
    message_descriptor: Option<prost_types::DescriptorProto>,
    file_descriptor_set: Option<prost_types::FileDescriptorSet>,
}

impl ProtoStreamEncoder {
    pub fn new() -> Self {
        Self::new_with_config(ProtoEncoderConfig::default())
    }

    pub fn new_with_config(config: ProtoEncoderConfig) -> Self {
        let mut encoder = Self {
            config,
            message_descriptor: None,
            file_descriptor_set: None,
        };

        if encoder.config.schema_path.is_some() || encoder.config.descriptor_set.is_some() {
            if let Err(e) = encoder.load_schema() {
                tracing::error!("Failed to load schema during encoder creation: {}", e);
            }
        }

        encoder
    }

    pub fn update_config(
        &mut self,
        config: ProtoEncoderConfig,
        reload_schema: bool,
    ) -> Result<(), Error> {
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
        use protox::file::GoogleFileResolver;
        use protox_parse::parse;
        use std::fs;

        info!(
            "Compiling protobuf schema for encoding from: {:?}",
            schema_path
        );

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
                    info!(
                        "Found message descriptor for encoding type: {}",
                        message_type
                    );
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
                    info!("Found message descriptor for encoding: {}", full_name);
                    return Ok(Some(message_desc.clone()));
                }

                if let Some(nested) = self.find_nested_message(message_desc, message_type, package)
                {
                    return Ok(Some(nested));
                }
            }
        }

        error!(
            "Message type '{}' not found in schema for encoding",
            message_type
        );
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

        for nested_message in &parent_message.nested_type {
            let nested_name = nested_message.name.as_deref().unwrap_or("");
            let full_name = if package.is_empty() {
                format!("{parent_name}.{nested_name}")
            } else {
                format!("{package}.{parent_name}.{nested_name}")
            };

            if full_name == target_type {
                info!(
                    "Found nested message descriptor for encoding: {}",
                    full_name
                );
                return Some(nested_message.clone());
            }

            if let Some(deeper) = self.find_nested_message(nested_message, target_type, package) {
                return Some(deeper);
            }
        }

        None
    }

    fn load_descriptor_set_internal(&mut self, descriptor_bytes: &[u8]) -> Result<(), Error> {
        use prost::Message;

        let file_descriptor_set = prost_types::FileDescriptorSet::decode(descriptor_bytes)
            .map_err(|_| Error::InvalidProtobufPayload)?;

        if let Some(message_type) = &self.config.message_type {
            self.message_descriptor =
                self.find_message_descriptor_by_name(&file_descriptor_set, message_type)?;
            if self.message_descriptor.is_some() {
                info!(
                    "Found message descriptor for encoding type: {}",
                    message_type
                );
            }
        }

        self.file_descriptor_set = Some(file_descriptor_set);

        Ok(())
    }

    fn apply_field_transformations(&self, payload: Payload) -> Result<Payload, Error> {
        if let Some(mappings) = &self.config.field_mappings {
            match payload {
                Payload::Json(json_value) => {
                    if let simd_json::OwnedValue::Object(mut map) = json_value {
                        let mut new_entries = Vec::new();

                        for (key, value) in map.iter() {
                            let proto_key = mappings
                                .iter()
                                .find(|(_, json_name)| *json_name == key)
                                .map(|(proto_name, _)| proto_name.clone())
                                .unwrap_or_else(|| key.clone());

                            new_entries.push((proto_key, value.clone()));
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

    fn encode_with_schema(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        if let (Some(message_descriptor), Some(file_descriptor_set)) =
            (&self.message_descriptor, &self.file_descriptor_set)
        {
            info!("Using schema-based protobuf encoding");

            match self.encode_with_message_descriptor(
                payload,
                message_descriptor,
                file_descriptor_set,
            ) {
                Ok(encoded_bytes) => {
                    info!("Successfully encoded message using schema");
                    Ok(encoded_bytes)
                }
                Err(e) => {
                    error!(
                        "Schema-based encoding failed: {}, falling back to Any wrapper",
                        e
                    );

                    Err(e)
                }
            }
        } else if self.config.use_any_wrapper {
            self.encode_with_any_wrapper(payload)
        } else {
            self.encode_as_raw_bytes(payload)
        }
    }

    fn encode_with_message_descriptor(
        &self,
        payload: Payload,
        message_descriptor: &prost_types::DescriptorProto,
        _file_descriptor_set: &prost_types::FileDescriptorSet,
    ) -> Result<Vec<u8>, Error> {
        let json_value = match payload {
            Payload::Json(json) => json,
            Payload::Text(text) => {
                match unsafe { simd_json::from_str::<simd_json::OwnedValue>(&mut text.clone()) } {
                    Ok(parsed) => parsed,
                    Err(_) => simd_json::json!({ "text": text }),
                }
            }
            Payload::Raw(data) => {
                match simd_json::from_slice::<simd_json::OwnedValue>(&mut data.clone()) {
                    Ok(parsed) => parsed,
                    Err(_) => simd_json::json!({
                        "data": general_purpose::STANDARD.encode(&data)
                    }),
                }
            }
            Payload::Proto(text) => simd_json::json!({ "proto_text": text }),
        };

        if let simd_json::OwnedValue::Object(json_map) = json_value {
            self.encode_json_to_protobuf(&json_map, message_descriptor)
        } else {
            let mut wrapped_map = simd_json::owned::Object::new();
            wrapped_map.insert("value".to_string(), json_value);
            self.encode_json_to_protobuf(&wrapped_map, message_descriptor)
        }
    }

    fn encode_json_to_protobuf(
        &self,
        json_map: &simd_json::owned::Object,
        message_descriptor: &prost_types::DescriptorProto,
    ) -> Result<Vec<u8>, Error> {
        let mut encoded_data = Vec::new();

        for field_desc in &message_descriptor.field {
            let field_name = field_desc.name.as_deref().unwrap_or("");
            let field_number = field_desc.number();

            if let Some(json_value) = json_map.get(field_name) {
                let field_bytes = self.encode_field_value(json_value, field_desc)?;

                let wire_type = self.get_wire_type_for_field(field_desc);
                let tag = ((field_number as u32) << 3) | (wire_type as u32);

                self.encode_varint(&mut encoded_data, tag as u64);

                encoded_data.extend_from_slice(&field_bytes);
            }
        }

        Ok(encoded_data)
    }

    fn encode_field_value(
        &self,
        json_value: &simd_json::OwnedValue,
        field_desc: &prost_types::FieldDescriptorProto,
    ) -> Result<Vec<u8>, Error> {
        use prost_types::field_descriptor_proto::Type;

        match field_desc.r#type() {
            Type::Bool => {
                let value = match json_value {
                    simd_json::OwnedValue::Static(simd_json::StaticNode::Bool(b)) => *b as u64,
                    simd_json::OwnedValue::String(s) => match s.as_str() {
                        "true" | "1" => 1,
                        _ => 0,
                    },
                    _ => 0,
                };
                let mut bytes = Vec::new();
                self.encode_varint(&mut bytes, value);
                Ok(bytes)
            }
            Type::Int32 | Type::Sint32 | Type::Sfixed32 => {
                let value = self.extract_i32_from_json(json_value)? as i64 as u64;
                let mut bytes = Vec::new();
                self.encode_varint(&mut bytes, value);
                Ok(bytes)
            }
            Type::Int64 | Type::Sint64 | Type::Sfixed64 => {
                let value = self.extract_i64_from_json(json_value)? as u64;
                let mut bytes = Vec::new();
                self.encode_varint(&mut bytes, value);
                Ok(bytes)
            }
            Type::Uint32 | Type::Fixed32 => {
                let value = self.extract_u32_from_json(json_value)? as u64;
                let mut bytes = Vec::new();
                self.encode_varint(&mut bytes, value);
                Ok(bytes)
            }
            Type::Uint64 | Type::Fixed64 => {
                let value = self.extract_u64_from_json(json_value)?;
                let mut bytes = Vec::new();
                self.encode_varint(&mut bytes, value);
                Ok(bytes)
            }
            Type::String => {
                let text = match json_value {
                    simd_json::OwnedValue::String(s) => s.as_str(),
                    _ => return Err(Error::InvalidJsonPayload),
                };
                let text_bytes = text.as_bytes();
                let mut result = Vec::new();

                self.encode_varint(&mut result, text_bytes.len() as u64);
                result.extend_from_slice(text_bytes);
                Ok(result)
            }
            Type::Bytes => {
                let bytes = match json_value {
                    simd_json::OwnedValue::String(s) => general_purpose::STANDARD
                        .decode(s.as_str())
                        .map_err(|_| Error::InvalidJsonPayload)?,
                    _ => return Err(Error::InvalidJsonPayload),
                };
                let mut result = Vec::new();

                self.encode_varint(&mut result, bytes.len() as u64);
                result.extend_from_slice(&bytes);
                Ok(result)
            }
            Type::Message => {
                let message_bytes = match json_value {
                    simd_json::OwnedValue::String(s) => general_purpose::STANDARD
                        .decode(s.as_str())
                        .map_err(|_| Error::InvalidJsonPayload)?,
                    simd_json::OwnedValue::Object(_) => {
                        return Err(Error::InvalidJsonPayload);
                    }
                    _ => return Err(Error::InvalidJsonPayload),
                };
                let mut result = Vec::new();

                self.encode_varint(&mut result, message_bytes.len() as u64);
                result.extend_from_slice(&message_bytes);
                Ok(result)
            }
            _ => {
                error!("Unsupported field type: {:?}", field_desc.r#type());
                Err(Error::InvalidJsonPayload)
            }
        }
    }

    fn get_wire_type_for_field(&self, field_desc: &prost_types::FieldDescriptorProto) -> u8 {
        use prost_types::field_descriptor_proto::Type;

        match field_desc.r#type() {
            Type::Bool
            | Type::Int32
            | Type::Sint32
            | Type::Int64
            | Type::Sint64
            | Type::Uint32
            | Type::Uint64 => 0,
            Type::Fixed64 | Type::Sfixed64 | Type::Double => 1,
            Type::String | Type::Bytes | Type::Message => 2,
            Type::Fixed32 | Type::Sfixed32 | Type::Float => 5,
            _ => 0,
        }
    }

    fn encode_varint(&self, buffer: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            buffer.push((value & 0x7F) as u8 | 0x80);
            value >>= 7;
        }
        buffer.push(value as u8);
    }

    fn extract_i32_from_json(&self, json_value: &simd_json::OwnedValue) -> Result<i32, Error> {
        match json_value {
            simd_json::OwnedValue::String(s) => {
                s.parse::<i32>().map_err(|_| Error::InvalidJsonPayload)
            }
            simd_json::OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(*f as i32),
            simd_json::OwnedValue::Static(simd_json::StaticNode::I64(i)) => Ok(*i as i32),
            simd_json::OwnedValue::Static(simd_json::StaticNode::U64(u)) => Ok(*u as i32),
            _ => Err(Error::InvalidJsonPayload),
        }
    }

    fn extract_i64_from_json(&self, json_value: &simd_json::OwnedValue) -> Result<i64, Error> {
        match json_value {
            simd_json::OwnedValue::String(s) => {
                s.parse::<i64>().map_err(|_| Error::InvalidJsonPayload)
            }
            simd_json::OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(*f as i64),
            simd_json::OwnedValue::Static(simd_json::StaticNode::I64(i)) => Ok(*i),
            simd_json::OwnedValue::Static(simd_json::StaticNode::U64(u)) => Ok(*u as i64),
            _ => Err(Error::InvalidJsonPayload),
        }
    }

    fn extract_u32_from_json(&self, json_value: &simd_json::OwnedValue) -> Result<u32, Error> {
        match json_value {
            simd_json::OwnedValue::String(s) => {
                s.parse::<u32>().map_err(|_| Error::InvalidJsonPayload)
            }
            simd_json::OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(*f as u32),
            simd_json::OwnedValue::Static(simd_json::StaticNode::I64(i)) => Ok(*i as u32),
            simd_json::OwnedValue::Static(simd_json::StaticNode::U64(u)) => Ok(*u as u32),
            _ => Err(Error::InvalidJsonPayload),
        }
    }

    fn extract_u64_from_json(&self, json_value: &simd_json::OwnedValue) -> Result<u64, Error> {
        match json_value {
            simd_json::OwnedValue::String(s) => {
                s.parse::<u64>().map_err(|_| Error::InvalidJsonPayload)
            }
            simd_json::OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(*f as u64),
            simd_json::OwnedValue::Static(simd_json::StaticNode::I64(i)) => Ok(*i as u64),
            simd_json::OwnedValue::Static(simd_json::StaticNode::U64(u)) => Ok(*u),
            _ => Err(Error::InvalidJsonPayload),
        }
    }

    fn encode_with_any_wrapper(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        let (type_url, value_bytes) = match payload {
            Payload::Json(json_value) => {
                let json_string =
                    simd_json::to_string(&json_value).map_err(|_| Error::InvalidJsonPayload)?;
                (
                    format!(
                        "{}/google.protobuf.StringValue",
                        self.config.format_options.type_url_prefix
                    ),
                    json_string.into_bytes(),
                )
            }
            Payload::Text(text) => {
                let json_value = simd_json::json!({
                    "text": text,
                    "timestamp": chrono::Utc::now().timestamp_millis(),
                    "encoding": "utf-8"
                });
                let json_string =
                    simd_json::to_string(&json_value).map_err(|_| Error::InvalidJsonPayload)?;
                (
                    format!(
                        "{}/google.protobuf.StringValue",
                        self.config.format_options.type_url_prefix
                    ),
                    json_string.into_bytes(),
                )
            }
            Payload::Raw(data) => (
                format!(
                    "{}/google.protobuf.BytesValue",
                    self.config.format_options.type_url_prefix
                ),
                data,
            ),
            Payload::Proto(text) => (
                format!(
                    "{}/google.protobuf.StringValue",
                    self.config.format_options.type_url_prefix
                ),
                text.into_bytes(),
            ),
        };

        let any = Any {
            type_url,
            value: value_bytes,
        };

        Ok(any.encode_to_vec())
    }

    fn encode_as_raw_bytes(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        match payload {
            Payload::Json(json_value) => {
                simd_json::to_vec(&json_value).map_err(|_| Error::InvalidJsonPayload)
            }
            Payload::Text(text) => Ok(text.into_bytes()),
            Payload::Raw(data) => Ok(data),
            Payload::Proto(text) => Ok(text.into_bytes()),
        }
    }

    pub fn convert_format(
        &self,
        payload: Payload,
        target_format: Schema,
    ) -> Result<Payload, Error> {
        match (payload, target_format) {
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

            (Payload::Proto(text), Schema::Text) => Ok(Payload::Text(text)),
            (Payload::Proto(text), Schema::Raw) => Ok(Payload::Raw(text.into_bytes())),
            (Payload::Proto(text), Schema::Json) => {
                let mut text_bytes = text.into_bytes();
                let json_value = simd_json::to_owned_value(&mut text_bytes)
                    .map_err(|_| Error::InvalidJsonPayload)?;
                Ok(Payload::Json(json_value))
            }

            (payload, _) => Ok(payload),
        }
    }
}

impl StreamEncoder for ProtoStreamEncoder {
    fn schema(&self) -> Schema {
        Schema::Proto
    }

    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        let transformed_payload = self.apply_field_transformations(payload)?;

        self.encode_with_schema(transformed_payload)
    }
}

impl Default for ProtoStreamEncoder {
    fn default() -> Self {
        Self::new()
    }
}
