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

use base64::Engine;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{error, info};

use super::{Transform, TransformType};
use crate::{DecodedMessage, Error, Payload, Schema, TopicMetadata};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtoConvertConfig {
    pub source_format: Schema,
    pub target_format: Schema,
    pub schema_path: Option<PathBuf>,
    pub message_type: Option<String>,
    pub field_mappings: Option<HashMap<String, String>>,
    pub schema_registry_url: Option<String>,
    pub descriptor_set: Option<Vec<u8>>,
    pub include_paths: Vec<PathBuf>,
    pub preserve_unknown_fields: bool,
    pub conversion_options: ConversionOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionOptions {
    pub validate_messages: bool,
    pub pretty_json: bool,
    pub include_metadata: bool,
    pub type_url_prefix: String,
    pub strict_mode: bool,
}

impl Default for ProtoConvertConfig {
    fn default() -> Self {
        Self {
            source_format: Schema::Proto,
            target_format: Schema::Json,
            schema_path: None,
            message_type: None,
            field_mappings: None,
            schema_registry_url: None,
            descriptor_set: None,
            include_paths: vec![PathBuf::from(".")],
            preserve_unknown_fields: false,
            conversion_options: ConversionOptions::default(),
        }
    }
}

impl Default for ConversionOptions {
    fn default() -> Self {
        Self {
            validate_messages: true,
            pretty_json: false,
            include_metadata: false,
            type_url_prefix: "type.googleapis.com".to_string(),
            strict_mode: false,
        }
    }
}

pub struct ProtoConvert {
    config: ProtoConvertConfig,
    message_descriptor: Option<prost_types::DescriptorProto>,
    file_descriptor_set: Option<prost_types::FileDescriptorSet>,
}

impl ProtoConvert {
    pub fn new(config: ProtoConvertConfig) -> Self {
        let mut converter = Self {
            config,
            message_descriptor: None,
            file_descriptor_set: None,
        };

        if converter.config.schema_path.is_some() || converter.config.descriptor_set.is_some() {
            if let Err(e) = converter.load_schema() {
                tracing::error!("Failed to load schema during converter creation: {}", e);
            }
        }

        converter
    }

    pub fn new_default() -> Self {
        Self::new(ProtoConvertConfig::default())
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
            "Compiling protobuf schema for conversion from: {:?}",
            schema_path
        );

        let proto_content = match fs::read_to_string(schema_path) {
            Ok(content) => content,
            Err(e) => {
                error!("Failed to read proto file: {}", e);
                error!("Falling back to basic conversion methods");
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
                        "Found message descriptor for conversion type: {}",
                        message_type
                    );
                }

                self.file_descriptor_set = Some(file_descriptor_set);
                Ok(())
            }
            Err(e) => {
                error!("Failed to compile proto schema: {}", e);
                error!("Falling back to basic conversion methods");

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
                    info!("Found message descriptor for conversion: {}", full_name);
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
                info!(
                    "Found nested message descriptor for conversion: {}",
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
                    "Found message descriptor for conversion type: {}",
                    message_type
                );
            }
        }

        self.file_descriptor_set = Some(file_descriptor_set);

        Ok(())
    }

    fn encode_json_with_schema(
        &self,
        json_value: &simd_json::OwnedValue,
        message_descriptor: &prost_types::DescriptorProto,
        _file_descriptor_set: &prost_types::FileDescriptorSet,
    ) -> Result<Vec<u8>, Error> {
        if let simd_json::OwnedValue::Object(json_map) = json_value {
            let mut buffer = Vec::new();

            for field_desc in &message_descriptor.field {
                let field_name = field_desc.name.as_deref().unwrap_or("");

                if let Some(json_field_value) = json_map.get(field_name) {
                    let field_number = field_desc.number() as u64;
                    let wire_type = self.get_wire_type_for_conversion_field(field_desc);
                    let tag = (field_number << 3) | (wire_type as u64);

                    self.encode_varint_for_conversion(&mut buffer, tag);

                    match self.encode_field_value_for_conversion(json_field_value, field_desc) {
                        Ok(field_data) => buffer.extend_from_slice(&field_data),
                        Err(e) => {
                            error!("Failed to encode field {}: {}", field_name, e);
                            continue;
                        }
                    }
                }
            }

            Ok(buffer)
        } else {
            Err(Error::InvalidJsonPayload)
        }
    }

    fn get_wire_type_for_conversion_field(
        &self,
        field_desc: &prost_types::FieldDescriptorProto,
    ) -> u8 {
        use prost_types::field_descriptor_proto::Type;

        match field_desc.r#type() {
            Type::Bool
            | Type::Int32
            | Type::Sint32
            | Type::Int64
            | Type::Sint64
            | Type::Uint32
            | Type::Uint64
            | Type::Enum => 0,
            Type::String | Type::Bytes | Type::Message | Type::Group => 2,
            Type::Fixed32 | Type::Sfixed32 | Type::Float => 5,
            Type::Fixed64 | Type::Sfixed64 | Type::Double => 1,
        }
    }

    fn encode_field_value_for_conversion(
        &self,
        json_value: &simd_json::OwnedValue,
        field_desc: &prost_types::FieldDescriptorProto,
    ) -> Result<Vec<u8>, Error> {
        use prost_types::field_descriptor_proto::Type;

        match field_desc.r#type() {
            Type::String => {
                if let simd_json::OwnedValue::String(s) = json_value {
                    let bytes = s.as_bytes();
                    let mut result = Vec::new();
                    self.encode_varint_for_conversion(&mut result, bytes.len() as u64);
                    result.extend_from_slice(bytes);
                    Ok(result)
                } else {
                    Err(Error::InvalidJsonPayload)
                }
            }
            Type::Int32 | Type::Sint32 => {
                let value = self.extract_i32_from_json_for_conversion(json_value)?;
                let mut result = Vec::new();
                self.encode_varint_for_conversion(&mut result, value as u64);
                Ok(result)
            }
            Type::Int64 | Type::Sint64 => {
                let value = self.extract_i64_from_json_for_conversion(json_value)?;
                let mut result = Vec::new();
                self.encode_varint_for_conversion(&mut result, value as u64);
                Ok(result)
            }
            Type::Bool => {
                let value = if let simd_json::OwnedValue::Static(simd_json::StaticNode::Bool(b)) =
                    json_value
                {
                    *b as u64
                } else {
                    return Err(Error::InvalidJsonPayload);
                };
                let mut result = Vec::new();
                self.encode_varint_for_conversion(&mut result, value);
                Ok(result)
            }
            Type::Enum => {
                let value = self.extract_i32_from_json_for_conversion(json_value)?;
                let mut result = Vec::new();
                self.encode_varint_for_conversion(&mut result, value as u64);
                Ok(result)
            }
            Type::Group => {
                let json_string =
                    simd_json::to_string(json_value).map_err(|_| Error::InvalidJsonPayload)?;
                let bytes = json_string.as_bytes();
                let mut result = Vec::new();
                self.encode_varint_for_conversion(&mut result, bytes.len() as u64);
                result.extend_from_slice(bytes);
                Ok(result)
            }
            _ => {
                let json_string =
                    simd_json::to_string(json_value).map_err(|_| Error::InvalidJsonPayload)?;
                let bytes = json_string.as_bytes();
                let mut result = Vec::new();
                self.encode_varint_for_conversion(&mut result, bytes.len() as u64);
                result.extend_from_slice(bytes);
                Ok(result)
            }
        }
    }

    fn encode_varint_for_conversion(&self, buffer: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            buffer.push((value & 0x7F) as u8 | 0x80);
            value >>= 7;
        }
        buffer.push(value as u8);
    }

    fn extract_i32_from_json_for_conversion(
        &self,
        json_value: &simd_json::OwnedValue,
    ) -> Result<i32, Error> {
        match json_value {
            simd_json::OwnedValue::Static(simd_json::StaticNode::I64(i)) => Ok(*i as i32),
            simd_json::OwnedValue::Static(simd_json::StaticNode::U64(u)) => Ok(*u as i32),
            simd_json::OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(*f as i32),
            _ => Err(Error::InvalidJsonPayload),
        }
    }

    fn extract_i64_from_json_for_conversion(
        &self,
        json_value: &simd_json::OwnedValue,
    ) -> Result<i64, Error> {
        match json_value {
            simd_json::OwnedValue::Static(simd_json::StaticNode::I64(i)) => Ok(*i),
            simd_json::OwnedValue::Static(simd_json::StaticNode::U64(u)) => Ok(*u as i64),
            simd_json::OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(*f as i64),
            _ => Err(Error::InvalidJsonPayload),
        }
    }

    fn apply_field_transformations(&self, payload: Payload) -> Result<Payload, Error> {
        if let Some(mappings) = &self.config.field_mappings {
            match payload {
                Payload::Json(json_value) => {
                    if let simd_json::OwnedValue::Object(mut map) = json_value {
                        let mut new_entries = Vec::new();

                        for (key, value) in map.iter() {
                            let new_key = mappings.get(key).cloned().unwrap_or_else(|| key.clone());
                            new_entries.push((new_key, value.clone()));
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

    fn convert_from_protobuf(&self, payload: Payload) -> Result<Payload, Error> {
        match self.config.target_format {
            Schema::Json => self.protobuf_to_json(payload),
            Schema::Text => self.protobuf_to_text(payload),
            Schema::Raw => self.protobuf_to_raw(payload),
            Schema::Proto => Ok(payload),
        }
    }

    fn convert_to_protobuf(&self, payload: Payload) -> Result<Payload, Error> {
        match self.config.source_format {
            Schema::Json => self.json_to_protobuf(payload),
            Schema::Text => self.text_to_protobuf(payload),
            Schema::Raw => self.raw_to_protobuf(payload),
            Schema::Proto => Ok(payload),
        }
    }

    fn protobuf_to_json(&self, payload: Payload) -> Result<Payload, Error> {
        match payload {
            Payload::Proto(proto_text) => {
                let mut proto_bytes = proto_text.clone().into_bytes();
                match simd_json::to_owned_value(&mut proto_bytes) {
                    Ok(json_value) => {
                        if self.config.conversion_options.include_metadata {
                            let enriched_json = simd_json::json!({
                                "data": json_value,
                                "metadata": {
                                    "converted_from": "protobuf",
                                    "timestamp": chrono::Utc::now().timestamp_millis(),
                                    "message_type": self.config.message_type.clone().unwrap_or_else(|| "unknown".to_string())
                                }
                            });
                            Ok(Payload::Json(enriched_json))
                        } else {
                            Ok(Payload::Json(json_value))
                        }
                    }
                    Err(_) => {
                        let json_value = simd_json::json!({
                            "proto_text": proto_text,
                            "format": "text"
                        });
                        Ok(Payload::Json(json_value))
                    }
                }
            }
            Payload::Raw(data) => {
                if let Ok(any) = prost_types::Any::decode(data.as_slice()) {
                    let json_value = if self.config.conversion_options.include_metadata {
                        simd_json::json!({
                            "type_url": any.type_url,
                            "value": base64::engine::general_purpose::STANDARD.encode(&any.value),
                            "metadata": {
                                "converted_from": "protobuf_any",
                                "timestamp": chrono::Utc::now().timestamp_millis()
                            }
                        })
                    } else {
                        simd_json::json!({
                            "type_url": any.type_url,
                            "value": base64::engine::general_purpose::STANDARD.encode(&any.value)
                        })
                    };
                    Ok(Payload::Json(json_value))
                } else {
                    let json_value = simd_json::json!({
                        "binary_data": base64::engine::general_purpose::STANDARD.encode(&data),
                        "format": "raw_binary"
                    });
                    Ok(Payload::Json(json_value))
                }
            }
            _other => Err(Error::InvalidPayloadType),
        }
    }

    fn json_to_protobuf(&self, payload: Payload) -> Result<Payload, Error> {
        match payload {
            Payload::Json(json_value) => {
                let json_string = if self.config.conversion_options.pretty_json {
                    simd_json::to_string_pretty(&json_value)
                } else {
                    simd_json::to_string(&json_value)
                }
                .map_err(|_| Error::InvalidJsonPayload)?;

                if let (Some(message_descriptor), Some(file_descriptor_set)) =
                    (&self.message_descriptor, &self.file_descriptor_set)
                {
                    info!("Using schema-based JSON to protobuf conversion");
                    match self.encode_json_with_schema(
                        &json_value,
                        message_descriptor,
                        file_descriptor_set,
                    ) {
                        Ok(binary_data) => Ok(Payload::Raw(binary_data)),
                        Err(e) => {
                            error!(
                                "Schema-based conversion failed: {}, falling back to proto text",
                                e
                            );
                            Ok(Payload::Proto(json_string))
                        }
                    }
                } else {
                    Ok(Payload::Proto(json_string))
                }
            }
            _other => Err(Error::InvalidPayloadType),
        }
    }

    fn protobuf_to_text(&self, payload: Payload) -> Result<Payload, Error> {
        match payload {
            Payload::Proto(proto_text) => Ok(Payload::Text(proto_text)),
            Payload::Raw(data) => {
                if let Ok(any) = prost_types::Any::decode(data.as_slice()) {
                    let text = format!(
                        "Type: {}\nData: {} bytes\nBase64: {}",
                        any.type_url,
                        any.value.len(),
                        base64::engine::general_purpose::STANDARD.encode(&any.value)
                    );
                    Ok(Payload::Text(text))
                } else {
                    let text = format!(
                        "Binary protobuf data: {} bytes\nBase64: {}",
                        data.len(),
                        base64::engine::general_purpose::STANDARD.encode(&data)
                    );
                    Ok(Payload::Text(text))
                }
            }
            _other => Err(Error::InvalidPayloadType),
        }
    }

    fn text_to_protobuf(&self, payload: Payload) -> Result<Payload, Error> {
        match payload {
            Payload::Text(text) => {
                let mut text_bytes = text.clone().into_bytes();
                if let Ok(json_value) = simd_json::to_owned_value(&mut text_bytes) {
                    self.json_to_protobuf(Payload::Json(json_value))
                } else {
                    Ok(Payload::Proto(text))
                }
            }
            _other => Err(Error::InvalidPayloadType),
        }
    }

    fn protobuf_to_raw(&self, payload: Payload) -> Result<Payload, Error> {
        match payload {
            Payload::Proto(proto_text) => Ok(Payload::Raw(proto_text.into_bytes())),
            Payload::Raw(data) => Ok(Payload::Raw(data)),
            _other => Err(Error::InvalidPayloadType),
        }
    }

    fn raw_to_protobuf(&self, payload: Payload) -> Result<Payload, Error> {
        match payload {
            Payload::Raw(data) => {
                if let Ok(text) = String::from_utf8(data.clone()) {
                    Ok(Payload::Proto(text))
                } else {
                    let base64_text = base64::engine::general_purpose::STANDARD.encode(&data);
                    Ok(Payload::Proto(format!("binary_data: \"{base64_text}\"")))
                }
            }
            _other => Err(Error::InvalidPayloadType),
        }
    }
}

impl Transform for ProtoConvert {
    fn r#type(&self) -> TransformType {
        TransformType::ProtoConvert
    }

    fn transform(
        &self,
        _metadata: &TopicMetadata,
        mut message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        let transformed_payload = self.apply_field_transformations(message.payload)?;

        let converted_payload = match (self.config.source_format, self.config.target_format) {
            (Schema::Proto, _) => self.convert_from_protobuf(transformed_payload)?,

            (_, Schema::Proto) => self.convert_to_protobuf(transformed_payload)?,

            (Schema::Json, Schema::Text) => {
                if let Payload::Json(json_value) = transformed_payload {
                    let text = if self.config.conversion_options.pretty_json {
                        simd_json::to_string_pretty(&json_value)
                    } else {
                        simd_json::to_string(&json_value)
                    }
                    .map_err(|_| Error::InvalidJsonPayload)?;
                    Payload::Text(text)
                } else {
                    return Err(Error::InvalidPayloadType);
                }
            }
            (Schema::Text, Schema::Json) => {
                if let Payload::Text(text) = transformed_payload {
                    let mut text_bytes = text.into_bytes();
                    let json_value = simd_json::to_owned_value(&mut text_bytes)
                        .map_err(|_| Error::InvalidJsonPayload)?;
                    Payload::Json(json_value)
                } else {
                    return Err(Error::InvalidPayloadType);
                }
            }
            (Schema::Json, Schema::Raw) => {
                if let Payload::Json(json_value) = transformed_payload {
                    let bytes =
                        simd_json::to_vec(&json_value).map_err(|_| Error::InvalidJsonPayload)?;
                    Payload::Raw(bytes)
                } else {
                    return Err(Error::InvalidPayloadType);
                }
            }
            (Schema::Raw, Schema::Json) => {
                if let Payload::Raw(mut data) = transformed_payload {
                    let json_value = simd_json::to_owned_value(&mut data)
                        .map_err(|_| Error::InvalidJsonPayload)?;
                    Payload::Json(json_value)
                } else {
                    return Err(Error::InvalidPayloadType);
                }
            }

            (source, target) if source == target => transformed_payload,

            _ => return Err(Error::InvalidPayloadType),
        };

        message.payload = converted_payload;
        Ok(Some(message))
    }
}

impl Default for ProtoConvert {
    fn default() -> Self {
        Self::new_default()
    }
}
