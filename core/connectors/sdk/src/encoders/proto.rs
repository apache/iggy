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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use base64::{Engine as Base64Engine, engine::general_purpose};
use bson::Bson;
use iggy_common::IggyTimestamp;
use prost::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor,
    Value as ProtoValue,
};
use prost_types::Any;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::{Error, Payload, Schema, StreamEncoder};

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
    dynamic_message_descriptor: Option<MessageDescriptor>,
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
            dynamic_message_descriptor: None,
            file_descriptor_set: None,
        };

        if (encoder.config.schema_path.is_some() || encoder.config.descriptor_set.is_some())
            && let Err(e) = encoder.load_schema()
        {
            tracing::error!("Failed to load schema during encoder creation: {}", e);
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
                self.cache_descriptors(file_descriptor_set)?;
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

        self.cache_descriptors(file_descriptor_set)
    }

    fn cache_descriptors(
        &mut self,
        file_descriptor_set: prost_types::FileDescriptorSet,
    ) -> Result<(), Error> {
        let descriptor_pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set.clone())
            .map_err(|error| {
                Error::InvalidConfigValue(format!(
                    "Failed to build Protobuf descriptor pool: {error}"
                ))
            })?;
        let message_type = self.config.message_type.clone();

        if let Some(message_type) = message_type {
            self.message_descriptor =
                self.find_message_descriptor_by_name(&file_descriptor_set, &message_type)?;
            self.dynamic_message_descriptor = descriptor_pool.get_message_by_name(&message_type);
            if self.message_descriptor.is_some() {
                info!(
                    "Found message descriptor for encoding type: {}",
                    message_type
                );
            }
        } else {
            self.message_descriptor = None;
            self.dynamic_message_descriptor = None;
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

    fn normalize_bson_payload(payload: Payload) -> Result<Payload, Error> {
        let Payload::Bson(document) = payload else {
            return Ok(payload);
        };

        let json_value = Bson::Document(document).into_relaxed_extjson();
        let mut json_bytes = serde_json::to_vec(&json_value).map_err(|error| {
            error!("Failed to serialize BSON document as JSON: {error}");
            Error::InvalidBsonPayload
        })?;
        let json_value = simd_json::to_owned_value(&mut json_bytes).map_err(|error| {
            error!("Failed to normalize BSON document as JSON: {error}");
            Error::InvalidBsonPayload
        })?;

        Ok(Payload::Json(json_value))
    }

    fn encode_bson_with_descriptor(
        &self,
        document: &bson::Document,
        descriptor: &MessageDescriptor,
    ) -> Result<Vec<u8>, Error> {
        let message = Self::bson_document_to_dynamic_message(
            document,
            descriptor,
            self.config.field_mappings.as_ref(),
            "$",
        )?;
        Ok(message.encode_to_vec())
    }

    fn bson_document_to_dynamic_message(
        document: &bson::Document,
        descriptor: &MessageDescriptor,
        field_mappings: Option<&HashMap<String, String>>,
        path: &str,
    ) -> Result<DynamicMessage, Error> {
        let mut message = DynamicMessage::new(descriptor.clone());
        let mut populated_oneofs = HashSet::new();

        for field in descriptor.fields() {
            let source_name = field_mappings
                .and_then(|mappings| mappings.get(field.name()))
                .map(String::as_str)
                .unwrap_or_else(|| field.name());
            let value = document.get(source_name).or_else(|| {
                (source_name == field.name())
                    .then(|| document.get(field.json_name()))
                    .flatten()
            });
            let field_path = format!("{path}.{}", field.name());
            let Some(value) = value else {
                if field.is_required() {
                    return Err(Error::Serialization(format!(
                        "Missing BSON field `{field_path}` required by Protobuf descriptor"
                    )));
                }
                continue;
            };

            if matches!(value, Bson::Null) {
                if field.is_required() {
                    return Err(Self::bson_protobuf_conversion_error(
                        value,
                        &field,
                        &field_path,
                    ));
                }
                continue;
            }

            if let Some(oneof) = field.containing_oneof()
                && !populated_oneofs.insert(oneof.name().to_string())
            {
                return Err(Error::Serialization(format!(
                    "Multiple BSON fields populate Protobuf oneof `{}` at `{path}`",
                    oneof.name()
                )));
            }

            let proto_value = Self::bson_to_dynamic_value(value, &field, &field_path)?;
            message
                .try_set_field(&field, proto_value)
                .map_err(|_| Self::bson_protobuf_conversion_error(value, &field, &field_path))?;
        }

        Ok(message)
    }

    fn bson_to_dynamic_value(
        value: &Bson,
        field: &FieldDescriptor,
        path: &str,
    ) -> Result<ProtoValue, Error> {
        if field.is_list() {
            let Bson::Array(items) = value else {
                return Err(Self::bson_protobuf_conversion_error(value, field, path));
            };
            let kind = field.kind();
            let values = items
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    Self::bson_to_dynamic_scalar(item, &kind, &format!("{path}[{index}]"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(ProtoValue::List(values));
        }

        if field.is_map() {
            let Bson::Document(document) = value else {
                return Err(Self::bson_protobuf_conversion_error(value, field, path));
            };
            let Kind::Message(map_entry) = field.kind() else {
                return Err(Self::bson_protobuf_conversion_error(value, field, path));
            };
            let key_field = map_entry.map_entry_key_field();
            let value_field = map_entry.map_entry_value_field();
            let value_kind = value_field.kind();
            let values = document
                .iter()
                .map(|(key, value)| {
                    let entry_path = format!("{path}.{key}");
                    let key = Self::bson_to_map_key(key, &key_field.kind(), &entry_path)?;
                    let value = Self::bson_to_dynamic_scalar(value, &value_kind, &entry_path)?;
                    Ok((key, value))
                })
                .collect::<Result<HashMap<_, _>, Error>>()?;
            return Ok(ProtoValue::Map(values));
        }

        Self::bson_to_dynamic_scalar(value, &field.kind(), path)
    }

    fn bson_to_dynamic_scalar(value: &Bson, kind: &Kind, path: &str) -> Result<ProtoValue, Error> {
        let conversion_error = || Self::bson_protobuf_kind_error(value, kind, path);
        let converted = match kind {
            Kind::Bool => match value {
                Bson::Boolean(value) => ProtoValue::Bool(*value),
                _ => return Err(Self::bson_protobuf_kind_error(value, kind, path)),
            },
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => match value {
                Bson::Int32(value) => ProtoValue::I32(*value),
                Bson::Int64(value) => {
                    ProtoValue::I32(i32::try_from(*value).map_err(|_| conversion_error())?)
                }
                _ => return Err(Self::bson_protobuf_kind_error(value, kind, path)),
            },
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => match value {
                Bson::Int32(value) => ProtoValue::I64(i64::from(*value)),
                Bson::Int64(value) => ProtoValue::I64(*value),
                Bson::DateTime(value) => ProtoValue::I64(value.timestamp_millis()),
                _ => return Err(Self::bson_protobuf_kind_error(value, kind, path)),
            },
            Kind::Uint32 | Kind::Fixed32 => match value {
                Bson::Int32(value) => {
                    ProtoValue::U32(u32::try_from(*value).map_err(|_| conversion_error())?)
                }
                Bson::Int64(value) => {
                    ProtoValue::U32(u32::try_from(*value).map_err(|_| conversion_error())?)
                }
                _ => return Err(Self::bson_protobuf_kind_error(value, kind, path)),
            },
            Kind::Uint64 | Kind::Fixed64 => match value {
                Bson::Int32(value) => {
                    ProtoValue::U64(u64::try_from(*value).map_err(|_| conversion_error())?)
                }
                Bson::Int64(value) => {
                    ProtoValue::U64(u64::try_from(*value).map_err(|_| conversion_error())?)
                }
                _ => return Err(Self::bson_protobuf_kind_error(value, kind, path)),
            },
            Kind::Float => {
                let value = match value {
                    Bson::Double(value) => *value,
                    Bson::Int32(value) => f64::from(*value),
                    Bson::Int64(value) => *value as f64,
                    _ => return Err(Self::bson_protobuf_kind_error(value, kind, path)),
                };
                ProtoValue::F32(Self::checked_f64_to_f32(value).ok_or_else(|| {
                    Error::Serialization(format!(
                        "BSON number at `{path}` is out of range for Protobuf float"
                    ))
                })?)
            }
            Kind::Double => match value {
                Bson::Double(value) if value.is_finite() => ProtoValue::F64(*value),
                Bson::Int32(value) => ProtoValue::F64(f64::from(*value)),
                Bson::Int64(value) => ProtoValue::F64(*value as f64),
                _ => return Err(Self::bson_protobuf_kind_error(value, kind, path)),
            },
            Kind::String => match value {
                Bson::String(value) | Bson::Symbol(value) => ProtoValue::String(value.clone()),
                Bson::ObjectId(value) => ProtoValue::String(value.to_hex()),
                Bson::Decimal128(value) => ProtoValue::String(value.to_string()),
                _ => return Err(Self::bson_protobuf_kind_error(value, kind, path)),
            },
            Kind::Bytes => match value {
                Bson::Binary(value) => ProtoValue::Bytes(value.bytes.clone().into()),
                _ => return Err(Self::bson_protobuf_kind_error(value, kind, path)),
            },
            Kind::Enum(descriptor) => {
                let number = match value {
                    Bson::String(value) | Bson::Symbol(value) => descriptor
                        .get_value_by_name(value)
                        .map(|value| value.number())
                        .ok_or_else(&conversion_error)?,
                    Bson::Int32(value) => *value,
                    Bson::Int64(value) => i32::try_from(*value).map_err(|_| conversion_error())?,
                    _ => return Err(Self::bson_protobuf_kind_error(value, kind, path)),
                };
                ProtoValue::EnumNumber(number)
            }
            Kind::Message(descriptor) => match value {
                Bson::Document(document) => ProtoValue::Message(
                    Self::bson_document_to_dynamic_message(document, descriptor, None, path)?,
                ),
                _ => return Err(Self::bson_protobuf_kind_error(value, kind, path)),
            },
        };

        Ok(converted)
    }

    fn bson_to_map_key(key: &str, kind: &Kind, path: &str) -> Result<MapKey, Error> {
        let invalid_key = || {
            Error::Serialization(format!(
                "BSON document key `{key}` at `{path}` cannot be converted to Protobuf map key `{kind:?}`"
            ))
        };

        match kind {
            Kind::Bool => key.parse().map(MapKey::Bool).map_err(|_| invalid_key()),
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
                key.parse().map(MapKey::I32).map_err(|_| invalid_key())
            }
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
                key.parse().map(MapKey::I64).map_err(|_| invalid_key())
            }
            Kind::Uint32 | Kind::Fixed32 => key.parse().map(MapKey::U32).map_err(|_| invalid_key()),
            Kind::Uint64 | Kind::Fixed64 => key.parse().map(MapKey::U64).map_err(|_| invalid_key()),
            Kind::String => Ok(MapKey::String(key.to_string())),
            _ => Err(invalid_key()),
        }
    }

    fn checked_f64_to_f32(value: f64) -> Option<f32> {
        if !value.is_finite() || !(f64::from(f32::MIN)..=f64::from(f32::MAX)).contains(&value) {
            return None;
        }
        let converted = value as f32;
        (value == 0.0 || converted != 0.0).then_some(converted)
    }

    fn bson_protobuf_conversion_error(value: &Bson, field: &FieldDescriptor, path: &str) -> Error {
        Self::bson_protobuf_kind_error(value, &field.kind(), path)
    }

    fn bson_protobuf_kind_error(value: &Bson, kind: &Kind, path: &str) -> Error {
        Error::Serialization(format!(
            "BSON value `{value:?}` at `{path}` cannot be converted to Protobuf type `{kind:?}`"
        ))
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
            Payload::FlatBuffer(data) => simd_json::json!({
                "flatbuffer_size": data.len(),
                "data": general_purpose::STANDARD.encode(&data)
            }),
            Payload::Avro(data) => simd_json::json!({
                "avro_size": data.len(),
                "data": general_purpose::STANDARD.encode(&data)
            }),
            Payload::Bson(_) => return Err(Error::InvalidPayloadType),
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
                    "timestamp": IggyTimestamp::now().as_millis(),
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
            Payload::FlatBuffer(data) => (
                format!(
                    "{}/google.protobuf.BytesValue",
                    self.config.format_options.type_url_prefix
                ),
                data,
            ),
            Payload::Avro(data) => (
                format!(
                    "{}/google.protobuf.BytesValue",
                    self.config.format_options.type_url_prefix
                ),
                data,
            ),
            Payload::Bson(_) => return Err(Error::InvalidPayloadType),
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
            Payload::FlatBuffer(data) => Ok(data),
            Payload::Avro(data) => Ok(data),
            Payload::Bson(_) => Err(Error::InvalidPayloadType),
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
        let payload = match payload {
            Payload::Bson(document) => {
                if let Some(descriptor) = &self.dynamic_message_descriptor {
                    return self.encode_bson_with_descriptor(&document, descriptor);
                }
                Payload::Bson(document)
            }
            other => other,
        };
        let normalized_payload = Self::normalize_bson_payload(payload)?;
        let transformed_payload = self.apply_field_transformations(normalized_payload)?;

        self.encode_with_schema(transformed_payload)
    }
}

impl Default for ProtoStreamEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use prost_types::{
        DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
        FileDescriptorProto, FileDescriptorSet, MessageOptions, OneofDescriptorProto,
        field_descriptor_proto::{Label, Type},
    };

    use super::*;

    fn test_field(
        name: &str,
        number: i32,
        label: Label,
        r#type: Type,
        type_name: Option<&str>,
    ) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(number),
            label: Some(label as i32),
            r#type: Some(r#type as i32),
            type_name: type_name.map(str::to_string),
            ..FieldDescriptorProto::default()
        }
    }

    fn test_descriptor_set() -> Vec<u8> {
        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("user.proto".to_string()),
                package: Some("test".to_string()),
                message_type: vec![DescriptorProto {
                    name: Some("User".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("id".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional as i32),
                            r#type: Some(Type::Int32 as i32),
                            ..FieldDescriptorProto::default()
                        },
                        FieldDescriptorProto {
                            name: Some("name".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional as i32),
                            r#type: Some(Type::String as i32),
                            ..FieldDescriptorProto::default()
                        },
                        FieldDescriptorProto {
                            name: Some("active".to_string()),
                            number: Some(3),
                            label: Some(Label::Optional as i32),
                            r#type: Some(Type::Bool as i32),
                            ..FieldDescriptorProto::default()
                        },
                    ],
                    ..DescriptorProto::default()
                }],
                ..FileDescriptorProto::default()
            }],
        }
        .encode_to_vec()
    }

    fn recursive_test_descriptor_set() -> Vec<u8> {
        let address = DescriptorProto {
            name: Some("Address".to_string()),
            field: vec![test_field("city", 1, Label::Optional, Type::String, None)],
            ..DescriptorProto::default()
        };
        let labels_entry = DescriptorProto {
            name: Some("LabelsEntry".to_string()),
            field: vec![
                test_field("key", 1, Label::Optional, Type::String, None),
                test_field("value", 2, Label::Optional, Type::Int32, None),
            ],
            options: Some(MessageOptions {
                map_entry: Some(true),
                ..MessageOptions::default()
            }),
            ..DescriptorProto::default()
        };
        let mut email = test_field("email", 7, Label::Optional, Type::String, None);
        email.oneof_index = Some(0);
        let mut phone = test_field("phone", 8, Label::Optional, Type::String, None);
        phone.oneof_index = Some(0);
        let user = DescriptorProto {
            name: Some("RecursiveUser".to_string()),
            field: vec![
                test_field("id", 1, Label::Optional, Type::Int32, None),
                test_field(
                    "address",
                    2,
                    Label::Optional,
                    Type::Message,
                    Some(".test.Address"),
                ),
                test_field("scores", 3, Label::Repeated, Type::Int64, None),
                test_field(
                    "labels",
                    4,
                    Label::Repeated,
                    Type::Message,
                    Some(".test.LabelsEntry"),
                ),
                test_field(
                    "status",
                    5,
                    Label::Optional,
                    Type::Enum,
                    Some(".test.Status"),
                ),
                test_field("avatar", 6, Label::Optional, Type::Bytes, None),
                email,
                phone,
                test_field("created_at", 9, Label::Optional, Type::Int64, None),
                test_field("object_id", 10, Label::Optional, Type::String, None),
                test_field("ratio", 11, Label::Optional, Type::Float, None),
            ],
            oneof_decl: vec![OneofDescriptorProto {
                name: Some("contact".to_string()),
                ..OneofDescriptorProto::default()
            }],
            ..DescriptorProto::default()
        };
        let status = EnumDescriptorProto {
            name: Some("Status".to_string()),
            value: vec![
                EnumValueDescriptorProto {
                    name: Some("UNKNOWN".to_string()),
                    number: Some(0),
                    ..EnumValueDescriptorProto::default()
                },
                EnumValueDescriptorProto {
                    name: Some("ACTIVE".to_string()),
                    number: Some(1),
                    ..EnumValueDescriptorProto::default()
                },
            ],
            ..EnumDescriptorProto::default()
        };

        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("recursive_user.proto".to_string()),
                package: Some("test".to_string()),
                message_type: vec![address, labels_entry, user],
                enum_type: vec![status],
                syntax: Some("proto3".to_string()),
                ..FileDescriptorProto::default()
            }],
        }
        .encode_to_vec()
    }

    fn recursive_test_encoder() -> ProtoStreamEncoder {
        ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
            message_type: Some("test.RecursiveUser".to_string()),
            use_any_wrapper: false,
            descriptor_set: Some(recursive_test_descriptor_set()),
            ..ProtoEncoderConfig::default()
        })
    }

    #[test]
    fn encode_should_succeed_given_json_payload_with_any_wrapper() {
        let encoder = ProtoStreamEncoder::default();

        let json_value = simd_json::json!({
            "user_id": 123,
            "name": "John Doe",
            "email": "john@example.com"
        });

        let result = encoder.encode(Payload::Json(json_value));

        assert!(result.is_ok());
        let encoded_bytes = result.unwrap();
        assert!(!encoded_bytes.is_empty());

        let decoded_any = Any::decode(encoded_bytes.as_slice());
        assert!(decoded_any.is_ok());

        let any = decoded_any.unwrap();
        assert!(any.type_url.contains("google.protobuf.StringValue"));
        assert!(!any.value.is_empty());
    }

    #[test]
    fn encode_should_encode_bson_document_with_descriptor() {
        let encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
            message_type: Some("test.User".to_string()),
            use_any_wrapper: false,
            field_mappings: Some(HashMap::from([("id".to_string(), "user_id".to_string())])),
            descriptor_set: Some(test_descriptor_set()),
            ..ProtoEncoderConfig::default()
        });

        let encoded = encoder
            .encode(Payload::Bson(bson::doc! {
                "user_id": 123,
                "name": "Alice",
                "active": true
            }))
            .expect("BSON document should encode using the Protobuf descriptor");

        assert_eq!(
            encoded,
            vec![
                0x08, 0x7b, 0x12, 0x05, b'A', b'l', b'i', b'c', b'e', 0x18, 0x01
            ]
        );
    }

    #[test]
    fn encode_should_recursively_encode_bson_with_descriptor() {
        let descriptor_set = recursive_test_descriptor_set();
        let encoder = recursive_test_encoder();
        let object_id = bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011")
            .expect("object ID fixture should be valid");

        let encoded = encoder
            .encode(Payload::Bson(bson::doc! {
                "id": 7,
                "address": {"city": "Paris"},
                "scores": [10_i64, 20_i64],
                "labels": {"priority": 3},
                "status": "ACTIVE",
                "avatar": bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: vec![1, 2, 3]
                },
                "email": "alice@example.com",
                "created_at": bson::DateTime::from_millis(1_700_000_000_000),
                "object_id": object_id,
                "ratio": 1.5
            }))
            .expect("nested BSON document should encode using reflection");

        let pool = DescriptorPool::decode(descriptor_set.as_slice())
            .expect("descriptor fixture should be valid");
        let descriptor = pool
            .get_message_by_name("test.RecursiveUser")
            .expect("message descriptor should exist");
        let decoded = DynamicMessage::decode(descriptor, encoded.as_slice())
            .expect("encoded bytes should decode as the target message");

        assert_eq!(
            decoded
                .get_field_by_name("id")
                .expect("id should exist")
                .as_ref(),
            &ProtoValue::I32(7)
        );
        assert_eq!(
            decoded
                .get_field_by_name("scores")
                .expect("scores should exist")
                .as_ref(),
            &ProtoValue::List(vec![ProtoValue::I64(10), ProtoValue::I64(20)])
        );
        assert_eq!(
            decoded
                .get_field_by_name("status")
                .expect("status should exist")
                .as_ref(),
            &ProtoValue::EnumNumber(1)
        );
        assert_eq!(
            decoded
                .get_field_by_name("avatar")
                .expect("avatar should exist")
                .as_ref(),
            &ProtoValue::Bytes(vec![1, 2, 3].into())
        );
        assert_eq!(
            decoded
                .get_field_by_name("created_at")
                .expect("created_at should exist")
                .as_ref(),
            &ProtoValue::I64(1_700_000_000_000)
        );
        assert_eq!(
            decoded
                .get_field_by_name("object_id")
                .expect("object_id should exist")
                .as_ref(),
            &ProtoValue::String("507f1f77bcf86cd799439011".to_string())
        );
        assert_eq!(
            decoded
                .get_field_by_name("ratio")
                .expect("ratio should exist")
                .as_ref(),
            &ProtoValue::F32(1.5)
        );

        let address = decoded
            .get_field_by_name("address")
            .expect("address should exist");
        let ProtoValue::Message(address) = address.as_ref() else {
            panic!("address should be a nested message");
        };
        assert_eq!(
            address
                .get_field_by_name("city")
                .expect("city should exist")
                .as_ref(),
            &ProtoValue::String("Paris".to_string())
        );

        let labels = decoded
            .get_field_by_name("labels")
            .expect("labels should exist");
        let ProtoValue::Map(labels) = labels.as_ref() else {
            panic!("labels should be a map");
        };
        assert_eq!(
            labels.get(&MapKey::String("priority".to_string())),
            Some(&ProtoValue::I32(3))
        );
    }

    #[test]
    fn encode_should_reject_multiple_bson_fields_for_oneof() {
        let encoder = recursive_test_encoder();

        let error = encoder
            .encode(Payload::Bson(bson::doc! {
                "email": "alice@example.com",
                "phone": "+123456789"
            }))
            .expect_err("multiple oneof fields should be rejected");

        assert!(matches!(
            error,
            Error::Serialization(message) if message.contains("oneof `contact`")
        ));
    }

    #[test]
    fn encode_should_report_nested_bson_protobuf_field_path() {
        let encoder = recursive_test_encoder();

        let error = encoder
            .encode(Payload::Bson(bson::doc! {
                "address": {"city": 123}
            }))
            .expect_err("nested type mismatch should be rejected");

        assert!(matches!(
            error,
            Error::Serialization(message) if message.contains("$.address.city")
        ));
    }

    #[test]
    fn encode_should_reject_bson_number_outside_protobuf_field_range() {
        let encoder = recursive_test_encoder();

        let error = encoder
            .encode(Payload::Bson(bson::doc! {"id": i64::MAX}))
            .expect_err("int64 outside the Protobuf int32 range should be rejected");

        assert!(matches!(
            error,
            Error::Serialization(message) if message.contains("$.id")
        ));
    }

    #[test]
    fn encode_should_wrap_bson_document_as_json_in_any() {
        let encoder = ProtoStreamEncoder::default();

        let encoded = encoder
            .encode(Payload::Bson(bson::doc! {
                "id": 123,
                "name": "Alice"
            }))
            .expect("BSON document should encode using the Any wrapper");
        let any = Any::decode(encoded.as_slice()).expect("encoded payload should be Protobuf Any");
        let mut json_bytes = any.value;
        let json = simd_json::to_owned_value(&mut json_bytes)
            .expect("Any value should contain normalized BSON JSON");

        assert!(any.type_url.contains("google.protobuf.StringValue"));
        assert_eq!(json["id"], 123);
        assert_eq!(json["name"], "Alice");
    }

    #[test]
    fn encode_should_serialize_bson_document_as_json_without_any() {
        let encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
            use_any_wrapper: false,
            ..ProtoEncoderConfig::default()
        });

        let mut encoded = encoder
            .encode(Payload::Bson(bson::doc! {
                "id": 123,
                "name": "Alice"
            }))
            .expect("BSON document should encode using the raw fallback");
        let json = simd_json::to_owned_value(&mut encoded)
            .expect("raw fallback should contain normalized BSON JSON");

        assert_eq!(json["id"], 123);
        assert_eq!(json["name"], "Alice");
    }

    #[test]
    fn encode_should_succeed_given_text_payload_with_metadata() {
        let encoder = ProtoStreamEncoder::default();

        let text_payload = Payload::Text("Hello, World!".to_string());
        let result = encoder.encode(text_payload);

        assert!(result.is_ok());
        let encoded_bytes = result.unwrap();

        let decoded_any = Any::decode(encoded_bytes.as_slice()).unwrap();
        let json_string = String::from_utf8(decoded_any.value).unwrap();
        let mut json_bytes = json_string.into_bytes();
        let json_value = simd_json::to_owned_value(&mut json_bytes).unwrap();

        if let simd_json::OwnedValue::Object(map) = json_value {
            assert!(map.contains_key("text"));
            assert!(map.contains_key("timestamp"));
            assert!(map.contains_key("encoding"));
        } else {
            panic!("Expected JSON object with metadata");
        }
    }

    #[test]
    fn encode_should_succeed_given_raw_payload_as_bytes() {
        let encoder = ProtoStreamEncoder::default();

        let raw_data = b"Binary data here".to_vec();
        let result = encoder.encode(Payload::Raw(raw_data.clone()));

        assert!(result.is_ok());
        let encoded_bytes = result.unwrap();

        let decoded_any = Any::decode(encoded_bytes.as_slice()).unwrap();
        assert!(decoded_any.type_url.contains("google.protobuf.BytesValue"));
        assert_eq!(decoded_any.value, raw_data);
    }

    #[test]
    fn encode_should_succeed_given_proto_string_payload() {
        let encoder = ProtoStreamEncoder::default();

        let proto_text = "syntax = \"proto3\"; message Test { string name = 1; }";
        let result = encoder.encode(Payload::Proto(proto_text.to_string()));

        assert!(result.is_ok());
        let encoded_bytes = result.unwrap();

        let decoded_any = Any::decode(encoded_bytes.as_slice()).unwrap();
        assert!(decoded_any.type_url.contains("google.protobuf.StringValue"));
        assert_eq!(decoded_any.value, proto_text.as_bytes());
    }

    #[test]
    fn encode_should_use_raw_bytes_when_any_wrapper_disabled() {
        let config = ProtoEncoderConfig {
            use_any_wrapper: false,
            ..ProtoEncoderConfig::default()
        };
        let encoder = ProtoStreamEncoder::new_with_config(config);

        let json_value = simd_json::json!({"test": "data"});
        let result = encoder.encode(Payload::Json(json_value.clone()));

        assert!(result.is_ok());
        let encoded_bytes = result.unwrap();

        let expected_bytes = simd_json::to_vec(&json_value).unwrap();
        assert_eq!(encoded_bytes, expected_bytes);
    }

    #[test]
    fn encode_should_apply_field_mappings_when_configured() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("user_id".to_string(), "id".to_string());
        field_mappings.insert("full_name".to_string(), "name".to_string());

        let config = ProtoEncoderConfig {
            field_mappings: Some(field_mappings),
            use_any_wrapper: false,
            ..ProtoEncoderConfig::default()
        };
        let encoder = ProtoStreamEncoder::new_with_config(config);

        let json_value = simd_json::json!({
            "id": 123,
            "name": "John Doe",
            "email": "john@example.com"
        });

        let result = encoder.encode(Payload::Json(json_value));

        assert!(result.is_ok());
        let encoded_bytes = result.unwrap();

        let mut encoded_bytes_mut = encoded_bytes;
        let decoded_json = simd_json::to_owned_value(&mut encoded_bytes_mut).unwrap();

        if let simd_json::OwnedValue::Object(map) = decoded_json {
            assert!(map.contains_key("user_id"));
            assert!(map.contains_key("full_name"));
            assert!(map.contains_key("email"));
            assert!(!map.contains_key("id"));
            assert!(!map.contains_key("name"));
        } else {
            panic!("Expected JSON object");
        }
    }

    #[test]
    fn encode_should_use_custom_type_url_prefix_when_configured() {
        let config = ProtoEncoderConfig {
            format_options: ProtoFormatOptions {
                type_url_prefix: "custom.domain.com".to_string(),
                ..ProtoFormatOptions::default()
            },
            ..ProtoEncoderConfig::default()
        };
        let encoder = ProtoStreamEncoder::new_with_config(config);

        let json_value = simd_json::json!({"test": "data"});
        let result = encoder.encode(Payload::Json(json_value));

        assert!(result.is_ok());
        let encoded_bytes = result.unwrap();

        let decoded_any = Any::decode(encoded_bytes.as_slice()).unwrap();
        assert!(decoded_any.type_url.starts_with("custom.domain.com/"));
    }

    #[test]
    fn load_schema_should_handle_missing_schema_path_gracefully() {
        let mut encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
            schema_path: None,
            message_type: None,
            ..ProtoEncoderConfig::default()
        });

        let result = encoder.load_schema();
        assert!(
            result.is_ok(),
            "Should handle missing schema path gracefully"
        );
    }

    #[test]
    fn load_schema_should_handle_missing_proto_file_gracefully() {
        let mut encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
            schema_path: Some(PathBuf::from("nonexistent.proto")),
            message_type: Some("com.example.Test".to_string()),
            ..ProtoEncoderConfig::default()
        });

        let result = encoder.load_schema();
        assert!(
            result.is_ok(),
            "Should handle missing proto file gracefully"
        );
    }

    #[test]
    fn convert_format_should_transform_json_to_text() {
        let encoder = ProtoStreamEncoder::default();

        let json_value = simd_json::json!({
            "name": "John",
            "age": 30
        });

        let result = encoder.convert_format(Payload::Json(json_value), Schema::Text);

        assert!(result.is_ok());
        if let Ok(Payload::Text(text)) = result {
            assert!(text.contains("\"name\""));
            assert!(text.contains("\"John\""));
            assert!(text.contains("\"age\""));
            assert!(text.contains("30"));
        } else {
            panic!("Expected Text payload");
        }
    }

    #[test]
    fn convert_format_should_transform_text_to_json() {
        let encoder = ProtoStreamEncoder::default();

        let json_text = r#"{"name": "John", "age": 30}"#;

        let result = encoder.convert_format(Payload::Text(json_text.to_string()), Schema::Json);

        assert!(result.is_ok());
        if let Ok(Payload::Json(json_value)) = result {
            if let simd_json::OwnedValue::Object(map) = json_value {
                assert!(map.contains_key("name"));
                assert!(map.contains_key("age"));
            } else {
                panic!("Expected JSON object");
            }
        } else {
            panic!("Expected JSON payload");
        }
    }

    #[test]
    fn convert_format_should_transform_raw_to_text() {
        let encoder = ProtoStreamEncoder::default();

        let text_data = "Hello, World!";
        let raw_payload = Payload::Raw(text_data.as_bytes().to_vec());

        let result = encoder.convert_format(raw_payload, Schema::Text);

        assert!(result.is_ok());
        if let Ok(Payload::Text(text)) = result {
            assert_eq!(text, text_data);
        } else {
            panic!("Expected Text payload");
        }
    }

    #[test]
    fn convert_format_should_handle_proto_to_json_conversion() {
        let encoder = ProtoStreamEncoder::default();

        let proto_json = r#"{"message": "test", "id": 123}"#;
        let proto_payload = Payload::Proto(proto_json.to_string());

        let result = encoder.convert_format(proto_payload, Schema::Json);

        assert!(result.is_ok());
        if let Ok(Payload::Json(json_value)) = result {
            if let simd_json::OwnedValue::Object(map) = json_value {
                assert!(map.contains_key("message"));
                assert!(map.contains_key("id"));
            } else {
                panic!("Expected JSON object");
            }
        } else {
            panic!("Expected JSON payload");
        }
    }

    #[test]
    fn config_should_have_sensible_defaults() {
        let config = ProtoEncoderConfig::default();

        assert!(config.schema_path.is_none());
        assert!(config.message_type.is_none());
        assert!(config.use_any_wrapper);
        assert!(config.field_mappings.is_none());
        assert!(config.schema_registry_url.is_none());
        assert!(config.descriptor_set.is_none());
        assert_eq!(config.include_paths, vec![PathBuf::from(".")]);
        assert!(!config.preserve_unknown_fields);

        let format_opts = &config.format_options;
        assert!(format_opts.compact_encoding);
        assert!(format_opts.validate_message);
        assert_eq!(format_opts.type_url_prefix, "type.googleapis.com");
        assert!(!format_opts.deterministic_encoding);
    }

    #[test]
    fn encoder_should_be_creatable_with_custom_config() {
        let config = ProtoEncoderConfig {
            schema_path: Some(PathBuf::from("schemas/user.proto")),
            message_type: Some("com.example.User".to_string()),
            use_any_wrapper: false,
            field_mappings: Some(HashMap::from([
                ("user_id".to_string(), "id".to_string()),
                ("full_name".to_string(), "name".to_string()),
            ])),
            schema_registry_url: Some("http://schema-registry:8081".to_string()),
            descriptor_set: None,
            include_paths: vec![PathBuf::from("."), PathBuf::from("schemas/common")],
            preserve_unknown_fields: true,
            format_options: ProtoFormatOptions {
                compact_encoding: false,
                validate_message: false,
                type_url_prefix: "custom.example.com".to_string(),
                deterministic_encoding: true,
            },
        };

        let encoder = ProtoStreamEncoder::new_with_config(config.clone());

        assert_eq!(encoder.config.schema_path, config.schema_path);
        assert_eq!(encoder.config.message_type, config.message_type);
        assert_eq!(encoder.config.use_any_wrapper, config.use_any_wrapper);
        assert_eq!(encoder.config.field_mappings, config.field_mappings);
        assert_eq!(
            encoder.config.schema_registry_url,
            config.schema_registry_url
        );
        assert_eq!(encoder.config.descriptor_set, config.descriptor_set);
        assert_eq!(encoder.config.include_paths, config.include_paths);
        assert_eq!(
            encoder.config.preserve_unknown_fields,
            config.preserve_unknown_fields
        );
        assert_eq!(
            encoder.config.format_options.compact_encoding,
            config.format_options.compact_encoding
        );
        assert_eq!(
            encoder.config.format_options.type_url_prefix,
            config.format_options.type_url_prefix
        );
    }

    #[test]
    fn encoder_should_return_proto_schema() {
        let encoder = ProtoStreamEncoder::default();
        assert_eq!(encoder.schema(), Schema::Proto);
    }

    #[test]
    fn integration_should_encode_real_protobuf_message_with_schema() {
        let encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
            schema_path: Some(PathBuf::from("examples/user.proto")),
            message_type: Some("com.example.User".to_string()),
            use_any_wrapper: false,
            ..ProtoEncoderConfig::default()
        });

        let json_payload = Payload::Json(simd_json::json!({
            "id": 123,
            "name": "John Doe",
            "email": "john@example.com",
            "active": true
        }));

        let result = encoder.encode(json_payload);

        assert!(
            result.is_ok(),
            "Encoding should succeed with schema or fallback"
        );

        let encoded_bytes = result.unwrap();
        assert!(
            !encoded_bytes.is_empty(),
            "Encoded data should not be empty"
        );

        println!(
            "Successfully encoded {} bytes of protobuf data",
            encoded_bytes.len()
        );

        let data_str = String::from_utf8_lossy(&encoded_bytes);
        assert!(data_str.contains("John Doe"), "Should contain user name");
        assert!(
            data_str.contains("john@example.com"),
            "Should contain user email"
        );
    }

    #[test]
    fn integration_should_handle_schema_loading_gracefully() {
        let encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
            schema_path: Some(PathBuf::from("nonexistent/schema.proto")),
            message_type: Some("com.example.NonExistent".to_string()),
            use_any_wrapper: true,
            ..ProtoEncoderConfig::default()
        });

        let json_payload = Payload::Json(simd_json::json!({
            "test": "data"
        }));

        let result = encoder.encode(json_payload);
        assert!(
            result.is_ok(),
            "Should fallback gracefully when schema loading fails"
        );
    }
}
