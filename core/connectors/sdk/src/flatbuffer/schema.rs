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

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;

use flatbuffers::{FILE_IDENTIFIER_LENGTH, SIZE_UOFFSET};
use flatbuffers_reflection::reflection::{
    AdvancedFeatures, BaseType, Field, Schema, Type, root_as_schema, schema_buffer_has_identifier,
};

use crate::Error;

const MAX_BFBS_SIZE_BYTES: usize = 16 * 1024 * 1024;
const MAX_OBJECTS: usize = 65_536;
const MAX_FIELDS_PER_OBJECT: usize = 65_536;
const MAX_ENUMS: usize = 65_536;
const MAX_VALUES_PER_ENUM: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaFeatures {
    bits: u64,
}

impl SchemaFeatures {
    pub fn bits(self) -> u64 {
        self.bits
    }

    pub fn advanced_arrays(self) -> bool {
        self.bits & AdvancedFeatures::AdvancedArrayFeatures.bits() != 0
    }

    pub fn advanced_unions(self) -> bool {
        self.bits & AdvancedFeatures::AdvancedUnionFeatures.bits() != 0
    }

    pub fn optional_scalars(self) -> bool {
        self.bits & AdvancedFeatures::OptionalScalars.bits() != 0
    }

    pub fn default_vectors_and_strings(self) -> bool {
        self.bits & AdvancedFeatures::DefaultVectorsAndStrings.bits() != 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeKind {
    None,
    UnionType,
    Bool,
    Int8,
    Uint8,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int64,
    Uint64,
    Float32,
    Float64,
    String,
    Vector,
    Object,
    Union,
    Array,
    Vector64,
    Unknown(i8),
}

impl From<BaseType> for TypeKind {
    fn from(base_type: BaseType) -> Self {
        match base_type {
            BaseType::None => Self::None,
            BaseType::UType => Self::UnionType,
            BaseType::Bool => Self::Bool,
            BaseType::Byte => Self::Int8,
            BaseType::UByte => Self::Uint8,
            BaseType::Short => Self::Int16,
            BaseType::UShort => Self::Uint16,
            BaseType::Int => Self::Int32,
            BaseType::UInt => Self::Uint32,
            BaseType::Long => Self::Int64,
            BaseType::ULong => Self::Uint64,
            BaseType::Float => Self::Float32,
            BaseType::Double => Self::Float64,
            BaseType::String => Self::String,
            BaseType::Vector => Self::Vector,
            BaseType::Obj => Self::Object,
            BaseType::Union => Self::Union,
            BaseType::Array => Self::Array,
            BaseType::Vector64 => Self::Vector64,
            unknown => Self::Unknown(unknown.0),
        }
    }
}

impl TypeKind {
    pub fn is_scalar(self) -> bool {
        matches!(
            self,
            Self::UnionType
                | Self::Bool
                | Self::Int8
                | Self::Uint8
                | Self::Int16
                | Self::Uint16
                | Self::Int32
                | Self::Uint32
                | Self::Int64
                | Self::Uint64
                | Self::Float32
                | Self::Float64
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeDescriptor {
    base_type: TypeKind,
    element_type: TypeKind,
    index: Option<usize>,
    fixed_length: u16,
    base_size: u32,
    element_size: u32,
}

impl TypeDescriptor {
    pub fn base_type(&self) -> TypeKind {
        self.base_type
    }

    pub fn element_type(&self) -> TypeKind {
        self.element_type
    }

    pub fn index(&self) -> Option<usize> {
        self.index
    }

    pub fn fixed_length(&self) -> u16 {
        self.fixed_length
    }

    pub fn base_size(&self) -> u32 {
        self.base_size
    }

    pub fn element_size(&self) -> u32 {
        self.element_size
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldDescriptor {
    name: String,
    id: u16,
    offset: u16,
    field_type: TypeDescriptor,
    default_integer: i64,
    default_real: f64,
    deprecated: bool,
    required: bool,
    key: bool,
    optional: bool,
    padding: u16,
    offset64: bool,
}

impl FieldDescriptor {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn offset(&self) -> u16 {
        self.offset
    }

    pub fn field_type(&self) -> &TypeDescriptor {
        &self.field_type
    }

    pub fn default_integer(&self) -> i64 {
        self.default_integer
    }

    pub fn default_real(&self) -> f64 {
        self.default_real
    }

    pub fn is_deprecated(&self) -> bool {
        self.deprecated
    }

    pub fn is_required(&self) -> bool {
        self.required
    }

    pub fn is_key(&self) -> bool {
        self.key
    }

    pub fn is_optional(&self) -> bool {
        self.optional
    }

    pub fn padding(&self) -> u16 {
        self.padding
    }

    pub fn uses_64_bit_offset(&self) -> bool {
        self.offset64
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectDescriptor {
    /// Fully qualified table or struct name from the BFBS schema.
    name: String,
    /// Position of the object in the BFBS `objects` vector.
    index: usize,
    /// Whether the object uses the fixed-layout FlatBuffer struct representation.
    is_struct: bool,
    /// Owned metadata for fields declared by the object.
    fields: Vec<FieldDescriptor>,
    /// Field name to `fields` index lookup.
    field_indices: BTreeMap<String, usize>,
}

impl ObjectDescriptor {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn is_struct(&self) -> bool {
        self.is_struct
    }

    pub fn fields(&self) -> &[FieldDescriptor] {
        &self.fields
    }

    pub fn field(&self, name: &str) -> Option<&FieldDescriptor> {
        self.field_indices
            .get(name)
            .and_then(|index| self.fields.get(*index))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumValueDescriptor {
    name: String,
    value: i64,
    union_type: Option<TypeDescriptor>,
}

impl EnumValueDescriptor {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value(&self) -> i64 {
        self.value
    }

    pub fn union_type(&self) -> Option<&TypeDescriptor> {
        self.union_type.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumDescriptor {
    name: String,
    index: usize,
    is_union: bool,
    underlying_type: TypeDescriptor,
    values: Vec<EnumValueDescriptor>,
    value_indices: BTreeMap<String, usize>,
    number_indices: BTreeMap<i64, usize>,
}

impl EnumDescriptor {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn is_union(&self) -> bool {
        self.is_union
    }

    pub fn underlying_type(&self) -> &TypeDescriptor {
        &self.underlying_type
    }

    pub fn values(&self) -> &[EnumValueDescriptor] {
        &self.values
    }

    pub fn value(&self, name: &str) -> Option<&EnumValueDescriptor> {
        self.value_indices
            .get(name)
            .and_then(|index| self.values.get(*index))
    }

    pub fn value_by_number(&self, value: i64) -> Option<&EnumValueDescriptor> {
        self.number_indices
            .get(&value)
            .and_then(|index| self.values.get(*index))
    }
}

#[derive(Debug, Clone)]
pub struct FlatBufferSchema {
    /// Verified BFBS bytes retained for creating short-lived reflection views.
    bytes: Arc<[u8]>,
    /// Index of the root table selected from the schema or explicit configuration.
    root_table_index: usize,
    /// Optional four-byte identifier expected in encoded business payloads.
    file_identifier: Option<[u8; 4]>,
    /// Owned metadata for every table and struct declared by the schema.
    objects: Vec<ObjectDescriptor>,
    /// Fully qualified object name to `objects` index lookup.
    object_indices: BTreeMap<String, usize>,
    /// Owned metadata for every enum and union declared by the schema.
    enums: Vec<EnumDescriptor>,
    /// Fully qualified enum or union name to `enums` index lookup.
    enum_indices: BTreeMap<String, usize>,
    /// Advanced schema features recognized by this loader version.
    features: SchemaFeatures,
}

impl FlatBufferSchema {
    pub fn from_path(
        schema_path: impl AsRef<Path>,
        root_table_name: Option<&str>,
    ) -> Result<Self, Error> {
        let schema_path = schema_path.as_ref();
        let schema_size = fs::metadata(schema_path)
            .map_err(|error| {
                Error::InvalidConfigValue(format!(
                    "Failed to inspect FlatBuffer BFBS schema '{}': {error}",
                    schema_path.display()
                ))
            })?
            .len();
        if schema_size > MAX_BFBS_SIZE_BYTES as u64 {
            return Err(Error::InvalidConfigValue(format!(
                "FlatBuffer BFBS schema '{}' exceeds the {MAX_BFBS_SIZE_BYTES}-byte size limit",
                schema_path.display()
            )));
        }
        let bytes = fs::read(schema_path).map_err(|error| {
            Error::InvalidConfigValue(format!(
                "Failed to read FlatBuffer BFBS schema '{}': {error}",
                schema_path.display()
            ))
        })?;

        Self::from_bytes(bytes, root_table_name)
    }

    pub fn from_bytes(
        bytes: impl Into<Arc<[u8]>>,
        root_table_name: Option<&str>,
    ) -> Result<Self, Error> {
        let bytes = bytes.into();
        let schema = parse_schema(&bytes)?;
        let features = parse_schema_features(&schema)?;
        let (objects, object_indices) = collect_objects(&schema)?;
        let (enums, enum_indices) = collect_enums(&schema)?;
        validate_references(&objects, &enums)?;
        let root_table_index =
            resolve_root_table(&schema, &objects, &object_indices, root_table_name)?;
        let file_identifier = parse_file_identifier(&schema)?;

        Ok(Self {
            bytes,
            root_table_index,
            file_identifier,
            objects,
            object_indices,
            enums,
            enum_indices,
            features,
        })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn reflection_schema(&self) -> Result<Schema<'_>, Error> {
        parse_schema(&self.bytes)
    }

    pub fn root_table(&self) -> &ObjectDescriptor {
        &self.objects[self.root_table_index]
    }

    pub fn file_identifier(&self) -> Option<[u8; 4]> {
        self.file_identifier
    }

    pub fn objects(&self) -> &[ObjectDescriptor] {
        &self.objects
    }

    pub fn object(&self, name: &str) -> Option<&ObjectDescriptor> {
        self.object_indices
            .get(name)
            .and_then(|index| self.objects.get(*index))
    }

    pub fn object_by_index(&self, index: usize) -> Option<&ObjectDescriptor> {
        self.objects.get(index)
    }

    pub fn enums(&self) -> &[EnumDescriptor] {
        &self.enums
    }

    pub fn enum_descriptor(&self, name: &str) -> Option<&EnumDescriptor> {
        self.enum_indices
            .get(name)
            .and_then(|index| self.enums.get(*index))
    }

    pub fn enum_by_index(&self, index: usize) -> Option<&EnumDescriptor> {
        self.enums.get(index)
    }

    pub fn features(&self) -> SchemaFeatures {
        self.features
    }
}

fn parse_schema(bytes: &[u8]) -> Result<Schema<'_>, Error> {
    if bytes.len() > MAX_BFBS_SIZE_BYTES {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer BFBS schema exceeds the {MAX_BFBS_SIZE_BYTES}-byte size limit"
        )));
    }
    if bytes.len() < SIZE_UOFFSET + FILE_IDENTIFIER_LENGTH || !schema_buffer_has_identifier(bytes) {
        return Err(Error::InvalidConfigValue(
            "FlatBuffer schema is missing the BFBS file identifier".to_string(),
        ));
    }

    root_as_schema(bytes).map_err(|error| {
        Error::InvalidConfigValue(format!("Invalid FlatBuffer BFBS schema: {error}"))
    })
}

fn parse_schema_features(schema: &Schema<'_>) -> Result<SchemaFeatures, Error> {
    let features = schema.advanced_features();
    let known_bits = AdvancedFeatures::AdvancedArrayFeatures.bits()
        | AdvancedFeatures::AdvancedUnionFeatures.bits()
        | AdvancedFeatures::OptionalScalars.bits()
        | AdvancedFeatures::DefaultVectorsAndStrings.bits();
    let unknown_bits = features.bits() & !known_bits;
    if unknown_bits != 0 {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer BFBS schema uses unknown advanced feature bits 0x{unknown_bits:x}"
        )));
    }

    Ok(SchemaFeatures {
        bits: features.bits(),
    })
}

fn collect_objects(
    schema: &Schema<'_>,
) -> Result<(Vec<ObjectDescriptor>, BTreeMap<String, usize>), Error> {
    let schema_objects = schema.objects();
    if schema_objects.len() > MAX_OBJECTS {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer BFBS schema contains more than {MAX_OBJECTS} objects"
        )));
    }
    let mut objects = Vec::with_capacity(schema_objects.len());
    let mut object_indices = BTreeMap::new();

    for index in 0..schema_objects.len() {
        let object = schema_objects.get(index);
        let name = object.name().to_string();
        if name.is_empty() {
            return Err(Error::InvalidConfigValue(
                "FlatBuffer BFBS schema contains an object with an empty name".to_string(),
            ));
        }
        if object_indices.insert(name.clone(), index).is_some() {
            return Err(Error::InvalidConfigValue(format!(
                "FlatBuffer BFBS schema contains duplicate object '{name}'"
            )));
        }
        let (fields, field_indices) = collect_fields(&name, object.fields())?;
        objects.push(ObjectDescriptor {
            name,
            index,
            is_struct: object.is_struct(),
            fields,
            field_indices,
        });
    }

    Ok((objects, object_indices))
}

fn collect_fields(
    object_name: &str,
    schema_fields: flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<Field<'_>>>,
) -> Result<(Vec<FieldDescriptor>, BTreeMap<String, usize>), Error> {
    if schema_fields.len() > MAX_FIELDS_PER_OBJECT {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer object '{object_name}' contains more than {MAX_FIELDS_PER_OBJECT} fields"
        )));
    }
    let mut fields = Vec::with_capacity(schema_fields.len());
    let mut field_indices = BTreeMap::new();
    let mut field_ids = BTreeSet::new();
    let mut field_offsets = BTreeSet::new();
    let mut has_key = false;

    for index in 0..schema_fields.len() {
        let field = schema_fields.get(index);
        let name = field.name().to_string();
        if name.is_empty() {
            return Err(Error::InvalidConfigValue(format!(
                "FlatBuffer object '{object_name}' contains a field with an empty name"
            )));
        }
        if field_indices.insert(name.clone(), index).is_some() {
            return Err(Error::InvalidConfigValue(format!(
                "FlatBuffer object '{object_name}' contains duplicate field '{name}'"
            )));
        }
        if !field_ids.insert(field.id()) {
            return Err(Error::InvalidConfigValue(format!(
                "FlatBuffer object '{object_name}' contains duplicate field ID {}",
                field.id()
            )));
        }
        if !field_offsets.insert(field.offset()) {
            return Err(Error::InvalidConfigValue(format!(
                "FlatBuffer object '{object_name}' contains duplicate field offset {}",
                field.offset()
            )));
        }
        if field.required() && field.optional() {
            return Err(Error::InvalidConfigValue(format!(
                "FlatBuffer field '{object_name}.{name}' cannot be both required and optional"
            )));
        }
        let type_context = format!("field '{object_name}.{name}'");
        let field_type = describe_type(field.type_(), &type_context)?;
        if field.key() {
            if has_key {
                return Err(Error::InvalidConfigValue(format!(
                    "FlatBuffer object '{object_name}' defines more than one key field"
                )));
            }
            if !field_type.base_type().is_scalar() && field_type.base_type() != TypeKind::String {
                return Err(Error::InvalidConfigValue(format!(
                    "FlatBuffer {type_context} uses key on an unsupported type"
                )));
            }
            has_key = true;
        }
        fields.push(FieldDescriptor {
            name,
            id: field.id(),
            offset: field.offset(),
            field_type,
            default_integer: field.default_integer(),
            default_real: field.default_real(),
            deprecated: field.deprecated(),
            required: field.required(),
            key: field.key(),
            optional: field.optional(),
            padding: field.padding(),
            offset64: field.offset64(),
        });
    }

    Ok((fields, field_indices))
}

fn collect_enums(
    schema: &Schema<'_>,
) -> Result<(Vec<EnumDescriptor>, BTreeMap<String, usize>), Error> {
    let schema_enums = schema.enums();
    if schema_enums.len() > MAX_ENUMS {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer BFBS schema contains more than {MAX_ENUMS} enums or unions"
        )));
    }
    let mut enums = Vec::with_capacity(schema_enums.len());
    let mut enum_indices = BTreeMap::new();

    for index in 0..schema_enums.len() {
        let schema_enum = schema_enums.get(index);
        let name = schema_enum.name().to_string();
        if name.is_empty() {
            return Err(Error::InvalidConfigValue(
                "FlatBuffer BFBS schema contains an enum or union with an empty name".to_string(),
            ));
        }
        if enum_indices.insert(name.clone(), index).is_some() {
            return Err(Error::InvalidConfigValue(format!(
                "FlatBuffer BFBS schema contains duplicate enum or union '{name}'"
            )));
        }

        let schema_values = schema_enum.values();
        if schema_values.len() > MAX_VALUES_PER_ENUM {
            return Err(Error::InvalidConfigValue(format!(
                "FlatBuffer enum or union '{name}' contains more than {MAX_VALUES_PER_ENUM} values"
            )));
        }
        let mut values = Vec::with_capacity(schema_values.len());
        let mut value_indices = BTreeMap::new();
        let mut number_indices = BTreeMap::new();
        for value_index in 0..schema_values.len() {
            let schema_value = schema_values.get(value_index);
            let value_name = schema_value.name().to_string();
            if value_name.is_empty() {
                return Err(Error::InvalidConfigValue(format!(
                    "FlatBuffer enum or union '{name}' contains a value with an empty name"
                )));
            }
            if value_indices
                .insert(value_name.clone(), value_index)
                .is_some()
            {
                return Err(Error::InvalidConfigValue(format!(
                    "FlatBuffer enum or union '{name}' contains duplicate value '{value_name}'"
                )));
            }
            let union_type = schema_value
                .union_type()
                .map(|field_type| {
                    describe_type(
                        field_type,
                        &format!("enum or union value '{name}.{}'", schema_value.name()),
                    )
                })
                .transpose()?
                .filter(|field_type| field_type.base_type() != TypeKind::None);
            values.push(EnumValueDescriptor {
                name: value_name,
                value: schema_value.value(),
                union_type,
            });
            number_indices
                .entry(schema_value.value())
                .or_insert(value_index);
        }

        let underlying_type = describe_type(
            schema_enum.underlying_type(),
            &format!("enum or union '{name}'"),
        )?;
        enums.push(EnumDescriptor {
            name,
            index,
            is_union: schema_enum.is_union(),
            underlying_type,
            values,
            value_indices,
            number_indices,
        });
    }

    Ok((enums, enum_indices))
}

fn describe_type(field_type: Type<'_>, context: &str) -> Result<TypeDescriptor, Error> {
    let index = match field_type.index() {
        -1 => None,
        index if index >= 0 => Some(index as usize),
        index => {
            return Err(Error::InvalidConfigValue(format!(
                "FlatBuffer {context} contains invalid type index {index}"
            )));
        }
    };
    let descriptor = TypeDescriptor {
        base_type: field_type.base_type().into(),
        element_type: field_type.element().into(),
        index,
        fixed_length: field_type.fixed_length(),
        base_size: field_type.base_size(),
        element_size: field_type.element_size(),
    };
    if let TypeKind::Unknown(value) = descriptor.base_type {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer {context} uses unknown base type {value}"
        )));
    }
    if let TypeKind::Unknown(value) = descriptor.element_type {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer {context} uses unknown element type {value}"
        )));
    }

    Ok(descriptor)
}

fn validate_references(
    objects: &[ObjectDescriptor],
    enums: &[EnumDescriptor],
) -> Result<(), Error> {
    for object in objects {
        for field in object.fields() {
            validate_type_reference(
                field.field_type(),
                objects,
                enums,
                &format!("field '{}.{}'", object.name(), field.name()),
            )?;
        }
    }

    for enum_descriptor in enums {
        validate_enum_underlying_type(enum_descriptor)?;
        for value in enum_descriptor.values() {
            if let Some(union_type) = value.union_type() {
                validate_type_reference(
                    union_type,
                    objects,
                    enums,
                    &format!(
                        "enum or union value '{}.{}'",
                        enum_descriptor.name(),
                        value.name()
                    ),
                )?;
            }
        }
    }

    Ok(())
}

fn validate_enum_underlying_type(enum_descriptor: &EnumDescriptor) -> Result<(), Error> {
    let base_type = enum_descriptor.underlying_type().base_type();
    let valid = if enum_descriptor.is_union() {
        base_type == TypeKind::UnionType
    } else {
        matches!(
            base_type,
            TypeKind::Int8
                | TypeKind::Uint8
                | TypeKind::Int16
                | TypeKind::Uint16
                | TypeKind::Int32
                | TypeKind::Uint32
                | TypeKind::Int64
                | TypeKind::Uint64
        )
    };
    if !valid {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer enum or union '{}' has invalid underlying type {base_type:?}",
            enum_descriptor.name()
        )));
    }

    Ok(())
}

fn validate_type_reference(
    field_type: &TypeDescriptor,
    objects: &[ObjectDescriptor],
    enums: &[EnumDescriptor],
    context: &str,
) -> Result<(), Error> {
    if field_type.base_type() == TypeKind::None {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer {context} does not define a base type"
        )));
    }
    if field_type.base_type() == TypeKind::Array && field_type.fixed_length() == 0 {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer {context} defines a fixed array with zero length"
        )));
    }
    if field_type.base_type() != TypeKind::Array && field_type.fixed_length() != 0 {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer {context} defines a fixed length for a non-array type"
        )));
    }

    match field_type.base_type() {
        TypeKind::Object => validate_object_index(field_type.index(), objects, context),
        TypeKind::Union | TypeKind::UnionType => {
            validate_enum_index(field_type.index(), enums, true, context)
        }
        TypeKind::Vector | TypeKind::Vector64 | TypeKind::Array => {
            match field_type.element_type() {
                TypeKind::None => Err(Error::InvalidConfigValue(format!(
                    "FlatBuffer {context} does not define a container element type"
                ))),
                TypeKind::Object => validate_object_index(field_type.index(), objects, context),
                TypeKind::Union | TypeKind::UnionType => {
                    validate_enum_index(field_type.index(), enums, true, context)
                }
                _ => match field_type.index() {
                    Some(_) => validate_enum_index(field_type.index(), enums, false, context),
                    None => Ok(()),
                },
            }
        }
        _ => match field_type.index() {
            Some(_) => validate_enum_index(field_type.index(), enums, false, context),
            None => Ok(()),
        },
    }
}

fn validate_object_index(
    index: Option<usize>,
    objects: &[ObjectDescriptor],
    context: &str,
) -> Result<(), Error> {
    let index = index.ok_or_else(|| {
        Error::InvalidConfigValue(format!(
            "FlatBuffer {context} is missing its object type index"
        ))
    })?;
    if objects.get(index).is_none() {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer {context} references missing object index {index}"
        )));
    }

    Ok(())
}

fn validate_enum_index(
    index: Option<usize>,
    enums: &[EnumDescriptor],
    require_union: bool,
    context: &str,
) -> Result<(), Error> {
    let index = index.ok_or_else(|| {
        Error::InvalidConfigValue(format!(
            "FlatBuffer {context} is missing its enum or union type index"
        ))
    })?;
    let enum_descriptor = enums.get(index).ok_or_else(|| {
        Error::InvalidConfigValue(format!(
            "FlatBuffer {context} references missing enum or union index {index}"
        ))
    })?;
    if enum_descriptor.is_union() != require_union {
        let expected = if require_union { "union" } else { "enum" };
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer {context} references '{}' as an {expected}",
            enum_descriptor.name()
        )));
    }

    Ok(())
}

fn resolve_root_table(
    schema: &Schema<'_>,
    objects: &[ObjectDescriptor],
    object_indices: &BTreeMap<String, usize>,
    configured_name: Option<&str>,
) -> Result<usize, Error> {
    let root_name = match configured_name {
        Some(name) if !name.trim().is_empty() => name,
        Some(_) => {
            return Err(Error::InvalidConfigValue(
                "FlatBuffer root table name cannot be empty".to_string(),
            ));
        }
        None => schema
            .root_table()
            .map(|object| object.name())
            .ok_or_else(|| {
                Error::InvalidConfigValue(
                    "FlatBuffer BFBS schema does not define a root table".to_string(),
                )
            })?,
    };

    let index = object_indices.get(root_name).copied().ok_or_else(|| {
        Error::InvalidConfigValue(format!(
            "FlatBuffer root table '{root_name}' was not found in the BFBS schema"
        ))
    })?;
    let root_table = &objects[index];
    if root_table.is_struct {
        return Err(Error::InvalidConfigValue(format!(
            "FlatBuffer root object '{}' is a struct, not a table",
            root_table.name
        )));
    }

    Ok(index)
}

fn parse_file_identifier(schema: &Schema<'_>) -> Result<Option<[u8; 4]>, Error> {
    let Some(identifier) = schema.file_ident() else {
        return Ok(None);
    };
    if identifier.is_empty() {
        return Ok(None);
    }
    let identifier = identifier.as_bytes().try_into().map_err(|_| {
        Error::InvalidConfigValue(format!(
            "FlatBuffer file identifier must contain exactly 4 bytes, got {}",
            identifier.len()
        ))
    })?;

    Ok(Some(identifier))
}

#[cfg(test)]
mod tests {
    use flatbuffers::{FlatBufferBuilder, WIPOffset};
    use flatbuffers_reflection::reflection::{
        AdvancedFeatures, Enum, Field, FieldArgs, Object, ObjectArgs, Schema, SchemaArgs, Type,
        TypeArgs, finish_schema_buffer,
    };

    use super::*;

    #[test]
    fn given_valid_bfbs_when_loaded_should_cache_schema_metadata() {
        let bytes = build_bfbs(
            &[("example.User", false), ("example.Address", false)],
            0,
            Some("USER"),
        );

        let schema = FlatBufferSchema::from_bytes(bytes, None).expect("schema should load");

        assert_eq!(schema.root_table().name(), "example.User");
        assert_eq!(schema.root_table().index(), 0);
        assert_eq!(schema.file_identifier(), Some(*b"USER"));
        assert_eq!(schema.objects().len(), 2);
        assert_eq!(
            schema
                .object("example.Address")
                .map(|object| object.index()),
            Some(1)
        );
        assert!(schema.reflection_schema().is_ok());
    }

    #[test]
    fn given_real_bfbs_file_when_loaded_should_read_schema_metadata() {
        // Precompiled from examples/user.fbs so CI verifies real flatc output without requiring
        // the compiler to be installed.
        let schema_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/flatbuffer/user.bfbs");

        let schema = FlatBufferSchema::from_path(schema_path, None)
            .expect("real BFBS schema should load from path");
        let reflection_schema = schema
            .reflection_schema()
            .expect("cached BFBS schema should remain valid");
        let root_table = reflection_schema
            .root_table()
            .expect("real BFBS schema should define a root table");

        assert_eq!(schema.root_table().name(), "com.example.User");
        assert_eq!(root_table.fields().len(), 7);
        assert!(schema.object("com.example.Address").is_some());
        assert!(schema.object("com.example.UserList").is_some());
        assert_eq!(schema.file_identifier(), None);
    }

    #[test]
    fn given_real_bfbs_with_supported_types_when_loaded_should_cache_type_metadata() {
        let schema_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/flatbuffer/types.bfbs");

        let schema = FlatBufferSchema::from_path(schema_path, None)
            .expect("real BFBS type schema should load from path");
        let root = schema.root_table();

        assert_eq!(root.name(), "test.types.AllTypes");
        assert_eq!(schema.file_identifier(), Some(*b"TYPE"));
        assert!(schema.features().optional_scalars());
        assert!(!schema.features().advanced_arrays());
        assert_field_type(root, "bool_value", TypeKind::Bool);
        assert_field_type(root, "int8_value", TypeKind::Int8);
        assert_field_type(root, "uint8_value", TypeKind::Uint8);
        assert_field_type(root, "int16_value", TypeKind::Int16);
        assert_field_type(root, "uint16_value", TypeKind::Uint16);
        assert_field_type(root, "int32_value", TypeKind::Int32);
        assert_field_type(root, "uint32_value", TypeKind::Uint32);
        assert_field_type(root, "int64_value", TypeKind::Int64);
        assert_field_type(root, "uint64_value", TypeKind::Uint64);
        assert_field_type(root, "float32_value", TypeKind::Float32);
        assert_field_type(root, "float64_value", TypeKind::Float64);
        assert_field_type(root, "string_value", TypeKind::String);

        let bool_field = root.field("bool_value").expect("bool field should exist");
        assert_eq!(bool_field.default_integer(), 1);
        let string_field = root
            .field("string_value")
            .expect("string field should exist");
        assert!(string_field.is_required());
        let optional_field = root
            .field("optional_score")
            .expect("optional scalar field should exist");
        assert!(optional_field.is_optional());

        let bytes_field = root.field("bytes").expect("byte vector should exist");
        assert_eq!(bytes_field.field_type().base_type(), TypeKind::Vector);
        assert_eq!(bytes_field.field_type().element_type(), TypeKind::Uint8);

        let point_field = root.field("point").expect("struct field should exist");
        assert_eq!(point_field.field_type().base_type(), TypeKind::Object);
        let point = schema
            .object_by_index(
                point_field
                    .field_type()
                    .index()
                    .expect("object field should have a schema index"),
            )
            .expect("object index should resolve");
        assert_eq!(point.name(), "test.types.Point");
        assert!(point.is_struct());

        let status_field = root.field("statuses").expect("enum vector should exist");
        let status = schema
            .enum_by_index(
                status_field
                    .field_type()
                    .index()
                    .expect("enum vector should have a schema index"),
            )
            .expect("enum index should resolve");
        assert_eq!(status.name(), "test.types.Status");
        assert!(!status.is_union());
        assert_eq!(status.underlying_type().base_type(), TypeKind::Uint8);
        assert_eq!(
            status.value("Active").map(EnumValueDescriptor::value),
            Some(1)
        );
        assert_eq!(
            status.value_by_number(2).map(EnumValueDescriptor::name),
            Some("Disabled")
        );

        let contact = schema
            .enum_descriptor("test.types.Contact")
            .expect("union descriptor should exist");
        assert!(contact.is_union());
        assert_field_type(root, "contact_type", TypeKind::UnionType);
        assert_field_type(root, "contact", TypeKind::Union);
        let email_contact = contact
            .value("EmailContact")
            .and_then(EnumValueDescriptor::union_type)
            .and_then(TypeDescriptor::index)
            .and_then(|index| schema.object_by_index(index))
            .expect("union object index should resolve");
        assert_eq!(email_contact.name(), "test.types.EmailContact");
    }

    #[test]
    fn given_reflection_base_types_when_described_should_preserve_every_type() {
        let base_types = [
            (BaseType::None, TypeKind::None),
            (BaseType::UType, TypeKind::UnionType),
            (BaseType::Bool, TypeKind::Bool),
            (BaseType::Byte, TypeKind::Int8),
            (BaseType::UByte, TypeKind::Uint8),
            (BaseType::Short, TypeKind::Int16),
            (BaseType::UShort, TypeKind::Uint16),
            (BaseType::Int, TypeKind::Int32),
            (BaseType::UInt, TypeKind::Uint32),
            (BaseType::Long, TypeKind::Int64),
            (BaseType::ULong, TypeKind::Uint64),
            (BaseType::Float, TypeKind::Float32),
            (BaseType::Double, TypeKind::Float64),
            (BaseType::String, TypeKind::String),
            (BaseType::Vector, TypeKind::Vector),
            (BaseType::Obj, TypeKind::Object),
            (BaseType::Union, TypeKind::Union),
            (BaseType::Array, TypeKind::Array),
            (BaseType::Vector64, TypeKind::Vector64),
            (BaseType(127), TypeKind::Unknown(127)),
        ];

        for (reflection_type, expected_type) in base_types {
            assert_eq!(TypeKind::from(reflection_type), expected_type);
        }
    }

    #[test]
    fn given_configured_root_table_when_loaded_should_use_override() {
        let bytes = build_bfbs(
            &[("example.User", false), ("example.Address", false)],
            0,
            None,
        );

        let schema = FlatBufferSchema::from_bytes(bytes, Some("example.Address"))
            .expect("configured root table should load");

        assert_eq!(schema.root_table().name(), "example.Address");
        assert_eq!(schema.root_table().index(), 1);
    }

    #[test]
    fn given_invalid_bytes_when_loaded_should_reject_schema() {
        let error = FlatBufferSchema::from_bytes(vec![1, 2, 3, 4], None)
            .expect_err("invalid BFBS should fail");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_malformed_bfbs_when_loaded_should_reject_schema() {
        let mut bytes = vec![0; SIZE_UOFFSET + FILE_IDENTIFIER_LENGTH];
        bytes[SIZE_UOFFSET..].copy_from_slice(b"BFBS");

        let error = FlatBufferSchema::from_bytes(bytes, None)
            .expect_err("malformed BFBS should fail verification");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_flatbuffer_without_bfbs_identifier_when_loaded_should_reject_schema() {
        let bytes = build_schema(false, &[("example.User", false)], Some(0), None);

        let error = FlatBufferSchema::from_bytes(bytes, None)
            .expect_err("schema without BFBS identifier should fail");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_missing_root_table_when_loaded_should_reject_schema() {
        let bytes = build_schema(true, &[("example.User", false)], None, None);

        let error = FlatBufferSchema::from_bytes(bytes, None)
            .expect_err("schema without a root table should fail");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_unknown_configured_root_when_loaded_should_reject_schema() {
        let bytes = build_bfbs(&[("example.User", false)], 0, None);

        let error = FlatBufferSchema::from_bytes(bytes, Some("example.Missing"))
            .expect_err("unknown root table should fail");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_empty_configured_root_when_loaded_should_reject_schema() {
        let bytes = build_bfbs(&[("example.User", false)], 0, None);

        let error = FlatBufferSchema::from_bytes(bytes, Some("  "))
            .expect_err("empty root table should fail");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_struct_as_configured_root_when_loaded_should_reject_schema() {
        let bytes = build_bfbs(&[("example.User", false), ("example.Point", true)], 0, None);

        let error = FlatBufferSchema::from_bytes(bytes, Some("example.Point"))
            .expect_err("struct cannot be selected as root table");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_invalid_file_identifier_when_loaded_should_reject_schema() {
        let bytes = build_bfbs(&[("example.User", false)], 0, Some("TOO_LONG"));

        let error = FlatBufferSchema::from_bytes(bytes, None)
            .expect_err("invalid file identifier should fail");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_unknown_advanced_feature_when_loaded_should_reject_schema() {
        let bytes = build_schema_with_features(
            &[("example.User", false)],
            Some(0),
            None,
            AdvancedFeatures::from_bits_retain(1 << 63),
        );

        let error = FlatBufferSchema::from_bytes(bytes, None)
            .expect_err("unknown advanced schema feature should fail");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_missing_object_reference_when_loaded_should_reject_schema() {
        let bytes = build_schema_with_field_type(TypeArgs {
            base_type: BaseType::Obj,
            index: 99,
            ..TypeArgs::default()
        });

        let error = FlatBufferSchema::from_bytes(bytes, None)
            .expect_err("missing object reference should fail");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    fn build_bfbs(
        objects: &[(&str, bool)],
        root_index: usize,
        file_identifier: Option<&str>,
    ) -> Vec<u8> {
        build_schema(true, objects, Some(root_index), file_identifier)
    }

    fn assert_field_type(object: &ObjectDescriptor, field_name: &str, expected_type: TypeKind) {
        let field = object.field(field_name).expect("field should exist");
        assert_eq!(field.field_type().base_type(), expected_type);
    }

    fn build_schema(
        include_bfbs_identifier: bool,
        objects: &[(&str, bool)],
        root_index: Option<usize>,
        file_identifier: Option<&str>,
    ) -> Vec<u8> {
        build_schema_internal(
            include_bfbs_identifier,
            objects,
            root_index,
            file_identifier,
            AdvancedFeatures::default(),
        )
    }

    fn build_schema_with_features(
        objects: &[(&str, bool)],
        root_index: Option<usize>,
        file_identifier: Option<&str>,
        advanced_features: AdvancedFeatures,
    ) -> Vec<u8> {
        build_schema_internal(
            true,
            objects,
            root_index,
            file_identifier,
            advanced_features,
        )
    }

    fn build_schema_internal(
        include_bfbs_identifier: bool,
        objects: &[(&str, bool)],
        root_index: Option<usize>,
        file_identifier: Option<&str>,
        advanced_features: AdvancedFeatures,
    ) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let empty_fields = builder.create_vector(&[] as &[WIPOffset<_>]);
        let mut object_offsets = Vec::with_capacity(objects.len());

        for (name, is_struct) in objects {
            let name = builder.create_string(name);
            let object = Object::create(
                &mut builder,
                &ObjectArgs {
                    name: Some(name),
                    fields: Some(empty_fields),
                    is_struct: *is_struct,
                    ..ObjectArgs::default()
                },
            );
            object_offsets.push(object);
        }

        let objects = builder.create_vector(&object_offsets);
        let empty_enums = builder.create_vector(&[] as &[WIPOffset<Enum<'_>>]);
        let file_identifier = file_identifier.map(|value| builder.create_string(value));
        let schema = Schema::create(
            &mut builder,
            &SchemaArgs {
                objects: Some(objects),
                enums: Some(empty_enums),
                file_ident: file_identifier,
                root_table: root_index.map(|index| object_offsets[index]),
                advanced_features,
                ..SchemaArgs::default()
            },
        );

        if include_bfbs_identifier {
            finish_schema_buffer(&mut builder, schema);
        } else {
            builder.finish_minimal(schema);
        }

        builder.finished_data().to_vec()
    }

    fn build_schema_with_field_type(type_args: TypeArgs) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let field_type = Type::create(&mut builder, &type_args);
        let field_name = builder.create_string("child");
        let field = Field::create(
            &mut builder,
            &FieldArgs {
                name: Some(field_name),
                type_: Some(field_type),
                offset: 4,
                ..FieldArgs::default()
            },
        );
        let fields = builder.create_vector(&[field]);
        let object_name = builder.create_string("example.Root");
        let object = Object::create(
            &mut builder,
            &ObjectArgs {
                name: Some(object_name),
                fields: Some(fields),
                ..ObjectArgs::default()
            },
        );
        let objects = builder.create_vector(&[object]);
        let enums = builder.create_vector(&[] as &[WIPOffset<Enum<'_>>]);
        let schema = Schema::create(
            &mut builder,
            &SchemaArgs {
                objects: Some(objects),
                enums: Some(enums),
                root_table: Some(object),
                ..SchemaArgs::default()
            },
        );
        finish_schema_buffer(&mut builder, schema);

        builder.finished_data().to_vec()
    }
}
