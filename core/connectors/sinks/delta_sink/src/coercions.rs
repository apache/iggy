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

use deltalake_core::kernel::Schema as DeltaSchema;
use deltalake_core::kernel::{DataType, PrimitiveType};

use chrono::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq)]
#[allow(unused)]
enum CoercionNode {
    Coercion(Coercion),
    Tree(CoercionTree),
    ArrayTree(CoercionTree),
    ArrayPrimitive(Coercion),
}

#[derive(Debug, Clone, PartialEq)]
enum Coercion {
    ToString,
    ToTimestamp,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoercionTree {
    root: HashMap<String, CoercionNode>,
}

/// Returns a [`CoercionTree`] so the schema can be walked efficiently level by level when performing conversions.
pub(crate) fn create_coercion_tree(schema: &DeltaSchema) -> CoercionTree {
    let mut root = HashMap::new();

    for field in schema.fields() {
        if let Some(node) = build_coercion_node(field.data_type()) {
            root.insert(field.name().to_string(), node);
        }
    }

    CoercionTree { root }
}

fn build_coercion_node(data_type: &DataType) -> Option<CoercionNode> {
    match data_type {
        DataType::Primitive(primitive) => match primitive {
            PrimitiveType::String => Some(CoercionNode::Coercion(Coercion::ToString)),
            PrimitiveType::Timestamp => Some(CoercionNode::Coercion(Coercion::ToTimestamp)),
            _ => None,
        },
        DataType::Struct(st) => {
            let nested_context = create_coercion_tree(st);
            if !nested_context.root.is_empty() {
                Some(CoercionNode::Tree(nested_context))
            } else {
                None
            }
        }
        DataType::Array(array) => {
            build_coercion_node(array.element_type()).and_then(|node| match node {
                CoercionNode::Coercion(c) => Some(CoercionNode::ArrayPrimitive(c)),
                CoercionNode::Tree(t) => Some(CoercionNode::ArrayTree(t)),
                _ => None,
            })
        }
        _ => None,
    }
}

/// Applies all data coercions specified by the [`CoercionTree`] to the [`Value`].
pub(crate) fn coerce(value: &mut Value, coercion_tree: &CoercionTree) {
    if let Some(context) = value.as_object_mut() {
        for (field_name, coercion) in coercion_tree.root.iter() {
            if let Some(value) = context.get_mut(field_name) {
                apply_coercion(value, coercion);
            }
        }
    }
}

fn apply_coercion(value: &mut Value, node: &CoercionNode) {
    match node {
        CoercionNode::Coercion(Coercion::ToString) => {
            if !value.is_string() {
                *value = Value::String(value.to_string());
            }
        }
        CoercionNode::Coercion(Coercion::ToTimestamp) => {
            if let Some(as_str) = value.as_str()
                && let Some(parsed) = string_to_timestamp(as_str)
            {
                *value = parsed
            }
        }
        CoercionNode::Tree(tree) => {
            for (name, node) in tree.root.iter() {
                let fields = value.as_object_mut();
                if let Some(fields) = fields
                    && let Some(value) = fields.get_mut(name)
                {
                    apply_coercion(value, node);
                }
            }
        }
        CoercionNode::ArrayPrimitive(coercion) => {
            let values = value.as_array_mut();
            if let Some(values) = values {
                let node = CoercionNode::Coercion(coercion.clone());
                for value in values {
                    apply_coercion(value, &node);
                }
            }
        }
        CoercionNode::ArrayTree(tree) => {
            let values = value.as_array_mut();
            if let Some(values) = values {
                let node = CoercionNode::Tree(tree.clone());
                for value in values {
                    apply_coercion(value, &node);
                }
            }
        }
    }
}

fn string_to_timestamp(string: &str) -> Option<Value> {
    let parsed = DateTime::from_str(string);
    if let Err(e) = parsed {
        tracing::error!(
            "Error coercing timestamp from string. String: {}. Error: {}",
            string,
            e
        )
    }
    parsed
        .ok()
        .map(|dt: DateTime<Utc>| Value::Number(dt.timestamp_micros().into()))
}
