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

use super::{Transform, TransformType};
use crate::{DecodedMessage, Error, Payload, TopicMetadata};
use regex::Regex;
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use simd_json::prelude::{TypedArrayValue, TypedObjectValue, TypedScalarValue, ValueAsScalar};
use std::collections::HashSet;

/// Pattern matching for field keys with various string matching strategies
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyPattern<T = String> {
    /// Exact string match
    Exact(String),
    /// Key starts with the given string
    StartsWith(String),
    /// Key ends with the given string
    EndsWith(String),
    /// Key contains the given string
    Contains(String),
    /// Key matches the given regular expression
    Regex(T),
}

impl KeyPattern<String> {
    /// Compiles string patterns into regex patterns for efficient matching
    pub fn compile(self) -> Result<KeyPattern<Regex>, Error> {
        Ok(match self {
            KeyPattern::Regex(pattern) => {
                KeyPattern::Regex(Regex::new(&pattern).map_err(|_| Error::InvalidConfig)?)
            }
            KeyPattern::Exact(s) => KeyPattern::Exact(s),
            KeyPattern::StartsWith(s) => KeyPattern::StartsWith(s),
            KeyPattern::EndsWith(s) => KeyPattern::EndsWith(s),
            KeyPattern::Contains(s) => KeyPattern::Contains(s),
        })
    }
}

impl KeyPattern<Regex> {
    /// Checks if a key matches this pattern
    pub fn matches(&self, k: &str) -> bool {
        match self {
            KeyPattern::Exact(s) => k == s,
            KeyPattern::StartsWith(s) => k.starts_with(s),
            KeyPattern::EndsWith(s) => k.ends_with(s),
            KeyPattern::Contains(s) => k.contains(s),
            KeyPattern::Regex(re) => re.is_match(k),
        }
    }
}

/// Pattern matching for field values with type checking and content matching
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValuePattern<T = String> {
    /// Value equals the specified JSON value
    Equals(OwnedValue),
    /// String value contains the specified substring
    Contains(String),
    /// String value matches the given regular expression
    Regex(T),
    /// Numeric value is greater than the specified number
    GreaterThan(f64),
    /// Numeric value is less than the specified number
    LessThan(f64),
    /// Numeric value is between the specified range (inclusive)
    Between(f64, f64),
    /// Value is null
    IsNull,
    /// Value is not null
    IsNotNull,
    /// Value is a string
    IsString,
    /// Value is a number
    IsNumber,
    /// Value is a boolean
    IsBoolean,
    /// Value is an object
    IsObject,
    /// Value is an array
    IsArray,
}

impl ValuePattern<String> {
    /// Compiles string patterns into regex patterns for efficient matching
    pub fn compile(self) -> Result<ValuePattern<Regex>, Error> {
        use ValuePattern::*;
        Ok(match self {
            Regex(pattern) => Regex(regex::Regex::new(&pattern).map_err(|_| Error::InvalidConfig)?),
            Equals(v) => Equals(v),
            Contains(s) => Contains(s),
            GreaterThan(n) => GreaterThan(n),
            LessThan(n) => LessThan(n),
            Between(a, b) => Between(a, b),
            IsNull => IsNull,
            IsNotNull => IsNotNull,
            IsString => IsString,
            IsNumber => IsNumber,
            IsBoolean => IsBoolean,
            IsObject => IsObject,
            IsArray => IsArray,
        })
    }
}

impl ValuePattern<Regex> {
    /// Checks if a value matches this pattern
    pub fn matches(&self, v: &OwnedValue) -> bool {
        use ValuePattern::*;
        match self {
            Equals(x) => v == x,
            Contains(s) => v.as_str().is_some_and(|x| x.contains(s)),
            Regex(re) => v.as_str().is_some_and(|x| re.is_match(x)),
            GreaterThan(t) => v.as_f64().is_some_and(|n| n > *t),
            LessThan(t) => v.as_f64().is_some_and(|n| n < *t),
            Between(a, b) => v.as_f64().is_some_and(|n| n >= *a && n <= *b),
            IsNull => v.is_null(),
            IsNotNull => !v.is_null(),
            IsString => v.is_str(),
            IsNumber => v.is_number(),
            IsBoolean => v.is_bool(),
            IsObject => v.is_object(),
            IsArray => v.is_array(),
        }
    }
}

/// Configuration for the FilterFields transform
#[derive(Debug, Serialize, Deserialize)]
pub struct FilterFieldsConfig {
    /// Fields to always keep regardless of pattern matching
    #[serde(default)]
    pub keep_fields: Vec<String>,
    /// Patterns to match against field keys and values
    #[serde(default)]
    pub patterns: Vec<FilterPattern>,
    /// Whether to include (true) or exclude (false) fields that match patterns
    #[serde(default = "default_include")]
    pub include_matching: bool,
}

/// Default value for include_matching field
fn default_include() -> bool {
    true
}

/// A pattern that can match both field keys and values
#[derive(Debug, Serialize, Deserialize)]
pub struct FilterPattern {
    /// Optional pattern to match against field keys
    #[serde(default)]
    pub key_pattern: Option<KeyPattern<String>>,
    /// Optional pattern to match against field values
    #[serde(default)]
    pub value_pattern: Option<ValuePattern<String>>,
}

/// A compiled pattern with regex patterns ready for efficient matching
pub struct CompiledPattern {
    /// Compiled key pattern
    pub key_pattern: Option<KeyPattern<Regex>>,
    /// Compiled value pattern
    pub value_pattern: Option<ValuePattern<Regex>>,
}

/// Transform that filters JSON message fields based on complex patterns
///
/// This transform supports sophisticated filtering based on:
/// - Field names (exact, prefix, suffix, contains, regex)
/// - Field values (equality, type checking, numeric comparisons, regex)
/// - Combination of key and value patterns
/// - Include or exclude matching behavior
pub struct FilterFields {
    /// Whether to include or exclude matching fields
    pub include_matching: bool,
    /// Set of field names to always keep
    pub keep_set: HashSet<String>,
    /// Compiled patterns for efficient matching
    pub patterns: Vec<CompiledPattern>,
}

impl FilterFields {
    /// Creates a new FilterFields transform from configuration
    ///
    /// This compiles all regex patterns for efficient matching during transformation.
    /// Returns an error if any regex patterns are invalid.
    pub fn new(cfg: FilterFieldsConfig) -> Result<Self, Error> {
        let keep_set = cfg.keep_fields.into_iter().collect();

        let mut patterns = Vec::with_capacity(cfg.patterns.len());
        for p in cfg.patterns {
            patterns.push(CompiledPattern {
                key_pattern: p.key_pattern.map(|kp| kp.compile()).transpose()?,
                value_pattern: p.value_pattern.map(|vp| vp.compile()).transpose()?,
            });
        }

        Ok(Self {
            include_matching: cfg.include_matching,
            keep_set,
            patterns,
        })
    }

    /// Checks if a field key-value pair matches any of the configured patterns
    ///
    /// A field matches if both the key pattern (if specified) and value pattern
    /// (if specified) match. If only one pattern type is specified, only that
    /// pattern needs to match.
    #[inline]
    pub fn matches_patterns(&self, k: &str, v: &OwnedValue) -> bool {
        self.patterns.iter().any(|pat| {
            let key_ok = pat.key_pattern.as_ref().is_none_or(|kp| kp.matches(k));
            let value_ok = pat.value_pattern.as_ref().is_none_or(|vp| vp.matches(v));
            key_ok && value_ok
        })
    }
}

impl Transform for FilterFields {
    fn r#type(&self) -> TransformType {
        TransformType::FilterFields
    }

    fn transform(
        &self,
        metadata: &TopicMetadata,
        message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        if self.keep_set.is_empty() && self.patterns.is_empty() {
            return Ok(Some(message)); // nothing to do
        }

        match &message.payload {
            Payload::Json(_) => self.transform_json(metadata, message),
            _ => Ok(Some(message)),
        }
    }
}
