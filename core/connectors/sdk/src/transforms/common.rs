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

use chrono::Utc;
use regex::Regex;
use serde::{Deserialize, Serialize};
use simd_json::{OwnedValue, prelude::*};
use strum_macros::{Display, IntoStaticStr};

use crate::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldValue {
    Static(OwnedValue),
    Computed(ComputedValue),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Display, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
pub enum ComputedValue {
    #[strum(to_string = "date_time")]
    DateTime,
    #[strum(to_string = "timestamp_nanos")]
    TimestampNanos,
    #[strum(to_string = "timestamp_micros")]
    TimestampMicros,
    #[strum(to_string = "timestamp_millis")]
    TimestampMillis,
    #[strum(to_string = "timestamp_seconds")]
    TimestampSeconds,
    #[strum(to_string = "uuid_v4")]
    UuidV4,
    #[strum(to_string = "uuid_v7")]
    UuidV7,
}

pub fn compute_value(kind: &ComputedValue) -> OwnedValue {
    let now = Utc::now();
    match kind {
        ComputedValue::DateTime => now.to_rfc3339().into(),
        ComputedValue::TimestampNanos => now.timestamp_nanos_opt().unwrap().into(),
        ComputedValue::TimestampMicros => now.timestamp_micros().into(),
        ComputedValue::TimestampMillis => now.timestamp_millis().into(),
        ComputedValue::TimestampSeconds => now.timestamp().into(),
        ComputedValue::UuidV4 => uuid::Uuid::new_v4().to_string().into(),
        ComputedValue::UuidV7 => uuid::Uuid::now_v7().to_string().into(),
    }
}

pub trait Compilable {
    fn compile(self) -> Result<Regex, Error>;
}

impl<S> Compilable for S
where
    S: AsRef<str>,
{
    fn compile(self) -> Result<Regex, Error> {
        Ok(Regex::new(self.as_ref()).map_err(|_| Error::InvalidConfig)?)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyPattern<T = String> {
    Exact(String),
    StartsWith(String),
    EndsWith(String),
    Contains(String),
    Regex(T),
}

pub type SerializedKeyPattern = KeyPattern<String>;
pub type CompiledKeyPattern = KeyPattern<Regex>;

impl<T: Compilable> KeyPattern<T> {
    pub fn compile(self) -> Result<KeyPattern<Regex>, Error> {
        Ok(match self {
            KeyPattern::Regex(p) => KeyPattern::Regex(p.compile()?),
            KeyPattern::Exact(s) => KeyPattern::Exact(s),
            KeyPattern::StartsWith(s) => KeyPattern::StartsWith(s),
            KeyPattern::EndsWith(s) => KeyPattern::EndsWith(s),
            KeyPattern::Contains(s) => KeyPattern::Contains(s),
        })
    }
}

impl KeyPattern<Regex> {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValuePattern<T = String> {
    Equals(OwnedValue),
    Contains(String),
    Regex(T),
    GreaterThan(f64),
    LessThan(f64),
    Between(f64, f64),
    IsNull,
    IsNotNull,
    IsString,
    IsNumber,
    IsBoolean,
    IsObject,
    IsArray,
}

pub type SerializedValuePattern = ValuePattern<String>;
pub type CompiledValuePattern = ValuePattern<Regex>;

impl<T: Compilable> ValuePattern<T> {
    pub fn compile(self) -> Result<ValuePattern<Regex>, Error> {
        use ValuePattern::*;
        Ok(match self {
            Regex(p) => Regex(p.compile()?),
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
    pub fn matches(&self, v: &OwnedValue) -> bool {
        use ValuePattern::*;
        match self {
            Equals(x) => v == x,
            Contains(s) => v.as_str().map_or(false, |x| x.contains(s)),
            Regex(re) => v.as_str().map_or(false, |x| re.is_match(x)),
            GreaterThan(t) => v.as_f64().map_or(false, |n| n > *t),
            LessThan(t) => v.as_f64().map_or(false, |n| n < *t),
            Between(a, b) => v.as_f64().map_or(false, |n| n >= *a && n <= *b),
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
