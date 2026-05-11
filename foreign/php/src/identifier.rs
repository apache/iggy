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

use std::str::FromStr;

use ext_php_rs::{convert::FromZval, flags::DataType, types::Zval};
use iggy::prelude::{IdKind, Identifier};

pub enum PhpIdentifier {
    String(String),
    Int(u32),
}

impl FromZval<'_> for PhpIdentifier {
    const TYPE: DataType = DataType::Mixed;

    fn from_zval(zval: &Zval) -> Option<Self> {
        if let Some(value) = zval.string() {
            return Some(Self::String(value));
        }

        zval.long()
            .and_then(|value| u32::try_from(value).ok())
            .map(Self::Int)
    }
}

impl From<PhpIdentifier> for Identifier {
    fn from(identifier: PhpIdentifier) -> Self {
        match identifier {
            PhpIdentifier::String(value) => Identifier::from_str(&value).unwrap(),
            PhpIdentifier::Int(value) => Identifier::numeric(value).unwrap(),
        }
    }
}

impl From<&Identifier> for PhpIdentifier {
    fn from(value: &Identifier) -> PhpIdentifier {
        match value.kind {
            IdKind::String => PhpIdentifier::String(value.get_string_value().unwrap()),
            IdKind::Numeric => PhpIdentifier::Int(value.get_u32_value().unwrap()),
        }
    }
}
