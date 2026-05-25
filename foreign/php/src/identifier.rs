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

use ext_php_rs::{convert::FromZval, exception::PhpException, flags::DataType, types::Zval};
use iggy::prelude::{IdKind, Identifier};

use crate::error::to_php_exception;

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

impl TryFrom<PhpIdentifier> for Identifier {
    type Error = PhpException;

    fn try_from(value: PhpIdentifier) -> Result<Self, Self::Error> {
        match value {
            PhpIdentifier::String(value) => Identifier::named(&value),
            PhpIdentifier::Int(value) => Identifier::numeric(value),
        }
        .map_err(to_php_exception)
    }
}

impl TryFrom<&Identifier> for PhpIdentifier {
    type Error = PhpException;

    fn try_from(value: &Identifier) -> Result<Self, Self::Error> {
        match value.kind {
            IdKind::String => value
                .get_string_value()
                .map(PhpIdentifier::String)
                .map_err(to_php_exception),
            IdKind::Numeric => value
                .get_u32_value()
                .map(PhpIdentifier::Int)
                .map_err(to_php_exception),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn php_string_identifier_is_always_named() {
        let identifier = Identifier::try_from(PhpIdentifier::String("5".to_string())).unwrap();

        assert_eq!(identifier.kind, IdKind::String);
        assert_eq!(identifier.get_string_value().unwrap(), "5");
    }

    #[test]
    fn php_int_identifier_is_numeric() {
        let identifier = Identifier::try_from(PhpIdentifier::Int(5)).unwrap();

        assert_eq!(identifier.kind, IdKind::Numeric);
        assert_eq!(identifier.get_u32_value().unwrap(), 5);
    }
}
