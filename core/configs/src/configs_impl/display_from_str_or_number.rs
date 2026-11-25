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

use serde::de::{Error as DeError, Visitor};
use serde::{Deserializer, Serializer};
use serde_with::{DeserializeAs, DisplayFromStr, SerializeAs};
use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::str::FromStr;

/// [`DisplayFromStr`] that also takes a number, parsed from its decimal text.
///
/// The environment provider hands every value that looks like a number over
/// as one, so `IGGY_LOGGING_RETENTION=0` has to mean what `retention = "0"`
/// means in the config file. Serializes exactly like [`DisplayFromStr`].
pub struct DisplayFromStrOrNumber;

impl<T> SerializeAs<T> for DisplayFromStrOrNumber
where
    T: Display,
{
    fn serialize_as<S>(source: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        DisplayFromStr::serialize_as(source, serializer)
    }
}

impl<'de, T> DeserializeAs<'de, T> for DisplayFromStrOrNumber
where
    T: FromStr,
    T::Err: Display,
{
    fn deserialize_as<D>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(TextOrNumberVisitor(PhantomData))
    }
}

struct TextOrNumberVisitor<T>(PhantomData<T>);

impl<T> Visitor<'_> for TextOrNumberVisitor<T>
where
    T: FromStr,
    T::Err: Display,
{
    type Value = T;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("a string or a number")
    }

    fn visit_str<E>(self, value: &str) -> Result<T, E>
    where
        E: DeError,
    {
        value.parse().map_err(E::custom)
    }

    fn visit_u64<E>(self, value: u64) -> Result<T, E>
    where
        E: DeError,
    {
        self.visit_str(&value.to_string())
    }

    fn visit_i64<E>(self, value: i64) -> Result<T, E>
    where
        E: DeError,
    {
        self.visit_str(&value.to_string())
    }

    fn visit_f64<E>(self, value: f64) -> Result<T, E>
    where
        E: DeError,
    {
        self.visit_str(&value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{IggyByteSize, IggyDuration, IggyExpiry};
    use serde_json::{Value, json};

    #[test]
    fn given_a_string_when_deserializing_should_parse_it() {
        let duration: IggyDuration = deserialize(json!("1m 5s")).expect("a duration string");

        assert_eq!(duration.as_secs(), 65);
    }

    #[test]
    fn given_zero_when_deserializing_a_duration_should_read_zero() {
        let duration: IggyDuration = deserialize(json!(0)).expect("a bare zero");

        assert!(duration.is_zero());
    }

    #[test]
    fn given_an_unsigned_integer_when_deserializing_a_byte_size_should_read_bytes() {
        let size: IggyByteSize = deserialize(json!(131_072)).expect("a bare byte count");

        assert_eq!(size.as_bytes_u64(), 131_072);
    }

    #[test]
    fn given_zero_when_deserializing_an_expiry_should_match_the_text() {
        let from_number: IggyExpiry = deserialize(json!(0)).expect("a bare zero");
        let from_text: IggyExpiry = deserialize(json!("0")).expect("a zero string");

        assert_eq!(from_number, from_text);
    }

    #[test]
    fn given_numbers_when_deserializing_should_read_their_decimal_text() {
        let unsigned: String = deserialize(json!(42)).expect("an unsigned integer");
        let negative: String = deserialize(json!(-7)).expect("a negative integer");
        let float: String = deserialize(json!(0.25)).expect("a float");

        assert_eq!(unsigned, "42");
        assert_eq!(negative, "-7");
        assert_eq!(float, "0.25");
    }

    #[test]
    fn given_a_number_the_text_rejects_when_deserializing_should_fail_like_the_text() {
        let from_number = deserialize::<IggyDuration>(json!(5)).expect_err("5 has no unit");
        let from_text = deserialize::<IggyDuration>(json!("5")).expect_err("5 has no unit");

        assert_eq!(from_number.to_string(), from_text.to_string());
    }

    #[test]
    fn given_a_bool_when_deserializing_should_fail() {
        let error = deserialize::<IggyDuration>(json!(true)).expect_err("a bool is no duration");

        assert!(
            error.to_string().contains("a string or a number"),
            "{error}"
        );
    }

    #[test]
    fn given_a_value_when_serializing_should_match_display_from_str() {
        let duration = IggyDuration::from_str("1h 1m").expect("a duration string");
        let size = IggyByteSize::from(131_072_u64);

        assert_eq!(
            serialize_with::<DisplayFromStrOrNumber, _>(&duration),
            serialize_with::<DisplayFromStr, _>(&duration)
        );
        assert_eq!(
            serialize_with::<DisplayFromStrOrNumber, _>(&size),
            serialize_with::<DisplayFromStr, _>(&size)
        );
    }

    fn deserialize<T>(value: Value) -> Result<T, serde_json::Error>
    where
        T: FromStr,
        T::Err: Display,
    {
        DisplayFromStrOrNumber::deserialize_as(value)
    }

    fn serialize_with<A, T>(value: &T) -> Value
    where
        A: SerializeAs<T>,
    {
        A::serialize_as(value, serde_json::value::Serializer).expect("serializes to JSON")
    }
}
