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

use chrono::{DateTime, Utc};
use iggy_connector_sdk::{Error, Schema};

pub(crate) struct PathContext<'a> {
    pub(crate) stream: &'a str,
    pub(crate) topic: &'a str,
    pub(crate) partition_id: u32,
    pub(crate) first_timestamp_micros: u64,
}

pub(crate) fn object_path(
    path_prefix: &str,
    path_template: &str,
    context: &PathContext<'_>,
    offset: u64,
    schema: Schema,
) -> Result<String, Error> {
    let rendered = render_template(path_template, context)?;
    let extension = match schema {
        Schema::Json => "json",
        Schema::Raw => "bin",
        Schema::Text => "txt",
        Schema::Proto => "proto",
        Schema::FlatBuffer => "flatbuffer",
        Schema::Avro => "avro",
    };

    // Partition ID is always embedded in the filename to prevent cross-partition
    // key collisions because partitions have independent offset spaces starting at
    // 0.
    let filename = format!("{:05}-{:020}.{}", context.partition_id, offset, extension);

    if path_prefix.is_empty() {
        Ok(format!("{rendered}/{filename}"))
    } else {
        Ok(format!("{path_prefix}/{rendered}/{filename}"))
    }
}

fn render_template(template: &str, context: &PathContext<'_>) -> Result<String, Error> {
    let timestamp = timestamp_to_datetime(context.first_timestamp_micros)?;
    let date = timestamp.format("%Y-%m-%d").to_string();
    let hour = timestamp.format("%H").to_string();
    let timestamp_millis = (context.first_timestamp_micros / 1_000).to_string();

    Ok(template
        .replace("{stream}", &sanitize_path_segment(context.stream))
        .replace("{topic}", &sanitize_path_segment(context.topic))
        .replace("{partition}", &context.partition_id.to_string())
        .replace("{date}", &date)
        .replace("{hour}", &hour)
        .replace("{timestamp}", &timestamp_millis))
}

fn sanitize_path_segment(segment: &str) -> String {
    segment
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric()
                || character == '.'
                || character == '_'
                || character == '-'
            {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn timestamp_to_datetime(micros: u64) -> Result<DateTime<Utc>, Error> {
    let seconds = (micros / 1_000_000) as i64;
    let nanoseconds = ((micros % 1_000_000) * 1_000) as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanoseconds).ok_or_else(|| {
        Error::CannotStoreData(format!(
            "Invalid message timestamp: {micros} micros is out of range"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_out_of_range_timestamp_when_building_path_should_return_error() {
        let context = PathContext {
            stream: "events",
            topic: "orders",
            partition_id: 7,
            first_timestamp_micros: u64::MAX,
        };

        let error = object_path(
            "",
            "{stream}/{topic}/{date}/{hour}",
            &context,
            42,
            Schema::Json,
        )
        .expect_err("timestamp should be rejected");

        assert!(matches!(error, Error::CannotStoreData(_)));
    }
}
